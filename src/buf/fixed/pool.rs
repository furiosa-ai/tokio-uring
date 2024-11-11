//! A dynamic collection of I/O buffers pre-registered with the kernel.
//!
//! This module provides [`FixedBufPool`], a collection that implements
//! dynamic management of sets of interchangeable memory buffers
//! registered with the kernel for `io-uring` operations. Asynchronous
//! rotation of the buffers shared by multiple tasks is also supported
//! by [`FixedBufPool`].
//!
//! [`FixedBufPool`]: self::FixedBufPool

use super::plumbing;

use crate::buf::BufferImpl;
use crate::runtime::CONTEXT;
use crate::Buffer;

use tokio::pin;
use tokio::sync::Notify;

use std::io;
use std::sync::Arc;
use std::sync::Mutex;

/// A dynamic collection of I/O buffers pre-registered with the kernel.
///
/// `FixedBufPool` allows the application to manage a collection of buffers
/// allocated in memory, that can be registered in the current `tokio-uring`
/// context using the [`register`] method. Unlike [`FixedBufRegistry`],
/// individual buffers are not retrieved by index; instead, an available
/// buffer matching a specified capacity can be retrieved with the [`try_next`]
/// method. In asynchronous contexts, the [`next`] method can be used to wait
/// until such a buffer becomes available.
/// This allows some flexibility in managing sets of buffers with
/// different capacity tiers. The need to maintain lists of free buffers,
/// however, imposes additional runtime overhead.
///
/// A `FixedBufPool` value is a lightweight handle for a collection of
/// allocated buffers. Cloning of a `FixedBufPool` creates a new reference to
/// the same collection of buffers.
///
/// The buffers of the collection are not deallocated until:
/// - all `FixedBufPool` references to the collection have been dropped;
/// - all [`Buffer`] handles to individual buffers in the collection have
///   been dropped, including the buffer handles owned by any I/O operations
///   in flight;
///
/// [`register`]: register
/// [`try_next`]: Self::try_next
/// [`next`]: Self::next
/// [`FixedBufRegistry`]: super::FixedBufRegistry
/// [`Runtime`]: crate::Runtime
/// [`Buffer`]: crate::Buffer
///
/// # Examples
///
/// ```no_compile
/// use tokio_uring::buf::fixed::pool::register;
/// use tokio_uring::buf::IoBuf;
/// use std::iter;
/// use std::mem;
///
/// # #[allow(non_snake_case)]
/// # fn main() -> Result<(), std::io::Error> {
/// # use nix::sys::resource::{getrlimit, Resource};
/// # let (memlock_limit, _) = getrlimit(Resource::RLIMIT_MEMLOCK)?;
/// # let BUF_SIZE_LARGE = memlock_limit as usize / 8;
/// # let BUF_SIZE_SMALL = memlock_limit as usize / 16;
/// tokio_uring::start(async {
///     let pool = register(
///          iter::once(Vec::with_capacity(BUF_SIZE_LARGE))
///              .chain(iter::repeat_with(|| Vec::with_capacity(BUF_SIZE_SMALL)).take(2))
///      )?;
///
///     let buf = pool.try_next(BUF_SIZE_LARGE).unwrap();
///     assert_eq!(buf.bytes_total(), BUF_SIZE_LARGE);
///     let next = pool.try_next(BUF_SIZE_LARGE);
///     assert!(next.is_none());
///     let buf1 = pool.try_next(BUF_SIZE_SMALL).unwrap();
///     assert_eq!(buf1.bytes_total(), BUF_SIZE_SMALL);
///     let buf2 = pool.try_next(BUF_SIZE_SMALL).unwrap();
///     assert_eq!(buf2.bytes_total(), BUF_SIZE_SMALL);
///     let next = pool.try_next(BUF_SIZE_SMALL);
///     assert!(next.is_none());
///     mem::drop(buf);
///     let buf = pool.try_next(BUF_SIZE_LARGE).unwrap();
///     assert_eq!(buf.bytes_total(), BUF_SIZE_LARGE);
///
///     Ok(())
/// })
/// # }
/// ```
#[derive(Clone)]
pub struct FixedBufPool {
    inner: Arc<Mutex<plumbing::Pool>>,
}

impl FixedBufPool {
    fn new(inner: plumbing::Pool) -> Self {
        FixedBufPool {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Returns a buffer of requested capacity from this pool
    /// that is not currently owned by any other [`FixedBuf`] handle.
    /// If no such free buffer is available, returns `None`.
    ///
    /// The buffer is released to be available again once the
    /// returned `FixedBuf` handle has been dropped. An I/O operation
    /// using the buffer takes ownership of it and returns it once completed,
    /// preventing shared use of the buffer while the operation is in flight.
    ///
    /// An application should not rely on any particular order
    /// in which available buffers are retrieved.
    pub fn try_next(&self, cap: usize) -> Option<Buffer> {
        let (iovec, index) = {
            let mut inner = self.inner.lock().unwrap();
            inner.try_next(cap)?
        };

        let pool_info = PoolInfo {
            pool: self.inner.clone(),
            index: index as u16,
        };
        let buf = FixedBuf { iovec, pool_info };

        Some(Buffer::new(buf))
    }

    /// Resolves to a buffer of requested capacity
    /// when it is or becomes available in this pool.
    /// This may happen when a [`Buffer`] handle owning a buffer
    /// of the same capacity is dropped.
    ///
    /// If no matching buffers are available and none are being released,
    /// this asynchronous function will never resolve. Applications should take
    /// care to wait on the returned future concurrently with some tasks that
    /// will complete I/O operations owning the buffers, or back it up with a
    /// timeout using, for example, `tokio::util::timeout`.
    pub async fn next(&self, cap: usize) -> Buffer {
        // Fast path: get the buffer if it's already available
        let notify = {
            if let Some(buffer) = self.try_next(cap) {
                return buffer;
            }
            self.inner.lock().unwrap().notify_on_next(cap)
        };

        // Poll for a buffer, engaging the `Notify` machinery.
        self.next_when_notified(cap, notify).await
    }

    #[cold]
    async fn next_when_notified(&self, cap: usize, notify: Arc<Notify>) -> Buffer {
        let notified = notify.notified();
        pin!(notified);
        loop {
            // In the single-threaded case, no buffers could get checked in
            // between us calling `try_next` and here, so we can't miss a wakeup.
            notified.as_mut().await;

            if let Some(buffer) = self.try_next(cap) {
                return buffer;
            }

            // It's possible that the task did not get a buffer from `try_next`.
            // The `Notify` entries are created once for each requested capacity
            // and never removed, so this `Notify` could have been holding
            // a permit from a buffer checked in previously when no tasks were
            // waiting. Then a task would call `next` on this pool and receive
            // the buffer without consuming the permit. It's also possible that
            // a task calls `try_next` directly.
            // Reset the `Notified` future to wait for another wakeup.
            notified.set(notify.notified());
        }
    }
}

/// Registers the buffers with the kernel and creates a new collection of buffers from the provided allocated vectors.
///
/// This method must be called in the context of a `tokio-uring` runtime.
/// The registration persists for the lifetime of the runtime, unless
/// revoked by the [`unregister`] method. Dropping the
/// `FixedBufPool` instance this method has been called on does not revoke
/// the registration or deallocate the buffers.
///
/// [`unregister`]: unregister
///
/// This call can be blocked in the kernel to complete any operations
/// in-flight on the same `io-uring` instance. The application is
/// recommended to register buffers before starting any I/O operations.
///
/// The buffers are assigned 0-based indices in the order of the iterable
/// input parameter. The returned collection takes up to [`UIO_MAXIOV`]
/// buffers from the input. Any items in excess of that amount are silently
/// dropped, unless the input iterator produces the vectors lazily.
///
/// [`UIO_MAXIOV`]: libc::UIO_MAXIOV
///
/// # Examples
///
/// When providing uninitialized vectors for the collection, take care to
/// not replicate a vector with `.clone()` as that does not preserve the
/// capacity and the resulting buffer pointer will be rejected by the kernel.
/// This means that the following use of [`iter::repeat`] would not work:
///
/// [`iter::repeat`]: std::iter::repeat
///
/// ```should_panic
/// use tokio_uring::buf::fixed::pool;
/// use std::iter;
///
/// # #[allow(non_snake_case)]
/// # fn main() -> Result<(), std::io::Error> {
/// # use nix::sys::resource::{getrlimit, Resource};
/// # let (memlock_limit, _) = getrlimit(Resource::RLIMIT_MEMLOCK)?;
/// # let NUM_BUFFERS = std::cmp::max(memlock_limit as usize / 4096 / 8, 1);
/// # let BUF_SIZE = 4096;
///
/// tokio_uring::start(async {
///     let pool = pool::register(iter::repeat(Vec::with_capacity(BUF_SIZE)).take(NUM_BUFFERS))?;
///     // ...
///     Ok(())
/// })
/// # }
/// ```
///
/// Instead, create the vectors with requested capacity directly:
///
/// ```
/// use tokio_uring::buf::fixed::pool;
/// use std::iter;
///
/// # #[allow(non_snake_case)]
/// # fn main() -> Result<(), std::io::Error> {
/// # use nix::sys::resource::{getrlimit, Resource};
/// # let (memlock_limit, _) = getrlimit(Resource::RLIMIT_MEMLOCK)?;
/// # let NUM_BUFFERS = std::cmp::max(memlock_limit as usize / 4096 / 8, 1);
/// # let BUF_SIZE = 4096;
///
/// tokio_uring::start(async {
///     let pool = pool::register(iter::repeat_with(|| Vec::with_capacity(BUF_SIZE)).take(NUM_BUFFERS))?;
///     // ...
///     Ok(())
/// })
/// # }
/// ```
///
/// # Errors
///
/// If a collection of buffers is currently registered in the context
/// of the `tokio-uring` runtime this call is made in, the function returns
/// an error.
pub fn register(bufs: impl Iterator<Item = Vec<u8>>) -> io::Result<FixedBufPool> {
    let pool_inner = plumbing::Pool::new(bufs);
    CONTEXT.with(|x| {
        x.handle()
            .as_ref()
            .expect("Not in a runtime context")
            .register_buffers(pool_inner.iovecs())
    })?;
    Ok(FixedBufPool::new(pool_inner))
}

/// Unregisters this collection of buffers.
///
/// This method must be called in the context of a `tokio-uring` runtime,
/// where the buffers should have been previously registered.
///
/// This operation invalidates any `Buffer` handles checked out from
/// this registry instance. Continued use of such handles in I/O
/// operations may result in an error.
///
/// # Errors
///
/// If another collection of buffers is currently registered in the context
/// of the `tokio-uring` runtime this call is made in, the function returns
/// an error. Calling `unregister` when no `FixedBufPool` is currently
/// registered on this runtime also returns an error.
pub fn unregister() -> io::Result<()> {
    CONTEXT.with(|x| {
        x.handle()
            .as_ref()
            .expect("Not in a runtime context")
            .unregister_buffers()
    })
}

pub(crate) struct FixedBuf {
    iovec: libc::iovec,
    pool_info: PoolInfo,
}

#[derive(Clone)]
pub(crate) struct PoolInfo {
    pool: Arc<Mutex<plumbing::Pool>>,
    pub index: u16,
}

unsafe impl BufferImpl for FixedBuf {
    type UserData = PoolInfo;

    fn dtor() -> Box<dyn Fn(*mut u8, usize, *mut ())> {
        Box::new(|_ptr: *mut u8, _len: usize, user: *mut ()| unsafe {
            let pool_info = Box::from_raw(user as *mut PoolInfo);
            let mut pool = pool_info.pool.lock().unwrap();
            pool.check_in(pool_info.index as usize);
        })
    }

    fn into_raw_parts(self) -> (Vec<*mut u8>, Vec<usize>, Vec<Self::UserData>) {
        let FixedBuf { iovec, pool_info } = self;
        (
            vec![iovec.iov_base as _],
            vec![iovec.iov_len],
            vec![pool_info],
        )
    }

    unsafe fn from_raw_parts(
        ptr: Vec<*mut u8>,
        len: Vec<usize>,
        user: Vec<Self::UserData>,
    ) -> Self {
        let iovec = libc::iovec {
            iov_base: ptr[0] as _,
            iov_len: len[0],
        };
        FixedBuf {
            iovec,
            pool_info: user[0].clone(),
        }
    }
}
