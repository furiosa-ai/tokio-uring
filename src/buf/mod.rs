//! Utilities for working with buffers.
//!
//! `io-uring` APIs require passing ownership of buffers to the runtime. The
//! crate defines [`IoBuf`] and [`IoBufMut`] traits which are implemented by buffer
//! types that respect the `io-uring` contract.
pub mod fixed;

mod io_buf;
use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::{
    iter::zip,
    mem::ManuallyDrop,
    ops::{Index, IndexMut},
};

pub use io_buf::IoBuf;

mod io_buf_mut;
pub use io_buf_mut::IoBufMut;

mod slice;
pub use slice::Slice;

mod bounded;
pub use bounded::{BoundedBuf, BoundedBufMut};

pub(crate) fn deref(buf: &impl IoBuf) -> &[u8] {
    // Safety: the `IoBuf` trait is marked as unsafe and is expected to be
    // implemented correctly.
    unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) }
}

pub(crate) fn deref_mut(buf: &mut impl IoBufMut) -> &mut [u8] {
    // Safety: the `IoBufMut` trait is marked as unsafe and is expected to be
    // implemented correct.
    unsafe { std::slice::from_raw_parts_mut(buf.stable_mut_ptr(), buf.bytes_init()) }
}

/// # Safety
///
/// Returned pointer and length from `into_raw_parts` must be valid.
///
/// If you implement `BufferImpl` for some type `B`, `from_raw_parts(into_raw_parts(buf))`
/// must be equal to origin `buf`.
///
#[allow(missing_docs)]
pub unsafe trait BufferImpl: Any {
    type UserData: Send + Sync + 'static;

    fn into_raw_parts(self) -> (Vec<*mut u8>, Vec<usize>, Vec<usize>, Self::UserData);

    /// # Safety
    /// `from_raw_parts(into_raw_parts(buf))` must be equal to `buf`
    unsafe fn from_raw_parts(
        ptr: Vec<*mut u8>,
        len: Vec<usize>,
        cap: Vec<usize>,
        user_data: Self::UserData,
    ) -> Self;
}

#[allow(missing_docs)]
pub struct Buffer {
    iovec: Vec<libc::iovec>,
    cap: Vec<usize>,
    user_data: *mut (),
    ty: TypeId,
    // SAFETY: Buffer cannot be used after execute `dtor`
    #[allow(clippy::type_complexity)]
    dtor: Option<Box<dyn FnOnce(Vec<*mut u8>, Vec<usize>, Vec<usize>, *mut ())>>,
}

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("iovec", &self.iovec)
            .field("cap", &self.cap)
            .field("user", &self.user_data)
            .field("ty", &self.ty)
            .finish()
    }
}

impl<B: BufferImpl> From<B> for Buffer {
    fn from(value: B) -> Self {
        Buffer::new(value)
    }
}

impl Buffer {
    #[allow(missing_docs)]
    pub fn new<B: BufferImpl>(buf: B) -> Self {
        let ty = buf.type_id();
        let (ptr, len, cap, user_data) = buf.into_raw_parts();
        let iovec = ptr
            .into_iter()
            .zip(len)
            .map(|(base, len)| libc::iovec {
                iov_base: base as _,
                iov_len: len,
            })
            .collect();
        let user_data = Box::into_raw(Box::new(user_data)) as _;
        Self {
            iovec,
            cap,
            user_data,
            ty,
            dtor: Some(Box::new(|ptr, len, cap, user_data| unsafe {
                let user_data = Box::from_raw(user_data as *mut B::UserData);
                drop(B::from_raw_parts(ptr, len, cap, *user_data));
            })),
        }
    }

    #[allow(missing_docs)]
    pub fn try_into<B: BufferImpl>(self) -> Result<B, Self> {
        // Convert only if the type id of source is equal to the type id of target
        if self.ty != TypeId::of::<B>() {
            return Err(self);
        }

        unsafe {
            let this = ManuallyDrop::new(self);
            let cap = std::ptr::read(&this.cap);
            let user_data = Box::from_raw(this.user_data as *mut B::UserData);
            let (ptrs, len) = this
                .iovec
                .iter()
                .map(|iovec| (iovec.iov_base as *mut u8, iovec.iov_len))
                .collect();

            let buf = B::from_raw_parts(ptrs, len, cap, *user_data);
            Ok(buf)
        }
    }
}

impl Buffer {
    #[allow(missing_docs)]
    pub fn len(&self) -> usize {
        self.iovec.len()
    }

    #[allow(missing_docs)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[allow(missing_docs)]
    pub fn fill(&mut self) {
        for (iovec, cap) in zip(&mut self.iovec, &self.cap) {
            iovec.iov_len = *cap;
        }
    }

    #[allow(missing_docs)]
    pub fn iter(&self) -> std::slice::Iter<'_, libc::iovec> {
        self.iovec.iter()
    }

    pub(crate) fn user_data(&self) -> *mut () {
        self.user_data
    }

    pub(crate) fn type_id(&self) -> TypeId {
        self.ty
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let dtor = self.dtor.take().unwrap();
        let (ptr, len) = self
            .iovec
            .iter()
            .map(|iovec| (iovec.iov_base as *mut u8, iovec.iov_len))
            .collect();

        let cap = std::mem::take(&mut self.cap);
        dtor(ptr, len, cap, self.user_data);
    }
}

impl Index<usize> for Buffer {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let iovec = &self.iovec[index];
        unsafe { std::slice::from_raw_parts(iovec.iov_base as *const u8, iovec.iov_len) }
    }
}

impl IndexMut<usize> for Buffer {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let iovec = &mut self.iovec[index];
        unsafe { std::slice::from_raw_parts_mut(iovec.iov_base as *mut u8, iovec.iov_len) }
    }
}

unsafe impl IoBuf for Buffer {
    fn stable_ptr(&self) -> *const u8 {
        if self.iovec.len() == 1 {
            self.iovec[0].iov_base as *const u8
        } else {
            self.iovec.as_ptr() as *const u8
        }
    }

    fn bytes_init(&self) -> usize {
        self.iovec.iter().map(|iovec| iovec.iov_len).sum()
    }

    fn bytes_total(&self) -> usize {
        self.cap.iter().sum()
    }
}

unsafe impl IoBufMut for Buffer {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        if self.iovec.len() == 1 {
            self.iovec[0].iov_base as *mut u8
        } else {
            self.iovec.as_mut_ptr() as *mut u8
        }
    }

    unsafe fn set_init(&mut self, mut pos: usize) {
        for (iovec, cap) in zip(&mut self.iovec, &self.cap) {
            let size = std::cmp::min(*cap, pos);
            iovec.iov_len = size;
            pos -= size;
        }
    }
}

unsafe impl BufferImpl for Vec<u8> {
    type UserData = ();

    fn into_raw_parts(self) -> (Vec<*mut u8>, Vec<usize>, Vec<usize>, Self::UserData) {
        let mut this = ManuallyDrop::new(self);
        let ptr = this.as_mut_ptr() as _;
        let len = this.len();
        let cap = this.capacity();
        (vec![ptr], vec![len], vec![cap], ())
    }

    unsafe fn from_raw_parts(
        ptr: Vec<*mut u8>,
        len: Vec<usize>,
        cap: Vec<usize>,
        _user: Self::UserData,
    ) -> Self {
        Vec::from_raw_parts(ptr[0], len[0], cap[0])
    }
}

unsafe impl BufferImpl for Vec<Vec<u8>> {
    type UserData = ();

    fn into_raw_parts(self) -> (Vec<*mut u8>, Vec<usize>, Vec<usize>, Self::UserData) {
        let mut ptr = Vec::with_capacity(self.len());
        let mut len = Vec::with_capacity(self.len());
        let mut cap = Vec::with_capacity(self.len());
        for vec in self.into_iter() {
            let mut this = ManuallyDrop::new(vec);
            ptr.push(this.as_mut_ptr() as _);
            len.push(this.len());
            cap.push(this.capacity());
        }
        (ptr, len, cap, ())
    }

    unsafe fn from_raw_parts(
        ptr: Vec<*mut u8>,
        len: Vec<usize>,
        cap: Vec<usize>,
        _user: Self::UserData,
    ) -> Self {
        let mut vec = Vec::with_capacity(ptr.len());
        for ((ptr, len), cap) in ptr.into_iter().zip(len).zip(cap) {
            vec.push(Vec::from_raw_parts(ptr, len, cap));
        }
        vec
    }
}
