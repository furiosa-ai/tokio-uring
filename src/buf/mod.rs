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

    fn dtor() -> Box<dyn Fn(*mut u8, usize, *mut ())>;
    fn into_raw_parts(self) -> (Vec<*mut u8>, Vec<usize>, Vec<Self::UserData>);
    /// # Safety
    /// `from_raw_parts(into_raw_parts(buf))` must be equal to `buf`
    unsafe fn from_raw_parts(ptr: Vec<*mut u8>, len: Vec<usize>, user: Vec<Self::UserData>)
        -> Self;
}

#[allow(missing_docs)]
pub struct Buffer {
    iovecs: Vec<libc::iovec>,
    user_data: Vec<*mut ()>,
    ty: TypeId,
    // SAFETY: Buffer cannot be used after execute `dtor`
    dtor: Box<dyn Fn(*mut u8, usize, *mut ())>,
}

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

// FIXME
impl Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("iovecs", &self.iovecs)
            .field("user", &self.user_data)
            .field("ty", &self.ty)
            // .field("dtor", &self.dtor)
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
        let (ptr, len, user_data) = buf.into_raw_parts();
        let iovecs = ptr
            .into_iter()
            .zip(len)
            .map(|(base, len)| libc::iovec {
                iov_base: base as _,
                iov_len: len,
            })
            .collect();
        let user_data = user_data
            .into_iter()
            .map(|user| Box::into_raw(Box::new(user)) as _)
            .collect();
        Self {
            iovecs,
            user_data,
            ty,
            dtor: B::dtor(),
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
            let user = this
                .user_data
                .iter()
                .map(|user| *Box::from_raw(*user as *mut B::UserData))
                .collect();
            let (ptrs, lens) = this
                .iovecs
                .iter()
                .map(|iovec| (iovec.iov_base as *mut u8, iovec.iov_len))
                .collect();
            let buf = B::from_raw_parts(ptrs, lens, user);
            Ok(buf)
        }
    }
}

impl Buffer {
    #[allow(missing_docs)]
    pub fn len(&self) -> usize {
        self.iovecs.len()
    }

    #[allow(missing_docs)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[allow(missing_docs)]
    pub fn iter(&self) -> std::slice::Iter<'_, libc::iovec> {
        self.iovecs.iter()
    }

    pub(crate) fn user_data(&self) -> &Vec<*mut ()> {
        &self.user_data
    }

    pub(crate) fn type_id(&self) -> TypeId {
        self.ty
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        for i in 0..self.iovecs.len() {
            let ptr = self.iovecs[i].iov_base as *mut u8;
            let len = self.iovecs[i].iov_len;
            let user = self.user_data[i];
            (self.dtor)(ptr, len, user);
        }
    }
}

impl Index<usize> for Buffer {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let iovec = &self.iovecs[index];
        unsafe { std::slice::from_raw_parts(iovec.iov_base as *const u8, iovec.iov_len) }
    }
}

impl IndexMut<usize> for Buffer {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let iovec = &mut self.iovecs[index];
        unsafe { std::slice::from_raw_parts_mut(iovec.iov_base as *mut u8, iovec.iov_len) }
    }
}

unsafe impl IoBuf for Buffer {
    fn stable_ptr(&self) -> *const u8 {
        if self.iovecs.len() == 1 {
            self.iovecs[0].iov_base as *const u8
        } else {
            self.iovecs.as_ptr() as *const u8
        }
    }

    fn bytes_init(&self) -> usize {
        self.iovecs.iter().map(|iovec| iovec.iov_len).sum()
    }

    fn bytes_total(&self) -> usize {
        IoBuf::bytes_init(self)
    }
}

unsafe impl IoBufMut for Buffer {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        if self.iovecs.len() == 1 {
            self.iovecs[0].iov_base as *mut u8
        } else {
            self.iovecs.as_mut_ptr() as *mut u8
        }
    }

    unsafe fn set_init(&mut self, mut _pos: usize) {
        // Do nothing. We don't distinguish length and capacity.
    }
}

unsafe impl BufferImpl for Vec<u8> {
    type UserData = ();

    fn dtor() -> Box<dyn Fn(*mut u8, usize, *mut ())> {
        Box::new(|ptr: *mut u8, len: usize, _user: *mut ()| unsafe {
            let _ = Vec::from_raw_parts(ptr, len, len);
        })
    }

    fn into_raw_parts(self) -> (Vec<*mut u8>, Vec<usize>, Vec<Self::UserData>) {
        let mut this = ManuallyDrop::new(self);
        let ptr = this.as_mut_ptr() as _;
        let cap = this.capacity();
        (vec![ptr], vec![cap], vec![()])
    }

    unsafe fn from_raw_parts(
        ptr: Vec<*mut u8>,
        len: Vec<usize>,
        _user: Vec<Self::UserData>,
    ) -> Self {
        Vec::from_raw_parts(ptr[0], len[0], len[0])
    }
}

unsafe impl BufferImpl for Vec<Vec<u8>> {
    type UserData = ();

    fn dtor() -> Box<dyn Fn(*mut u8, usize, *mut ())> {
        Box::new(|ptr: *mut u8, len: usize, _user: *mut ()| unsafe {
            let _ = Vec::from_raw_parts(ptr, len, len);
        })
    }

    fn into_raw_parts(self) -> (Vec<*mut u8>, Vec<usize>, Vec<Self::UserData>) {
        let mut ptr = Vec::with_capacity(self.len());
        let mut cap = Vec::with_capacity(self.len());
        let user = std::iter::repeat(()).take(self.len()).collect();
        for vec in self.into_iter() {
            let mut this = ManuallyDrop::new(vec);
            ptr.push(this.as_mut_ptr() as _);
            cap.push(this.capacity());
        }
        (ptr, cap, user)
    }

    unsafe fn from_raw_parts(
        ptr: Vec<*mut u8>,
        len: Vec<usize>,
        _user: Vec<Self::UserData>,
    ) -> Self {
        let mut vec = Vec::with_capacity(ptr.len());
        for (p, l) in ptr.into_iter().zip(len) {
            vec.push(Vec::from_raw_parts(p, l, l));
        }
        vec
    }
}
