use io_uring::cqueue::Entry;

use crate::buf::BoundedBufMut;
use crate::io::SharedFd;
use crate::{BufResult, OneshotOutputTransform, UnsubmittedOneshot};

use crate::runtime::driver::op::{Completable, CqeResult, Op};
use crate::runtime::CONTEXT;
use std::io;
use std::marker::PhantomData;

pub(crate) struct Read<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(dead_code)]
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
}

impl<T: BoundedBufMut> Op<Read<T>> {
    pub(crate) fn read_at(fd: &SharedFd, buf: T, offset: u64) -> io::Result<Op<Read<T>>> {
        use io_uring::{opcode, types};

        CONTEXT.with(|x| {
            x.handle().expect("Not in a runtime context").submit_op(
                Read {
                    fd: fd.clone(),
                    buf,
                },
                |read| {
                    // Get raw buffer info
                    let ptr = read.buf.stable_mut_ptr();
                    let len = read.buf.bytes_total();
                    opcode::Read::new(types::Fd(fd.raw_fd()), ptr, len as _)
                        .offset(offset as _)
                        .build()
                },
            )
        })
    }
}

impl<T> Completable for Read<T>
where
    T: BoundedBufMut,
{
    type Output = BufResult<usize, T>;

    fn complete(self, cqe: CqeResult) -> Self::Output {
        // Convert the operation result to `usize`
        let res = cqe.result.map(|v| v as usize);
        // Recover the buffer
        let mut buf = self.buf;

        // If the operation was successful, advance the initialized cursor.
        if let Ok(n) = res {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                buf.set_init(n);
            }
        }

        (res, buf)
    }
}


/// An unsubmitted read operation.
pub type UnsubmittedRead<T> = UnsubmittedOneshot<ReadData<T>, ReadTransform<T>>;

#[allow(missing_docs)]
pub struct ReadData<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    _fd: SharedFd,

    buf: T,
}

#[allow(missing_docs)]
pub struct ReadTransform<T> {
    _phantom: PhantomData<T>,
}

impl<T> OneshotOutputTransform for ReadTransform<T>
where
    T: BoundedBufMut,
{
    type Output = BufResult<usize, T>;
    type StoredData = ReadData<T>;

    fn transform_oneshot_output(self, mut data: Self::StoredData, cqe: Entry) -> Self::Output {
        let n = cqe.result();
        let res = if n >= 0 {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe { data.buf.set_init(n as usize) };
            Ok(n as usize)
        } else {
            Err(io::Error::from_raw_os_error(-n))
        };

        (res, data.buf)
    }
}

impl<T: BoundedBufMut> UnsubmittedRead<T> {
    pub(crate) fn read_at(fd: &SharedFd, mut buf: T, offset: u64) -> Self {
        use io_uring::{opcode, types};

        // Get raw buffer info
        let ptr = buf.stable_mut_ptr();
        let len = buf.bytes_total();

        Self::new(
            ReadData {
                _fd: fd.clone(),
                buf,
            },
            ReadTransform {
                _phantom: PhantomData,
            },
            opcode::Read::new(types::Fd(fd.raw_fd()), ptr, len as _)
                .offset(offset as _)
                .build(),
        )
    }
}

