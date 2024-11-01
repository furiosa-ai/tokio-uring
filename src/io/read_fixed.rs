use crate::buf::{BoundedBufMut, BufferSource};
use crate::io::SharedFd;
use crate::runtime::driver::op::{self, Completable, Op};
use crate::WithBuffer;
use crate::{Buffer, Result};

use crate::runtime::CONTEXT;
use std::io;

pub(crate) struct ReadFixed<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(dead_code)]
    fd: SharedFd,

    /// The in-flight buffer.
    buf: T,
}

impl<T> Op<ReadFixed<T>>
where
    T: BoundedBufMut<BufMut = Buffer>,
{
    pub(crate) fn read_fixed_at(
        fd: &SharedFd,
        buf: T,
        offset: u64,
    ) -> io::Result<Op<ReadFixed<T>>> {
        use io_uring::{opcode, types};

        CONTEXT.with(|x| {
            x.handle().expect("Not in a runtime context").submit_op(
                ReadFixed {
                    fd: fd.clone(),
                    buf,
                },
                |read_fixed| {
                    // Get raw buffer info
                    let ptr = read_fixed.buf.stable_mut_ptr();
                    let len = read_fixed.buf.bytes_total();
                    let source = read_fixed.buf.get_buf().source().next().unwrap();
                    let BufferSource::FixedBuf { buf_index } = source else {
                        unreachable!("the source of buffer must be FixedBuf")
                    };
                    opcode::ReadFixed::new(types::Fd(fd.raw_fd()), ptr, len as _, *buf_index)
                        .offset(offset as _)
                        .build()
                },
            )
        })
    }
}

impl<T> Completable for ReadFixed<T>
where
    T: BoundedBufMut<BufMut = Buffer>,
{
    type Output = Result<usize, T>;

    fn complete(self, cqe: op::CqeResult) -> Self::Output {
        // Recover the buffer
        let mut buf = self.buf;
        // Convert the operation result to `usize`
        let res = cqe.result.map(|v| v as usize);

        // If the operation was successful, advance the initialized cursor.
        if let Ok(n) = res {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                buf.set_init(n);
            }
        }

        res.with_buffer(buf)
    }
}
