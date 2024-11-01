use crate::buf::{BoundedBuf, BufferSource};
use crate::io::SharedFd;
use crate::runtime::driver::op::{self, Completable, Op};
use crate::WithBuffer;
use crate::{Buffer, Result};

use crate::runtime::CONTEXT;
use std::io;

pub(crate) struct WriteFixed<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(dead_code)]
    fd: SharedFd,

    buf: T,
}

impl<T> Op<WriteFixed<T>>
where
    T: BoundedBuf<Buf = Buffer>,
{
    pub(crate) fn write_fixed_at(
        fd: &SharedFd,
        buf: T,
        offset: u64,
    ) -> io::Result<Op<WriteFixed<T>>> {
        use io_uring::{opcode, types};

        CONTEXT.with(|x| {
            x.handle().expect("Not in a runtime context").submit_op(
                WriteFixed {
                    fd: fd.clone(),
                    buf,
                },
                |write_fixed| {
                    // Get raw buffer info
                    let ptr = write_fixed.buf.stable_ptr();
                    let len = write_fixed.buf.bytes_init();
                    let source = write_fixed.buf.get_buf().source().next().unwrap();
                    let BufferSource::FixedBuf { buf_index } = source else {
                        unreachable!("the source of buffer must be FixedBuf")
                    };
                    opcode::WriteFixed::new(types::Fd(fd.raw_fd()), ptr, len as _, *buf_index)
                        .offset(offset as _)
                        .build()
                },
            )
        })
    }
}

impl<T> Completable for WriteFixed<T> {
    type Output = Result<usize, T>;

    fn complete(self, cqe: op::CqeResult) -> Self::Output {
        cqe.result.map(|v| v as usize).with_buffer(self.buf)
    }
}
