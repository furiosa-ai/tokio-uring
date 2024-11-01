use libc::iovec;

use crate::buf::{BoundedBufMut, Buffer};
use crate::WithBuffer;
use crate::{buf::BoundedBuf, io::SharedFd, OneshotOutputTransform, Result, UnsubmittedOneshot};
use std::io;

#[allow(missing_docs)]
pub type Unsubmitted = UnsubmittedOneshot<ReadWriteData, ReadWriteTransform>;

#[allow(missing_docs)]
pub struct ReadWriteData {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    _fd: SharedFd,

    buf: Buffer,
}

enum Kind {
    Read,
    Write,
}

#[allow(missing_docs)]
pub struct ReadWriteTransform(Kind);

impl OneshotOutputTransform for ReadWriteTransform {
    type Output = Result<usize, Buffer>;

    type StoredData = ReadWriteData;

    fn transform_oneshot_output(
        self,
        mut data: Self::StoredData,
        cqe: io_uring::cqueue::Entry,
    ) -> Self::Output {
        let n = cqe.result();
        if n < 0 {
            return Err(io::Error::from_raw_os_error(-n)).with_buffer(data.buf);
        }

        if matches!(self.0, Kind::Read) {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe { data.buf.set_init(n as usize) };
        }

        Ok((n as usize, data.buf))
    }
}

impl Unsubmitted {
    pub(crate) fn write_at(fd: &SharedFd, buf: Buffer, offset: u64) -> Self {
        use io_uring::{opcode, types};

        // Get raw buffer info
        let ptr = buf.stable_ptr();
        let len = buf.bytes_init();

        let sqe = if buf.len() == 1 {
            // Fixed buffer io not support vectored io
            if buf.source().is_fixed() {
                let index = buf.source().buf_index();
                opcode::WriteFixed::new(types::Fd(fd.raw_fd()), ptr, len as _, index)
                    .offset(offset as _)
                    .build()
            } else {
                opcode::Write::new(types::Fd(fd.raw_fd()), ptr, len as _)
                    .offset(offset as _)
                    .build()
            }
        } else {
            opcode::Writev::new(types::Fd(fd.raw_fd()), ptr as *const iovec, buf.len() as _)
                .offset(offset as _)
                .build()
        };

        Self::new(
            ReadWriteData {
                _fd: fd.clone(),
                buf,
            },
            ReadWriteTransform(Kind::Write),
            sqe,
        )
    }

    pub(crate) fn read_at(fd: &SharedFd, mut buf: Buffer, offset: u64) -> Self {
        use io_uring::{opcode, types};

        // Get raw buffer info
        let ptr = buf.stable_mut_ptr();
        let len = buf.bytes_total();

        buf.fill();

        let sqe = if buf.len() == 1 {
            // Fixed buffer io not support vectored io
            if buf.source().is_fixed() {
                let index = buf.source().buf_index();
                opcode::ReadFixed::new(types::Fd(fd.raw_fd()), ptr, len as _, index)
                    .offset(offset as _)
                    .build()
            } else {
                opcode::Read::new(types::Fd(fd.raw_fd()), ptr, len as _)
                    .offset(offset as _)
                    .build()
            }
        } else {
            opcode::Readv::new(types::Fd(fd.raw_fd()), ptr as *mut iovec, buf.len() as _)
                .offset(offset as _)
                .build()
        };

        Self::new(
            ReadWriteData {
                _fd: fd.clone(),
                buf,
            },
            ReadWriteTransform(Kind::Read),
            sqe,
        )
    }
}
