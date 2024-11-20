use libc::iovec;

use crate::buf::fixed::pool::PoolInfo;
use crate::buf::fixed::registry::RegistryInfo;
use crate::buf::fixed::{pool, registry};
use crate::buf::{BoundedBufMut, Buffer};
use crate::WithBuffer;
use crate::{buf::BoundedBuf, io::SharedFd, OneshotOutputTransform, Result, UnsubmittedOneshot};
use std::any::TypeId;
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

            // Get buf_index from raw pointer
            if buf.type_id() == TypeId::of::<registry::FixedBuf>() {
                // Safety: The condition above indicates that the source of buffer is `registry::FixedBuf`.
                // According to the `BufferImpl` implementation for `registry::FixedBuf`, the user_data
                // pointer contains a raw pointer of type `RegistryInfo`, so this raw pointer casting is safe.
                let buf_index = unsafe {
                    let registry_info = buf.user_data() as *const RegistryInfo;
                    (*registry_info).index
                };
                opcode::WriteFixed::new(types::Fd(fd.raw_fd()), ptr, len as _, buf_index)
                    .offset(offset as _)
                    .build()
            } else if buf.type_id() == TypeId::of::<pool::FixedBuf>() {
                // Safety: This raw pointer casting is also safe as above.
                let buf_index = unsafe {
                    let pool_info = buf.user_data() as *const PoolInfo;
                    (*pool_info).index
                };
                opcode::WriteFixed::new(types::Fd(fd.raw_fd()), ptr, len as _, buf_index)
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

        let sqe = if buf.len() == 1 {
            // Fixed buffer io not support vectored io

            // Get buf_index from raw pointer
            if buf.type_id() == TypeId::of::<registry::FixedBuf>() {
                // Safety: The condition above indicates that the source of buffer is `registry::FixedBuf`.
                // According to the `BufferImpl` implementation for `registry::FixedBuf`, the user_data
                // pointer contains a raw pointer of type `RegistryInfo`, so this raw pointer casting is safe.
                let buf_index = unsafe {
                    let registry_info = buf.user_data() as *const RegistryInfo;
                    (*registry_info).index
                };
                opcode::ReadFixed::new(types::Fd(fd.raw_fd()), ptr, len as _, buf_index)
                    .offset(offset as _)
                    .build()
            } else if buf.type_id() == TypeId::of::<pool::FixedBuf>() {
                // Safety: This raw pointer casting is also safe as above.
                let buf_index = unsafe {
                    let pool_info = buf.user_data() as *const PoolInfo;
                    (*pool_info).index
                };
                opcode::ReadFixed::new(types::Fd(fd.raw_fd()), ptr, len as _, buf_index)
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
