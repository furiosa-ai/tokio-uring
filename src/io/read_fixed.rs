use crate::buf::fixed::pool::PoolInfo;
use crate::buf::fixed::registry::RegistryInfo;
use crate::buf::fixed::{pool, registry};
use crate::buf::BoundedBufMut;
use crate::io::SharedFd;
use crate::runtime::driver::op::{self, Completable, Op};
use crate::WithBuffer;
use crate::{Buffer, Result};

use crate::runtime::CONTEXT;
use std::any::TypeId;
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
                    let buf_type = read_fixed.buf.get_buf().type_id();
                    // Get buf_index from raw pointer
                    let buf_index = if buf_type == TypeId::of::<registry::FixedBuf>() {
                        // Safety: The condition above indicates that the source of buffer is `registry::FixedBuf`.
                        // According to the `BufferImpl` implementation for `registry::FixedBuf`, the user_data
                        // pointer contains a raw pointer of type `RegistryInfo`, so this raw pointer casting is safe.
                        unsafe {
                            let registry_info =
                                read_fixed.buf.get_buf().user_data() as *const RegistryInfo;
                            (*registry_info).index
                        }
                    } else if buf_type == TypeId::of::<pool::FixedBuf>() {
                        // Safety: This raw pointer casting is also safe as above.
                        unsafe {
                            let pool_info = read_fixed.buf.get_buf().user_data() as *const PoolInfo;
                            (*pool_info).index
                        }
                    } else {
                        panic!("Buffer must be created from FixedBuf");
                    };
                    opcode::ReadFixed::new(types::Fd(fd.raw_fd()), ptr, len as _, buf_index)
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
