use std::marker::PhantomData;

use io_uring::cqueue::Entry;
use io_uring::{opcode, types};

use crate::buf::BoundedBuf;
use crate::io::SharedFd;

use crate::{OneshotOutputTransform, UnsubmittedOneshot};

/// An unsubmitted read operation.
pub type UnsubmittedCmd<T> = UnsubmittedOneshot<CmdData<T>, CmdTransform<T>>;

impl<T> UnsubmittedCmd<T> {
    pub(crate) fn cmd(fd: &SharedFd, op: u32, cmd: [u8; 16], data: T) -> Self {
        Self::new(
            CmdData { data },
            CmdTransform {
                _phantom: PhantomData,
            },
            opcode::UringCmd16::new(types::Fd(fd.raw_fd()), op)
                .cmd(cmd)
                .build(),
        )
    }
}

#[allow(missing_docs)]
pub struct CmdData<T> {
    data: T,
}

#[allow(missing_docs)]
pub struct CmdTransform<B> {
    _phantom: PhantomData<B>,
}

impl<T> OneshotOutputTransform for CmdTransform<T> {
    type Output = (i32, T);
    type StoredData = CmdData<T>;

    fn transform_oneshot_output(self, mut data: Self::StoredData, cqe: Entry) -> Self::Output {
        // let result = cqe.result();
        // let res = if n >= 0 {
        //     // Safety: the kernel wrote `n` bytes to the buffer.
        //     unsafe { data.buf.set_init(n as usize) };
        //     Ok(n as usize)
        // } else {
        //     Err(io::Error::from_raw_os_error(-n))
        // };

        (cqe.result(), data.data)
    }
}
