use std::marker::PhantomData;
use std::pin::Pin;

use io_uring::cqueue::Entry;
use io_uring::{opcode, types};

use crate::io::SharedFd;

use crate::{OneshotOutputTransform, UnsubmittedOneshot};

/// An unsubmitted read operation.
pub type UnsubmittedCmd<T> = UnsubmittedOneshot<CmdData<T>, CmdTransform<T>>;

impl<T> UnsubmittedCmd<T> {
    pub(crate) fn cmd(fd: &SharedFd, op: u32, cmd: [u8; 16], data: Pin<Box<T>>) -> Self {
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
    data: Pin<Box<T>>,
}

#[allow(missing_docs)]
pub struct CmdTransform<B> {
    _phantom: PhantomData<B>,
}

impl<T> OneshotOutputTransform for CmdTransform<T> {
    type Output = (i32, Pin<Box<T>>);
    type StoredData = CmdData<T>;

    fn transform_oneshot_output(self, data: Self::StoredData, cqe: Entry) -> Self::Output {
        (cqe.result(), data.data)
    }
}
