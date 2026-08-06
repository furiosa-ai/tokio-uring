use io_uring::cqueue::Entry;
use io_uring::{opcode, types};

use crate::io::SharedFd;

use crate::{Buffer, OneshotOutputTransform, UnsubmittedOneshot};

/// An unsubmitted cmd operation.
pub type UnsubmittedCmd = UnsubmittedOneshot<CmdData, CmdTransform>;

impl UnsubmittedCmd {
    pub(crate) fn cmd(fd: &SharedFd, op: u32, cmd: [u8; 16], buf: Buffer) -> Self {
        Self::new(
            CmdData { buf },
            CmdTransform {},
            opcode::UringCmd16::new(types::Fd(fd.raw_fd()), op)
                .cmd(cmd)
                .build(),
        )
    }
}

#[allow(missing_docs)]
pub struct CmdData {
    buf: Buffer,
}

#[allow(missing_docs)]
pub struct CmdTransform {}

impl OneshotOutputTransform for CmdTransform {
    type Output = (i32, Buffer);
    type StoredData = CmdData;

    fn transform_oneshot_output(self, data: Self::StoredData, cqe: Entry) -> Self::Output {
        (cqe.result(), data.buf)
    }
}
