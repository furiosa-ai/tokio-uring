use io_uring::{cqueue::Entry, opcode};

use crate::{OneshotOutputTransform, UnsubmittedOneshot};

#[allow(missing_docs)]
pub type UnsubmittedFutexWait = UnsubmittedOneshot<FutexWaitData, FutexWaitTransform>;

#[allow(missing_docs)]
pub struct FutexWaitData {}

#[allow(missing_docs)]
pub struct FutexWaitTransform {}

impl OneshotOutputTransform for FutexWaitTransform {
    type Output = Result<(), std::io::Error>;
    type StoredData = FutexWaitData;

    fn transform_oneshot_output(self, _data: Self::StoredData, cqe: Entry) -> Self::Output {
        let n = cqe.result();
        if n >= 0 {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(-n))
        }
    }
}

impl UnsubmittedFutexWait {
    pub(crate) fn wait(futex: *const u32, val: u64, mask: u64, futex_flags: u32) -> Self {
        Self::new(
            FutexWaitData {},
            FutexWaitTransform {},
            opcode::FutexWait::new(futex, val, mask, futex_flags).build(),
        )
    }
}

#[allow(missing_docs)]
pub type UnsubmittedFutexWake = UnsubmittedOneshot<FutexWakeData, FutexWakeTransform>;

#[allow(missing_docs)]
pub struct FutexWakeData {}

#[allow(missing_docs)]
pub struct FutexWakeTransform {}

impl OneshotOutputTransform for FutexWakeTransform {
    type Output = Result<i32, std::io::Error>;
    type StoredData = FutexWakeData;

    fn transform_oneshot_output(self, _data: Self::StoredData, cqe: Entry) -> Self::Output {
        let n = cqe.result();
        if n >= 0 {
            Ok(n)
        } else {
            Err(std::io::Error::from_raw_os_error(-n))
        }
    }
}

impl UnsubmittedFutexWake {
    pub(crate) fn wake(futex: *const u32, val: u64, mask: u64, futex_flags: u32) -> Self {
        Self::new(
            FutexWakeData {},
            FutexWakeTransform {},
            opcode::FutexWake::new(futex, val, mask, futex_flags).build(),
        )
    }
}
