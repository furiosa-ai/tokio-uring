use crate::io::futex::{UnsubmittedFutexWait, UnsubmittedFutexWake};

#[allow(missing_docs)]
pub struct Futex;
impl Futex {
    #[allow(missing_docs)]
    pub fn wait(futex: *const u32, val: u64, mask: u64, futex_flags: u32) -> UnsubmittedFutexWait {
        UnsubmittedFutexWait::wait(futex, val, mask, futex_flags)
    }

    #[allow(missing_docs)]
    pub fn wake(futex: *const u32, val: u64, mask: u64, futex_flags: u32) -> UnsubmittedFutexWake {
        UnsubmittedFutexWake::wake(futex, val, mask, futex_flags)
    }
}
