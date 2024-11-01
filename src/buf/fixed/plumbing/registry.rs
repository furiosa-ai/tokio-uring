use std::{cmp, mem::ManuallyDrop};

// Internal state shared by FixedBufRegistry and Buffers.
pub(crate) struct Registry {
    // Vector of iovec records referencing
    // the allocated buffers. The number of initialized records is the
    // same as the length of the states array.
    iovecs: Vec<libc::iovec>,
    // Original capacities of original buffers.
    origin_caps: Vec<usize>,
    // State information on the buffers. Indices in this array correspond to
    // the indices in the array at iovecs.
    states: Vec<BufState>,
}

unsafe impl Send for Registry {}
unsafe impl Sync for Registry {}

// State information of a buffer in the registry,
enum BufState {
    // The buffer is not in use.
    // The field records the length of the initialized part.
    Free { init_len: usize },
    // The buffer is checked out.
    // Its data are logically owned by the Buffer,
    // which also keeps track of the length of the initialized part.
    CheckedOut,
}

impl Registry {
    pub(crate) fn new(bufs: impl Iterator<Item = Vec<u8>>) -> Self {
        // Limit the number of buffers to the maximum allowable number.
        let bufs = bufs.take(cmp::min(libc::UIO_MAXIOV as usize, u16::MAX as usize));
        // Collect into `buffers`, which holds the backing buffers for
        // the lifetime of the pool. Using collect may allow
        // the compiler to apply collect in place specialization,
        // to avoid an allocation.
        let buffers = bufs.collect::<Vec<_>>();
        let mut iovecs = Vec::with_capacity(buffers.len());
        let mut origin_caps = Vec::with_capacity(buffers.len());
        let mut states = Vec::with_capacity(buffers.len());
        for buffer in buffers {
            // Origin buffer will be dropped when Registry is dropped
            let mut buffer = ManuallyDrop::new(buffer);
            let iovec = libc::iovec {
                iov_base: buffer.as_mut_ptr() as _,
                iov_len: buffer.len(),
            };
            iovecs.push(iovec);
            origin_caps.push(buffer.capacity());
            states.push(BufState::Free {
                init_len: buffer.len(),
            });
        }
        debug_assert_eq!(iovecs.len(), states.len());
        debug_assert_eq!(iovecs.len(), origin_caps.len());

        Self {
            iovecs,
            origin_caps,
            states,
        }
    }

    pub(crate) fn iovecs(&self) -> &[libc::iovec] {
        &self.iovecs
    }

    // If the indexed buffer is free, changes its state to checked out
    // and returns its data.
    // If the buffer is already checked out, returns None.
    pub(crate) fn check_out(&mut self, index: usize) -> Option<libc::iovec> {
        let state = self.states.get_mut(index).expect("invalid buffer index");
        let BufState::Free { init_len } = *state else {
            return None;
        };
        *state = BufState::CheckedOut;

        let mut iovec = self.iovecs[index];
        iovec.iov_len = init_len;

        Some(iovec)
    }

    pub(crate) fn check_in(&mut self, index: usize, init_len: usize) {
        let state = self.states.get_mut(index).expect("invalid buffer index");
        debug_assert!(
            matches!(state, BufState::CheckedOut),
            "the buffer must be checked out"
        );
        *state = BufState::Free { init_len };
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        for (i, state) in self.states.iter().enumerate() {
            match state {
                BufState::Free { init_len } => {
                    // Update buffer initialization.
                    // The origin buffers are dropped here.
                    let _ = unsafe {
                        Vec::from_raw_parts(
                            self.iovecs[i].iov_base as _,
                            *init_len,
                            self.origin_caps[i],
                        )
                    };
                }
                BufState::CheckedOut => unreachable!("all buffers must be checked in"),
            }
        }
    }
}
