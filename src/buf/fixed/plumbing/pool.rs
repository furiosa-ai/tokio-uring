use tokio::sync::Notify;

use std::cmp;
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::sync::Arc;

// Internal state shared by FixedBufPool and FixedBuf handles.
pub(crate) struct Pool {
    // Pointer to an allocated array of iovec records referencing
    // the allocated buffers. The number of initialized records is the
    // same as the length of the states array.
    iovecs: Vec<libc::iovec>,
    states: Vec<BufState>,
    // Table of head indices of the free buffer lists in each size bucket.
    free_buf_head_by_cap: HashMap<usize, u16>,
    // Used to notify tasks pending on `next`
    notify_next_by_cap: HashMap<usize, Arc<Notify>>,
}

unsafe impl Send for Pool {}
unsafe impl Sync for Pool {}

// State information of a buffer in the registry,
enum BufState {
    // The buffer is not in use.
    Free {
        // Index of the next buffer of the same capacity in a free buffer list, if any.
        next: Option<u16>,
    },
    // The buffer is checked out.
    // Its data are logically owned by the FixedBuf handle,
    // which also keeps track of the length of the initialized part.
    CheckedOut,
}

impl Pool {
    pub(crate) fn new(bufs: impl Iterator<Item = Vec<u8>>) -> Self {
        // Limit the number of buffers to the maximum allowable number.
        let bufs = bufs.take(cmp::min(libc::UIO_MAXIOV as usize, u16::MAX as usize));
        // Collect into `buffers`, which holds the backing buffers for
        // the lifetime of the pool. Using collect may allow
        // the compiler to apply collect in place specialization,
        // to avoid an allocation.
        let buffers = bufs.collect::<Vec<_>>();
        let mut iovecs = Vec::with_capacity(buffers.len());
        let mut states = Vec::with_capacity(buffers.len());
        let mut free_buf_head_by_cap = HashMap::new();
        for (i, buf) in buffers.into_iter().enumerate() {
            let mut buf = ManuallyDrop::new(buf);
            let cap = buf.capacity();
            let iovec = libc::iovec {
                iov_base: buf.as_mut_ptr() as _,
                iov_len: cap,
            };
            iovecs.push(iovec);

            // Link the buffer as the head of the free list for its capacity.
            // This constructs the free buffer list to be initially retrieved
            // back to front, which should be of no difference to the user.
            let next = free_buf_head_by_cap.insert(cap, i as u16);
            states.push(BufState::Free { next });
        }
        debug_assert_eq!(iovecs.len(), states.len());

        Pool {
            iovecs,
            states,
            free_buf_head_by_cap,
            notify_next_by_cap: HashMap::new(),
        }
    }

    pub(crate) fn iovecs(&self) -> &[libc::iovec] {
        &self.iovecs
    }

    // If the free buffer list for this capacity is not empty, checks out the first buffer
    // from the list and returns its data. Otherwise, returns None.
    pub(crate) fn try_next(&mut self, cap: usize) -> Option<(libc::iovec, usize)> {
        let free_head = self.free_buf_head_by_cap.get_mut(&cap)?;
        let index = *free_head as usize;
        let state = self.states.get_mut(index).expect("invalid buffer index");
        let BufState::Free { next } = *state else {
            panic!("buffer is checked out")
        };
        *state = BufState::CheckedOut;

        // Update the head of the free list for this capacity.
        match next {
            Some(i) => {
                *free_head = i;
            }
            None => {
                self.free_buf_head_by_cap.remove(&cap);
            }
        }

        let iovec = self.iovecs[index];

        Some((iovec, index))
    }

    // Returns a `Notify` to use for waking up tasks awaiting a buffer of
    // the specified capacity.
    pub(crate) fn notify_on_next(&mut self, cap: usize) -> Arc<Notify> {
        let notify = self.notify_next_by_cap.entry(cap).or_default();
        Arc::clone(notify)
    }

    pub(crate) fn check_in(&mut self, index: usize) {
        let cap = self.iovecs[index].iov_len;
        let state = &mut self.states[index];
        debug_assert!(
            matches!(state, BufState::CheckedOut),
            "the buffer must be checked out"
        );

        // Link the buffer as the new head of the free list for its capacity.
        // Recently checked in buffers will be first to be reused,
        // improving cache locality.
        let next = self.free_buf_head_by_cap.insert(cap, index as u16);

        *state = BufState::Free { next };

        if let Some(notify) = self.notify_next_by_cap.get(&cap) {
            // Wake up a single task pending on `next`
            notify.notify_one();
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        for (i, state) in self.states.iter().enumerate() {
            match state {
                BufState::Free { .. } => {
                    // Update buffer initialization.
                    // The origin Vec<u8>s are dropped here.
                    let _ = unsafe {
                        Vec::from_raw_parts(
                            self.iovecs[i].iov_base as _,
                            self.iovecs[i].iov_len,
                            self.iovecs[i].iov_len,
                        )
                    };
                }
                BufState::CheckedOut => unreachable!("all buffers must be checked in"),
            }
        }
    }
}
