use futures_util::{stream, Stream};
use io_uring::squeue::Flags;
use std::future::Future;

use crate::{OneshotOutputTransform, Submit, UnsubmittedOneshot};

pub trait Linkable {
    fn set_flags(self, flags: Flags) -> Self;
}

/// A Link struct to represent linked operations.
pub struct Link<L> {
    links: Vec<L>,
}

impl<D> Link<D> {
    /// Ensure at least one or more data will be held by the struct
    pub fn new(data: D) -> Self {
        Self { links: vec![data] }
    }
}

impl<L> Link<L>
where
    L: Linkable,
{
    pub fn link(&mut self, other: L) {
        if let Some(data) = self.links.pop() {
            self.links.push(data.set_flags(Flags::IO_LINK));
        }
        self.links.push(other);
    }

    pub fn hard_link(&mut self, other: L) {
        if let Some(data) = self.links.pop() {
            self.links.push(data.set_flags(Flags::IO_HARDLINK));
        }
        self.links.push(other);
    }
}

impl<L, O> Submit for Link<L>
where
    L: Submit<Output = O>,
{
    type Output = Link<O>;

    fn submit(self) -> Self::Output {
        Link {
            links: self
                .links
                .into_iter()
                .map(|data| data.submit())
                .collect::<Vec<_>>(),
        }
    }
}

impl<D, T: OneshotOutputTransform<StoredData = D>> From<UnsubmittedOneshot<D, T>>
    for Link<UnsubmittedOneshot<D, T>>
{
    fn from(value: UnsubmittedOneshot<D, T>) -> Self {
        Self::new(value)
    }
}

impl<L> Link<L>
where
    L: Future,
{
    pub fn join_all(self) -> impl Future<Output = Vec<L::Output>> {
        futures_util::future::join_all(self.links.into_iter())
    }

    pub fn into_stream(self) -> impl Stream<Item = L::Output> {
        stream::unfold(self.links.into_iter(), |mut in_flights| async move {
            if let Some(fut) = in_flights.next() {
                let output = fut.await;
                Some((output, in_flights))
            } else {
                None
            }
        })
    }
}
