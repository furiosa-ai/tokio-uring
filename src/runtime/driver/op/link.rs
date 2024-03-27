use io_uring::squeue::Flags;
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{OneshotOutputTransform, Submit, UnsubmittedOneshot};

/// A Link struct to represent linked operations.
pub struct Link<D> {
    data: D,
    next: Option<Box<Link<D>>>,
}

impl<D> Link<D> {
    /// Construct a new Link with actual data and next node (Link or UnsubmittedOneshot).
    pub fn new(data: D, next: Option<Box<Link<D>>>) -> Self {
        Self { data, next }
    }
}

impl<D> Submit for Link<D>
where
    D: Submit,
{
    type Output = LinkedInFlightOneshot<D::Output>;

    fn submit(self) -> Self::Output {
        LinkedInFlightOneshot {
            data: self.data.submit(),
            next: self.next.map(|link| Box::new(link.submit())),
        }
    }
}

impl<D, T: OneshotOutputTransform<StoredData = D>> Link<UnsubmittedOneshot<D, T>> {
    /// Construct a new soft Link with current Link and other UnsubmittedOneshot.
    pub fn link(self, other: Self) -> Link<UnsubmittedOneshot<D, T>> {
        Link {
            data: self.data.set_flags(Flags::IO_LINK),
            next: Some(Box::new(other)),
        }
    }

    /// Construct a new hard Link with current Link and other UnsubmittedOneshot.
    pub fn hard_link(self, other: Self) -> Link<UnsubmittedOneshot<D, T>> {
        Link {
            data: self.data.set_flags(Flags::IO_HARDLINK),
            next: Some(Box::new(other)),
        }
    }
}

pin_project! {
    /// An in-progress linked oneshot operations which can be polled for completion.
    pub struct LinkedInFlightOneshot<D> {
        #[pin]
        data: D,
        next: Option<Box<LinkedInFlightOneshot<D>>>,
    }
}

impl<D> Future for LinkedInFlightOneshot<D>
where
    D: Future,
{
    type Output = (D::Output, Option<Box<LinkedInFlightOneshot<D>>>); // Will return actual output and next linked future.

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let output = ready!(this.data.poll(cx));

        Poll::Ready((output, this.next.take()))
    }
}
