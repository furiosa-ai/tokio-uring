use std::{
    io,
    net::SocketAddr,
    os::unix::prelude::{AsRawFd, FromRawFd, RawFd},
};

use crate::{
    buf::{BoundedBuf, Buffer},
    io::{SharedFd, Socket},
    Submit, Unsubmitted,
};

/// A TCP stream between a local and a remote socket.
///
/// A TCP stream can either be created by connecting to an endpoint, via the
/// [`connect`] method, or by [`accepting`] a connection from a [`listener`].
///
/// # Examples
///
/// ```no_run
/// use tokio_uring::net::TcpStream;
/// use tokio_uring::Submit;
/// use std::net::ToSocketAddrs;
///
/// fn main() -> std::io::Result<()> {
///     tokio_uring::start(async {
///         // Connect to a peer
///         let mut stream = TcpStream::connect("127.0.0.1:8080".parse().unwrap()).await?;
///
///         // Write some data.
///         stream.write(b"hello world!".to_vec().into()).submit().await.unwrap();
///
///         Ok(())
///     })
/// }
/// ```
///
/// [`connect`]: TcpStream::connect
/// [`accepting`]: crate::net::TcpListener::accept
/// [`listener`]: crate::net::TcpListener
pub struct TcpStream {
    pub(super) inner: Socket,
}

impl TcpStream {
    /// Opens a TCP connection to a remote host at the given `SocketAddr`
    pub async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
        let socket = Socket::new(addr, libc::SOCK_STREAM)?;
        socket.connect(socket2::SockAddr::from(addr)).await?;
        let tcp_stream = TcpStream { inner: socket };
        Ok(tcp_stream)
    }

    /// Creates new `TcpStream` from a previously bound `std::net::TcpStream`.
    ///
    /// This function is intended to be used to wrap a TCP stream from the
    /// standard library in the tokio-uring equivalent. The conversion assumes nothing
    /// about the underlying socket; it is left up to the user to decide what socket
    /// options are appropriate for their use case.
    ///
    /// This can be used in conjunction with socket2's `Socket` interface to
    /// configure a socket before it's handed off, such as setting options like
    /// `reuse_address` or binding to multiple addresses.
    pub fn from_std(socket: std::net::TcpStream) -> Self {
        let inner = Socket::from_std(socket);
        Self { inner }
    }

    pub(crate) fn from_socket(inner: Socket) -> Self {
        Self { inner }
    }

    /// Read some data from the stream into the buffer.
    ///
    /// Returns the original buffer and quantity of data read.
    pub async fn read(&self, buf: Buffer) -> crate::Result<usize, Buffer> {
        self.inner.read(buf).await
    }

    /// Read some data from the stream into a registered buffer.
    ///
    /// Like [`read`], but using a pre-mapped buffer
    /// registered with [`FixedBufRegistry`].
    ///
    /// [`read`]: Self::read
    /// [`FixedBufRegistry`]: crate::buf::fixed::FixedBufRegistry
    ///
    /// # Errors
    ///
    /// In addition to errors that can be reported by `read`,
    /// this operation fails if the buffer is not registered in the
    /// current `tokio-uring` runtime.
    pub async fn read_fixed(&self, buf: Buffer) -> crate::Result<usize, Buffer> {
        self.inner.read_fixed(buf).await
    }

    /// Write some data to the stream from the buffer.
    ///
    /// Returns the original buffer and quantity of data written.
    pub fn write(&self, buf: Buffer) -> Unsubmitted {
        self.inner.write(buf)
    }

    /// Writes data into the socket from a registered buffer.
    ///
    /// Like [`write`], but using a pre-mapped buffer
    /// registered with [`FixedBufRegistry`].
    ///
    /// [`write`]: Self::write
    /// [`FixedBufRegistry`]: crate::buf::fixed::FixedBufRegistry
    ///
    /// # Errors
    ///
    /// In addition to errors that can be reported by `write`,
    /// this operation fails if the buffer is not registered in the
    /// current `tokio-uring` runtime.
    pub async fn write_fixed<T>(&self, buf: T) -> crate::Result<usize, T>
    where
        T: BoundedBuf<Buf = Buffer>,
    {
        self.inner.write_fixed(buf).await
    }

    /// Attempts to write an entire buffer to the stream.
    ///
    /// This method will continuously call [`write_fixed`] until there is no more data to be
    /// written or an error is returned. This method will not return until the entire
    /// buffer has been successfully written or an error has occurred.
    ///
    /// If the buffer contains no data, this will never call [`write_fixed`].
    ///
    /// # Errors
    ///
    /// This function will return the first error that [`write_fixed`] returns.
    ///
    /// [`write_fixed`]: Self::write_fixed
    pub async fn write_fixed_all<T>(&self, buf: T) -> crate::Result<(), T>
    where
        T: BoundedBuf<Buf = Buffer>,
    {
        self.inner.write_fixed_all(buf).await
    }

    /// Writes data from multiple buffers into this socket using the scatter/gather IO style.
    ///
    /// This function will attempt to write the entire contents of `bufs`, but
    /// the entire write may not succeed, or the write may also generate an
    /// error. The bytes will be written starting at the specified offset.
    ///
    /// # Return
    ///
    /// The method returns the operation result and the same array of buffers
    /// passed in as an argument. A return value of `0` typically means that the
    /// underlying socket is no longer able to accept bytes and will likely not
    /// be able to in the future as well, or that the buffer provided is empty.
    ///
    /// # Errors
    ///
    /// Each call to `write` may generate an I/O error indicating that the
    /// operation could not be completed. If an error is returned then no bytes
    /// in the buffer were written to this writer.
    ///
    /// It is **not** considered an error if the entire buffer could not be
    /// written to this writer.
    ///
    /// [`Ok(n)`]: Ok
    pub async fn writev(&self, buf: Buffer) -> crate::Result<usize, Buffer> {
        self.inner.write(buf).submit().await
    }

    /// Shuts down the read, write, or both halves of this connection.
    ///
    /// This function will cause all pending and future I/O on the specified portions to return
    /// immediately with an appropriate value.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.inner.shutdown(how)
    }

    /// Sets the value of the TCP_NODELAY option on this socket.
    ///
    /// If set, this option disables the Nagle algorithm. This means that segments are always sent
    /// as soon as possible, even if there is only a small amount of data. When not set, data is
    /// buffered until there is a sufficient amount to send out, thereby avoiding the frequent
    /// sending of small packets.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.inner.set_nodelay(nodelay)
    }
}

impl FromRawFd for TcpStream {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        TcpStream::from_socket(Socket::from_shared_fd(SharedFd::new(fd)))
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}
