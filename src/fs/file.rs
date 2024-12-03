use crate::buf::{BoundedBuf, BoundedBufMut, Buffer, Slice};
use crate::fs::OpenOptions;
use crate::io::SharedFd;

use crate::runtime::driver::op::Op;
use crate::MapResult;
use crate::Unsubmitted;
use std::fmt;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::path::Path;

/// A reference to an open file on the filesystem.
///
/// An instance of a `File` can be read and/or written depending on what options
/// it was opened with. The `File` type provides **positional** read and write
/// operations. The file does not maintain an internal cursor. The caller is
/// required to specify an offset when issuing an operation.
///
/// While files are automatically closed when they go out of scope, the
/// operation happens asynchronously in the background. It is recommended to
/// call the `close()` function in order to guarantee that the file successfully
/// closed before exiting the scope. Closing a file does not guarantee writes
/// have persisted to disk. Use [`sync_all`] to ensure all writes have reached
/// the filesystem.
///
/// [`sync_all`]: File::sync_all
///
/// # Examples
///
/// Creates a new file and write data to it:
///
/// ```no_run
/// use tokio_uring::fs::File;
/// use tokio_uring::Submit;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     tokio_uring::start(async {
///         // Open a file
///         let file = File::create("hello.txt").await?;
///
///         // Write some data
///         let (n, buf) = file.write_at(b"hello world".to_vec().into(), 0).submit().await?;
///
///         println!("wrote {} bytes", n);
///
///         // Sync data to the file system.
///         file.sync_all().await?;
///
///         // Close the file
///         file.close().await?;
///
///         Ok(())
///     })
/// }
/// ```
pub struct File {
    /// Open file descriptor
    pub(crate) fd: SharedFd,
}

impl File {
    /// Attempts to open a file in read-only mode.
    ///
    /// See the [`OpenOptions::open`] method for more details.
    ///
    /// # Errors
    ///
    /// This function will return an error if `path` does not already exist.
    /// Other errors may also be returned according to [`OpenOptions::open`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let f = File::open("foo.txt").await?;
    ///
    ///         // Close the file
    ///         f.close().await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn open(path: impl AsRef<Path>) -> io::Result<File> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Opens a file in write-only mode.
    ///
    /// This function will create a file if it does not exist,
    /// and will truncate it if it does.
    ///
    /// See the [`OpenOptions::open`] function for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let f = File::create("foo.txt").await?;
    ///
    ///         // Close the file
    ///         f.close().await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn create(path: impl AsRef<Path>) -> io::Result<File> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    pub(crate) fn from_shared_fd(fd: SharedFd) -> File {
        File { fd }
    }

    /// Converts a [`std::fs::File`][std] to a [`tokio_uring::fs::File`][file].
    ///
    /// [std]: std::fs::File
    /// [file]: File
    pub fn from_std(file: std::fs::File) -> File {
        File::from_shared_fd(SharedFd::new(file.into_raw_fd()))
    }

    /// Read some bytes at the specified offset from the file into the specified
    /// buffer, returning how many bytes were read.
    ///
    /// # Return
    ///
    /// The method returns the operation result and the same buffer value passed
    /// as an argument.
    ///
    /// If the method returns [`Ok(n)`], then the read was successful. A nonzero
    /// `n` value indicates that the buffer has been filled with `n` bytes of
    /// data from the file. If `n` is `0`, then one of the following happened:
    ///
    /// 1. The specified offset is the end of the file.
    /// 2. The buffer specified was 0 bytes in length.
    ///
    /// It is not an error if the returned value `n` is smaller than the buffer
    /// size, even when the file contains enough data to fill the buffer.
    ///
    /// # Errors
    ///
    /// If this function encounters any form of I/O or other error, an error
    /// variant will be returned. The buffer is returned on error.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    /// use tokio_uring::Submit;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let f = File::open("foo.txt").await?;
    ///         let buffer = vec![0; 10].into();
    ///
    ///         // Read up to 10 bytes
    ///         let (n, buffer) = f.read_at(buffer, 0).submit().await?;
    ///
    ///         println!("The bytes: {:?}", &buffer[0][..n]);
    ///
    ///         // Close the file
    ///         f.close().await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub fn read_at(&self, buf: Buffer, pos: u64) -> Unsubmitted {
        Unsubmitted::read_at(&self.fd, buf, pos)
    }

    /// Read some bytes at the specified offset from the file into the specified
    /// array of buffers, returning how many bytes were read.
    ///
    /// # Return
    ///
    /// The method returns the operation result and the same array of buffers
    /// passed as an argument.
    ///
    /// If the method returns [`Ok(n)`], then the read was successful. A nonzero
    /// `n` value indicates that the buffers have been filled with `n` bytes of
    /// data from the file. If `n` is `0`, then one of the following happened:
    ///
    /// 1. The specified offset is the end of the file.
    /// 2. The buffers specified were 0 bytes in length.
    ///
    /// It is not an error if the returned value `n` is smaller than the buffer
    /// size, even when the file contains enough data to fill the buffer.
    ///
    /// # Errors
    ///
    /// If this function encounters any form of I/O or other error, an error
    /// variant will be returned. The buffer is returned on error.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    /// use tokio_uring::Submit;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let f = File::open("foo.txt").await?;
    ///         let buffers = vec![Vec::<u8>::with_capacity(10), Vec::<u8>::with_capacity(10)].into();
    ///
    ///         // Read up to 20 bytes
    ///         let (n, _) = f.read_at(buffers, 0).submit().await?;
    ///
    ///         println!("Read {} bytes", n);
    ///
    ///         // Close the file
    ///         f.close().await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    // pub fn readv_at<T: BoundedBufMut>(&self, bufs: Vec<T>, pos: u64) -> UnsubmittedReadv<T> {
    // UnsubmittedOneshot::readv_at(&self.fd, bufs, pos)
    // }

    /// Write data from buffers into this file at the specified offset,
    /// returning how many bytes were written.
    ///
    /// This function will attempt to write the entire contents of `bufs`, but
    /// the entire write may not succeed, or the write may also generate an
    /// error. The bytes will be written starting at the specified offset.
    ///
    /// # Return
    ///
    /// The method returns the operation result and the same array of buffers passed
    /// in as an argument. A return value of `0` typically means that the
    /// underlying file is no longer able to accept bytes and will likely not be
    /// able to in the future as well, or that the buffer provided is empty.
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
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    /// use tokio_uring::Submit;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let file = File::create("foo.txt").await?;
    ///
    ///         // Writes some prefix of the byte string, not necessarily all of it.
    ///         let bufs = vec!["some".to_owned().into_bytes(), " bytes".to_owned().into_bytes()].into();
    ///         let (n, _) = file.write_at(bufs, 0).submit().await?;
    ///
    ///         println!("wrote {} bytes", n);
    ///
    ///         // Close the file
    ///         file.close().await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    ///
    /// [`Ok(n)`]: Ok
    // pub fn writev_at(&self, bufs: Vec<T>, pos: u64) -> UnsubmittedWritev<T> {
    // UnsubmittedOneshot::writev_at(&self.fd, bufs, pos)
    // }

    /// Like [`read_at`], but using a pre-mapped buffer
    /// registered with [`FixedBufRegistry`].
    ///
    /// [`read_at`]: Self::read_at
    /// [`FixedBufRegistry`]: crate::buf::fixed::FixedBufRegistry
    ///
    /// # Errors
    ///
    /// In addition to errors that can be reported by `read_at`,
    /// this operation fails if the buffer is not registered in the
    /// current `tokio-uring` runtime.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///# fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use tokio_uring::fs::File;
    /// use tokio_uring::buf::fixed::registry;
    /// use tokio_uring::buf::BoundedBuf;
    /// use tokio_uring::Buffer;
    /// use std::iter;
    ///
    /// tokio_uring::start(async {
    ///     let registry = registry::register(iter::repeat(vec![0u8; 10]).take(10).map(Buffer::from))?;
    ///
    ///     let f = File::open("foo.txt").await?;
    ///     let buffer = registry.check_out(2).unwrap();
    ///
    ///     // Read up to 10 bytes
    ///     let (n, buffer) = f.read_fixed_at(buffer, 0).await?;
    ///
    ///     println!("The bytes: {:?}", &buffer[0][..n]);
    ///
    ///     // Close the file
    ///     f.close().await?;
    ///     Ok(())
    /// })
    ///# }
    /// ```
    pub async fn read_fixed_at<T>(&self, buf: T, pos: u64) -> crate::Result<usize, T>
    where
        T: BoundedBufMut<BufMut = Buffer>,
    {
        // Submit the read operation
        let op = Op::read_fixed_at(&self.fd, buf, pos).unwrap();
        op.await
    }

    /// Write a buffer into this file at the specified offset, returning how
    /// many bytes were written.
    ///
    /// This function will attempt to write the entire contents of `buf`, but
    /// the entire write may not succeed, or the write may also generate an
    /// error. The bytes will be written starting at the specified offset.
    ///
    /// # Return
    ///
    /// The method returns the operation result and the same buffer value passed
    /// in as an argument. A return value of `0` typically means that the
    /// underlying file is no longer able to accept bytes and will likely not be
    /// able to in the future as well, or that the buffer provided is empty.
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
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    /// use tokio_uring::Submit;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let file = File::create("foo.txt").await?;
    ///
    ///         // Writes some prefix of the byte string, not necessarily all of it.
    ///         let (n, _) = file.write_at(b"some bytes".to_vec().into(), 0).submit().await?;
    ///
    ///         println!("wrote {} bytes", n);
    ///
    ///         // Close the file
    ///         file.close().await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    ///
    /// [`Ok(n)`]: Ok
    pub fn write_at(&self, buf: Buffer, pos: u64) -> Unsubmitted {
        Unsubmitted::write_at(&self.fd, buf, pos)
    }

    /// Like [`write_at`], but using a pre-mapped buffer
    /// registered with [`FixedBufRegistry`].
    ///
    /// [`write_at`]: Self::write_at
    /// [`FixedBufRegistry`]: crate::buf::fixed::FixedBufRegistry
    ///
    /// # Errors
    ///
    /// In addition to errors that can be reported by `write_at`,
    /// this operation fails if the buffer is not registered in the
    /// current `tokio-uring` runtime.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///# fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use tokio_uring::fs::File;
    /// use tokio_uring::buf::fixed::registry;
    /// use tokio_uring::buf::BoundedBuf;
    /// use tokio_uring::Buffer;
    ///
    /// tokio_uring::start(async {
    ///     let registry = registry::register(vec![b"some bytes".to_vec()].into_iter().map(Buffer::from))?;
    ///
    ///     let file = File::create("foo.txt").await?;
    ///
    ///     let buffer = registry.check_out(0).unwrap();
    ///
    ///     // Writes some prefix of the buffer content,
    ///     // not necessarily all of it.
    ///     let (n, _) = file.write_fixed_at(buffer, 0).await?;
    ///
    ///     println!("wrote {} bytes", n);
    ///
    ///     // Close the file
    ///     file.close().await?;
    ///     Ok(())
    /// })
    ///# }
    /// ```
    pub async fn write_fixed_at<T>(&self, buf: T, pos: u64) -> crate::Result<usize, T>
    where
        T: BoundedBuf<Buf = Buffer>,
    {
        let op = Op::write_fixed_at(&self.fd, buf, pos).unwrap();
        op.await
    }

    /// Attempts to write an entire buffer into this file at the specified offset.
    ///
    /// This method will continuously call [`write_fixed_at`] until there is no more data
    /// to be written or an error is returned.
    /// This method will not return until the entire buffer has been successfully
    /// written or an error occurs.
    ///
    /// If the buffer contains no data, this will never call [`write_fixed_at`].
    ///
    /// # Return
    ///
    /// The method returns the operation result and the same buffer value passed
    /// in as an argument.
    ///
    /// # Errors
    ///
    /// This function will return the first error that [`write_fixed_at`] returns.
    ///
    /// [`write_fixed_at`]: Self::write_fixed_at
    pub async fn write_fixed_all_at<T>(&self, buf: T, pos: u64) -> crate::Result<(), T>
    where
        T: BoundedBuf<Buf = Buffer>,
    {
        let orig_bounds = buf.bounds();
        self.write_fixed_all_at_slice(buf.slice_full(), pos)
            .await
            .map_buf(|buf| T::from_buf_bounds(buf, orig_bounds))
    }

    async fn write_fixed_all_at_slice(
        &self,
        mut buf: Slice<Buffer>,
        mut pos: u64,
    ) -> crate::Result<(), Buffer> {
        if pos.checked_add(buf.bytes_init() as u64).is_none() {
            return Err(crate::Error(
                io::Error::new(io::ErrorKind::InvalidInput, "buffer too large for file"),
                buf.into_inner(),
            ));
        }

        while buf.bytes_init() != 0 {
            match self.write_fixed_at(buf, pos).await {
                Ok((0, slice)) => {
                    return Err(crate::Error(
                        io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer"),
                        slice.into_inner(),
                    ))
                }
                Ok((n, slice)) => {
                    pos += n as u64;
                    buf = slice.slice(n..);
                }

                // No match on an EINTR error is performed because this
                // crate's design ensures we are not calling the 'wait' option
                // in the ENTER syscall. Only an Enter with 'wait' can generate
                // an EINTR according to the io_uring man pages.
                Err(e) => return Err(e.map(|slice| slice.into_inner())),
            };
        }

        Ok(((), buf.into_inner()))
    }

    /// Attempts to sync all OS-internal metadata to disk.
    ///
    /// This function will attempt to ensure that all in-memory data reaches the
    /// filesystem before completing.
    ///
    /// This can be used to handle errors that would otherwise only be caught
    /// when the `File` is closed.  Dropping a file will ignore errors in
    /// synchronizing this in-memory data.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    /// use tokio_uring::Submit;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let f = File::create("foo.txt").await?;
    ///         f.write_at(b"Hello, world!".to_vec().into(), 0).submit().await?;
    ///
    ///         f.sync_all().await?;
    ///
    ///         // Close the file
    ///         f.close().await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn sync_all(&self) -> io::Result<()> {
        Op::fsync(&self.fd)?.await
    }

    /// Attempts to sync file data to disk.
    ///
    /// This method is similar to [`sync_all`], except that it may not
    /// synchronize file metadata to the filesystem.
    ///
    /// This is intended for use cases that must synchronize content, but don't
    /// need the metadata on disk. The goal of this method is to reduce disk
    /// operations.
    ///
    /// Note that some platforms may simply implement this in terms of
    /// [`sync_all`].
    ///
    /// [`sync_all`]: File::sync_all
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    /// use tokio_uring::Submit;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let f = File::create("foo.txt").await?;
    ///         f.write_at(b"Hello, world!".to_vec().into(), 0).submit().await?;
    ///
    ///         f.sync_data().await?;
    ///
    ///         // Close the file
    ///         f.close().await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn sync_data(&self) -> io::Result<()> {
        Op::datasync(&self.fd)?.await
    }

    /// Manipulate the allocated disk space of the file.
    ///
    /// The manipulated range starts at the `offset` and continues for `len` bytes.
    ///
    /// The specific manipulation to the allocated disk space are specified by
    /// the `flags`, to understand what are the possible values here check
    /// the `fallocate(2)` man page.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let f = File::create("foo.txt").await?;
    ///
    ///         // Allocate a 1024 byte file setting all the bytes to zero
    ///         f.fallocate(0, 1024, libc::FALLOC_FL_ZERO_RANGE).await?;
    ///
    ///         // Close the file
    ///         f.close().await?;
    ///         Ok(())
    ///     })
    /// }
    pub async fn fallocate(&self, offset: u64, len: u64, flags: i32) -> io::Result<()> {
        Op::fallocate(&self.fd, offset, len, flags)?.await
    }

    /// Closes the file using the uring asynchronous close operation and returns the possible error
    /// as described in the close(2) man page.
    ///
    /// The programmer has the choice of calling this asynchronous close and waiting for the result
    /// or letting the library close the file automatically and simply letting the file go out of
    /// scope and having the library close the file descriptor automatically and synchronously.
    ///
    /// Calling this asynchronous close is to be preferred because it returns the close result
    /// which as the man page points out, should not be ignored. This asynchronous close also
    /// avoids the synchronous close system call and may result in better throughput as the thread
    /// is not blocked during the close.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio_uring::fs::File;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         // Open the file
    ///         let f = File::open("foo.txt").await?;
    ///         // Close the file
    ///         f.close().await?;
    ///
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub async fn close(mut self) -> io::Result<()> {
        self.fd.close().await
    }
}

impl FromRawFd for File {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        File::from_shared_fd(SharedFd::new(fd))
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.raw_fd()
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("fd", &self.fd.raw_fd())
            .finish()
    }
}

/// Removes a File
///
/// This function will return an error in the following situations, but is not
/// limited to just these cases:
///
/// * `path` doesn't exist.
///      * [`io::ErrorKind`] would be set to `NotFound`
/// * The user lacks permissions to modify/remove the file at the provided `path`.
///      * [`io::ErrorKind`] would be set to `PermissionDenied`
///
/// # Examples
///
/// ```no_run
/// use tokio_uring::fs::remove_file;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     tokio_uring::start(async {
///         remove_file("/some/file.txt").await?;
///         Ok::<(), std::io::Error>(())
///     })?;
///     Ok(())
/// }
/// ```
pub async fn remove_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    Op::unlink_file(path.as_ref())?.await
}

/// Renames a file or directory to a new name, replacing the original file if
/// `to` already exists.
///
/// #Errors
///
/// * `path` doesn't exist.
///      * [`io::ErrorKind`] would be set to `NotFound`
/// * The user lacks permissions to modify/remove the file at the provided `path`.
///      * [`io::ErrorKind`] would be set to `PermissionDenied`
/// * The new name/path is on a different mount point.
///      * [`io::ErrorKind`] would be set to `CrossesDevices`
///
/// # Example
///
/// ```no_run
/// use tokio_uring::fs::rename;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     tokio_uring::start(async {
///         rename("a.txt", "b.txt").await?; // Rename a.txt to b.txt
///         Ok::<(), std::io::Error>(())
///     })?;
///     Ok(())
/// }
/// ```
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    Op::rename_at(from.as_ref(), to.as_ref(), 0)?.await
}
