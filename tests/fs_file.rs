use std::{
    io::prelude::*,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
};

use tempfile::NamedTempFile;

use tokio_uring::fs::File;
use tokio_uring::Submit;
use tokio_uring::{
    buf::{fixed::registry, BoundedBuf, BoundedBufMut},
    Buffer,
};

#[path = "../src/future.rs"]
#[allow(warnings)]
mod future;

const HELLO: &[u8] = b"hello world...";

async fn read_hello(file: &File) {
    let buf = Buffer::new(Vec::<u8>::with_capacity(1024));
    let (n, buf) = file.read_at(buf, 0).submit().await.unwrap();
    assert_eq!(n, HELLO.len());
    assert_eq!(&buf[0][..n], HELLO);
}

#[test]
fn basic_read() {
    tokio_uring::start(async {
        let mut tempfile = tempfile();
        tempfile.write_all(HELLO).unwrap();

        let file = File::open(tempfile.path()).await.unwrap();
        read_hello(&file).await;
    });
}

#[test]
fn basic_write() {
    tokio_uring::start(async {
        let tempfile = tempfile();

        let file = File::create(tempfile.path()).await.unwrap();

        file.write_at(Buffer::new(HELLO.to_vec()), 0)
            .submit()
            .await
            .unwrap();

        let file = std::fs::read(tempfile.path()).unwrap();
        assert_eq!(file, HELLO);
    });
}

#[test]
fn vectored_read() {
    tokio_uring::start(async {
        let mut tempfile = tempfile();
        tempfile.write_all(HELLO).unwrap();

        let file = File::open(tempfile.path()).await.unwrap();
        let bufs = Buffer::new(vec![
            Vec::<u8>::with_capacity(5),
            Vec::<u8>::with_capacity(9),
        ]);
        let (n, bufs) = file.read_at(bufs, 0).submit().await.unwrap();

        assert_eq!(n, HELLO.len());
        assert_eq!(bufs[1][0], b' ');
    });
}

#[test]
fn vectored_write() {
    tokio_uring::start(async {
        let tempfile = tempfile();

        let file = File::create(tempfile.path()).await.unwrap();
        let buf1 = "hello".to_owned().into_bytes();
        let buf2 = " world...".to_owned().into_bytes();
        let bufs = Buffer::new(vec![buf1, buf2]);

        file.write_at(bufs, 0).submit().await.unwrap();

        let file = std::fs::read(tempfile.path()).unwrap();
        assert_eq!(file, HELLO);
    });
}

#[test]
fn cancel_read() {
    tokio_uring::start(async {
        let mut tempfile = tempfile();
        tempfile.write_all(HELLO).unwrap();

        let file = File::open(tempfile.path()).await.unwrap();

        // Poll the future once, then cancel it
        poll_once(async { read_hello(&file).await }).await;

        read_hello(&file).await;
    });
}

#[test]
fn explicit_close() {
    let mut tempfile = tempfile();
    tempfile.write_all(HELLO).unwrap();

    tokio_uring::start(async {
        let file = File::open(tempfile.path()).await.unwrap();
        let fd = file.as_raw_fd();

        file.close().await.unwrap();

        assert_invalid_fd(fd);
    })
}

#[test]
fn drop_open() {
    tokio_uring::start(async {
        let tempfile = tempfile();
        let _ = File::create(tempfile.path());

        // Do something else
        let file = File::create(tempfile.path()).await.unwrap();

        file.write_at(Buffer::new(HELLO.to_vec()), 0)
            .submit()
            .await
            .unwrap();

        let file = std::fs::read(tempfile.path()).unwrap();
        assert_eq!(file, HELLO);
    });
}

#[test]
fn drop_off_runtime() {
    let file = tokio_uring::start(async {
        let tempfile = tempfile();
        File::open(tempfile.path()).await.unwrap()
    });

    let fd = file.as_raw_fd();
    drop(file);

    assert_invalid_fd(fd);
}

#[test]
fn sync_doesnt_kill_anything() {
    let tempfile = tempfile();

    tokio_uring::start(async {
        let file = File::create(tempfile.path()).await.unwrap();
        file.sync_all().await.unwrap();
        file.sync_data().await.unwrap();
        file.write_at(Buffer::new("foo".to_owned().into_bytes()), 0)
            .submit()
            .await
            .unwrap();
        file.sync_all().await.unwrap();
        file.sync_data().await.unwrap();
    });
}

#[test]
fn rename() {
    use std::ffi::OsStr;
    tokio_uring::start(async {
        let mut tempfile = tempfile();
        tempfile.write_all(HELLO).unwrap();

        let old_path = tempfile.path();
        let old_file = File::open(old_path).await.unwrap();
        read_hello(&old_file).await;
        old_file.close().await.unwrap();

        let mut new_file_name = old_path
            .file_name()
            .unwrap_or_else(|| OsStr::new(""))
            .to_os_string();
        new_file_name.push("_renamed");

        let new_path = old_path.with_file_name(new_file_name);

        tokio_uring::fs::rename(&old_path, &new_path).await.unwrap();

        let new_file = File::open(&new_path).await.unwrap();
        read_hello(&new_file).await;

        let old_file = File::open(old_path).await;
        assert!(old_file.is_err());

        // Since the file has been renamed, it won't be deleted
        // in the TempPath destructor. We have to manually delete it.
        std::fs::remove_file(&new_path).unwrap();
    })
}

#[test]
fn read_fixed() {
    tokio_uring::start(async {
        let mut tempfile = tempfile();
        tempfile.write_all(HELLO).unwrap();

        let buffers =
            registry::register(vec![Vec::with_capacity(6), Vec::with_capacity(1024)].into_iter())
                .unwrap();

        let file = File::open(tempfile.path()).await.unwrap();

        let fixed_buf = buffers.check_out(0).unwrap();
        assert_eq!(fixed_buf.bytes_total(), 6);
        let (n, buf) = file.read_fixed_at(fixed_buf.slice(..), 0).await.unwrap();

        assert_eq!(n, 6);
        assert_eq!(&buf[..], &HELLO[..6]);

        let fixed_buf = buffers.check_out(1).unwrap();
        assert_eq!(fixed_buf.bytes_total(), 1024);
        let (n, buf) = file.read_fixed_at(fixed_buf.slice(..), 6).await.unwrap();

        assert_eq!(n, HELLO.len() - 6);
        assert_eq!(&buf[..], &HELLO[6..]);
    });
}

#[test]
fn write_fixed() {
    tokio_uring::start(async {
        let tempfile = tempfile();

        let file = File::create(tempfile.path()).await.unwrap();

        let buffers =
            registry::register(vec![Vec::with_capacity(6), Vec::with_capacity(1024)].into_iter())
                .unwrap();

        let fixed_buf = buffers.check_out(0).unwrap();
        let mut buf = fixed_buf;
        buf.put_slice(&HELLO[..6]);

        let (n, _) = file.write_fixed_at(buf, 0).await.unwrap();
        assert_eq!(n, 6);

        let fixed_buf = buffers.check_out(1).unwrap();
        let mut buf = fixed_buf;
        buf.put_slice(&HELLO[6..]);

        let (n, _) = file.write_fixed_at(buf, 6).await.unwrap();
        assert_eq!(n, HELLO.len() - 6);

        let file = std::fs::read(tempfile.path()).unwrap();
        assert_eq!(file, HELLO);
    });
}

#[test]
fn basic_fallocate() {
    tokio_uring::start(async {
        let tempfile = tempfile();

        let file = File::create(tempfile.path()).await.unwrap();

        file.fallocate(0, 1024, libc::FALLOC_FL_ZERO_RANGE)
            .await
            .unwrap();
        file.sync_all().await.unwrap();

        let statx = file.statx().await.unwrap();
        let size = statx.stx_size;
        assert_eq!(size, 1024);

        // using the FALLOC_FL_KEEP_SIZE flag causes the file metadata to reflect the previous size
        file.fallocate(
            0,
            2048,
            libc::FALLOC_FL_ZERO_RANGE | libc::FALLOC_FL_KEEP_SIZE,
        )
        .await
        .unwrap();
        file.sync_all().await.unwrap();

        let statx = file.statx().await.unwrap();
        let size = statx.stx_size;
        assert_eq!(size, 1024);
    });
}

#[test]
fn write_linked() {
    tokio_uring::start(async {
        let tempfile = tempfile();
        let file = File::create(tempfile.path()).await.unwrap();

        let write1 = file.write_at(Buffer::new(HELLO.to_vec()), 0);
        let write2 = file.write_at(Buffer::new(HELLO.to_vec()), HELLO.len() as u64);

        let future1 = write1.link(write2).submit();

        let (res1, future2) = future1.await;
        let res2 = future2.await;

        res1.unwrap();
        res2.unwrap();

        let file = std::fs::read(tempfile.path()).unwrap();
        assert_eq!(file, [HELLO, HELLO].concat());
    });
}

fn tempfile() -> NamedTempFile {
    NamedTempFile::new().unwrap()
}

async fn poll_once(future: impl std::future::Future) {
    use std::future::poll_fn;
    // use std::future::Future;
    use std::task::Poll;
    use tokio::pin;

    pin!(future);

    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

fn assert_invalid_fd(fd: RawFd) {
    use std::fs::File;

    let mut f = unsafe { File::from_raw_fd(fd) };
    let mut buf = vec![];

    match f.read_to_end(&mut buf) {
        Err(ref e) if e.raw_os_error() == Some(libc::EBADF) => {}
        res => panic!("assert_invalid_fd finds for fd {:?}, res = {:?}", fd, res),
    }
}
