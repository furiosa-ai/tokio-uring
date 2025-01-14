use tokio_uring::buf::fixed::{pool, registry};
use tokio_uring::buf::{BoundedBuf, BoundedBufMut};
use tokio_uring::fs::File;
use tokio_uring::Buffer;

use std::fs::File as StdFile;
use std::io::prelude::*;
use std::iter;
use std::mem;
use tempfile::NamedTempFile;

const HELLO: &[u8] = b"hello world...";

#[test]
fn fixed_buf_turnaround() {
    tokio_uring::start(async {
        let mut tempfile = tempfile();
        tempfile.write_all(HELLO).unwrap();

        let file = File::open(tempfile.path()).await.unwrap();

        let buffers = registry::register(
            [30, 20, 10]
                .iter()
                .map(|&n| Vec::<u8>::with_capacity(n).into()),
        )
        .unwrap();

        let fixed_buf = buffers.check_out(0).unwrap();
        assert_eq!(fixed_buf.bytes_total(), 30);

        // Can't check out the same buffer twice.
        assert!(buffers.check_out(0).is_none());

        // Checking out another buffer from the same registry is possible,
        // but does not affect the status of the first buffer.
        let fixed_buf1 = buffers.check_out(1).unwrap();
        assert_eq!(fixed_buf1.bytes_total(), 20);
        assert!(buffers.check_out(0).is_none());
        mem::drop(fixed_buf1);
        assert!(buffers.check_out(0).is_none());

        let op = file.read_fixed_at(fixed_buf, 0);

        // The buffer is used by the pending operation, can't check it out
        // for another instance.
        assert!(buffers.check_out(0).is_none());

        let (n, buf) = op.await.unwrap();
        assert_eq!(n, HELLO.len());

        // The buffer is owned by `buf`, can't check it out
        // for another instance.
        assert!(buffers.check_out(0).is_none());

        mem::drop(buf);

        // The buffer has been released, check it out again.
        let fixed_buf = buffers.check_out(0).unwrap();
        assert_eq!(fixed_buf.bytes_total(), 30);
        assert_eq!(fixed_buf.bytes_init(), HELLO.len());
    });
}

#[test]
fn unregister_invalidates_checked_out_buffers() {
    tokio_uring::start(async {
        let mut tempfile = tempfile();
        tempfile.write_all(HELLO).unwrap();

        let file = File::open(tempfile.path()).await.unwrap();

        let buffers =
            registry::register(vec![Vec::<u8>::with_capacity(1024).into()].into_iter()).unwrap();

        let fixed_buf = buffers.check_out(0).unwrap();

        // The checked out handle keeps the buffer allocation alive.
        // Meanwhile, we replace buffer registration in the kernel:
        registry::unregister().unwrap();
        let buffers =
            registry::register(vec![Vec::<u8>::with_capacity(1024).into()].into_iter()).unwrap();

        // The old buffer's index no longer matches the memory area of the
        // currently registered buffer, so the read operation using the old
        // buffer's memory should fail.
        let res = file.read_fixed_at(fixed_buf, 0).await;
        assert!(res.is_err());

        let fixed_buf = buffers.check_out(0).unwrap();
        let (n, buf) = file.read_fixed_at(fixed_buf, 0).await.unwrap();
        assert_eq!(n, HELLO.len());
        assert_eq!(&buf[0][..], HELLO);
    });
}

#[test]
fn slicing() {
    tokio_uring::start(async {
        let mut tempfile = tempfile();
        tempfile.write_all(HELLO).unwrap();

        let file = File::from_std(
            StdFile::options()
                .read(true)
                .write(true)
                .open(tempfile.path())
                .unwrap(),
        );

        let buffers =
            registry::register(vec![Vec::<u8>::with_capacity(1024).into()].into_iter()).unwrap();

        let fixed_buf = buffers.check_out(0).unwrap();

        // Read no more than 8 bytes into the fixed buffer.
        let (n, slice) = file.read_fixed_at(fixed_buf.slice(..8), 3).await.unwrap();
        assert_eq!(n, 8);
        assert_eq!(slice[..], HELLO[3..11]);
        let fixed_buf = slice.into_inner();

        // Write from the fixed buffer, starting at offset 1,
        // up to the end of the initialized bytes in the buffer.
        let (n, slice) = file
            .write_fixed_at(fixed_buf.slice(1..), HELLO.len() as u64)
            .await
            .unwrap();
        assert_eq!(n, 7);
        assert_eq!(slice[..], HELLO[4..11]);
        let fixed_buf = slice.into_inner();

        // Read into the fixed buffer, overwriting bytes starting from offset 3
        // and then extending the initialized part with as many bytes as
        // the operation can read.
        let (n, slice) = file.read_fixed_at(fixed_buf.slice(3..), 0).await.unwrap();
        assert_eq!(n, HELLO.len() + 7);
        assert_eq!(slice[..HELLO.len()], HELLO[..]);
        assert_eq!(slice[HELLO.len()..], HELLO[4..11]);
    })
}

#[test]
fn pool_next_as_concurrency_limit() {
    tokio_uring::start(async move {
        const BUF_SIZE: usize = 80;

        let mut tempfile = tempfile();
        let file = StdFile::options()
            .write(true)
            .open(tempfile.path())
            .unwrap();

        let buffers = pool::register(
            iter::repeat_with(|| Vec::<u8>::with_capacity(BUF_SIZE))
                .take(2)
                .map(Buffer::from),
        )
        .unwrap();

        let mut join_handles = vec![];
        for i in 0..10 {
            let mut buf = buffers.next(BUF_SIZE).await;
            let cloned_file = file.try_clone().unwrap();

            let handle = tokio_uring::spawn(async move {
                let file = File::from_std(cloned_file);
                let data = [b'0' + i as u8; BUF_SIZE];
                buf.put_slice(&data);
                let (_, _buf) = file
                    .write_fixed_all_at(buf, BUF_SIZE as u64 * i)
                    .await
                    .unwrap();
            });

            join_handles.push(handle);
        }
        for (i, handle) in join_handles.into_iter().enumerate() {
            handle
                .await
                .unwrap_or_else(|e| panic!("worker {} terminated abnormally: {}", i, e));
        }

        mem::drop(file);
        let mut content = String::new();
        tempfile.read_to_string(&mut content).unwrap();
        println!("{}", content);
    })
}

fn tempfile() -> NamedTempFile {
    NamedTempFile::new().unwrap()
}
