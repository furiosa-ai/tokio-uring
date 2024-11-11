use tempfile::NamedTempFile;

use tokio_uring::{fs::File, Buffer, Submit};

#[path = "../src/future.rs"]
#[allow(warnings)]
mod future;

#[test]
fn too_many_submissions() {
    let tempfile = tempfile();

    tokio_uring::start(async {
        let file = File::create(tempfile.path()).await.unwrap();
        for _ in 0..600 {
            poll_once(async {
                file.write_at(Buffer::new(b"hello world".to_vec()), 0)
                    .submit()
                    .await
                    .unwrap();
            })
            .await;
        }
    });
}

#[test]
fn completion_overflow() {
    use std::process;
    use std::{thread, time};
    use tokio::task::JoinSet;

    let spawn_cnt = 50;
    let squeue_entries = 2;
    let cqueue_entries = 2 * squeue_entries;

    std::thread::spawn(|| {
        thread::sleep(time::Duration::from_secs(8)); // 1000 times longer than it takes on a slow machine
        eprintln!("Timeout reached. The uring completions are hung.");
        process::exit(1);
    });

    tokio_uring::builder()
        .entries(squeue_entries)
        .uring_builder(tokio_uring::uring_builder().setup_cqsize(cqueue_entries))
        .start(async move {
            let mut js = JoinSet::new();

            for _ in 0..spawn_cnt {
                js.spawn_local(tokio_uring::no_op());
            }

            while let Some(res) = js.join_next().await {
                res.unwrap().unwrap();
            }
        });
}

fn tempfile() -> NamedTempFile {
    NamedTempFile::new().unwrap()
}

async fn poll_once(future: impl std::future::Future) {
    // use std::future::Future;
    use std::task::Poll;
    use tokio::pin;

    pin!(future);

    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
