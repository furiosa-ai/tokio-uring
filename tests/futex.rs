use libc::FUTEX_BITSET_MATCH_ANY;
use tokio::task::spawn_local;
use tokio_uring::Submit;

const FUTEX2_SIZE_U32: u32 = 0x2;

#[test]
fn mutex_test() {
    tokio_uring::start(async {
        let f = Box::new(0u32);
        let ptr: *const u32 = &*f;

        let ptr_clone = ptr.clone();
        let mask = FUTEX_BITSET_MATCH_ANY as u32 as u64;
        let handle = spawn_local(async move {
            tokio_uring::fs::Futex::wait(ptr_clone, 0, mask, FUTEX2_SIZE_U32)
                .submit()
                .await
                .unwrap();
        });
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let res = tokio_uring::fs::Futex::wake(ptr, 1, mask, FUTEX2_SIZE_U32)
            .submit()
            .await
            .unwrap();
        assert!(res == 1);

        handle.await.unwrap();
    });
}
