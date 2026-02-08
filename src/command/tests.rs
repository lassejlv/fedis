use super::*;
use crate::auth::User;
use crate::persistence::{Aof, AofFsync};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_ID: AtomicU64 = AtomicU64::new(1);

async fn make_executor() -> (CommandExecutor, SessionAuth, PathBuf) {
    let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("fedis-test-{}-{}.aof", std::process::id(), id));
    let aof = Aof::open(&path, AofFsync::Always).await.expect("open aof");
    let store = Store::new(aof, None).await.expect("new store");
    let users: HashMap<String, User> = HashMap::new();
    let auth = Auth::new(users, "default".to_string());
    let executor = CommandExecutor::new(
        auth,
        store,
        Arc::new(ServerStats::new()),
        "127.0.0.1:0".to_string(),
    );
    (executor, SessionAuth::default(), path)
}

async fn run(executor: &CommandExecutor, session: &mut SessionAuth, cmd: &[&str]) -> RespValue {
    let args = cmd.iter().map(|v| v.as_bytes().to_vec()).collect();
    let (resp, _) = executor.execute(args, session).await;
    resp
}

fn expect_int(value: RespValue) -> i64 {
    if let RespValue::Integer(v) = value {
        v
    } else {
        panic!("expected integer response");
    }
}

fn expect_bulk(value: RespValue) -> Option<Vec<u8>> {
    if let RespValue::Bulk(v) = value {
        v
    } else {
        panic!("expected bulk response");
    }
}

fn expect_error(value: RespValue) -> String {
    if let RespValue::Error(v) = value {
        v
    } else {
        panic!("expected error response");
    }
}

#[tokio::test]
async fn setnx_sets_only_when_missing() {
    let (executor, mut session, path) = make_executor().await;

    assert_eq!(
        expect_int(run(&executor, &mut session, &["SETNX", "a", "1"]).await),
        1
    );
    assert_eq!(
        expect_int(run(&executor, &mut session, &["SETNX", "a", "2"]).await),
        0
    );
    assert_eq!(
        expect_bulk(run(&executor, &mut session, &["GET", "a"]).await),
        Some(b"1".to_vec())
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn getdel_returns_and_deletes_value() {
    let (executor, mut session, path) = make_executor().await;

    let _ = run(&executor, &mut session, &["SET", "a", "value"]).await;
    assert_eq!(
        expect_bulk(run(&executor, &mut session, &["GETDEL", "a"]).await),
        Some(b"value".to_vec())
    );
    assert_eq!(
        expect_bulk(run(&executor, &mut session, &["GET", "a"]).await),
        None
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn msetnx_is_all_or_nothing() {
    let (executor, mut session, path) = make_executor().await;

    let _ = run(&executor, &mut session, &["SET", "a", "old"]).await;
    assert_eq!(
        expect_int(run(&executor, &mut session, &["MSETNX", "a", "1", "b", "2"]).await),
        0
    );
    assert_eq!(
        expect_bulk(run(&executor, &mut session, &["GET", "b"]).await),
        None
    );

    let _ = run(&executor, &mut session, &["DEL", "a"]).await;
    assert_eq!(
        expect_int(run(&executor, &mut session, &["MSETNX", "a", "1", "b", "2"]).await),
        1
    );
    assert_eq!(
        expect_bulk(run(&executor, &mut session, &["GET", "a"]).await),
        Some(b"1".to_vec())
    );
    assert_eq!(
        expect_bulk(run(&executor, &mut session, &["GET", "b"]).await),
        Some(b"2".to_vec())
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn unlink_behaves_like_del() {
    let (executor, mut session, path) = make_executor().await;

    let _ = run(&executor, &mut session, &["SET", "a", "1"]).await;
    let _ = run(&executor, &mut session, &["SET", "b", "2"]).await;
    assert_eq!(
        expect_int(run(&executor, &mut session, &["UNLINK", "a", "b"]).await),
        2
    );
    assert_eq!(
        expect_int(run(&executor, &mut session, &["EXISTS", "a", "b"]).await),
        0
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn exists_counts_duplicates_like_redis() {
    let (executor, mut session, path) = make_executor().await;

    let _ = run(&executor, &mut session, &["SET", "a", "1"]).await;
    assert_eq!(
        expect_int(run(&executor, &mut session, &["EXISTS", "a", "a", "missing"]).await),
        2
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn exists_returns_arity_error_without_keys() {
    let (executor, mut session, path) = make_executor().await;

    let err = expect_error(run(&executor, &mut session, &["EXISTS"]).await);
    assert_eq!(err, "ERR wrong number of arguments for 'exists' command");

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn set_rejects_conflicting_nx_xx_options() {
    let (executor, mut session, path) = make_executor().await;

    let err = expect_error(run(&executor, &mut session, &["SET", "a", "1", "NX", "XX"]).await);
    assert_eq!(err, "ERR syntax error");

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn getrange_and_setrange_work_with_offsets() {
    let (executor, mut session, path) = make_executor().await;

    let _ = run(&executor, &mut session, &["SET", "s", "hello"]).await;
    assert_eq!(
        expect_bulk(run(&executor, &mut session, &["GETRANGE", "s", "1", "3"]).await),
        Some(b"ell".to_vec())
    );
    assert_eq!(
        expect_int(run(&executor, &mut session, &["SETRANGE", "s", "1", "ZZ"]).await),
        5
    );
    assert_eq!(
        expect_bulk(run(&executor, &mut session, &["GET", "s"]).await),
        Some(b"hZZlo".to_vec())
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn setrange_zero_fills_for_new_keys() {
    let (executor, mut session, path) = make_executor().await;

    assert_eq!(
        expect_int(run(&executor, &mut session, &["SETRANGE", "x", "3", "ab"]).await),
        5
    );
    let value = expect_bulk(run(&executor, &mut session, &["GET", "x"]).await).unwrap();
    assert_eq!(value, vec![0, 0, 0, b'a', b'b']);

    let _ = std::fs::remove_file(path);
}
