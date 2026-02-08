use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct RunningServer {
    child: Child,
    port: u16,
    data_dir: PathBuf,
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn start_server(extra_env: &[(&str, &str)]) -> RunningServer {
    let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe listener");
    let port = probe.local_addr().expect("probe local addr").port();
    drop(probe);

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!("fedis-it-{}-{}", std::process::id(), stamp));
    std::fs::create_dir_all(&data_dir).expect("create temp data dir");

    let mut cmd = Command::new("cargo");
    cmd.current_dir(env!("CARGO_MANIFEST_DIR"));
    cmd.arg("run").arg("--quiet");
    cmd.env("FEDIS_HOST", "127.0.0.1")
        .env("FEDIS_PORT", port.to_string())
        .env("FEDIS_DATA_PATH", &data_dir)
        .env("FEDIS_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    for (k, v) in extra_env {
        cmd.env(k, v);
    }

    let child = cmd.spawn().expect("spawn fedis server");

    for _ in 0..120 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return RunningServer {
                child,
                port,
                data_dir,
            };
        }
        thread::sleep(Duration::from_millis(50));
    }

    panic!("server did not become ready");
}

fn ping_frame() -> &'static [u8] {
    b"*1\r\n$4\r\nPING\r\n"
}

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .expect("lock tests")
}

#[test]
fn max_connections_limit_rejects_extra_clients() {
    let _lock = test_lock();
    let server = start_server(&[
        ("FEDIS_MAX_CONNECTIONS", "1"),
        ("FEDIS_IDLE_TIMEOUT_SEC", "30"),
    ]);

    let first_deadline = std::time::Instant::now() + Duration::from_secs(5);
    let _first = loop {
        let mut candidate =
            TcpStream::connect(("127.0.0.1", server.port)).expect("first connection");
        candidate
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set first read timeout");
        candidate
            .write_all(ping_frame())
            .expect("write ping on first");
        let mut first_buf = [0_u8; 64];
        let first_n = candidate.read(&mut first_buf).expect("read first response");
        assert!(first_n > 0, "expected response on first connection");
        let first_response = String::from_utf8_lossy(&first_buf[..first_n]);
        if first_response.contains("PONG") {
            break candidate;
        }
        if first_response.contains("max number of clients reached") {
            if std::time::Instant::now() >= first_deadline {
                panic!("timed out waiting for first accepted connection");
            }
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        panic!("unexpected first response: {first_response}");
    };

    let mut second = TcpStream::connect(("127.0.0.1", server.port)).expect("second connection");
    second
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("set read timeout");

    let mut buf = [0_u8; 128];
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let n = loop {
        match second.read(&mut buf) {
            Ok(n) => break n,
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                if std::time::Instant::now() >= deadline {
                    panic!("timed out waiting for rejection message");
                }
            }
            Err(e) => panic!("read reject message: {e}"),
        }
    };
    assert!(n > 0, "expected rejection message for second client");
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("max number of clients reached"),
        "unexpected response: {response}"
    );
}

#[test]
fn idle_timeout_closes_inactive_connection() {
    let _lock = test_lock();
    let server = start_server(&[("FEDIS_IDLE_TIMEOUT_SEC", "1")]);

    let mut client = TcpStream::connect(("127.0.0.1", server.port)).expect("connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    thread::sleep(Duration::from_secs(2));

    match client.write_all(ping_frame()) {
        Err(_) => return,
        Ok(()) => {}
    }

    let mut buf = [0_u8; 64];
    match client.read(&mut buf) {
        Ok(0) => {}
        Ok(n) => panic!(
            "expected closed connection after idle timeout, got: {}",
            String::from_utf8_lossy(&buf[..n])
        ),
        Err(_) => {}
    }
}
