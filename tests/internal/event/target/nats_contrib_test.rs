use std::io::{self, Read};
use std::net::TcpListener;
use std::thread;

use minio_rust::internal::event::target::{Host, NatsArgs};

pub const SOURCE_FILE: &str = "internal/event/target/nats_contrib_test.go";

fn spawn_plain_server() -> (u16, thread::JoinHandle<io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("plain server should bind");
    let port = listener
        .local_addr()
        .expect("listener should have addr")
        .port();
    let handle = thread::spawn(move || -> io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(200)))?;
        let mut buf = [0_u8; 64];
        let _ = stream.read(&mut buf);
        Ok(())
    });
    (port, handle)
}

fn test_nkey_path() -> String {
    format!(
        "{}/tests/fixtures/nats/test.nkey",
        env!("CARGO_MANIFEST_DIR")
    )
}

#[test]
fn nats_conn_plain_matches_reference_case() {
    let (port, handle) = spawn_plain_server();
    let connection = NatsArgs {
        enable: true,
        address: Host::new("localhost", port),
        subject: "test".to_owned(),
        ..Default::default()
    }
    .connect_nats()
    .expect("plain nats connection should succeed");
    connection.close();
    handle
        .join()
        .expect("server thread should join")
        .expect("server should accept client");
}

#[test]
fn nats_conn_user_pass_matches_reference_case() {
    let (port, handle) = spawn_plain_server();
    let connection = NatsArgs {
        enable: true,
        address: Host::new("localhost", port),
        subject: "test".to_owned(),
        username: "testminio".to_owned(),
        password: "miniotest".to_owned(),
        ..Default::default()
    }
    .connect_nats()
    .expect("user/pass nats connection should succeed");
    connection.close();
    handle
        .join()
        .expect("server thread should join")
        .expect("server should accept client");
}

#[test]
fn nats_conn_token_matches_reference_case() {
    let (port, handle) = spawn_plain_server();
    let connection = NatsArgs {
        enable: true,
        address: Host::new("localhost", port),
        subject: "test".to_owned(),
        token: "s3cr3t".to_owned(),
        ..Default::default()
    }
    .connect_nats()
    .expect("token nats connection should succeed");
    connection.close();
    handle
        .join()
        .expect("server thread should join")
        .expect("server should accept client");
}

#[test]
fn nats_conn_nkey_seed_matches_reference_case() {
    let (port, handle) = spawn_plain_server();
    let connection = NatsArgs {
        enable: true,
        address: Host::new("localhost", port),
        subject: "test".to_owned(),
        nkey_seed: test_nkey_path(),
        ..Default::default()
    }
    .connect_nats()
    .expect("nkey-seed nats connection should succeed");
    connection.close();
    handle
        .join()
        .expect("server thread should join")
        .expect("server should accept client");
}
