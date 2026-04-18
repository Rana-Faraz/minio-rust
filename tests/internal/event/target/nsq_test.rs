use minio_rust::internal::event::target::{Host, NsqArgs, NsqConnection};
use rustls::pki_types::PrivateKeyDer;
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

pub const SOURCE_FILE: &str = "internal/event/target/nsq_test.go";

fn fixture(path: &str) -> String {
    format!("{}/tests/fixtures/nats/{path}", env!("CARGO_MANIFEST_DIR"))
}

fn ensure_rustls_provider() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_certificates(path: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    let file = File::open(path).expect("certificate file should open");
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .expect("certificates should parse")
}

fn load_private_key(path: &str) -> PrivateKeyDer<'static> {
    let file = File::open(path).expect("private key file should open");
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .expect("private key should parse")
        .expect("private key should exist")
}

fn spawn_tls_nsq_server() -> (u16, thread::JoinHandle<io::Result<Vec<u8>>>) {
    ensure_rustls_provider();
    let listener = TcpListener::bind("127.0.0.1:0").expect("tls listener");
    let port = listener.local_addr().expect("listener addr").port();
    let cert = fixture("certs/nats_server_cert.pem");
    let key = fixture("certs/nats_server_key.pem");

    let join = thread::spawn(move || -> io::Result<Vec<u8>> {
        let (stream, _) = listener.accept()?;
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(load_certificates(&cert), load_private_key(&key))
            .expect("server config");
        let connection = ServerConnection::new(Arc::new(config))
            .map_err(|error| io::Error::other(error.to_string()))?;
        let mut tls = StreamOwned::new(connection, stream);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock)?;
        }
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match tls.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => {
                    buf.extend_from_slice(&chunk[..read]);
                    if String::from_utf8_lossy(&buf).contains("PUB topic\n") {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(buf)
    });

    (port, join)
}

#[test]
fn nsq_args_validate_matches_reference_cases() {
    let cases = [
        (
            "test1_missing_topic",
            NsqArgs {
                enable: true,
                nsqd_address: Host::new("127.0.0.1", 4150),
                topic: String::new(),
                ..Default::default()
            },
            true,
        ),
        (
            "test2_disabled",
            NsqArgs {
                enable: false,
                topic: "topic".to_owned(),
                ..Default::default()
            },
            false,
        ),
        (
            "test3_ok",
            NsqArgs {
                enable: true,
                nsqd_address: Host::new("127.0.0.1", 4150),
                topic: "topic".to_owned(),
                ..Default::default()
            },
            false,
        ),
        (
            "test4_emptynsqdaddr",
            NsqArgs {
                enable: true,
                topic: "topic".to_owned(),
                ..Default::default()
            },
            true,
        ),
    ];

    for (name, args, should_err) in cases {
        assert_eq!(args.validate().is_err(), should_err, "case {name}");
    }
}

#[test]
fn nsq_args_validate_subcases_match_reference_cases() {
    let cases = [
        NsqArgs {
            enable: true,
            nsqd_address: Host::new("127.0.0.1", 4150),
            topic: String::new(),
            ..Default::default()
        },
        NsqArgs {
            enable: false,
            topic: "topic".to_owned(),
            ..Default::default()
        },
        NsqArgs {
            enable: true,
            nsqd_address: Host::new("127.0.0.1", 4150),
            topic: "topic".to_owned(),
            ..Default::default()
        },
        NsqArgs {
            enable: true,
            topic: "topic".to_owned(),
            ..Default::default()
        },
    ];

    let expected = [true, false, false, true];

    for (args, should_err) in cases.into_iter().zip(expected) {
        assert_eq!(args.validate().is_err(), should_err);
    }
}

#[test]
fn nsq_args_connects_over_tls_when_skip_verify_is_enabled() {
    let (port, join) = spawn_tls_nsq_server();
    let args = NsqArgs {
        enable: true,
        nsqd_address: Host::new("127.0.0.1", port),
        topic: "topic".to_string(),
        tls: minio_rust::internal::event::target::NsqTlsArgs {
            enable: true,
            skip_verify: true,
        },
        ..Default::default()
    };

    let mut connection = args.connect_nsq().expect("tls connect");
    match &mut connection {
        NsqConnection::Tls(stream) => {
            use std::io::Write;
            stream.write_all(b"  V2").expect("write magic");
            stream.write_all(b"PUB topic\n").expect("write command");
            stream.flush().expect("flush");
        }
        other => panic!("expected tls nsq connection, got {other:?}"),
    }
    connection.close();

    let frames = join.join().expect("join tls server").expect("tls server");
    let text = String::from_utf8_lossy(&frames);
    assert!(text.contains("PUB topic\n"), "{text}");
}
