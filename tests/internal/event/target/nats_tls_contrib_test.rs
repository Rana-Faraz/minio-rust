use std::fs::File;
use std::io::{self, BufReader};
use std::net::TcpListener;
use std::sync::{Arc, OnceLock};
use std::thread;

use minio_rust::internal::event::target::{Host, NatsArgs};
use rustls::pki_types::PrivateKeyDer;
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};

pub const SOURCE_FILE: &str = "internal/event/target/nats_tls_contrib_test.go";

fn fixture(path: &str) -> String {
    format!("{}/tests/fixtures/nats/{path}", env!("CARGO_MANIFEST_DIR"))
}

fn ensure_rustls_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
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

fn spawn_tls_server(require_client_cert: bool) -> (u16, thread::JoinHandle<io::Result<()>>) {
    ensure_rustls_provider();
    let listener = TcpListener::bind("127.0.0.1:0").expect("tls server should bind");
    let port = listener
        .local_addr()
        .expect("listener should have addr")
        .port();

    let server_cert = fixture("certs/nats_server_cert.pem");
    let server_key = fixture("certs/nats_server_key.pem");
    let root_ca = fixture("certs/root_ca_cert.pem");

    let handle = thread::spawn(move || -> io::Result<()> {
        let config = if require_client_cert {
            let mut roots = RootCertStore::empty();
            for certificate in load_certificates(&root_ca) {
                roots.add(certificate).expect("root CA should add");
            }
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .expect("client verifier should build");
            ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(
                    load_certificates(&server_cert),
                    load_private_key(&server_key),
                )
                .expect("server config should build")
        } else {
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    load_certificates(&server_cert),
                    load_private_key(&server_key),
                )
                .expect("server config should build")
        };

        let (stream, _) = listener.accept()?;
        let connection = ServerConnection::new(Arc::new(config))
            .map_err(|error| io::Error::other(error.to_string()))?;
        let mut tls = StreamOwned::new(connection, stream);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock)?;
        }
        Ok(())
    });

    (port, handle)
}

#[test]
fn nats_conn_tls_custom_ca_matches_reference_case() {
    let (port, handle) = spawn_tls_server(false);
    let connection = NatsArgs {
        enable: true,
        address: Host::new("localhost", port),
        subject: "test".to_owned(),
        secure: true,
        cert_authority: fixture("certs/root_ca_cert.pem"),
        ..Default::default()
    }
    .connect_nats()
    .expect("tls custom ca nats connection should succeed");
    connection.close();
    handle
        .join()
        .expect("server thread should join")
        .expect("server should accept client");
}

#[test]
fn nats_conn_tls_skip_verify_matches_reference_case() {
    let (port, handle) = spawn_tls_server(false);
    let connection = NatsArgs {
        enable: true,
        address: Host::new("localhost", port),
        subject: "test".to_owned(),
        secure: true,
        tls_skip_verify: true,
        ..Default::default()
    }
    .connect_nats()
    .expect("tls skip-verify nats connection should succeed");
    connection.close();
    handle
        .join()
        .expect("server thread should join")
        .expect("server should accept client");
}

#[test]
fn nats_conn_tls_custom_ca_handshake_first_matches_reference_case() {
    let (port, handle) = spawn_tls_server(false);
    let connection = NatsArgs {
        enable: true,
        address: Host::new("localhost", port),
        subject: "test".to_owned(),
        secure: true,
        cert_authority: fixture("certs/root_ca_cert.pem"),
        tls_handshake_first: true,
        ..Default::default()
    }
    .connect_nats()
    .expect("tls handshake-first nats connection should succeed");
    connection.close();
    handle
        .join()
        .expect("server thread should join")
        .expect("server should accept client");
}

#[test]
fn nats_conn_tls_client_authorization_matches_reference_case() {
    let (port, handle) = spawn_tls_server(true);
    let connection = NatsArgs {
        enable: true,
        address: Host::new("localhost", port),
        subject: "test".to_owned(),
        secure: true,
        cert_authority: fixture("certs/root_ca_cert.pem"),
        client_cert: fixture("certs/nats_client_cert.pem"),
        client_key: fixture("certs/nats_client_key.pem"),
        ..Default::default()
    }
    .connect_nats()
    .expect("mutual tls nats connection should succeed");
    connection.close();
    handle
        .join()
        .expect("server thread should join")
        .expect("server should accept client");
}
