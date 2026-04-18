use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::thread;

use minio_rust::cmd::*;
use minio_rust::internal::bucket::encryption::{BucketSseConfig, EncryptionAction, Rule, AWS_KMS};
use minio_rust::internal::kms::{self, Context};
use rustls::pki_types::PrivateKeyDer;
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};

pub const SOURCE_FILE: &str = "cmd/kms_service_test.go";

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn kms_bucket_config(key_id: &str) -> BucketSseConfig {
    BucketSseConfig {
        xmlns: String::new(),
        rules: vec![Rule {
            default_encryption_action: EncryptionAction {
                algorithm: AWS_KMS.to_string(),
                master_key_id: key_id.to_string(),
            },
        }],
    }
}

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

fn spawn_tls_kms_status_server(
    require_client_cert: bool,
) -> (u16, thread::JoinHandle<io::Result<()>>) {
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
        let connection = ServerConnection::new(Arc::new(config.clone()))
            .map_err(|error| io::Error::other(error.to_string()))?;
        let mut tls = StreamOwned::new(connection, stream);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock)?;
        }

        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let read = tls.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let request_text = String::from_utf8_lossy(&request);
        let request_line = request_text.lines().next().unwrap_or_default();
        assert!(
            request_line.contains("/minio/kms/v1/key/status?key-id=my-key"),
            "unexpected tls kms request line: {request_line}"
        );

        let body = serde_json::json!({
            "key_id": "my-key",
            "backend": "Kes",
            "exists": true,
            "validation_succeeded": true,
            "create_supported": true,
            "error": "",
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        tls.write_all(response.as_bytes())?;
        tls.flush()?;
        Ok(())
    });

    (port, handle)
}

#[test]
fn test_kms_service_validates_static_key_and_wraps_config_crypto() {
    let env = HashMap::from([(
        kms::ENV_KMS_SECRET_KEY.to_string(),
        "my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=".to_string(),
    )]);

    let service = KmsServiceFacade::from_env_map(&env).expect("service");
    assert_eq!(
        service.status(),
        &KmsServiceStatus {
            configured: true,
            backend: KmsServiceBackend::StaticKey,
            endpoint: String::new(),
            enclave: String::new(),
            default_key: "my-key".to_string(),
            auth_mode: "static-key".to_string(),
            config_encryption_supported: true,
        }
    );

    let ciphertext = service
        .encrypt_config_bytes(b"config-data", Context::from([("scope", "cluster")]))
        .expect("encrypt");
    let plaintext = service
        .decrypt_config_bytes(&ciphertext, Context::from([("scope", "cluster")]))
        .expect("decrypt");
    assert_eq!(plaintext, b"config-data");

    let choice = service
        .resolve_object_default_key(
            None,
            &BTreeMap::from([(
                "x-amz-server-side-encryption".to_string(),
                "aws:kms".to_string(),
            )]),
        )
        .expect("default key");
    assert_eq!(
        choice,
        KmsKeyChoice {
            key_id: "my-key".to_string(),
            source: KmsKeySource::ServiceDefault,
        }
    );
}

#[test]
fn test_kms_service_exposes_kes_status_and_resolves_key_precedence() {
    let env = HashMap::from([
        (
            kms::ENV_KES_ENDPOINT.to_string(),
            "https://127.0.0.1:7373".to_string(),
        ),
        (
            kms::ENV_KES_DEFAULT_KEY.to_string(),
            "kes-default".to_string(),
        ),
        (kms::ENV_KES_API_KEY.to_string(), "kes:v1:token".to_string()),
    ]);

    let service = KmsServiceFacade::from_env_map(&env).expect("service");
    assert_eq!(service.status().backend, KmsServiceBackend::Kes);
    assert_eq!(service.status().endpoint, "https://127.0.0.1:7373");
    assert_eq!(service.status().default_key, "kes-default");
    assert_eq!(service.status().auth_mode, "api-key");
    assert!(service.status().config_encryption_supported);

    let bucket_choice = service
        .resolve_bucket_default_key(Some(&kms_bucket_config("arn:aws:kms:bucket-key")))
        .expect("bucket key");
    assert_eq!(
        bucket_choice,
        KmsKeyChoice {
            key_id: "bucket-key".to_string(),
            source: KmsKeySource::BucketConfig,
        }
    );

    let request_choice = service
        .resolve_object_default_key(
            Some(&kms_bucket_config("arn:aws:kms:bucket-key")),
            &BTreeMap::from([
                (
                    "x-amz-server-side-encryption".to_string(),
                    "aws:kms".to_string(),
                ),
                (
                    "x-amz-server-side-encryption-aws-kms-key-id".to_string(),
                    "arn:aws:kms:request-key".to_string(),
                ),
            ]),
        )
        .expect("request key");
    assert_eq!(
        request_choice,
        KmsKeyChoice {
            key_id: "request-key".to_string(),
            source: KmsKeySource::RequestHeader,
        }
    );
}

#[test]
fn test_kms_service_rejects_mixed_env_and_supports_secret_key_file() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let key_path = tempdir.path().join("kms.key");
    fs::write(
        &key_path,
        "file-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=\n",
    )
    .expect("write key file");

    let file_env = HashMap::from([(
        kms::ENV_KMS_SECRET_KEY_FILE.to_string(),
        key_path.display().to_string(),
    )]);
    let file_service = KmsServiceFacade::from_env_map(&file_env).expect("service from file");
    assert_eq!(file_service.status().backend, KmsServiceBackend::StaticKey);
    assert_eq!(file_service.status().default_key, "file-key");

    let mixed_env = HashMap::from([
        (
            kms::ENV_KMS_ENDPOINT.to_string(),
            "https://127.0.0.1:7373".to_string(),
        ),
        (
            kms::ENV_KMS_SECRET_KEY.to_string(),
            "my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=".to_string(),
        ),
    ]);
    let err = KmsServiceFacade::from_env_map(&mixed_env).expect_err("mixed env must fail");
    assert!(
        err.contains("MinIO KMS and static KMS key"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_kms_service_remote_http_backed_key_operations() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_secret = std::env::var(kms::ENV_KMS_SECRET_KEY).ok();
    unsafe {
        std::env::set_var(
            kms::ENV_KMS_SECRET_KEY,
            "my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=",
        );
    }

    let tempdir = tempfile::tempdir().expect("tempdir");
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: DEFAULT_ROOT_USER.to_string(),
        root_password: DEFAULT_ROOT_PASSWORD.to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let endpoint = format!(
        "http://{}:{}@{}",
        DEFAULT_ROOT_USER,
        DEFAULT_ROOT_PASSWORD,
        handle.address()
    );

    let minio_env = HashMap::from([
        (kms::ENV_KMS_ENDPOINT.to_string(), endpoint.clone()),
        (kms::ENV_KMS_ENCLAVE.to_string(), "minio-rust".to_string()),
        (kms::ENV_KMS_DEFAULT_KEY.to_string(), "my-key".to_string()),
        (kms::ENV_KMS_API_KEY.to_string(), "test-api-key".to_string()),
    ]);
    let minio_service = KmsServiceFacade::from_env_map(&minio_env).expect("minio kms service");
    let minio_status = minio_service
        .key_status(Some("my-key"))
        .expect("remote key status");
    assert_eq!(minio_status.backend, KmsServiceBackend::StaticKey);
    assert!(minio_status.exists);
    assert!(minio_status.validation_succeeded);
    assert_eq!(minio_service.list_keys(Some("my*")).len(), 1);
    assert!(minio_service.metrics().online);
    assert_eq!(
        minio_service
            .create_key("my-key")
            .expect("remote create key")
            .key_id,
        "my-key"
    );
    let ciphertext = minio_service
        .encrypt_config_bytes(b"remote-config", Context::from([("scope", "cluster")]))
        .expect("remote config encrypt");
    let plaintext = minio_service
        .decrypt_config_bytes(&ciphertext, Context::from([("scope", "cluster")]))
        .expect("remote config decrypt");
    assert_eq!(plaintext, b"remote-config");

    let kes_env = HashMap::from([
        (kms::ENV_KES_ENDPOINT.to_string(), endpoint),
        (kms::ENV_KES_DEFAULT_KEY.to_string(), "my-key".to_string()),
        (kms::ENV_KES_API_KEY.to_string(), "kes:v1:test".to_string()),
    ]);
    let kes_service = KmsServiceFacade::from_env_map(&kes_env).expect("kes service");
    let kes_status = kes_service
        .key_status(Some("my-key"))
        .expect("kes key status");
    assert!(kes_status.exists);
    assert!(kes_status.validation_succeeded);
    let kes_ciphertext = kes_service
        .encrypt_config_bytes(b"kes-remote-config", Context::from([("scope", "cluster")]))
        .expect("kes remote config encrypt");
    let kes_plaintext = kes_service
        .decrypt_config_bytes(&kes_ciphertext, Context::from([("scope", "cluster")]))
        .expect("kes remote config decrypt");
    assert_eq!(kes_plaintext, b"kes-remote-config");

    handle.shutdown().expect("shutdown server");
    unsafe {
        if let Some(value) = previous_secret {
            std::env::set_var(kms::ENV_KMS_SECRET_KEY, value);
        } else {
            std::env::remove_var(kms::ENV_KMS_SECRET_KEY);
        }
    }
}

#[test]
fn test_kms_service_remote_http_api_key_only_operations() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_secret = std::env::var(kms::ENV_KMS_SECRET_KEY).ok();
    let previous_server = std::env::var(kms::ENV_KMS_ENDPOINT).ok();
    let previous_enclave = std::env::var(kms::ENV_KMS_ENCLAVE).ok();
    let previous_default_key = std::env::var(kms::ENV_KMS_DEFAULT_KEY).ok();
    let previous_api_key = std::env::var(kms::ENV_KMS_API_KEY).ok();

    unsafe {
        std::env::set_var(
            kms::ENV_KMS_SECRET_KEY,
            "my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=",
        );
        std::env::remove_var(kms::ENV_KMS_ENDPOINT);
        std::env::remove_var(kms::ENV_KMS_ENCLAVE);
        std::env::remove_var(kms::ENV_KMS_DEFAULT_KEY);
        std::env::remove_var(kms::ENV_KMS_API_KEY);
    }

    let inner_tempdir = tempfile::tempdir().expect("tempdir");
    let inner_disk = inner_tempdir.path().join("disk1");
    let inner = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: DEFAULT_ROOT_USER.to_string(),
        root_password: DEFAULT_ROOT_PASSWORD.to_string(),
        disks: vec![inner_disk],
    })
    .expect("spawn inner server");

    unsafe {
        std::env::remove_var(kms::ENV_KMS_SECRET_KEY);
        std::env::set_var(
            kms::ENV_KMS_ENDPOINT,
            format!(
                "http://{}:{}@{}",
                DEFAULT_ROOT_USER,
                DEFAULT_ROOT_PASSWORD,
                inner.address()
            ),
        );
        std::env::set_var(kms::ENV_KMS_ENCLAVE, "minio-rust");
        std::env::set_var(kms::ENV_KMS_DEFAULT_KEY, "my-key");
        std::env::set_var(kms::ENV_KMS_API_KEY, "kms-router-token");
    }

    let outer_tempdir = tempfile::tempdir().expect("tempdir");
    let outer_disk = outer_tempdir.path().join("disk1");
    let outer = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: DEFAULT_ROOT_USER.to_string(),
        root_password: DEFAULT_ROOT_PASSWORD.to_string(),
        disks: vec![outer_disk],
    })
    .expect("spawn outer server");

    let minio_env = HashMap::from([
        (
            kms::ENV_KMS_ENDPOINT.to_string(),
            format!("http://{}", outer.address()),
        ),
        (kms::ENV_KMS_ENCLAVE.to_string(), "minio-rust".to_string()),
        (kms::ENV_KMS_DEFAULT_KEY.to_string(), "my-key".to_string()),
        (
            kms::ENV_KMS_API_KEY.to_string(),
            "kms-router-token".to_string(),
        ),
    ]);
    let minio_service = KmsServiceFacade::from_env_map(&minio_env).expect("minio kms service");
    let minio_status = minio_service
        .key_status(Some("my-key"))
        .expect("api-key remote key status");
    assert!(minio_status.exists);
    assert!(minio_status.validation_succeeded);
    assert_eq!(minio_service.list_keys(Some("my*")).len(), 1);
    assert!(minio_service.metrics().online);
    let ciphertext = minio_service
        .encrypt_config_bytes(
            b"api-key-remote-config",
            Context::from([("scope", "cluster")]),
        )
        .expect("api-key remote config encrypt");
    let plaintext = minio_service
        .decrypt_config_bytes(&ciphertext, Context::from([("scope", "cluster")]))
        .expect("api-key remote config decrypt");
    assert_eq!(plaintext, b"api-key-remote-config");

    let kes_env = HashMap::from([
        (
            kms::ENV_KES_ENDPOINT.to_string(),
            format!("http://{}", outer.address()),
        ),
        (kms::ENV_KES_DEFAULT_KEY.to_string(), "my-key".to_string()),
        (
            kms::ENV_KES_API_KEY.to_string(),
            "kms-router-token".to_string(),
        ),
    ]);
    let kes_service = KmsServiceFacade::from_env_map(&kes_env).expect("kes service");
    let kes_status = kes_service
        .key_status(Some("my-key"))
        .expect("api-key kes key status");
    assert!(kes_status.exists);
    assert!(kes_status.validation_succeeded);
    let kes_ciphertext = kes_service
        .encrypt_config_bytes(
            b"api-key-kes-remote-config",
            Context::from([("scope", "cluster")]),
        )
        .expect("api-key kes remote config encrypt");
    let kes_plaintext = kes_service
        .decrypt_config_bytes(&kes_ciphertext, Context::from([("scope", "cluster")]))
        .expect("api-key kes remote config decrypt");
    assert_eq!(kes_plaintext, b"api-key-kes-remote-config");

    outer.shutdown().expect("shutdown outer");
    inner.shutdown().expect("shutdown inner");
    unsafe {
        if let Some(value) = previous_secret {
            std::env::set_var(kms::ENV_KMS_SECRET_KEY, value);
        } else {
            std::env::remove_var(kms::ENV_KMS_SECRET_KEY);
        }
        if let Some(value) = previous_server {
            std::env::set_var(kms::ENV_KMS_ENDPOINT, value);
        } else {
            std::env::remove_var(kms::ENV_KMS_ENDPOINT);
        }
        if let Some(value) = previous_enclave {
            std::env::set_var(kms::ENV_KMS_ENCLAVE, value);
        } else {
            std::env::remove_var(kms::ENV_KMS_ENCLAVE);
        }
        if let Some(value) = previous_default_key {
            std::env::set_var(kms::ENV_KMS_DEFAULT_KEY, value);
        } else {
            std::env::remove_var(kms::ENV_KMS_DEFAULT_KEY);
        }
        if let Some(value) = previous_api_key {
            std::env::set_var(kms::ENV_KMS_API_KEY, value);
        } else {
            std::env::remove_var(kms::ENV_KMS_API_KEY);
        }
    }
}

#[test]
fn test_kms_service_supports_kes_client_certificate_transport() {
    let (port, handle) = spawn_tls_kms_status_server(true);
    let env = HashMap::from([
        (
            kms::ENV_KES_ENDPOINT.to_string(),
            format!("https://127.0.0.1:{port}"),
        ),
        (kms::ENV_KES_DEFAULT_KEY.to_string(), "my-key".to_string()),
        (
            kms::ENV_KES_SERVER_CA.to_string(),
            fixture("certs/root_ca_cert.pem"),
        ),
        (
            kms::ENV_KES_CLIENT_CERT.to_string(),
            fixture("certs/nats_client_cert.pem"),
        ),
        (
            kms::ENV_KES_CLIENT_KEY.to_string(),
            fixture("certs/nats_client_key.pem"),
        ),
    ]);

    let service = KmsServiceFacade::from_env_map(&env).expect("kes service");
    assert_eq!(service.status().backend, KmsServiceBackend::Kes);
    assert_eq!(service.status().auth_mode, "client-cert");
    let status = service
        .key_status(Some("my-key"))
        .expect("kes client-cert key status");
    assert!(status.exists);
    assert!(status.validation_succeeded);

    handle
        .join()
        .expect("server thread should join")
        .expect("server should complete");
}

#[test]
fn test_kms_service_reports_encrypted_kes_client_key_as_unsupported() {
    let env = HashMap::from([
        (
            kms::ENV_KES_ENDPOINT.to_string(),
            "https://127.0.0.1:7373".to_string(),
        ),
        (kms::ENV_KES_DEFAULT_KEY.to_string(), "my-key".to_string()),
        (
            kms::ENV_KES_SERVER_CA.to_string(),
            fixture("certs/root_ca_cert.pem"),
        ),
        (
            kms::ENV_KES_CLIENT_CERT.to_string(),
            fixture("certs/nats_client_cert.pem"),
        ),
        (
            kms::ENV_KES_CLIENT_KEY.to_string(),
            fixture("certs/nats_client_key.pem"),
        ),
        (
            kms::ENV_KES_CLIENT_PASSWORD.to_string(),
            "secret".to_string(),
        ),
    ]);

    let service = KmsServiceFacade::from_env_map(&env).expect("kes service");
    let error = service
        .key_status(Some("my-key"))
        .expect_err("encrypted client key should be unsupported");
    assert!(
        error.contains("encrypted KES client private keys are not supported"),
        "unexpected error: {error}"
    );
}
