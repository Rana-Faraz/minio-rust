use super::*;
use chrono::{DateTime, Utc};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufReader;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, OnceLock};

use rustls::pki_types::PrivateKeyDer;
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use tempfile::TempDir;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn fixture(path: &str) -> String {
    format!("{}/tests/fixtures/nats/{path}", env!("CARGO_MANIFEST_DIR"))
}

fn server_test_temp_root() -> &'static Path {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join("server-mod");
        std::fs::create_dir_all(&root).expect("create server test temp root");
        root
    })
    .as_path()
}

fn new_test_tempdir() -> TempDir {
    TempDir::new_in(server_test_temp_root()).expect("tempdir")
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

fn http_request(address: &str, request: &str) -> (u16, String, Vec<u8>) {
    let mut stream = TcpStream::connect(address).expect("connect");
    stream.write_all(request.as_bytes()).expect("write request");
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown write");

    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).expect("read response");
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("headers/body separator");
    let headers = String::from_utf8(bytes[..split].to_vec()).expect("response headers");
    let body = bytes[split + 4..].to_vec();
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .expect("status code");
    (status, headers, body)
}

fn response_header_value(headers: &str, name: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (header_name, value) = line.split_once(':')?;
        header_name
            .eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

fn send_test_request(address: &str, req: &TestRequest) -> (u16, String, Vec<u8>) {
    let mut target = req.url.path().to_string();
    if let Some(query) = req.url.query() {
        target.push('?');
        target.push_str(query);
    }
    let host = match req.url.port() {
        Some(port) => format!("{}:{port}", req.url.host_str().expect("request host")),
        None => req.url.host_str().expect("request host").to_string(),
    };
    let mut raw = format!("{} {} HTTP/1.1\r\nHost: {host}\r\n", req.method, target);
    for (key, value) in &req.headers {
        if key.eq_ignore_ascii_case("host") {
            continue;
        }
        raw.push_str(&format!("{key}: {value}\r\n"));
    }
    if !req.body.is_empty() {
        raw.push_str(&format!("content-length: {}\r\n", req.body.len()));
    }
    raw.push_str("\r\n");

    let mut bytes = raw.into_bytes();
    bytes.extend_from_slice(&req.body);

    let mut stream = TcpStream::connect(address).expect("connect");
    stream.write_all(&bytes).expect("write request");
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown write");

    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("headers/body separator");
    let headers = String::from_utf8(response[..split].to_vec()).expect("response headers");
    let body = response[split + 4..].to_vec();
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .expect("status code");
    (status, headers, body)
}

#[test]
fn server_handles_basic_bucket_and_object_flow() {
    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /demo HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let payload = b"hello from server";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /demo/hello.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let (status, headers, body) = http_request(
        handle.address(),
        &format!(
            "GET /demo/hello.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert!(headers.to_ascii_lowercase().contains("content-length: 17"));
    assert_eq!(body, payload);

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_handles_sigv4_bucket_and_object_flow() {
    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let when = DateTime::<Utc>::from_timestamp(1_713_654_000, 0).expect("timestamp");

    let mut make_bucket = new_test_request(
        "PUT",
        &format!("http://{}/sigv4-demo", handle.address()),
        0,
        None,
    )
    .expect("make bucket request");
    sign_request_v4_standard(
        &mut make_bucket,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign make bucket");
    let (status, _, _) = send_test_request(handle.address(), &make_bucket);
    assert_eq!(status, 200);

    let payload = b"hello via sigv4".to_vec();
    let mut put_object = new_test_request(
        "PUT",
        &format!("http://{}/sigv4-demo/hello.txt", handle.address()),
        payload.len() as i64,
        Some(&payload),
    )
    .expect("put request");
    sign_request_v4_standard(
        &mut put_object,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign put object");
    let (status, _, _) = send_test_request(handle.address(), &put_object);
    assert_eq!(status, 200);

    let mut get_object = new_test_request(
        "GET",
        &format!("http://{}/sigv4-demo/hello.txt", handle.address()),
        0,
        None,
    )
    .expect("get request");
    sign_request_v4_standard(
        &mut get_object,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign get object");
    let (status, headers, body) = send_test_request(handle.address(), &get_object);
    assert_eq!(status, 200);
    assert!(headers.to_ascii_lowercase().contains("content-length: 15"));
    assert_eq!(body, payload);

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_handles_presigned_sigv4_get() {
    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    let payload = b"hello via presign";

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /presign-demo HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /presign-demo/hello.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let when = Utc::now();
    let mut req = new_test_request(
        "GET",
        &format!("http://{}/presign-demo/hello.txt", handle.address()),
        0,
        None,
    )
    .expect("presigned request");
    pre_sign_v4_standard(&mut req, "minioadmin", "minioadmin", "us-east-1", when, 300)
        .expect("presign request");

    let (status, headers, body) = send_test_request(handle.address(), &req);
    assert_eq!(status, 200);
    assert!(headers.to_ascii_lowercase().contains("content-length: 17"));
    assert_eq!(body, payload);

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_handles_sigv2_bucket_and_object_flow() {
    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let when = DateTime::<Utc>::from_timestamp(1_713_654_000, 0).expect("timestamp");

    let mut make_bucket = new_test_request(
        "PUT",
        &format!("http://{}/sigv2-demo", handle.address()),
        0,
        None,
    )
    .expect("make bucket request");
    sign_request_v2_standard(&mut make_bucket, "minioadmin", "minioadmin", when)
        .expect("sign v2 make bucket");
    let (status, _, _) = send_test_request(handle.address(), &make_bucket);
    assert_eq!(status, 200);

    let payload = b"hello via sigv2".to_vec();
    let mut put_object = new_test_request(
        "PUT",
        &format!("http://{}/sigv2-demo/hello.txt", handle.address()),
        payload.len() as i64,
        Some(&payload),
    )
    .expect("put request");
    sign_request_v2_standard(&mut put_object, "minioadmin", "minioadmin", when)
        .expect("sign v2 put");
    let (status, _, _) = send_test_request(handle.address(), &put_object);
    assert_eq!(status, 200);

    let mut get_object = new_test_request(
        "GET",
        &format!("http://{}/sigv2-demo/hello.txt", handle.address()),
        0,
        None,
    )
    .expect("get request");
    sign_request_v2_standard(&mut get_object, "minioadmin", "minioadmin", when)
        .expect("sign v2 get");
    let (status, headers, body) = send_test_request(handle.address(), &get_object);
    assert_eq!(status, 200);
    assert!(headers.to_ascii_lowercase().contains("content-length: 15"));
    assert_eq!(body, payload);

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_handles_bucket_policy_and_extended_config_routes() {
    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /cfgbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::cfgbucket/*"]}]}"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /cfgbucket?policy HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                policy.len(),
                policy,
            ),
        );
    assert_eq!(status, 204);

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /cfgbucket?policy HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(String::from_utf8(body).expect("policy body"), policy);

    let lifecycle = r#"<LifecycleConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /cfgbucket?lifecycle HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                lifecycle.len(),
                lifecycle,
            ),
        );
    assert_eq!(status, 200);

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /cfgbucket?lifecycle HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(String::from_utf8(body).expect("lifecycle body"), lifecycle);

    let replication = r#"<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Priority>1</Priority><Filter><Prefix>logs/</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1::cfgbucket-replica</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /cfgbucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 400);

    let versioning =
        r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /cfgbucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /cfgbucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert!(String::from_utf8(body)
        .expect("versioning body")
        .contains("Enabled"));

    let encryption = r#"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /cfgbucket?encryption HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                encryption.len(),
                encryption,
            ),
        );
    assert_eq!(status, 200);

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /cfgbucket?encryption HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(
        String::from_utf8(body).expect("encryption body"),
        encryption
    );

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>1</Id><Queue>arn:minio:sqs:us-east-1:1:webhook</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /cfgbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /cfgbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(
        String::from_utf8(body).expect("notification body"),
        notification
    );

    let payload = b"encrypted by bucket default";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /cfgbucket/secret.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let (status, headers, _) = http_request(
        handle.address(),
        &format!(
            "HEAD /cfgbucket/secret.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert!(headers.to_ascii_lowercase().contains(
        "x-amz-server-side-encryption: AES256"
            .to_ascii_lowercase()
            .as_str()
    ));

    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /cfgbucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 200);

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /cfgbucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(
        String::from_utf8(body).expect("replication body"),
        replication
    );

    let when = DateTime::<Utc>::from_timestamp(1_713_654_000, 0).expect("timestamp");
    let mut notifications = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/notifications?bucket=cfgbucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("notifications request");
    sign_request_v4_standard(
        &mut notifications,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign notifications");
    let (status, _, body) = send_test_request(handle.address(), &notifications);
    assert_eq!(status, 200);
    let notifications_body = String::from_utf8(body).expect("notifications body");
    assert!(notifications_body.contains("s3:ObjectCreated:Put"));
    assert!(notifications_body.contains("cfgbucket"));
    assert!(notifications_body.contains("secret.txt"));

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_replicates_matching_objects_to_replica_bucket() {
    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    for bucket in ["sourcebucket", "sourcebucket-replica"] {
        let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /{bucket} HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
                handle.address()
            ),
        );
        assert_eq!(status, 200);
    }

    let versioning =
        r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /sourcebucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let replication = r#"<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Priority>1</Priority><Filter><Prefix>logs/</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1::sourcebucket-replica</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /sourcebucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"replicated payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /sourcebucket/logs/app.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /sourcebucket-replica/logs/app.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, payload);

    let (status, headers, _) = http_request(
        handle.address(),
        &format!(
            "HEAD /sourcebucket-replica/logs/app.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert!(headers.to_ascii_lowercase().contains(
        "x-amz-bucket-replication-status: REPLICA"
            .to_ascii_lowercase()
            .as_str()
    ));

    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /sourcebucket/skip.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: 4\r\n\r\nskip",
                handle.address(),
            ),
        );
    assert_eq!(status, 200);

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "GET /sourcebucket-replica/skip.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 404);

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_replicates_delete_markers_to_replica_bucket() {
    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    for bucket in ["deletebucket", "deletebucket-replica"] {
        let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /{bucket} HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
                handle.address()
            ),
        );
        assert_eq!(status, 200);
    }

    let versioning =
        r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /deletebucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /deletebucket-replica?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let replication = r#"<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Priority>1</Priority><Filter><Prefix>logs/</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1::deletebucket-replica</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /deletebucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 200);

    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /deletebucket/logs/deleteme.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: 6\r\n\r\nremove",
                handle.address(),
            ),
        );
    assert_eq!(status, 200);

    let (status, headers, _) = http_request(
            handle.address(),
            &format!(
                "HEAD /deletebucket-replica/logs/deleteme.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
                handle.address()
            ),
        );
    assert_eq!(status, 200);
    let replica_object_version_id =
        response_header_value(&headers, "x-amz-version-id").expect("replica object version id");

    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "GET /deletebucket-replica/logs/deleteme.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
                handle.address()
            ),
        );
    assert_eq!(status, 200);

    let (status, headers, _) = http_request(
        handle.address(),
        &format!(
            "DELETE /deletebucket/logs/deleteme.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 204);
    assert_eq!(
        response_header_value(&headers, "x-amz-delete-marker").as_deref(),
        Some("true")
    );
    let delete_marker_version_id =
        response_header_value(&headers, "x-amz-version-id").expect("delete marker version id");

    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "GET /deletebucket-replica/logs/deleteme.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
                handle.address()
            ),
        );
    assert_eq!(status, 404);

    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "DELETE /deletebucket-replica/logs/deleteme.txt?versionId={} HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
                delete_marker_version_id,
                handle.address()
            ),
        );
    assert_eq!(status, 204);

    let (status, _, body) = http_request(
            handle.address(),
            &format!(
                "GET /deletebucket-replica/logs/deleteme.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
                handle.address()
            ),
        );
    assert_eq!(status, 200);
    assert_eq!(body, b"remove");

    let (status, headers, _) = http_request(
            handle.address(),
            &format!(
                "HEAD /deletebucket-replica/logs/deleteme.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
                handle.address()
            ),
        );
    assert_eq!(status, 200);
    assert_eq!(
        response_header_value(&headers, "x-amz-version-id").as_deref(),
        Some(replica_object_version_id.as_str())
    );

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_replicates_objects_to_remote_target_server() {
    let remote_tempdir = new_test_tempdir();
    let remote_disk = remote_tempdir.path().join("disk1");
    let remote = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![remote_disk],
    })
    .expect("spawn remote server");

    let source_tempdir = new_test_tempdir();
    let source_disk = source_tempdir.path().join("disk1");
    let source = spawn_server_with_replication_targets(
        MinioServerConfig {
            address: "127.0.0.1:0".to_string(),
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            disks: vec![source_disk],
        },
        BTreeMap::from([(
            "remote1".to_string(),
            ReplicationRemoteTarget {
                target_id: "remote1".to_string(),
                endpoint: format!("http://{}", remote.address()),
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
            },
        )]),
    )
    .expect("spawn source server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        remote.address(),
        &format!(
            "PUT /remotebucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            remote.address()
        ),
    );
    assert_eq!(status, 200);
    let versioning =
        r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    let (status, _, _) = http_request(
            remote.address(),
            &format!(
                "PUT /remotebucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                remote.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let (status, _, _) = http_request(
        source.address(),
        &format!(
            "PUT /sourcebucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            source.address()
        ),
    );
    assert_eq!(status, 200);

    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let replication = r#"<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Priority>1</Priority><Filter><Prefix>logs/</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1:remote1:remotebucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"remote replication payload";
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket/logs/remote.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let (status, _, body) = http_request(
        remote.address(),
        &format!(
            "GET /remotebucket/logs/remote.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            remote.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, payload);

    let (status, headers, _) = http_request(
        remote.address(),
        &format!(
            "HEAD /remotebucket/logs/remote.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            remote.address()
        ),
    );
    assert_eq!(status, 200);
    assert!(headers.to_ascii_lowercase().contains(
        "x-amz-bucket-replication-status: REPLICA"
            .to_ascii_lowercase()
            .as_str()
    ));
    let remote_object_version_id =
        response_header_value(&headers, "x-amz-version-id").expect("remote object version id");

    let (status, headers, _) = http_request(
        source.address(),
        &format!(
            "DELETE /sourcebucket/logs/remote.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            source.address()
        ),
    );
    assert_eq!(status, 204);
    assert_eq!(
        response_header_value(&headers, "x-amz-delete-marker").as_deref(),
        Some("true")
    );
    let remote_delete_marker_version_id =
        response_header_value(&headers, "x-amz-version-id").expect("delete marker version id");

    let (status, _, _) = http_request(
        remote.address(),
        &format!(
            "GET /remotebucket/logs/remote.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            remote.address()
        ),
    );
    assert_eq!(status, 404);

    let (status, _, _) = http_request(
            remote.address(),
            &format!(
                "DELETE /remotebucket/logs/remote.txt?versionId={} HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
                remote_delete_marker_version_id,
                remote.address()
            ),
        );
    assert_eq!(status, 204);

    let (status, _, body) = http_request(
        remote.address(),
        &format!(
            "GET /remotebucket/logs/remote.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            remote.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, payload);

    let (status, headers, _) = http_request(
        remote.address(),
        &format!(
            "HEAD /remotebucket/logs/remote.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            remote.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(
        response_header_value(&headers, "x-amz-version-id").as_deref(),
        Some(remote_object_version_id.as_str())
    );

    source.shutdown().expect("shutdown source");
    remote.shutdown().expect("shutdown remote");
}

#[test]
fn server_queues_failed_remote_replication_and_exposes_admin_status() {
    let source_tempdir = new_test_tempdir();
    let source_disk = source_tempdir.path().join("disk1");
    let source = spawn_server_with_replication_targets(
        MinioServerConfig {
            address: "127.0.0.1:0".to_string(),
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            disks: vec![source_disk],
        },
        BTreeMap::from([(
            "remote1".to_string(),
            ReplicationRemoteTarget {
                target_id: "remote1".to_string(),
                endpoint: "http://127.0.0.1:1".to_string(),
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
            },
        )]),
    )
    .expect("spawn source server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        source.address(),
        &format!(
            "PUT /sourcebucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            source.address()
        ),
    );
    assert_eq!(status, 200);

    let versioning =
        r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let replication = r#"<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Priority>1</Priority><Filter><Prefix>logs/</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1:remote1:remotebucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"queue me";
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket/logs/fail.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    std::thread::sleep(Duration::from_millis(150));

    let when = DateTime::<Utc>::from_timestamp(1_713_654_000, 0).expect("timestamp");
    let mut status_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/replication/status?bucket=sourcebucket",
            source.address()
        ),
        0,
        None,
    )
    .expect("replication status request");
    sign_request_v4_standard(
        &mut status_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign replication status");
    let (status, _, body) = send_test_request(source.address(), &status_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("status body json");
    assert_eq!(body["queue"]["node_count"].as_u64(), Some(1));
    assert_eq!(body["queue"]["queue_curr_count"].as_f64(), Some(1.0));

    source.shutdown().expect("shutdown source");
}

#[test]
fn server_rehydrates_persisted_replication_queue_after_restart() {
    let source_tempdir = new_test_tempdir();
    let source_disk = source_tempdir.path().join("disk1");
    let remote_targets = BTreeMap::from([(
        "remote1".to_string(),
        ReplicationRemoteTarget {
            target_id: "remote1".to_string(),
            endpoint: "http://127.0.0.1:1".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
        },
    )]);
    let source = spawn_server_with_replication_targets(
        MinioServerConfig {
            address: "127.0.0.1:0".to_string(),
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            disks: vec![source_disk.clone()],
        },
        remote_targets.clone(),
    )
    .expect("spawn source server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        source.address(),
        &format!(
            "PUT /sourcebucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            source.address()
        ),
    );
    assert_eq!(status, 200);

    let versioning =
        r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let replication = r#"<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Priority>1</Priority><Filter><Prefix>logs/</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1:remote1:remotebucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"persist me";
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket/logs/persist.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    std::thread::sleep(Duration::from_millis(150));
    assert!(source_disk.join(REPLICATION_QUEUE_FILE).exists());

    source.shutdown().expect("shutdown source");

    let restarted = spawn_server_with_replication_targets(
        MinioServerConfig {
            address: "127.0.0.1:0".to_string(),
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            disks: vec![source_disk],
        },
        remote_targets,
    )
    .expect("restart source server");

    let when = DateTime::<Utc>::from_timestamp(1_713_654_100, 0).expect("timestamp");
    let mut status_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/replication/status?bucket=sourcebucket",
            restarted.address()
        ),
        0,
        None,
    )
    .expect("replication status request");
    sign_request_v4_standard(
        &mut status_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign replication status");
    let (status, _, body) = send_test_request(restarted.address(), &status_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("status body json");
    assert_eq!(body["queue"]["node_count"].as_u64(), Some(1));
    assert_eq!(body["queue"]["queue_curr_count"].as_f64(), Some(1.0));

    restarted.shutdown().expect("shutdown restarted source");
}

#[test]
fn server_resyncs_existing_objects_to_remote_target_server() {
    let remote_tempdir = new_test_tempdir();
    let remote_disk = remote_tempdir.path().join("disk1");
    let remote = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![remote_disk],
    })
    .expect("spawn remote server");

    let source_tempdir = new_test_tempdir();
    let source_disk = source_tempdir.path().join("disk1");
    let source = spawn_server_with_replication_targets(
        MinioServerConfig {
            address: "127.0.0.1:0".to_string(),
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            disks: vec![source_disk],
        },
        BTreeMap::from([(
            "remote1".to_string(),
            ReplicationRemoteTarget {
                target_id: "remote1".to_string(),
                endpoint: format!("http://{}", remote.address()),
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
            },
        )]),
    )
    .expect("spawn source server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    for (address, bucket) in [
        (source.address(), "sourcebucket"),
        (remote.address(), "remotebucket"),
    ] {
        let (status, _, _) = http_request(
            address,
            &format!("PUT /{bucket} HTTP/1.1\r\nHost: {address}\r\n{authorization}\r\n"),
        );
        assert_eq!(status, 200);
    }

    let versioning =
        r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"existing object";
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket/logs/existing.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let replication = r#"<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication><Priority>1</Priority><Filter><Prefix>logs/</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1:remote1:remotebucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 200);

    let when = DateTime::<Utc>::from_timestamp(1_713_654_100, 0).expect("timestamp");
    let mut resync_req = new_test_request(
        "POST",
        &format!(
            "http://{}/minio/admin/v3/replication/resync?bucket=sourcebucket&arn={}",
            source.address(),
            "arn:minio:replication:us-east-1:remote1:remotebucket"
        ),
        0,
        None,
    )
    .expect("replication resync request");
    sign_request_v4_standard(
        &mut resync_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign replication resync");
    let (status, _, body) = send_test_request(source.address(), &resync_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("resync body json");
    assert_eq!(body["bucket"].as_str(), Some("sourcebucket"));
    assert_eq!(body["enqueued"].as_u64(), Some(1));

    std::thread::sleep(Duration::from_millis(200));

    let (status, headers, body) = http_request(
        remote.address(),
        &format!(
            "GET /remotebucket/logs/existing.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            remote.address(),
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, payload);
    assert_eq!(
        response_header_value(&headers, "x-amz-bucket-replication-status").as_deref(),
        Some("REPLICA")
    );

    let mut status_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/replication/status?bucket=sourcebucket",
            source.address()
        ),
        0,
        None,
    )
    .expect("replication status request");
    sign_request_v4_standard(
        &mut status_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign replication status");
    let (status, _, body) = send_test_request(source.address(), &status_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("status body json");
    let resync_targets = body["resync_targets"]
        .as_array()
        .expect("resync target array");
    assert_eq!(resync_targets.len(), 1);
    assert_eq!(
        resync_targets[0]["arn"].as_str(),
        Some("arn:minio:replication:us-east-1:remote1:remotebucket")
    );
    assert_eq!(resync_targets[0]["scheduled_count"].as_u64(), Some(1));
    assert_eq!(resync_targets[0]["completed_count"].as_u64(), Some(1));
    assert_eq!(resync_targets[0]["status"].as_str(), Some("COMPLETED"));

    source.shutdown().expect("shutdown source");
    remote.shutdown().expect("shutdown remote");
}

#[test]
fn server_rehydrates_persisted_resync_targets_after_restart() {
    let source_tempdir = new_test_tempdir();
    let source_disk = source_tempdir.path().join("disk1");
    let source_config = MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![source_disk.clone()],
    };
    let targets = BTreeMap::from([(
        "remote1".to_string(),
        ReplicationRemoteTarget {
            target_id: "remote1".to_string(),
            endpoint: "http://127.0.0.1:1".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
        },
    )]);

    let source = spawn_server_with_replication_targets(source_config.clone(), targets.clone())
        .expect("spawn source server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    let (status, _, _) = http_request(
        source.address(),
        &format!(
            "PUT /sourcebucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            source.address()
        ),
    );
    assert_eq!(status, 200);

    let versioning =
        r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"restart me";
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket/logs/restart.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let replication = r#"<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication><Priority>1</Priority><Filter><Prefix>logs/</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1:remote1:remotebucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 200);

    let when = DateTime::<Utc>::from_timestamp(1_713_654_200, 0).expect("timestamp");
    let mut resync_req = new_test_request(
        "POST",
        &format!(
            "http://{}/minio/admin/v3/replication/resync?bucket=sourcebucket&arn={}",
            source.address(),
            "arn:minio:replication:us-east-1:remote1:remotebucket"
        ),
        0,
        None,
    )
    .expect("replication resync request");
    sign_request_v4_standard(
        &mut resync_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign replication resync");
    let (status, _, body) = send_test_request(source.address(), &resync_req);
    assert_eq!(status, 200);
    let initial: serde_json::Value = serde_json::from_slice(&body).expect("resync body json");
    let initial_resync_id = initial["resyncId"]
        .as_str()
        .expect("initial resync id")
        .to_string();

    source.shutdown().expect("shutdown source");

    let restarted =
        spawn_server_with_replication_targets(source_config, targets).expect("restart source");

    let mut status_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/replication/status?bucket=sourcebucket",
            restarted.address()
        ),
        0,
        None,
    )
    .expect("replication status request");
    sign_request_v4_standard(
        &mut status_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign replication status");
    let (status, _, body) = send_test_request(restarted.address(), &status_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("status body json");
    let resync_targets = body["resync_targets"]
        .as_array()
        .expect("resync targets array");
    assert_eq!(resync_targets.len(), 1);
    assert_eq!(
        resync_targets[0]["resync_id"].as_str(),
        Some(initial_resync_id.as_str())
    );
    assert_eq!(
        resync_targets[0]["arn"].as_str(),
        Some("arn:minio:replication:us-east-1:remote1:remotebucket")
    );
    assert_eq!(resync_targets[0]["scheduled_count"].as_u64(), Some(1));
    assert_eq!(resync_targets[0]["status"].as_str(), Some("PENDING"));

    restarted.shutdown().expect("shutdown restarted source");
}

#[test]
fn server_preserves_remote_replication_metadata_parity() {
    let remote_tempdir = new_test_tempdir();
    let remote_disk = remote_tempdir.path().join("disk1");
    let remote = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![remote_disk],
    })
    .expect("spawn remote server");

    let source_tempdir = new_test_tempdir();
    let source_disk = source_tempdir.path().join("disk1");
    let source = spawn_server_with_replication_targets(
        MinioServerConfig {
            address: "127.0.0.1:0".to_string(),
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            disks: vec![source_disk],
        },
        BTreeMap::from([(
            "remote1".to_string(),
            ReplicationRemoteTarget {
                target_id: "remote1".to_string(),
                endpoint: format!("http://{}", remote.address()),
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
            },
        )]),
    )
    .expect("spawn source server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    for (address, bucket) in [
        (remote.address(), "remotebucket"),
        (source.address(), "sourcebucket"),
    ] {
        let (status, _, _) = http_request(
            address,
            &format!("PUT /{bucket} HTTP/1.1\r\nHost: {address}\r\n{authorization}\r\n",),
        );
        assert_eq!(status, 200);
    }

    let versioning =
        r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?versioning HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                versioning.len(),
                versioning,
            ),
        );
    assert_eq!(status, 200);

    let replication = r#"<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Priority>1</Priority><Filter><Prefix>logs/</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1:remote1:remotebucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let (status, _, _) = http_request(
            source.address(),
            &format!(
                "PUT /sourcebucket?replication HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                source.address(),
                replication.len(),
                replication,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"remote metadata parity payload";
    let checksum = base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(payload));
    let (status, _, _) = http_request(
        source.address(),
        &format!(
            concat!(
                "PUT /sourcebucket/logs/metadata.txt HTTP/1.1\r\n",
                "Host: {}\r\n",
                "{}",
                "Content-Length: {}\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Encoding: identity\r\n",
                "x-amz-meta-origin: primary-site\r\n",
                "x-amz-meta-owner: codex\r\n",
                "x-amz-server-side-encryption: AES256\r\n",
                "x-amz-checksum-sha256: {}\r\n",
                "\r\n{}"
            ),
            source.address(),
            authorization,
            payload.len(),
            checksum,
            String::from_utf8_lossy(payload),
        ),
    );
    assert_eq!(status, 200);

    let (status, headers, body) = http_request(
        remote.address(),
        &format!(
            "GET /remotebucket/logs/metadata.txt HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            remote.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, payload);
    let headers = headers.to_ascii_lowercase();
    assert!(headers.contains("content-type: text/plain; charset=utf-8"));
    assert!(headers.contains("content-encoding: identity"));
    assert!(headers.contains("x-amz-meta-origin: primary-site"));
    assert!(headers.contains("x-amz-meta-owner: codex"));
    assert!(headers.contains("x-amz-server-side-encryption: aes256"));
    assert!(headers.contains(&format!(
        "x-amz-checksum-sha256: {}",
        checksum.to_ascii_lowercase()
    )));
    assert!(headers.contains("x-amz-bucket-replication-status: replica"));

    source.shutdown().expect("shutdown source");
    remote.shutdown().expect("shutdown remote");
}

#[test]
fn server_handles_post_policy_and_admin_routes() {
    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    let when = DateTime::<Utc>::from_timestamp(1_713_654_000, 0).expect("timestamp");

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /uploads HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>1</Id><Queue>arn:minio:sqs:us-east-1:1:webhook</Queue><Event>s3:ObjectCreated:Post</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /uploads?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let boundary = "----minio-rust-boundary";
    let policy = r#"{"expiration":"2026-12-30T12:00:00.000Z","conditions":[{"bucket":"uploads"},["starts-with","$key","photos/"],["content-length-range",1,20]]}"#;
    let mut multipart = Vec::new();
    multipart.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nphotos/image.jpg\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"policy\"\r\n\r\n{policy}\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"image.jpg\"\r\nContent-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
    multipart.extend_from_slice(&[0, 255, 65, 66, 67]);
    multipart.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let mut post = new_test_request(
        "POST",
        &format!("http://{}/uploads", handle.address()),
        multipart.len() as i64,
        Some(&multipart),
    )
    .expect("post policy request");
    post.set_header(
        "content-type",
        &format!("multipart/form-data; boundary={boundary}"),
    );
    let (status, _, _) = send_test_request(handle.address(), &post);
    assert_eq!(status, 204);

    let mut notifications = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/notifications?bucket=uploads",
            handle.address()
        ),
        0,
        None,
    )
    .expect("notifications request");
    sign_request_v4_standard(
        &mut notifications,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign notifications");
    let (status, _, body) = send_test_request(handle.address(), &notifications);
    assert_eq!(status, 200);
    let notifications_body = String::from_utf8(body).expect("notifications body");
    assert!(
        notifications_body.contains("photos/image.jpg"),
        "{notifications_body}"
    );
    assert!(notifications_body.contains("s3:ObjectCreated:Post"));

    let mut admin_info = new_test_request(
        "GET",
        &format!("http://{}/minio/admin/v3/info?info=", handle.address()),
        0,
        None,
    )
    .expect("admin info request");
    sign_request_v4_standard(
        &mut admin_info,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign admin info");
    let (status, _, body) = send_test_request(handle.address(), &admin_info);
    assert_eq!(status, 200);
    let admin_info_body = String::from_utf8(body).expect("admin info body");
    assert!(admin_info_body.contains("\"region\""));
    assert!(admin_info_body.contains("\"mode\":\"online\""));
    assert!(admin_info_body.contains("\"type\":\"LocalObjectLayer\""));
    assert!(admin_info_body.contains("\"notifications\""));
    assert!(admin_info_body.contains("\"count\":1"));

    let mut storage_info = new_test_request(
        "GET",
        &format!("http://{}/minio/admin/v3/storageinfo", handle.address()),
        0,
        None,
    )
    .expect("storage info request");
    sign_request_v4_standard(
        &mut storage_info,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign storage info");
    let (status, _, body) = send_test_request(handle.address(), &storage_info);
    assert_eq!(status, 200);
    let storage_info_body = String::from_utf8(body).expect("storage info body");
    assert!(storage_info_body.contains("\"type\":\"LocalObjectLayer\""));
    assert!(storage_info_body.contains("\"buckets\":1"));
    assert!(storage_info_body.contains("\"objects\":1"));

    let mut restart = new_test_request(
        "POST",
        &format!(
            "http://{}/minio/admin/v3/service?action=restart&type=2",
            handle.address()
        ),
        0,
        None,
    )
    .expect("restart request");
    sign_request_v4_standard(&mut restart, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign restart");
    let (status, _, body) = send_test_request(handle.address(), &restart);
    assert_eq!(status, 200);
    assert!(String::from_utf8(body)
        .expect("restart body")
        .contains("restart"));

    let user_body = br#"{"secretKey":"demo-secret","status":"enabled"}"#.to_vec();
    let mut add_user = new_test_request(
        "PUT",
        &format!(
            "http://{}/minio/admin/v3/add-user?accessKey=demo",
            handle.address()
        ),
        user_body.len() as i64,
        Some(&user_body),
    )
    .expect("add user request");
    add_user.set_header("content-type", "application/json");
    sign_request_v4_standard(&mut add_user, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign add user");
    let (status, _, body) = send_test_request(handle.address(), &add_user);
    assert_eq!(status, 200);
    assert!(String::from_utf8(body)
        .expect("add user body")
        .contains("demo"));

    let mut list_users = new_test_request(
        "GET",
        &format!("http://{}/minio/admin/v3/list-users", handle.address()),
        0,
        None,
    )
    .expect("list users request");
    sign_request_v4_standard(
        &mut list_users,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign list users");
    let (status, _, body) = send_test_request(handle.address(), &list_users);
    assert_eq!(status, 200);
    let users_body = String::from_utf8(body).expect("list users body");
    assert!(users_body.contains("demo"));
    assert!(users_body.contains("enabled"));

    let mut disable_user = new_test_request(
        "PUT",
        &format!(
            "http://{}/minio/admin/v3/set-user-status?accessKey=demo&status=disabled",
            handle.address()
        ),
        0,
        None,
    )
    .expect("disable user request");
    sign_request_v4_standard(
        &mut disable_user,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign disable user");
    let (status, _, body) = send_test_request(handle.address(), &disable_user);
    assert_eq!(status, 200);
    assert!(String::from_utf8(body)
        .expect("disable user body")
        .contains("disabled"));

    let heal_body = br#"{"recursive":true,"dryRun":true,"remove":false,"scanMode":1}"#.to_vec();
    let mut heal = new_test_request(
        "POST",
        &format!(
            "http://{}/minio/admin/v3/heal/uploads/photos?forceStart=",
            handle.address()
        ),
        heal_body.len() as i64,
        Some(&heal_body),
    )
    .expect("heal request");
    heal.set_header("content-type", "application/json");
    sign_request_v4_standard(&mut heal, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign heal");
    let (status, _, body) = send_test_request(handle.address(), &heal);
    assert_eq!(status, 200);
    let heal_body = String::from_utf8(body).expect("heal body");
    assert!(heal_body.contains("\"bucket\":\"uploads\""));
    assert!(heal_body.contains("\"prefix\":\"photos\""));
    assert!(heal_body.contains("\"forceStart\":true"));

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/health/live HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"OK");

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_reports_cluster_health_and_maintenance_status() {
    let tempdir = new_test_tempdir();
    let disk1 = tempdir.path().join("disk1");
    let disk2 = tempdir.path().join("disk2");
    let disk3 = tempdir.path().join("disk3");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk1.clone(), disk2.clone(), disk3.clone()],
    })
    .expect("spawn server");

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/health/cluster HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"OK");

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/health/cluster/read HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"OK");

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/health/cluster?maintenance=true HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"OK");

    std::fs::remove_dir_all(&disk3).expect("remove disk3");

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/health/cluster HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"OK");

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/health/cluster/read HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"OK");

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/health/cluster?maintenance=true HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 412);
    assert_eq!(body, b"PRECONDITION FAILED");

    std::fs::remove_dir_all(&disk2).expect("remove disk2");

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/health/cluster HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 503);
    assert_eq!(body, b"UNAVAILABLE");

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/health/cluster/read HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 503);
    assert_eq!(body, b"UNAVAILABLE");

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_exposes_authenticated_cluster_metrics() {
    let tempdir = new_test_tempdir();
    let disk1 = tempdir.path().join("disk1");
    let disk2 = tempdir.path().join("disk2");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk1, disk2],
    })
    .expect("spawn server");

    let when = DateTime::<Utc>::from_timestamp(1_730_100_000, 0).expect("timestamp");
    let mut request = new_test_request(
        "GET",
        &format!("http://{}/minio/v2/metrics/cluster", handle.address()),
        0,
        None,
    )
    .expect("metrics request");
    sign_request_v4_standard(&mut request, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign metrics request");
    let (status, headers, body) = send_test_request(handle.address(), &request);
    assert_eq!(status, 200);
    assert_eq!(
        response_header_value(&headers, "content-type").as_deref(),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let body = String::from_utf8(body).expect("metrics body");
    assert!(body.contains("minio_cluster_health_status"));
    assert!(body.contains("minio_cluster_bucket_total 0"));
    assert!(body.contains("minio_cluster_drive_online_total 2"));
    assert!(body.contains("minio_cluster_kms_configured"));

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_exposes_public_cluster_metrics_when_configured() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_auth_type = std::env::var("MINIO_PROMETHEUS_AUTH_TYPE").ok();
    unsafe {
        std::env::set_var("MINIO_PROMETHEUS_AUTH_TYPE", "public");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let (status, _, body) = http_request(
        handle.address(),
        &format!(
            "GET /minio/v2/metrics/cluster HTTP/1.1\r\nHost: {}\r\n\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    let body = String::from_utf8(body).expect("metrics body");
    assert!(body.contains("minio_cluster_nodes_online_total 1"));
    assert!(body.contains("minio_cluster_drive_online_total 1"));

    handle.shutdown().expect("shutdown server");
    unsafe {
        if let Some(value) = previous_auth_type {
            std::env::set_var("MINIO_PROMETHEUS_AUTH_TYPE", value);
        } else {
            std::env::remove_var("MINIO_PROMETHEUS_AUTH_TYPE");
        }
    }
}

#[test]
fn server_exposes_all_v2_metrics_endpoints() {
    let tempdir = new_test_tempdir();
    let disk1 = tempdir.path().join("disk1");
    let disk2 = tempdir.path().join("disk2");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk1.clone(), disk2.clone()],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /metricsbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);
    let payload = b"metrics payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /metricsbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let when = DateTime::<Utc>::from_timestamp(1_730_100_100, 0).expect("timestamp");
    for path in [
        "/minio/v2/metrics/cluster",
        "/minio/v2/metrics/node",
        "/minio/v2/metrics/bucket",
        "/minio/v2/metrics/resource",
    ] {
        let mut request = new_test_request(
            "GET",
            &format!("http://{}{}", handle.address(), path),
            0,
            None,
        )
        .expect("metrics request");
        sign_request_v4_standard(&mut request, "minioadmin", "minioadmin", "us-east-1", when)
            .expect("sign metrics request");
        let (status, headers, body) = send_test_request(handle.address(), &request);
        assert_eq!(status, 200, "path {path}");
        assert_eq!(
            response_header_value(&headers, "content-type").as_deref(),
            Some("text/plain; version=0.0.4; charset=utf-8")
        );
        let body = String::from_utf8(body).expect("metrics body");
        assert!(body.contains("# HELP"), "path {path}");
        assert!(body.contains("# TYPE"), "path {path}");
    }

    let mut bucket_request = new_test_request(
        "GET",
        &format!("http://{}/minio/v2/metrics/bucket", handle.address()),
        0,
        None,
    )
    .expect("bucket metrics request");
    sign_request_v4_standard(
        &mut bucket_request,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign bucket metrics request");
    let (status, _, body) = send_test_request(handle.address(), &bucket_request);
    assert_eq!(status, 200);
    let body = String::from_utf8(body).expect("bucket metrics body");
    assert!(body.contains("minio_bucket_usage_object_total{bucket=\"metricsbucket\"} 1"));

    let mut resource_request = new_test_request(
        "GET",
        &format!("http://{}/minio/v2/metrics/resource", handle.address()),
        0,
        None,
    )
    .expect("resource metrics request");
    sign_request_v4_standard(
        &mut resource_request,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign resource metrics request");
    let (status, _, body) = send_test_request(handle.address(), &resource_request);
    assert_eq!(status, 200);
    let body = String::from_utf8(body).expect("resource metrics body");
    assert!(body.contains("minio_node_drive_status{disk="));
    assert!(body.contains("minio_node_drive_total 2"));

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_binds_console_address_and_serves_placeholder_console() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_console = std::env::var("MINIO_CONSOLE_ADDRESS").ok();
    unsafe {
        std::env::set_var("MINIO_CONSOLE_ADDRESS", ":0");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: ":0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let console_address = handle.console_address().expect("console address");
    assert_ne!(console_address, handle.address());

    let (status, headers, body) = http_request(
        console_address,
        &format!("GET / HTTP/1.1\r\nHost: {console_address}\r\n\r\n"),
    );
    assert_eq!(status, 200);
    assert!(headers
        .to_ascii_lowercase()
        .contains("content-type: text/html"));
    assert!(String::from_utf8(body)
        .expect("console body")
        .contains("MinIO Rust Console"));

    let (status, _, _) = http_request(
        console_address,
        &format!("GET /minio/health/ready HTTP/1.1\r\nHost: {console_address}\r\n\r\n"),
    );
    assert_eq!(status, 200);

    handle.shutdown().expect("shutdown server");
    unsafe {
        if let Some(value) = previous_console {
            std::env::set_var("MINIO_CONSOLE_ADDRESS", value);
        } else {
            std::env::remove_var("MINIO_CONSOLE_ADDRESS");
        }
    }
}

#[test]
fn server_handles_admin_iam_and_sts_routes() {
    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let when = DateTime::<Utc>::from_timestamp(1_713_654_500, 0).expect("timestamp");

    let import_body = serde_json::to_vec(&ExportedIam {
        users: Vec::new(),
        groups: vec![StsGroup {
            name: "cn=ops,dc=example,dc=com".to_string(),
            policies: ["openid-read".to_string()].into_iter().collect(),
        }],
        policies: vec![StsPolicy {
            name: "openid-read".to_string(),
            allow_actions: ["s3:GetObject".to_string()].into_iter().collect(),
            deny_actions: BTreeSet::new(),
            resource_patterns: vec!["arn:aws:s3:::data/*".to_string()],
        }],
        ldap_config: LdapConfig::default(),
    })
    .expect("encode iam import");
    let mut import_req = new_test_request(
        "POST",
        &format!("http://{}/minio/admin/v3/iam/import", handle.address()),
        import_body.len() as i64,
        Some(&import_body),
    )
    .expect("iam import request");
    import_req.set_header("content-type", "application/json");
    sign_request_v4_standard(
        &mut import_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign iam import");
    let (status, _, body) = send_test_request(handle.address(), &import_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("iam import body");
    assert_eq!(body["groups"].as_u64(), Some(1));
    assert_eq!(body["policies"].as_u64(), Some(1));

    let openid_body = serde_json::to_vec(&OpenIdProvider {
        name: "dex".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([(
            "writer".to_string(),
            ["openid-read".to_string()].into_iter().collect(),
        )]),
    })
    .expect("encode openid provider");
    let mut openid_req = new_test_request(
        "PUT",
        &format!("http://{}/minio/admin/v3/idp/openid/add", handle.address()),
        openid_body.len() as i64,
        Some(&openid_body),
    )
    .expect("openid request");
    openid_req.set_header("content-type", "application/json");
    sign_request_v4_standard(
        &mut openid_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign openid request");
    let (status, _, body) = send_test_request(handle.address(), &openid_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("openid body");
    assert_eq!(body["provider"].as_str(), Some("dex"));

    let ldap_body = serde_json::to_vec(&LdapConfig {
        normalized_base_dn: true,
    })
    .expect("encode ldap config");
    let mut ldap_req = new_test_request(
        "PUT",
        &format!("http://{}/minio/admin/v3/idp/ldap/config", handle.address()),
        ldap_body.len() as i64,
        Some(&ldap_body),
    )
    .expect("ldap request");
    ldap_req.set_header("content-type", "application/json");
    sign_request_v4_standard(&mut ldap_req, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign ldap request");
    let (status, _, body) = send_test_request(handle.address(), &ldap_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("ldap body");
    assert_eq!(body["normalizedBaseDn"].as_bool(), Some(true));

    let add_user_body = br#"{"secretKey":"alice-secret","status":"enabled"}"#.to_vec();
    let mut add_user_req = new_test_request(
        "PUT",
        &format!(
            "http://{}/minio/admin/v3/add-user?accessKey=alice",
            handle.address()
        ),
        add_user_body.len() as i64,
        Some(&add_user_body),
    )
    .expect("identity add user request");
    add_user_req.set_header("content-type", "application/json");
    sign_request_v4_standard(
        &mut add_user_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign identity add user");
    let (status, _, body) = send_test_request(handle.address(), &add_user_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("identity user body");
    assert_eq!(body["accessKey"].as_str(), Some("alice"));

    let canned_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::tenant/*"]
        }]
    });
    let canned_policy_body = serde_json::to_vec(&canned_policy).expect("canned policy body");
    let mut add_policy_req = new_test_request(
        "PUT",
        &format!(
            "http://{}/minio/admin/v3/policy/add?name=tenant-read",
            handle.address()
        ),
        canned_policy_body.len() as i64,
        Some(&canned_policy_body),
    )
    .expect("add policy request");
    add_policy_req.set_header("content-type", "application/json");
    sign_request_v4_standard(
        &mut add_policy_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign add policy");
    let (status, _, body) = send_test_request(handle.address(), &add_policy_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("add policy body");
    assert_eq!(body["name"].as_str(), Some("tenant-read"));

    let mut attach_policy_req = new_test_request(
        "PUT",
        &format!(
            "http://{}/minio/admin/v3/policy/attach?accessKey=alice&policy=tenant-read",
            handle.address()
        ),
        0,
        None,
    )
    .expect("attach policy request");
    sign_request_v4_standard(
        &mut attach_policy_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign attach policy");
    let (status, _, body) = send_test_request(handle.address(), &attach_policy_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("attach policy body");
    assert_eq!(body["policy"].as_str(), Some("tenant-read"));

    let service_account_body = serde_json::json!({
        "access_key": "svc-alice",
        "secret_key": "svc-alice-secret",
        "session_policy_json": {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": ["arn:aws:s3:::tenant/private/*"]
            }]
        }
    })
    .to_string()
    .into_bytes();
    let mut add_service_account_req = new_test_request(
        "PUT",
        &format!(
            "http://{}/minio/admin/v3/service-account/add?targetUser=alice",
            handle.address()
        ),
        service_account_body.len() as i64,
        Some(&service_account_body),
    )
    .expect("add service account request");
    add_service_account_req.set_header("content-type", "application/json");
    sign_request_v4_standard(
        &mut add_service_account_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign add service account");
    let (status, _, body) = send_test_request(handle.address(), &add_service_account_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("add service account body");
    assert_eq!(body["access_key"].as_str(), Some("svc-alice"));

    let mut list_policies_req = new_test_request(
        "GET",
        &format!("http://{}/minio/admin/v3/policy/list", handle.address()),
        0,
        None,
    )
    .expect("list policies request");
    sign_request_v4_standard(
        &mut list_policies_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign list policies");
    let (status, _, body) = send_test_request(handle.address(), &list_policies_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("list policies body");
    assert!(body["policies"]
        .as_array()
        .is_some_and(|items| items.iter().any(|item| item["name"] == "tenant-read")));

    let mut list_service_accounts_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/service-account/list?targetUser=alice",
            handle.address()
        ),
        0,
        None,
    )
    .expect("list service accounts request");
    sign_request_v4_standard(
        &mut list_service_accounts_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign list service accounts");
    let (status, _, body) = send_test_request(handle.address(), &list_service_accounts_req);
    assert_eq!(status, 200);
    let body: serde_json::Value =
        serde_json::from_slice(&body).expect("list service accounts body");
    assert_eq!(body["accounts"].as_array().map(Vec::len), Some(1));

    let update_service_account_body = serde_json::json!({
        "secret_key": "svc-alice-secret-2",
        "status": "disabled"
    })
    .to_string()
    .into_bytes();
    let mut update_service_account_req = new_test_request(
        "POST",
        &format!(
            "http://{}/minio/admin/v3/service-account/update?accessKey=svc-alice",
            handle.address()
        ),
        update_service_account_body.len() as i64,
        Some(&update_service_account_body),
    )
    .expect("update service account request");
    update_service_account_req.set_header("content-type", "application/json");
    sign_request_v4_standard(
        &mut update_service_account_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign update service account");
    let (status, _, _) = send_test_request(handle.address(), &update_service_account_req);
    assert_eq!(status, 200);

    let mut export_req = new_test_request(
        "GET",
        &format!("http://{}/minio/admin/v3/iam/export", handle.address()),
        0,
        None,
    )
    .expect("iam export request");
    sign_request_v4_standard(
        &mut export_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign iam export");
    let (status, _, body) = send_test_request(handle.address(), &export_req);
    assert_eq!(status, 200);
    let exported: serde_json::Value = serde_json::from_slice(&body).expect("iam export body");
    assert_eq!(exported["groups"].as_array().map(Vec::len), Some(1));
    assert_eq!(exported["policies"].as_array().map(Vec::len), Some(1));

    let mut root_req = new_test_request(
        "POST",
        &format!("http://{}/minio/sts/v1/assume-role/root", handle.address()),
        0,
        None,
    )
    .expect("root sts request");
    sign_request_v4_standard(&mut root_req, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign root sts");
    let (status, _, body) = send_test_request(handle.address(), &root_req);
    assert_eq!(status, 200);
    let root_creds: serde_json::Value = serde_json::from_slice(&body).expect("root sts body");
    assert_eq!(root_creds["source"].as_str(), Some("internal"));

    let user_assume_body = serde_json::json!({
        "username": "alice",
        "secret_key": "alice-secret",
        "tags": { "tenant": "acme" }
    })
    .to_string()
    .into_bytes();
    let mut user_assume_req = new_test_request(
        "POST",
        &format!("http://{}/minio/sts/v1/assume-role/user", handle.address()),
        user_assume_body.len() as i64,
        Some(&user_assume_body),
    )
    .expect("user sts request");
    user_assume_req.set_header("content-type", "application/json");
    let (status, _, body) = send_test_request(handle.address(), &user_assume_req);
    assert_eq!(status, 200);
    let user_creds: serde_json::Value = serde_json::from_slice(&body).expect("user sts body");
    assert_eq!(user_creds["username"].as_str(), Some("alice"));
    assert_eq!(user_creds["source"].as_str(), Some("internal"));
    assert_eq!(user_creds["tags"]["tenant"].as_str(), Some("acme"));
    let session_access_key = user_creds["access_key"]
        .as_str()
        .expect("session access key")
        .to_string();

    let mut revoke_sts_req = new_test_request(
        "POST",
        &format!(
            "http://{}/minio/admin/v3/sts/revoke?accessKey={session_access_key}",
            handle.address()
        ),
        0,
        None,
    )
    .expect("revoke sts request");
    sign_request_v4_standard(
        &mut revoke_sts_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign revoke sts");
    let (status, _, body) = send_test_request(handle.address(), &revoke_sts_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("revoke sts body");
    assert_eq!(body["revoked"].as_bool(), Some(true));

    let openid_assume_body = serde_json::json!({
        "provider_name": "dex",
        "claims": {
            "subject": "sub-1",
            "preferred_username": "alice",
            "roles": ["writer"],
            "custom": {}
        },
        "requested_role": null
    })
    .to_string()
    .into_bytes();
    let mut openid_assume_req = new_test_request(
        "POST",
        &format!(
            "http://{}/minio/sts/v1/assume-role/openid",
            handle.address()
        ),
        openid_assume_body.len() as i64,
        Some(&openid_assume_body),
    )
    .expect("openid sts request");
    openid_assume_req.set_header("content-type", "application/json");
    let (status, _, body) = send_test_request(handle.address(), &openid_assume_req);
    assert_eq!(status, 200);
    let openid_creds: serde_json::Value = serde_json::from_slice(&body).expect("openid sts body");
    assert_eq!(openid_creds["source"].as_str(), Some("openid"));
    assert_eq!(openid_creds["username"].as_str(), Some("alice"));

    let ldap_assume_body = serde_json::json!({
        "username": "bob",
        "dn": "cn=bob,dc=example,dc=com",
        "group_dns": ["cn=ops,dc=example,dc=com"]
    })
    .to_string()
    .into_bytes();
    let mut ldap_assume_req = new_test_request(
        "POST",
        &format!("http://{}/minio/sts/v1/assume-role/ldap", handle.address()),
        ldap_assume_body.len() as i64,
        Some(&ldap_assume_body),
    )
    .expect("ldap sts request");
    ldap_assume_req.set_header("content-type", "application/json");
    let (status, _, body) = send_test_request(handle.address(), &ldap_assume_req);
    assert_eq!(status, 200);
    let ldap_creds: serde_json::Value = serde_json::from_slice(&body).expect("ldap sts body");
    assert_eq!(ldap_creds["source"].as_str(), Some("ldap"));
    assert_eq!(ldap_creds["username"].as_str(), Some("bob"));

    let mut detach_policy_req = new_test_request(
        "PUT",
        &format!(
            "http://{}/minio/admin/v3/policy/detach?accessKey=alice&policy=tenant-read",
            handle.address()
        ),
        0,
        None,
    )
    .expect("detach policy request");
    sign_request_v4_standard(
        &mut detach_policy_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign detach policy");
    let (status, _, _) = send_test_request(handle.address(), &detach_policy_req);
    assert_eq!(status, 200);

    let mut remove_policy_req = new_test_request(
        "DELETE",
        &format!(
            "http://{}/minio/admin/v3/policy/remove?name=tenant-read",
            handle.address()
        ),
        0,
        None,
    )
    .expect("remove policy request");
    sign_request_v4_standard(
        &mut remove_policy_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign remove policy");
    let (status, _, _) = send_test_request(handle.address(), &remove_policy_req);
    assert_eq!(status, 204);

    let mut remove_service_account_req = new_test_request(
        "DELETE",
        &format!(
            "http://{}/minio/admin/v3/service-account/remove?accessKey=svc-alice",
            handle.address()
        ),
        0,
        None,
    )
    .expect("remove service account request");
    sign_request_v4_standard(
        &mut remove_service_account_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign remove service account");
    let (status, _, _) = send_test_request(handle.address(), &remove_service_account_req);
    assert_eq!(status, 204);

    let mut list_service_accounts_after_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/service-account/list?targetUser=alice",
            handle.address()
        ),
        0,
        None,
    )
    .expect("list service accounts after remove request");
    sign_request_v4_standard(
        &mut list_service_accounts_after_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign list service accounts after remove");
    let (status, _, body) = send_test_request(handle.address(), &list_service_accounts_after_req);
    assert_eq!(status, 200);
    let body: serde_json::Value =
        serde_json::from_slice(&body).expect("list service accounts after body");
    assert_eq!(body["accounts"].as_array().map(Vec::len), Some(0));

    handle.shutdown().expect("shutdown server");
}

#[test]
fn server_reports_kms_status_and_bucket_key_resolution() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_secret = std::env::var(crate::internal::kms::ENV_KMS_SECRET_KEY).ok();
    let previous_default = std::env::var(crate::internal::kms::ENV_KMS_DEFAULT_KEY).ok();
    unsafe {
        std::env::set_var(
            crate::internal::kms::ENV_KMS_SECRET_KEY,
            "my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=",
        );
        std::env::remove_var(crate::internal::kms::ENV_KMS_DEFAULT_KEY);
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: DEFAULT_ROOT_USER.to_string(),
        root_password: DEFAULT_ROOT_PASSWORD.to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let encryption_xml = r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>arn:aws:kms:bucket-key</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#;
    let mut put_bucket = new_test_request(
        "PUT",
        &format!("http://{}/secure-bucket", handle.address()),
        0,
        None,
    )
    .expect("put bucket request");
    let when = DateTime::<Utc>::from_timestamp(1_730_000_000, 0).expect("timestamp");
    sign_request_v4_standard(
        &mut put_bucket,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign bucket create");
    let (status, _, _) = send_test_request(handle.address(), &put_bucket);
    assert_eq!(status, 200);

    let mut put_encryption = new_test_request(
        "PUT",
        &format!("http://{}/secure-bucket?encryption", handle.address()),
        encryption_xml.len() as i64,
        Some(encryption_xml.as_bytes()),
    )
    .expect("put encryption request");
    sign_request_v4_standard(
        &mut put_encryption,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign encryption request");
    let (status, _, _) = send_test_request(handle.address(), &put_encryption);
    assert_eq!(status, 200);

    let mut status_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/kms/status?bucket=secure-bucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("kms status request");
    sign_request_v4_standard(
        &mut status_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign kms status");
    let (status, _, body) = send_test_request(handle.address(), &status_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("kms status body");
    assert_eq!(body["configured"].as_bool(), Some(true));
    assert_eq!(body["backend"].as_str(), Some("StaticKey"));
    assert_eq!(body["default_key"].as_str(), Some("my-key"));
    assert_eq!(body["bucket"].as_str(), Some("secure-bucket"));
    assert_eq!(body["bucketKey"]["key_id"].as_str(), Some("bucket-key"));
    assert_eq!(body["bucketKey"]["source"].as_str(), Some("BucketConfig"));

    handle.shutdown().expect("shutdown server");
    unsafe {
        if let Some(value) = previous_secret {
            std::env::set_var(crate::internal::kms::ENV_KMS_SECRET_KEY, value);
        } else {
            std::env::remove_var(crate::internal::kms::ENV_KMS_SECRET_KEY);
        }
        if let Some(value) = previous_default {
            std::env::set_var(crate::internal::kms::ENV_KMS_DEFAULT_KEY, value);
        } else {
            std::env::remove_var(crate::internal::kms::ENV_KMS_DEFAULT_KEY);
        }
    }
}

#[test]
fn server_reports_kms_key_status_and_create_behavior() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_secret = std::env::var(crate::internal::kms::ENV_KMS_SECRET_KEY).ok();
    unsafe {
        std::env::set_var(
            crate::internal::kms::ENV_KMS_SECRET_KEY,
            "my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=",
        );
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: DEFAULT_ROOT_USER.to_string(),
        root_password: DEFAULT_ROOT_PASSWORD.to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let when = DateTime::<Utc>::from_timestamp(1_730_000_100, 0).expect("timestamp");

    let mut key_status = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/kms/key/status?key-id=my-key",
            handle.address()
        ),
        0,
        None,
    )
    .expect("kms key status request");
    sign_request_v4_standard(
        &mut key_status,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign key status");
    let (status, _, body) = send_test_request(handle.address(), &key_status);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("key status body");
    assert_eq!(body["key_id"].as_str(), Some("my-key"));
    assert_eq!(body["exists"].as_bool(), Some(true));
    assert_eq!(body["validation_succeeded"].as_bool(), Some(true));

    let mut create_default = new_test_request(
        "POST",
        &format!(
            "http://{}/minio/admin/v3/kms/key/create?key-id=my-key",
            handle.address()
        ),
        0,
        None,
    )
    .expect("kms key create request");
    sign_request_v4_standard(
        &mut create_default,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign create default key");
    let (status, _, body) = send_test_request(handle.address(), &create_default);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("create key body");
    assert_eq!(body["key_id"].as_str(), Some("my-key"));
    assert_eq!(body["exists"].as_bool(), Some(true));

    let mut create_other = new_test_request(
        "POST",
        &format!(
            "http://{}/minio/admin/v3/kms/key/create?key-id=other-key",
            handle.address()
        ),
        0,
        None,
    )
    .expect("kms other key create request");
    sign_request_v4_standard(
        &mut create_other,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign create other key");
    let (status, _, body) = send_test_request(handle.address(), &create_other);
    assert_eq!(status, 400);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("create other body");
    assert!(body["error"]
        .as_str()
        .is_some_and(|value| value.contains("cannot create additional keys")));

    handle.shutdown().expect("shutdown server");
    unsafe {
        if let Some(value) = previous_secret {
            std::env::set_var(crate::internal::kms::ENV_KMS_SECRET_KEY, value);
        } else {
            std::env::remove_var(crate::internal::kms::ENV_KMS_SECRET_KEY);
        }
    }
}

#[test]
fn server_exposes_public_kms_router() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_secret = std::env::var(crate::internal::kms::ENV_KMS_SECRET_KEY).ok();
    unsafe {
        std::env::set_var(
            crate::internal::kms::ENV_KMS_SECRET_KEY,
            "my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=",
        );
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: DEFAULT_ROOT_USER.to_string(),
        root_password: DEFAULT_ROOT_PASSWORD.to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let when = DateTime::<Utc>::from_timestamp(1_730_000_200, 0).expect("timestamp");

    let mut version_req = new_test_request(
        "GET",
        &format!("http://{}/minio/kms/v1/version", handle.address()),
        0,
        None,
    )
    .expect("kms version request");
    sign_request_v4_standard(
        &mut version_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign kms version");
    let (status, _, body) = send_test_request(handle.address(), &version_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("kms version body");
    assert_eq!(body["version"].as_str(), Some("minio-rust-kms/v1"));

    let mut apis_req = new_test_request(
        "GET",
        &format!("http://{}/minio/kms/v1/apis", handle.address()),
        0,
        None,
    )
    .expect("kms apis request");
    sign_request_v4_standard(&mut apis_req, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign kms apis");
    let (status, _, body) = send_test_request(handle.address(), &apis_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("kms apis body");
    assert!(body["endpoints"].as_array().is_some_and(|items| items
        .iter()
        .any(|item| item["path"] == "/minio/kms/v1/key/status")));
    assert!(body["endpoints"].as_array().is_some_and(|items| items
        .iter()
        .any(|item| item["path"] == "/minio/kms/v1/key/list")));
    assert!(body["endpoints"].as_array().is_some_and(|items| items
        .iter()
        .any(|item| item["path"] == "/minio/kms/v1/key/generate")));
    assert!(body["endpoints"].as_array().is_some_and(|items| items
        .iter()
        .any(|item| item["path"] == "/minio/kms/v1/key/decrypt")));

    let mut metrics_req = new_test_request(
        "GET",
        &format!("http://{}/minio/kms/v1/metrics", handle.address()),
        0,
        None,
    )
    .expect("kms metrics request");
    sign_request_v4_standard(
        &mut metrics_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign kms metrics");
    let (status, _, body) = send_test_request(handle.address(), &metrics_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("kms metrics body");
    assert_eq!(body["online"].as_bool(), Some(true));
    assert_eq!(body["default_key"].as_str(), Some("my-key"));

    let mut list_keys_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/kms/v1/key/list?pattern=my*",
            handle.address()
        ),
        0,
        None,
    )
    .expect("kms key list request");
    sign_request_v4_standard(
        &mut list_keys_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign kms key list");
    let (status, _, body) = send_test_request(handle.address(), &list_keys_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("kms key list body");
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(body[0]["key_id"].as_str(), Some("my-key"));

    let mut key_status_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/kms/v1/key/status?key-id=my-key",
            handle.address()
        ),
        0,
        None,
    )
    .expect("public kms key status request");
    sign_request_v4_standard(
        &mut key_status_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign public key status");
    let (status, _, body) = send_test_request(handle.address(), &key_status_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("public key status body");
    assert_eq!(body["key_id"].as_str(), Some("my-key"));
    assert_eq!(body["exists"].as_bool(), Some(true));

    let generate_body = serde_json::json!({
        "key_id": "my-key",
        "associated_data": { "scope": "cluster" }
    })
    .to_string();
    let mut generate_req = new_test_request(
        "POST",
        &format!("http://{}/minio/kms/v1/key/generate", handle.address()),
        generate_body.len() as i64,
        Some(generate_body.as_bytes()),
    )
    .expect("public kms key generate request");
    sign_request_v4_standard(
        &mut generate_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign public key generate");
    let (status, _, body) = send_test_request(handle.address(), &generate_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("public key generate body");
    assert_eq!(body["key_id"].as_str(), Some("my-key"));
    assert!(body["plaintext"].as_str().is_some());
    assert!(body["ciphertext"].as_str().is_some());

    let decrypt_body = serde_json::json!({
        "key_id": "my-key",
        "version": body["version"].as_i64().unwrap_or_default(),
        "ciphertext": body["ciphertext"].as_str().unwrap_or_default(),
        "associated_data": { "scope": "cluster" }
    })
    .to_string();
    let expected_plaintext = body["plaintext"].as_str().unwrap_or_default().to_string();
    let mut decrypt_req = new_test_request(
        "POST",
        &format!("http://{}/minio/kms/v1/key/decrypt", handle.address()),
        decrypt_body.len() as i64,
        Some(decrypt_body.as_bytes()),
    )
    .expect("public kms key decrypt request");
    sign_request_v4_standard(
        &mut decrypt_req,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign public key decrypt");
    let (status, _, body) = send_test_request(handle.address(), &decrypt_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("public key decrypt body");
    assert_eq!(
        body["plaintext"].as_str(),
        Some(expected_plaintext.as_str())
    );

    handle.shutdown().expect("shutdown server");
    unsafe {
        if let Some(value) = previous_secret {
            std::env::set_var(crate::internal::kms::ENV_KMS_SECRET_KEY, value);
        } else {
            std::env::remove_var(crate::internal::kms::ENV_KMS_SECRET_KEY);
        }
    }
}

#[test]
fn server_accepts_bearer_api_key_for_public_kms_router() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_secret = std::env::var(crate::internal::kms::ENV_KMS_SECRET_KEY).ok();
    let previous_server = std::env::var(crate::internal::kms::ENV_KMS_ENDPOINT).ok();
    let previous_enclave = std::env::var(crate::internal::kms::ENV_KMS_ENCLAVE).ok();
    let previous_default_key = std::env::var(crate::internal::kms::ENV_KMS_DEFAULT_KEY).ok();
    let previous_api_key = std::env::var(crate::internal::kms::ENV_KMS_API_KEY).ok();

    unsafe {
        std::env::set_var(
            crate::internal::kms::ENV_KMS_SECRET_KEY,
            "my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=",
        );
        std::env::remove_var(crate::internal::kms::ENV_KMS_ENDPOINT);
        std::env::remove_var(crate::internal::kms::ENV_KMS_ENCLAVE);
        std::env::remove_var(crate::internal::kms::ENV_KMS_DEFAULT_KEY);
        std::env::remove_var(crate::internal::kms::ENV_KMS_API_KEY);
    }

    let inner_tempdir = new_test_tempdir();
    let inner_disk = inner_tempdir.path().join("disk1");
    let inner = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: DEFAULT_ROOT_USER.to_string(),
        root_password: DEFAULT_ROOT_PASSWORD.to_string(),
        disks: vec![inner_disk],
    })
    .expect("spawn inner server");

    unsafe {
        std::env::remove_var(crate::internal::kms::ENV_KMS_SECRET_KEY);
        std::env::set_var(
            crate::internal::kms::ENV_KMS_ENDPOINT,
            format!("http://minioadmin:minioadmin@{}", inner.address()),
        );
        std::env::set_var(crate::internal::kms::ENV_KMS_ENCLAVE, "minio-rust");
        std::env::set_var(crate::internal::kms::ENV_KMS_DEFAULT_KEY, "my-key");
        std::env::set_var(crate::internal::kms::ENV_KMS_API_KEY, "kms-router-token");
    }

    let outer_tempdir = new_test_tempdir();
    let outer_disk = outer_tempdir.path().join("disk1");
    let outer = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: DEFAULT_ROOT_USER.to_string(),
        root_password: DEFAULT_ROOT_PASSWORD.to_string(),
        disks: vec![outer_disk],
    })
    .expect("spawn outer server");

    let mut public_req = new_test_request(
        "GET",
        &format!("http://{}/minio/kms/v1/status", outer.address()),
        0,
        None,
    )
    .expect("public kms status request");
    public_req.set_header("Authorization", "Bearer kms-router-token");
    let (status, _, body) = send_test_request(outer.address(), &public_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("public kms status body");
    assert_eq!(body["backend"].as_str(), Some("MinioKms"));
    assert_eq!(body["default_key"].as_str(), Some("my-key"));

    let mut list_req = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/kms/v1/key/list?pattern=my*",
            outer.address()
        ),
        0,
        None,
    )
    .expect("public kms key list request");
    list_req.set_header("Authorization", "Bearer kms-router-token");
    let (status, _, body) = send_test_request(outer.address(), &list_req);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("public kms key list body");
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(body[0]["key_id"].as_str(), Some("my-key"));

    let generate_body = serde_json::json!({
        "key_id": "my-key",
        "associated_data": { "scope": "cluster" }
    })
    .to_string();
    let mut generate_req = new_test_request(
        "POST",
        &format!("http://{}/minio/kms/v1/key/generate", outer.address()),
        generate_body.len() as i64,
        Some(generate_body.as_bytes()),
    )
    .expect("public kms key generate request");
    generate_req.set_header("Authorization", "Bearer kms-router-token");
    let (status, _, body) = send_test_request(outer.address(), &generate_req);
    assert_eq!(status, 200);
    let body: serde_json::Value =
        serde_json::from_slice(&body).expect("public kms key generate body");
    assert_eq!(body["key_id"].as_str(), Some("my-key"));
    assert!(body["plaintext"].as_str().is_some());
    assert!(body["ciphertext"].as_str().is_some());

    let decrypt_body = serde_json::json!({
        "key_id": "my-key",
        "version": body["version"].as_i64().unwrap_or_default(),
        "ciphertext": body["ciphertext"].as_str().unwrap_or_default(),
        "associated_data": { "scope": "cluster" }
    })
    .to_string();
    let expected_plaintext = body["plaintext"].as_str().unwrap_or_default().to_string();
    let mut decrypt_req = new_test_request(
        "POST",
        &format!("http://{}/minio/kms/v1/key/decrypt", outer.address()),
        decrypt_body.len() as i64,
        Some(decrypt_body.as_bytes()),
    )
    .expect("public kms key decrypt request");
    decrypt_req.set_header("Authorization", "Bearer kms-router-token");
    let (status, _, body) = send_test_request(outer.address(), &decrypt_req);
    assert_eq!(status, 200);
    let body: serde_json::Value =
        serde_json::from_slice(&body).expect("public kms key decrypt body");
    assert_eq!(
        body["plaintext"].as_str(),
        Some(expected_plaintext.as_str())
    );

    let mut admin_req = new_test_request(
        "GET",
        &format!("http://{}/minio/admin/v3/kms/status", outer.address()),
        0,
        None,
    )
    .expect("admin kms status request");
    admin_req.set_header("Authorization", "Bearer kms-router-token");
    let (status, _, _) = send_test_request(outer.address(), &admin_req);
    assert_eq!(status, 403);

    outer.shutdown().expect("shutdown outer");
    inner.shutdown().expect("shutdown inner");
    unsafe {
        if let Some(value) = previous_secret {
            std::env::set_var(crate::internal::kms::ENV_KMS_SECRET_KEY, value);
        } else {
            std::env::remove_var(crate::internal::kms::ENV_KMS_SECRET_KEY);
        }
        if let Some(value) = previous_server {
            std::env::set_var(crate::internal::kms::ENV_KMS_ENDPOINT, value);
        } else {
            std::env::remove_var(crate::internal::kms::ENV_KMS_ENDPOINT);
        }
        if let Some(value) = previous_enclave {
            std::env::set_var(crate::internal::kms::ENV_KMS_ENCLAVE, value);
        } else {
            std::env::remove_var(crate::internal::kms::ENV_KMS_ENCLAVE);
        }
        if let Some(value) = previous_default_key {
            std::env::set_var(crate::internal::kms::ENV_KMS_DEFAULT_KEY, value);
        } else {
            std::env::remove_var(crate::internal::kms::ENV_KMS_DEFAULT_KEY);
        }
        if let Some(value) = previous_api_key {
            std::env::set_var(crate::internal::kms::ENV_KMS_API_KEY, value);
        } else {
            std::env::remove_var(crate::internal::kms::ENV_KMS_API_KEY);
        }
    }
}

#[test]
fn server_delivers_notifications_to_webhook() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind webhook listener");
    let webhook_addr = listener.local_addr().expect("webhook addr");
    let webhook_server = Server::from_listener(listener, None).expect("webhook server");
    let (tx, rx) = mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let mut request = webhook_server
            .recv_timeout(Duration::from_secs(10))
            .expect("recv timeout")
            .expect("receive webhook request");
        let auth = request
            .headers()
            .iter()
            .find(|header| {
                header
                    .field
                    .as_str()
                    .to_string()
                    .eq_ignore_ascii_case("authorization")
            })
            .map(|header| header.value.as_str().to_string())
            .unwrap_or_default();
        let mut body = Vec::new();
        request
            .as_reader()
            .read_to_end(&mut body)
            .expect("read webhook body");
        request
            .respond(Response::from_string("ok").with_status_code(StatusCode(200)))
            .expect("respond webhook");
        tx.send((auth, body)).expect("send webhook payload");
    });

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server_with_webhook_targets(
        MinioServerConfig {
            address: "127.0.0.1:0".to_string(),
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            disks: vec![disk],
        },
        BTreeMap::from([(
            "1:webhook".to_string(),
            WebhookNotificationTarget {
                target_id: crate::internal::event::TargetId::new("1", "webhook"),
                endpoint: format!("http://{}/minio/events", webhook_addr),
                auth_token: "secret-token".to_string(),
            },
        )]),
    )
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /hookbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>hook</Id><Queue>arn:minio:sqs:us-east-1:1:webhook</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /hookbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"webhook payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /hookbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let (auth, body) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("webhook payload received");
    assert_eq!(auth, "Bearer secret-token");
    let body = String::from_utf8(body).expect("webhook body utf8");
    assert!(body.contains("s3:ObjectCreated:Put"));
    assert!(body.contains("hookbucket/object.txt"), "{body}");

    let when = DateTime::<Utc>::from_timestamp(1_713_654_000, 0).expect("timestamp");
    let mut deliveries = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/webhook-deliveries",
            handle.address()
        ),
        0,
        None,
    )
    .expect("webhook deliveries request");
    sign_request_v4_standard(
        &mut deliveries,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign deliveries");
    let (status, _, body) = send_test_request(handle.address(), &deliveries);
    assert_eq!(status, 200);
    let body = String::from_utf8(body).expect("deliveries body");
    assert!(body.contains("\"delivered\":true"));
    assert!(body.contains("1:webhook"));

    handle.shutdown().expect("shutdown server");
    receiver.join().expect("join receiver");
}

#[test]
fn server_rehydrates_persisted_notification_history_after_restart() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind webhook listener");
    let webhook_addr = listener.local_addr().expect("webhook addr");
    let webhook_server = Server::from_listener(listener, None).expect("webhook server");
    let (tx, rx) = mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let mut request = webhook_server
            .recv_timeout(Duration::from_secs(10))
            .expect("recv timeout")
            .expect("receive webhook request");
        let mut body = Vec::new();
        request
            .as_reader()
            .read_to_end(&mut body)
            .expect("read webhook body");
        request
            .respond(Response::from_string("ok").with_status_code(StatusCode(200)))
            .expect("respond webhook");
        tx.send(body).expect("send webhook payload");
    });

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let webhook_targets = BTreeMap::from([(
        "1:webhook".to_string(),
        WebhookNotificationTarget {
            target_id: crate::internal::event::TargetId::new("1", "webhook"),
            endpoint: format!("http://{}/minio/events", webhook_addr),
            auth_token: String::new(),
        },
    )]);

    let handle = spawn_server_with_webhook_targets(
        MinioServerConfig {
            address: "127.0.0.1:0".to_string(),
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            disks: vec![disk.clone()],
        },
        webhook_targets.clone(),
    )
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /persistbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>hook</Id><Queue>arn:minio:sqs:us-east-1:1:webhook</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /persistbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"persisted webhook payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /persistbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);
    let _ = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("webhook payload received");

    assert!(disk.join(NOTIFICATION_HISTORY_FILE).exists());
    handle.shutdown().expect("shutdown server");
    receiver.join().expect("join receiver");

    let restarted = spawn_server_with_webhook_targets(
        MinioServerConfig {
            address: "127.0.0.1:0".to_string(),
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            disks: vec![disk],
        },
        webhook_targets,
    )
    .expect("restart server");

    let when = DateTime::<Utc>::from_timestamp(1_730_000_600, 0).expect("timestamp");
    let mut notifications = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/notifications?bucket=persistbucket",
            restarted.address()
        ),
        0,
        None,
    )
    .expect("notifications request");
    sign_request_v4_standard(
        &mut notifications,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign notifications");
    let (status, _, body) = send_test_request(restarted.address(), &notifications);
    assert_eq!(status, 200);
    let body = String::from_utf8(body).expect("notifications body");
    assert!(body.contains("persistbucket"));
    assert!(body.contains("object.txt"));

    let mut deliveries = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/webhook-deliveries?bucket=persistbucket",
            restarted.address()
        ),
        0,
        None,
    )
    .expect("webhook deliveries request");
    sign_request_v4_standard(
        &mut deliveries,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign deliveries");
    let (status, _, body) = send_test_request(restarted.address(), &deliveries);
    assert_eq!(status, 200);
    let body = String::from_utf8(body).expect("deliveries body");
    assert!(body.contains("1:webhook"));
    assert!(body.contains("\"delivered\":true"));

    restarted.shutdown().expect("shutdown restarted server");
}

#[test]
fn server_records_notifications_to_queue_targets() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_enable = std::env::var("MINIO_NOTIFY_QUEUE_ENABLE").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_QUEUE_ENABLE", "on");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /queuebucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>queue</Id><Queue>arn:minio:sqs:us-east-1:1:queue</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /queuebucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"queue payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /queuebucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let when = DateTime::<Utc>::from_timestamp(1_730_000_300, 0).expect("timestamp");
    let mut queue_deliveries = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/queue-deliveries?bucket=queuebucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("queue deliveries request");
    sign_request_v4_standard(
        &mut queue_deliveries,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign queue deliveries");
    let (status, _, body) = send_test_request(handle.address(), &queue_deliveries);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("queue deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["target_id"].as_str(), Some("1:queue"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("queuebucket"));
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert_eq!(
        body["records"][0]["payload"]["EventName"].as_str(),
        Some("s3:ObjectCreated:Put")
    );

    handle.shutdown().expect("shutdown server");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_QUEUE_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_QUEUE_ENABLE");
        }
    }
}

#[test]
fn server_delivers_notifications_to_nats() {
    let _guard = env_lock().lock().expect("env lock");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind nats listener");
    let nats_addr = listener.local_addr().expect("nats addr");
    let (tx, rx) = mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept nats");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set nats read timeout");
        stream.write_all(b"INFO {}\r\n").expect("write nats info");
        let mut published = String::new();
        let mut body = vec![0u8; 8192];
        loop {
            match stream.read(&mut body) {
                Ok(0) => break,
                Ok(read) => {
                    published.push_str(&String::from_utf8_lossy(&body[..read]));
                    if published.contains("PUB minio.events ") {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("read nats publish: {error}"),
            }
        }
        tx.send(published).expect("send nats payload");
    });

    let previous_enable = std::env::var("MINIO_NOTIFY_NATS_ENABLE").ok();
    let previous_address = std::env::var("MINIO_NOTIFY_NATS_ADDRESS").ok();
    let previous_subject = std::env::var("MINIO_NOTIFY_NATS_SUBJECT").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_NATS_ENABLE", "on");
        std::env::set_var("MINIO_NOTIFY_NATS_ADDRESS", nats_addr.to_string());
        std::env::set_var("MINIO_NOTIFY_NATS_SUBJECT", "minio.events");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /natsbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>nats</Id><Queue>arn:minio:sqs:us-east-1:1:nats</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /natsbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"nats payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /natsbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let published = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("nats publish received");
    assert!(published.contains("CONNECT "), "{published}");
    assert!(published.contains("PUB minio.events "), "{published}");
    assert!(published.contains("s3:ObjectCreated:Put"), "{published}");
    assert!(published.contains("natsbucket/object.txt"), "{published}");

    let when = DateTime::<Utc>::from_timestamp(1_730_000_400, 0).expect("timestamp");
    let mut deliveries = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/nats-deliveries?bucket=natsbucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("nats deliveries request");
    sign_request_v4_standard(
        &mut deliveries,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign nats deliveries");
    let (status, _, body) = send_test_request(handle.address(), &deliveries);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("nats deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["targetId"].as_str(), Some("1:nats"));
    assert_eq!(body["records"][0]["subject"].as_str(), Some("minio.events"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("natsbucket"));
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert_eq!(body["records"][0]["delivered"].as_bool(), Some(true));

    handle.shutdown().expect("shutdown server");
    receiver.join().expect("join nats receiver");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_NATS_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NATS_ENABLE");
        }
        if let Some(value) = previous_address {
            std::env::set_var("MINIO_NOTIFY_NATS_ADDRESS", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NATS_ADDRESS");
        }
        if let Some(value) = previous_subject {
            std::env::set_var("MINIO_NOTIFY_NATS_SUBJECT", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NATS_SUBJECT");
        }
    }
}

#[test]
fn server_delivers_notifications_to_nats_tls_skip_verify() {
    let _guard = env_lock().lock().expect("env lock");
    ensure_rustls_provider();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind tls nats listener");
    let nats_addr = listener.local_addr().expect("tls nats addr");
    let cert = fixture("certs/nats_server_cert.pem");
    let key = fixture("certs/nats_server_key.pem");
    let (tx, rx) = mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept tls nats");
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(load_certificates(&cert), load_private_key(&key))
            .expect("server config");
        let connection = ServerConnection::new(Arc::new(config)).expect("server connection");
        let mut tls = StreamOwned::new(connection, stream);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).expect("handshake");
        }
        tls.write_all(b"INFO {}\r\n").expect("write nats info");
        tls.flush().expect("flush nats info");
        let mut published = String::new();
        let mut body = vec![0u8; 8192];
        loop {
            match tls.read(&mut body) {
                Ok(0) => break,
                Ok(read) => {
                    published.push_str(&String::from_utf8_lossy(&body[..read]));
                    if published.contains("PUB minio.events ") {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("read tls nats publish: {error}"),
            }
        }
        tx.send(published).expect("send tls nats payload");
    });

    let previous_enable = std::env::var("MINIO_NOTIFY_NATS_ENABLE").ok();
    let previous_address = std::env::var("MINIO_NOTIFY_NATS_ADDRESS").ok();
    let previous_subject = std::env::var("MINIO_NOTIFY_NATS_SUBJECT").ok();
    let previous_secure = std::env::var("MINIO_NOTIFY_NATS_SECURE").ok();
    let previous_skip_verify = std::env::var("MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_NATS_ENABLE", "on");
        std::env::set_var("MINIO_NOTIFY_NATS_ADDRESS", nats_addr.to_string());
        std::env::set_var("MINIO_NOTIFY_NATS_SUBJECT", "minio.events");
        std::env::set_var("MINIO_NOTIFY_NATS_SECURE", "on");
        std::env::set_var("MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY", "on");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /natssecure HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>nats</Id><Queue>arn:minio:sqs:us-east-1:1:nats</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /natssecure?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"tls nats payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /natssecure/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let published = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("tls nats publish received");
    assert!(published.contains("CONNECT "), "{published}");
    assert!(published.contains("PUB minio.events "), "{published}");
    assert!(published.contains("s3:ObjectCreated:Put"), "{published}");
    assert!(published.contains("natssecure/object.txt"), "{published}");

    let when = DateTime::<Utc>::from_timestamp(1_730_000_401, 0).expect("timestamp");
    let mut deliveries = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/nats-deliveries?bucket=natssecure",
            handle.address()
        ),
        0,
        None,
    )
    .expect("nats deliveries request");
    sign_request_v4_standard(
        &mut deliveries,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign nats deliveries");
    let (status, _, body) = send_test_request(handle.address(), &deliveries);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("nats deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["targetId"].as_str(), Some("1:nats"));
    assert_eq!(body["records"][0]["subject"].as_str(), Some("minio.events"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("natssecure"));
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert_eq!(body["records"][0]["delivered"].as_bool(), Some(true));

    handle.shutdown().expect("shutdown server");
    receiver.join().expect("join tls nats receiver");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_NATS_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NATS_ENABLE");
        }
        if let Some(value) = previous_address {
            std::env::set_var("MINIO_NOTIFY_NATS_ADDRESS", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NATS_ADDRESS");
        }
        if let Some(value) = previous_subject {
            std::env::set_var("MINIO_NOTIFY_NATS_SUBJECT", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NATS_SUBJECT");
        }
        if let Some(value) = previous_secure {
            std::env::set_var("MINIO_NOTIFY_NATS_SECURE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NATS_SECURE");
        }
        if let Some(value) = previous_skip_verify {
            std::env::set_var("MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY");
        }
    }
}

#[test]
fn server_delivers_notifications_to_elasticsearch() {
    let _guard = env_lock().lock().expect("env lock");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind elastic listener");
    let elastic_server = Server::from_listener(listener, None).expect("elastic server");
    let elastic_addr = elastic_server.server_addr().to_ip().expect("elastic addr");
    let (tx, rx) = mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let mut request = elastic_server.recv().expect("elastic request");
        let url = request.url().to_string();
        let auth = request
            .headers()
            .iter()
            .find(|header| {
                header
                    .field
                    .as_str()
                    .as_str()
                    .eq_ignore_ascii_case("authorization")
            })
            .map(|header| header.value.as_str().to_string())
            .unwrap_or_default();
        let mut body = Vec::new();
        request
            .as_reader()
            .read_to_end(&mut body)
            .expect("read elastic");
        request
            .respond(Response::from_string("{\"result\":\"created\"}"))
            .expect("respond elastic");
        tx.send((url, auth, body)).expect("send elastic payload");
    });

    let previous_enable = std::env::var("MINIO_NOTIFY_ELASTICSEARCH_ENABLE").ok();
    let previous_url = std::env::var("MINIO_NOTIFY_ELASTICSEARCH_URL").ok();
    let previous_index = std::env::var("MINIO_NOTIFY_ELASTICSEARCH_INDEX").ok();
    let previous_user = std::env::var("MINIO_NOTIFY_ELASTICSEARCH_USERNAME").ok();
    let previous_pass = std::env::var("MINIO_NOTIFY_ELASTICSEARCH_PASSWORD").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_ENABLE", "on");
        std::env::set_var(
            "MINIO_NOTIFY_ELASTICSEARCH_URL",
            format!("http://{}", elastic_addr),
        );
        std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_INDEX", "minio-events");
        std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_USERNAME", "elastic");
        std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_PASSWORD", "secret");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /elasticbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>elastic</Id><Queue>arn:minio:sqs:us-east-1:1:elasticsearch</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /elasticbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"elastic payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /elasticbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let (url, auth, body) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("elastic recv");
    assert_eq!(url, "/minio-events/_doc");
    assert!(auth.starts_with("Basic "), "{auth}");
    let body = String::from_utf8(body).expect("elastic body");
    assert!(body.contains("s3:ObjectCreated:Put"));
    assert!(body.contains("elasticbucket/object.txt"));

    let when = DateTime::<Utc>::from_timestamp(1_730_000_500, 0).expect("timestamp");
    let mut deliveries = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/elasticsearch-deliveries?bucket=elasticbucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("elastic deliveries request");
    sign_request_v4_standard(
        &mut deliveries,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign elastic deliveries");
    let (status, _, body) = send_test_request(handle.address(), &deliveries);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("elastic deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        body["records"][0]["targetId"].as_str(),
        Some("1:elasticsearch")
    );
    assert_eq!(body["records"][0]["index"].as_str(), Some("minio-events"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("elasticbucket"));

    handle.shutdown().expect("shutdown server");
    receiver.join().expect("join elastic receiver");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_ENABLE");
        }
        if let Some(value) = previous_url {
            std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_URL", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_URL");
        }
        if let Some(value) = previous_index {
            std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_INDEX", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_INDEX");
        }
        if let Some(value) = previous_user {
            std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_USERNAME", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_USERNAME");
        }
        if let Some(value) = previous_pass {
            std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_PASSWORD", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_PASSWORD");
        }
    }
}

#[test]
fn server_delivers_notifications_to_redis() {
    let _guard = env_lock().lock().expect("env lock");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind redis listener");
    let redis_addr = listener.local_addr().expect("redis addr");
    let (tx, rx) = mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept redis");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set redis timeout");
        let mut published = Vec::new();
        let mut body = vec![0u8; 8192];
        let mut replied_auth = false;
        loop {
            match stream.read(&mut body) {
                Ok(0) => break,
                Ok(read) => {
                    published.extend_from_slice(&body[..read]);
                    let text = String::from_utf8_lossy(&published);
                    if text.contains("AUTH") && !replied_auth {
                        stream.write_all(b"+OK\r\n").expect("auth ok");
                        replied_auth = true;
                    }
                    if text.contains("RPUSH") && text.contains("s3:ObjectCreated:Put") {
                        stream.write_all(b":1\r\n").expect("rpush ok");
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("read redis publish: {error}"),
            }
        }
        tx.send(published).expect("send redis payload");
    });

    let previous_enable = std::env::var("MINIO_NOTIFY_REDIS_ENABLE").ok();
    let previous_address = std::env::var("MINIO_NOTIFY_REDIS_ADDRESS").ok();
    let previous_key = std::env::var("MINIO_NOTIFY_REDIS_KEY").ok();
    let previous_pass = std::env::var("MINIO_NOTIFY_REDIS_PASSWORD").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_REDIS_ENABLE", "on");
        std::env::set_var("MINIO_NOTIFY_REDIS_ADDRESS", redis_addr.to_string());
        std::env::set_var("MINIO_NOTIFY_REDIS_KEY", "minio-events");
        std::env::set_var("MINIO_NOTIFY_REDIS_PASSWORD", "secret");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /redisbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>redis</Id><Queue>arn:minio:sqs:us-east-1:1:redis</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /redisbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"redis payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /redisbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let published = String::from_utf8(
        rx.recv_timeout(Duration::from_secs(10))
            .expect("redis recv"),
    )
    .expect("redis utf8");
    assert!(published.contains("AUTH"));
    assert!(published.contains("RPUSH"));
    assert!(published.contains("minio-events"));
    assert!(published.contains("redisbucket/object.txt"));

    let when = DateTime::<Utc>::from_timestamp(1_730_000_510, 0).expect("timestamp");
    let mut deliveries = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/redis-deliveries?bucket=redisbucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("redis deliveries request");
    sign_request_v4_standard(
        &mut deliveries,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign redis deliveries");
    let (status, _, body) = send_test_request(handle.address(), &deliveries);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("redis deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["targetId"].as_str(), Some("1:redis"));
    assert_eq!(body["records"][0]["key"].as_str(), Some("minio-events"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("redisbucket"));

    handle.shutdown().expect("shutdown server");
    receiver.join().expect("join redis receiver");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_REDIS_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_REDIS_ENABLE");
        }
        if let Some(value) = previous_address {
            std::env::set_var("MINIO_NOTIFY_REDIS_ADDRESS", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_REDIS_ADDRESS");
        }
        if let Some(value) = previous_key {
            std::env::set_var("MINIO_NOTIFY_REDIS_KEY", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_REDIS_KEY");
        }
        if let Some(value) = previous_pass {
            std::env::set_var("MINIO_NOTIFY_REDIS_PASSWORD", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_REDIS_PASSWORD");
        }
    }
}

#[test]
fn server_delivers_notifications_to_nsq() {
    let _guard = env_lock().lock().expect("env lock");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind nsq listener");
    let nsq_addr = listener.local_addr().expect("nsq addr");
    let (tx, rx) = mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept nsq");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set nsq read timeout");
        let mut published = Vec::new();
        let mut body = vec![0u8; 8192];
        loop {
            match stream.read(&mut body) {
                Ok(0) => break,
                Ok(read) => {
                    published.extend_from_slice(&body[..read]);
                    if String::from_utf8_lossy(&published).contains("s3:ObjectCreated:Put") {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("read nsq publish: {error}"),
            }
        }
        tx.send(published).expect("send nsq payload");
    });

    let previous_enable = std::env::var("MINIO_NOTIFY_NSQ_ENABLE").ok();
    let previous_address = std::env::var("MINIO_NOTIFY_NSQ_NSQD_ADDRESS").ok();
    let previous_topic = std::env::var("MINIO_NOTIFY_NSQ_TOPIC").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_NSQ_ENABLE", "on");
        std::env::set_var("MINIO_NOTIFY_NSQ_NSQD_ADDRESS", nsq_addr.to_string());
        std::env::set_var("MINIO_NOTIFY_NSQ_TOPIC", "minio-events");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /nsqbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>nsq</Id><Queue>arn:minio:sqs:us-east-1:1:nsq</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /nsqbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"nsq payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /nsqbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let published = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("nsq publish received");
    assert!(published.starts_with(b"  V2"), "{:?}", published);
    let published_text = String::from_utf8_lossy(&published);
    assert!(
        published_text.contains("PUB minio-events\n"),
        "{published_text}"
    );
    assert!(
        published_text.contains("s3:ObjectCreated:Put"),
        "{published_text}"
    );
    assert!(
        published_text.contains("nsqbucket/object.txt"),
        "{published_text}"
    );

    let when = DateTime::<Utc>::from_timestamp(1_730_000_500, 0).expect("timestamp");
    let mut deliveries = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/nsq-deliveries?bucket=nsqbucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("nsq deliveries request");
    sign_request_v4_standard(
        &mut deliveries,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign nsq deliveries");
    let (status, _, body) = send_test_request(handle.address(), &deliveries);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("nsq deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["targetId"].as_str(), Some("1:nsq"));
    assert_eq!(body["records"][0]["topic"].as_str(), Some("minio-events"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("nsqbucket"));
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert_eq!(body["records"][0]["delivered"].as_bool(), Some(true));

    handle.shutdown().expect("shutdown server");
    receiver.join().expect("join nsq receiver");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_NSQ_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NSQ_ENABLE");
        }
        if let Some(value) = previous_address {
            std::env::set_var("MINIO_NOTIFY_NSQ_NSQD_ADDRESS", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NSQ_NSQD_ADDRESS");
        }
        if let Some(value) = previous_topic {
            std::env::set_var("MINIO_NOTIFY_NSQ_TOPIC", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_NSQ_TOPIC");
        }
    }
}

#[test]
fn server_rehydrates_persisted_queue_delivery_history_after_restart() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_enable = std::env::var("MINIO_NOTIFY_QUEUE_ENABLE").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_QUEUE_ENABLE", "on");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk.clone()],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );

    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /queuepersist HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>queue</Id><Queue>arn:minio:sqs:us-east-1:1:queue</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /queuepersist?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"queue persisted payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /queuepersist/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);
    assert!(disk.join(NOTIFICATION_HISTORY_FILE).exists());

    handle.shutdown().expect("shutdown server");

    let restarted = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("restart server");

    let when = DateTime::<Utc>::from_timestamp(1_730_000_700, 0).expect("timestamp");
    let mut queue_deliveries = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/queue-deliveries?bucket=queuepersist",
            restarted.address()
        ),
        0,
        None,
    )
    .expect("queue deliveries request");
    sign_request_v4_standard(
        &mut queue_deliveries,
        "minioadmin",
        "minioadmin",
        "us-east-1",
        when,
    )
    .expect("sign queue deliveries");
    let (status, _, body) = send_test_request(restarted.address(), &queue_deliveries);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("queue deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["target_id"].as_str(), Some("1:queue"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("queuepersist"));
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert_eq!(
        body["records"][0]["payload"]["EventName"].as_str(),
        Some("s3:ObjectCreated:Put")
    );

    restarted.shutdown().expect("shutdown restarted server");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_QUEUE_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_QUEUE_ENABLE");
        }
    }
}

#[test]
fn server_records_failed_mysql_notification_delivery() {
    let _guard = env_lock().lock().expect("env lock");
    let blocker = TcpListener::bind("127.0.0.1:0").expect("bind mysql blocker");
    let blocked_port = blocker.local_addr().expect("mysql blocker addr").port();
    drop(blocker);

    let previous_enable = std::env::var("MINIO_NOTIFY_MYSQL_ENABLE").ok();
    let previous_table = std::env::var("MINIO_NOTIFY_MYSQL_TABLE").ok();
    let previous_host = std::env::var("MINIO_NOTIFY_MYSQL_HOST").ok();
    let previous_port = std::env::var("MINIO_NOTIFY_MYSQL_PORT").ok();
    let previous_database = std::env::var("MINIO_NOTIFY_MYSQL_DATABASE").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_MYSQL_ENABLE", "on");
        std::env::set_var("MINIO_NOTIFY_MYSQL_TABLE", "events");
        std::env::set_var("MINIO_NOTIFY_MYSQL_HOST", "127.0.0.1");
        std::env::set_var("MINIO_NOTIFY_MYSQL_PORT", blocked_port.to_string());
        std::env::set_var("MINIO_NOTIFY_MYSQL_DATABASE", "minio");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /mysqlbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>mysql</Id><Queue>arn:minio:sqs:us-east-1:1:mysql</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /mysqlbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"mysql payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /mysqlbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let when = DateTime::<Utc>::from_timestamp(1_730_001_000, 0).expect("timestamp");
    let mut request = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/mysql-deliveries?bucket=mysqlbucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("mysql deliveries request");
    sign_request_v4_standard(&mut request, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign mysql deliveries");
    let (status, _, body) = send_test_request(handle.address(), &request);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("mysql deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["targetId"].as_str(), Some("1:mysql"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("mysqlbucket"));
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert_eq!(body["records"][0]["delivered"].as_bool(), Some(false));
    assert!(body["records"][0]["error"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));

    handle.shutdown().expect("shutdown mysql server");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_MYSQL_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_MYSQL_ENABLE");
        }
        if let Some(value) = previous_table {
            std::env::set_var("MINIO_NOTIFY_MYSQL_TABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_MYSQL_TABLE");
        }
        if let Some(value) = previous_host {
            std::env::set_var("MINIO_NOTIFY_MYSQL_HOST", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_MYSQL_HOST");
        }
        if let Some(value) = previous_port {
            std::env::set_var("MINIO_NOTIFY_MYSQL_PORT", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_MYSQL_PORT");
        }
        if let Some(value) = previous_database {
            std::env::set_var("MINIO_NOTIFY_MYSQL_DATABASE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_MYSQL_DATABASE");
        }
    }
}

#[test]
fn server_records_failed_postgresql_notification_delivery() {
    let _guard = env_lock().lock().expect("env lock");
    let blocker = TcpListener::bind("127.0.0.1:0").expect("bind postgresql blocker");
    let blocked_port = blocker
        .local_addr()
        .expect("postgresql blocker addr")
        .port();
    drop(blocker);

    let previous_enable = std::env::var("MINIO_NOTIFY_POSTGRES_ENABLE").ok();
    let previous_table = std::env::var("MINIO_NOTIFY_POSTGRES_TABLE").ok();
    let previous_host = std::env::var("MINIO_NOTIFY_POSTGRES_HOST").ok();
    let previous_port = std::env::var("MINIO_NOTIFY_POSTGRES_PORT").ok();
    let previous_database = std::env::var("MINIO_NOTIFY_POSTGRES_DATABASE").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_POSTGRES_ENABLE", "on");
        std::env::set_var("MINIO_NOTIFY_POSTGRES_TABLE", "events");
        std::env::set_var("MINIO_NOTIFY_POSTGRES_HOST", "127.0.0.1");
        std::env::set_var("MINIO_NOTIFY_POSTGRES_PORT", blocked_port.to_string());
        std::env::set_var("MINIO_NOTIFY_POSTGRES_DATABASE", "minio");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /postgresbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>postgresql</Id><Queue>arn:minio:sqs:us-east-1:1:postgresql</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /postgresbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"postgres payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /postgresbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let when = DateTime::<Utc>::from_timestamp(1_730_001_100, 0).expect("timestamp");
    let mut request = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/postgresql-deliveries?bucket=postgresbucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("postgres deliveries request");
    sign_request_v4_standard(&mut request, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign postgres deliveries");
    let (status, _, body) = send_test_request(handle.address(), &request);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("postgres deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        body["records"][0]["targetId"].as_str(),
        Some("1:postgresql")
    );
    assert_eq!(
        body["records"][0]["bucket"].as_str(),
        Some("postgresbucket")
    );
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert_eq!(body["records"][0]["delivered"].as_bool(), Some(false));
    assert!(body["records"][0]["error"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));

    handle.shutdown().expect("shutdown postgres server");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_POSTGRES_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_POSTGRES_ENABLE");
        }
        if let Some(value) = previous_table {
            std::env::set_var("MINIO_NOTIFY_POSTGRES_TABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_POSTGRES_TABLE");
        }
        if let Some(value) = previous_host {
            std::env::set_var("MINIO_NOTIFY_POSTGRES_HOST", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_POSTGRES_HOST");
        }
        if let Some(value) = previous_port {
            std::env::set_var("MINIO_NOTIFY_POSTGRES_PORT", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_POSTGRES_PORT");
        }
        if let Some(value) = previous_database {
            std::env::set_var("MINIO_NOTIFY_POSTGRES_DATABASE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_POSTGRES_DATABASE");
        }
    }
}

#[test]
fn server_records_failed_amqp_notification_delivery() {
    let _guard = env_lock().lock().expect("env lock");
    let blocker = TcpListener::bind("127.0.0.1:0").expect("bind amqp blocker");
    let blocked_port = blocker.local_addr().expect("amqp blocker addr").port();
    drop(blocker);

    let previous_enable = std::env::var("MINIO_NOTIFY_AMQP_ENABLE").ok();
    let previous_url = std::env::var("MINIO_NOTIFY_AMQP_URL").ok();
    let previous_exchange = std::env::var("MINIO_NOTIFY_AMQP_EXCHANGE").ok();
    let previous_routing_key = std::env::var("MINIO_NOTIFY_AMQP_ROUTING_KEY").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_AMQP_ENABLE", "on");
        std::env::set_var(
            "MINIO_NOTIFY_AMQP_URL",
            format!("amqp://guest:guest@127.0.0.1:{blocked_port}"),
        );
        std::env::set_var("MINIO_NOTIFY_AMQP_EXCHANGE", "minio.events");
        std::env::set_var("MINIO_NOTIFY_AMQP_ROUTING_KEY", "objects");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /amqpbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>amqp</Id><Queue>arn:minio:sqs:us-east-1:1:amqp</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /amqpbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"amqp payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /amqpbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let when = DateTime::<Utc>::from_timestamp(1_730_001_200, 0).expect("timestamp");
    let mut request = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/amqp-deliveries?bucket=amqpbucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("amqp deliveries request");
    sign_request_v4_standard(&mut request, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign amqp deliveries");
    let (status, _, body) = send_test_request(handle.address(), &request);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("amqp deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["targetId"].as_str(), Some("1:amqp"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("amqpbucket"));
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert_eq!(body["records"][0]["delivered"].as_bool(), Some(false));
    assert!(body["records"][0]["error"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));

    handle.shutdown().expect("shutdown amqp server");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_AMQP_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_AMQP_ENABLE");
        }
        if let Some(value) = previous_url {
            std::env::set_var("MINIO_NOTIFY_AMQP_URL", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_AMQP_URL");
        }
        if let Some(value) = previous_exchange {
            std::env::set_var("MINIO_NOTIFY_AMQP_EXCHANGE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_AMQP_EXCHANGE");
        }
        if let Some(value) = previous_routing_key {
            std::env::set_var("MINIO_NOTIFY_AMQP_ROUTING_KEY", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_AMQP_ROUTING_KEY");
        }
    }
}

#[test]
fn server_records_failed_mqtt_notification_delivery() {
    let _guard = env_lock().lock().expect("env lock");
    let blocker = TcpListener::bind("127.0.0.1:0").expect("bind mqtt blocker");
    let blocked_port = blocker.local_addr().expect("mqtt blocker addr").port();
    drop(blocker);

    let previous_enable = std::env::var("MINIO_NOTIFY_MQTT_ENABLE").ok();
    let previous_broker = std::env::var("MINIO_NOTIFY_MQTT_BROKER").ok();
    let previous_topic = std::env::var("MINIO_NOTIFY_MQTT_TOPIC").ok();
    let previous_qos = std::env::var("MINIO_NOTIFY_MQTT_QOS").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_MQTT_ENABLE", "on");
        std::env::set_var(
            "MINIO_NOTIFY_MQTT_BROKER",
            format!("tcp://127.0.0.1:{blocked_port}"),
        );
        std::env::set_var("MINIO_NOTIFY_MQTT_TOPIC", "minio/events");
        std::env::set_var("MINIO_NOTIFY_MQTT_QOS", "1");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /mqttbucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>mqtt</Id><Queue>arn:minio:sqs:us-east-1:1:mqtt</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /mqttbucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"mqtt payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /mqttbucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let when = DateTime::<Utc>::from_timestamp(1_730_001_300, 0).expect("timestamp");
    let mut request = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/mqtt-deliveries?bucket=mqttbucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("mqtt deliveries request");
    sign_request_v4_standard(&mut request, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign mqtt deliveries");
    let (status, _, body) = send_test_request(handle.address(), &request);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("mqtt deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["targetId"].as_str(), Some("1:mqtt"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("mqttbucket"));
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert!(body["records"][0]["delivered"].is_boolean());
    assert_eq!(body["records"][0]["topic"].as_str(), Some("minio/events"));

    handle.shutdown().expect("shutdown mqtt server");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_MQTT_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_MQTT_ENABLE");
        }
        if let Some(value) = previous_broker {
            std::env::set_var("MINIO_NOTIFY_MQTT_BROKER", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_MQTT_BROKER");
        }
        if let Some(value) = previous_topic {
            std::env::set_var("MINIO_NOTIFY_MQTT_TOPIC", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_MQTT_TOPIC");
        }
        if let Some(value) = previous_qos {
            std::env::set_var("MINIO_NOTIFY_MQTT_QOS", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_MQTT_QOS");
        }
    }
}

#[test]
fn server_records_failed_kafka_notification_delivery() {
    let _guard = env_lock().lock().expect("env lock");
    let blocker = TcpListener::bind("127.0.0.1:0").expect("bind kafka blocker");
    let blocked_port = blocker.local_addr().expect("kafka blocker addr").port();
    drop(blocker);

    let previous_enable = std::env::var("MINIO_NOTIFY_KAFKA_ENABLE").ok();
    let previous_brokers = std::env::var("MINIO_NOTIFY_KAFKA_BROKERS").ok();
    let previous_topic = std::env::var("MINIO_NOTIFY_KAFKA_TOPIC").ok();
    unsafe {
        std::env::set_var("MINIO_NOTIFY_KAFKA_ENABLE", "on");
        std::env::set_var(
            "MINIO_NOTIFY_KAFKA_BROKERS",
            format!("127.0.0.1:{blocked_port}"),
        );
        std::env::set_var("MINIO_NOTIFY_KAFKA_TOPIC", "minio.events");
    }

    let tempdir = new_test_tempdir();
    let disk = tempdir.path().join("disk1");
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: "minioadmin".to_string(),
        root_password: "minioadmin".to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");

    let authorization = format!(
        "Authorization: Basic {}\r\n",
        base64::engine::general_purpose::STANDARD.encode("minioadmin:minioadmin")
    );
    let (status, _, _) = http_request(
        handle.address(),
        &format!(
            "PUT /kafkabucket HTTP/1.1\r\nHost: {}\r\n{authorization}\r\n",
            handle.address()
        ),
    );
    assert_eq!(status, 200);

    let notification = r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>kafka</Id><Queue>arn:minio:sqs:us-east-1:1:kafka</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>"#;
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /kafkabucket?notification HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                notification.len(),
                notification,
            ),
        );
    assert_eq!(status, 200);

    let payload = b"kafka payload";
    let (status, _, _) = http_request(
            handle.address(),
            &format!(
                "PUT /kafkabucket/object.txt HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Length: {}\r\n\r\n{}",
                handle.address(),
                payload.len(),
                String::from_utf8_lossy(payload),
            ),
        );
    assert_eq!(status, 200);

    let when = DateTime::<Utc>::from_timestamp(1_730_001_400, 0).expect("timestamp");
    let mut request = new_test_request(
        "GET",
        &format!(
            "http://{}/minio/admin/v3/kafka-deliveries?bucket=kafkabucket",
            handle.address()
        ),
        0,
        None,
    )
    .expect("kafka deliveries request");
    sign_request_v4_standard(&mut request, "minioadmin", "minioadmin", "us-east-1", when)
        .expect("sign kafka deliveries");
    let (status, _, body) = send_test_request(handle.address(), &request);
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("kafka deliveries body");
    assert_eq!(body["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["records"][0]["targetId"].as_str(), Some("1:kafka"));
    assert_eq!(body["records"][0]["bucket"].as_str(), Some("kafkabucket"));
    assert_eq!(body["records"][0]["object"].as_str(), Some("object.txt"));
    assert_eq!(body["records"][0]["delivered"].as_bool(), Some(false));
    assert!(body["records"][0]["error"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));

    handle.shutdown().expect("shutdown kafka server");
    unsafe {
        if let Some(value) = previous_enable {
            std::env::set_var("MINIO_NOTIFY_KAFKA_ENABLE", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_KAFKA_ENABLE");
        }
        if let Some(value) = previous_brokers {
            std::env::set_var("MINIO_NOTIFY_KAFKA_BROKERS", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_KAFKA_BROKERS");
        }
        if let Some(value) = previous_topic {
            std::env::set_var("MINIO_NOTIFY_KAFKA_TOPIC", value);
        } else {
            std::env::remove_var("MINIO_NOTIFY_KAFKA_TOPIC");
        }
    }
}
