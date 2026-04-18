use std::env;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::{Mutex, OnceLock};

use base64::Engine;
use minio_rust::cmd::{spawn_server, MinioServerConfig, ServerHandle};
use tempfile::TempDir;

const ROOT_USER: &str = "minioadmin";
const ROOT_PASSWORD: &str = "minioadmin";

fn replication_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct EnvGuard {
    saved: Vec<(String, Option<String>)>,
}

impl EnvGuard {
    fn set(pairs: &[(&str, String)]) -> Self {
        let mut saved = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            saved.push(((*key).to_string(), env::var(key).ok()));
            env::set_var(key, value);
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.saved.drain(..).rev() {
            match value {
                Some(previous) => env::set_var(&key, previous),
                None => env::remove_var(&key),
            }
        }
    }
}

fn spawn_test_server(name: &str) -> (TempDir, ServerHandle) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let disk = tempdir.path().join(format!("{name}-disk1"));
    let handle = spawn_server(MinioServerConfig {
        address: "127.0.0.1:0".to_string(),
        root_user: ROOT_USER.to_string(),
        root_password: ROOT_PASSWORD.to_string(),
        disks: vec![disk],
    })
    .expect("spawn server");
    (tempdir, handle)
}

fn spawn_source_server_with_remote_target(
    name: &str,
    target_id: &str,
    remote: &ServerHandle,
) -> (TempDir, ServerHandle) {
    let _lock = replication_env_lock().lock().expect("env lock");
    let upper = target_id.to_ascii_uppercase();
    let _guard = EnvGuard::set(&[
        (
            &format!("MINIO_REPLICATION_REMOTE_ENDPOINT_{upper}"),
            format!("http://{}", remote.address()),
        ),
        (
            &format!("MINIO_REPLICATION_REMOTE_ACCESS_KEY_{upper}"),
            ROOT_USER.to_string(),
        ),
        (
            &format!("MINIO_REPLICATION_REMOTE_SECRET_KEY_{upper}"),
            ROOT_PASSWORD.to_string(),
        ),
    ]);
    spawn_test_server(name)
}

fn basic_authorization() -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{ROOT_USER}:{ROOT_PASSWORD}"))
    )
}

fn http_request(
    address: &str,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> (u16, String, Vec<u8>) {
    let mut raw = format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\n");
    for (key, value) in headers {
        raw.push_str(key);
        raw.push_str(": ");
        raw.push_str(value);
        raw.push_str("\r\n");
    }
    if !body.is_empty() {
        raw.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    raw.push_str("\r\n");

    let mut bytes = raw.into_bytes();
    bytes.extend_from_slice(body);

    let mut stream = TcpStream::connect(address).expect("connect");
    stream.write_all(&bytes).expect("write request");
    stream.shutdown(Shutdown::Write).expect("shutdown write");

    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("headers/body separator");
    let headers = String::from_utf8(response[..split].to_vec()).expect("response headers");
    let body = decode_http_body(&headers, &response[split + 4..]);
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .expect("status code");
    (status, headers, body)
}

fn decode_http_body(headers: &str, body: &[u8]) -> Vec<u8> {
    if !headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return body.to_vec();
    }

    let mut decoded = Vec::new();
    let mut cursor = 0usize;
    while cursor < body.len() {
        let Some(line_end) = body[cursor..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|offset| cursor + offset)
        else {
            break;
        };
        let size_line = std::str::from_utf8(&body[cursor..line_end])
            .expect("chunk size line")
            .split(';')
            .next()
            .expect("chunk size");
        let chunk_size = usize::from_str_radix(size_line.trim(), 16).expect("chunk size hex");
        cursor = line_end + 2;
        if chunk_size == 0 {
            break;
        }
        let chunk_end = cursor + chunk_size;
        decoded.extend_from_slice(&body[cursor..chunk_end]);
        cursor = chunk_end + 2;
    }

    decoded
}

fn request_ok(address: &str, method: &str, path: &str, headers: &[(&str, String)], body: &[u8]) {
    let (status, _, response_body) = http_request(address, method, path, headers, body);
    assert_eq!(
        status,
        200,
        "unexpected status for {method} {path}: {}",
        String::from_utf8_lossy(&response_body)
    );
}

fn make_bucket(address: &str, bucket: &str) {
    request_ok(
        address,
        "PUT",
        &format!("/{bucket}"),
        &[("Authorization", basic_authorization())],
        b"",
    );
}

fn enable_versioning(address: &str, bucket: &str) {
    request_ok(
        address,
        "PUT",
        &format!("/{bucket}?versioning"),
        &[("Authorization", basic_authorization())],
        br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#,
    );
}

fn put_replication_config(
    address: &str,
    bucket: &str,
    target_id: &str,
    destination_bucket: &str,
    prefix: &str,
    delete_marker_status: &str,
    delete_status: &str,
) {
    let config = format!(
        "<ReplicationConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>{delete_marker_status}</Status></DeleteMarkerReplication><DeleteReplication><Status>{delete_status}</Status></DeleteReplication><Priority>1</Priority><Filter><Prefix>{prefix}</Prefix></Filter><Destination><Bucket>arn:minio:replication:us-east-1:{target_id}:{destination_bucket}</Bucket></Destination></Rule></ReplicationConfiguration>"
    );
    request_ok(
        address,
        "PUT",
        &format!("/{bucket}?replication"),
        &[("Authorization", basic_authorization())],
        config.as_bytes(),
    );
}

fn put_object(address: &str, bucket: &str, object: &str, body: &[u8]) {
    request_ok(
        address,
        "PUT",
        &format!("/{bucket}/{object}"),
        &[("Authorization", basic_authorization())],
        body,
    );
}

fn copy_object(address: &str, bucket: &str, source: &str, destination: &str) {
    request_ok(
        address,
        "PUT",
        &format!("/{bucket}/{destination}"),
        &[
            ("Authorization", basic_authorization()),
            ("x-amz-copy-source", format!("/{bucket}/{source}")),
        ],
        b"",
    );
}

fn get_object(address: &str, bucket: &str, object: &str) -> (u16, String, Vec<u8>) {
    http_request(
        address,
        "GET",
        &format!("/{bucket}/{object}"),
        &[("Authorization", basic_authorization())],
        b"",
    )
}

fn head_object(address: &str, bucket: &str, object: &str) -> (u16, String, Vec<u8>) {
    http_request(
        address,
        "HEAD",
        &format!("/{bucket}/{object}"),
        &[("Authorization", basic_authorization())],
        b"",
    )
}

fn delete_object(address: &str, bucket: &str, object: &str) -> (u16, String, Vec<u8>) {
    http_request(
        address,
        "DELETE",
        &format!("/{bucket}/{object}"),
        &[("Authorization", basic_authorization())],
        b"",
    )
}

fn start_multipart_upload(address: &str, bucket: &str, object: &str) -> String {
    let (status, _, body) = http_request(
        address,
        "POST",
        &format!("/{bucket}/{object}?uploads"),
        &[("Authorization", basic_authorization())],
        b"",
    );
    assert_eq!(status, 200);
    extract_xml_tag(&body, "UploadId")
}

fn put_object_part(
    address: &str,
    bucket: &str,
    object: &str,
    upload_id: &str,
    part_number: i32,
    body: &[u8],
) -> String {
    let (status, headers, response_body) = http_request(
        address,
        "PUT",
        &format!("/{bucket}/{object}?partNumber={part_number}&uploadId={upload_id}"),
        &[("Authorization", basic_authorization())],
        body,
    );
    assert_eq!(
        status,
        200,
        "unexpected part upload status: {}",
        String::from_utf8_lossy(&response_body)
    );
    extract_header_value(&headers, "etag")
        .trim_matches('"')
        .to_string()
}

fn complete_multipart_upload(
    address: &str,
    bucket: &str,
    object: &str,
    upload_id: &str,
    parts: &[(i32, String)],
) {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (part_number, etag) in parts {
        xml.push_str(&format!(
            "<Part><PartNumber>{part_number}</PartNumber><ETag>\"{etag}\"</ETag></Part>"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    request_ok(
        address,
        "POST",
        &format!("/{bucket}/{object}?uploadId={upload_id}"),
        &[("Authorization", basic_authorization())],
        xml.as_bytes(),
    );
}

fn extract_xml_tag(body: &[u8], tag: &str) -> String {
    let text = String::from_utf8(body.to_vec()).expect("xml body");
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = text.find(&open).expect("open tag") + open.len();
    let end = text[start..].find(&close).expect("close tag") + start;
    text[start..end].to_string()
}

fn extract_header_value(headers: &str, header: &str) -> String {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case(header)
                .then(|| value.trim().to_string())
        })
        .unwrap_or_default()
}

#[test]
fn remote_replication_replays_copy_object_flow() {
    let (_remote_tmp, remote) = spawn_test_server("remote-copy-target");
    let (_source_tmp, source) =
        spawn_source_server_with_remote_target("source-copy", "REMOTECOPY", &remote);

    make_bucket(remote.address(), "replica-copy");
    make_bucket(source.address(), "source-copy");
    enable_versioning(source.address(), "source-copy");
    put_replication_config(
        source.address(),
        "source-copy",
        "REMOTECOPY",
        "replica-copy",
        "replicated/",
        "Disabled",
        "Disabled",
    );

    let payload = b"copy flow payload";
    put_object(source.address(), "source-copy", "seed.txt", payload);
    copy_object(
        source.address(),
        "source-copy",
        "seed.txt",
        "replicated/copied.txt",
    );

    let (status, _, body) = get_object(remote.address(), "replica-copy", "replicated/copied.txt");
    assert_eq!(status, 200);
    assert_eq!(body, payload);

    let (status, headers, _) =
        head_object(remote.address(), "replica-copy", "replicated/copied.txt");
    assert_eq!(status, 200);
    assert!(headers
        .to_ascii_lowercase()
        .contains("x-amz-bucket-replication-status: replica"));

    source.shutdown().expect("shutdown source");
    remote.shutdown().expect("shutdown remote");
}

#[test]
fn remote_replication_replays_complete_multipart_upload_flow() {
    let (_remote_tmp, remote) = spawn_test_server("remote-multipart-target");
    let (_source_tmp, source) =
        spawn_source_server_with_remote_target("source-multipart", "REMOTEMP", &remote);

    make_bucket(remote.address(), "replica-multipart");
    make_bucket(source.address(), "source-multipart");
    enable_versioning(source.address(), "source-multipart");
    put_replication_config(
        source.address(),
        "source-multipart",
        "REMOTEMP",
        "replica-multipart",
        "replicated/",
        "Disabled",
        "Disabled",
    );

    let upload_id = start_multipart_upload(
        source.address(),
        "source-multipart",
        "replicated/archive.bin",
    );
    let mut part_one_payload = vec![b'a'; 5 * 1024 * 1024];
    part_one_payload.extend_from_slice(b"-alpha");
    let part_one = put_object_part(
        source.address(),
        "source-multipart",
        "replicated/archive.bin",
        &upload_id,
        1,
        &part_one_payload,
    );
    let part_two = put_object_part(
        source.address(),
        "source-multipart",
        "replicated/archive.bin",
        &upload_id,
        2,
        b"omega",
    );
    complete_multipart_upload(
        source.address(),
        "source-multipart",
        "replicated/archive.bin",
        &upload_id,
        &[(1, part_one), (2, part_two)],
    );

    let (status, _, body) = get_object(
        remote.address(),
        "replica-multipart",
        "replicated/archive.bin",
    );
    assert_eq!(status, 200);
    let mut expected = part_one_payload;
    expected.extend_from_slice(b"omega");
    assert_eq!(body, expected);

    let (status, headers, _) = head_object(
        remote.address(),
        "replica-multipart",
        "replicated/archive.bin",
    );
    assert_eq!(status, 200);
    assert!(headers
        .to_ascii_lowercase()
        .contains("x-amz-bucket-replication-status: replica"));

    source.shutdown().expect("shutdown source");
    remote.shutdown().expect("shutdown remote");
}

#[test]
fn remote_replication_delete_transport_respects_rule_flags() {
    let (_remote_tmp, remote) = spawn_test_server("remote-delete-target");
    let (_source_tmp, source) =
        spawn_source_server_with_remote_target("source-delete", "REMOTEDEL", &remote);

    make_bucket(remote.address(), "replica-delete");
    make_bucket(source.address(), "source-delete");
    enable_versioning(source.address(), "source-delete");
    put_replication_config(
        source.address(),
        "source-delete",
        "REMOTEDEL",
        "replica-delete",
        "logs/",
        "Disabled",
        "Disabled",
    );

    put_object(
        source.address(),
        "source-delete",
        "logs/keep.txt",
        b"do not fan out delete",
    );

    let (status, _, body) = get_object(remote.address(), "replica-delete", "logs/keep.txt");
    assert_eq!(status, 200);
    assert_eq!(body, b"do not fan out delete");

    let (status, _, _) = delete_object(source.address(), "source-delete", "logs/keep.txt");
    assert_eq!(status, 204);

    let (status, _, body) = get_object(remote.address(), "replica-delete", "logs/keep.txt");
    assert_eq!(status, 200);
    assert_eq!(body, b"do not fan out delete");

    source.shutdown().expect("shutdown source");
    remote.shutdown().expect("shutdown remote");
}
