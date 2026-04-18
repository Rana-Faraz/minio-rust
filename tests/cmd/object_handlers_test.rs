// Rust test snapshot derived from cmd/object-handlers_test.go.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use crc::{Crc, CRC_32_ISO_HDLC};
use roxmltree::Document;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use minio_rust::cmd::{
    get_md5_hash, HandlerCredentials, LocalObjectLayer, MakeBucketOptions, ObjectApiHandlers,
    ObjectOptions, PutObjReader, RequestAuth,
};

pub const SOURCE_FILE: &str = "cmd/object-handlers_test.go";

fn new_handlers(disk_count: usize) -> (ObjectApiHandlers, Vec<TempDir>, HandlerCredentials) {
    let temp_dirs: Vec<TempDir> = (0..disk_count)
        .map(|_| tempfile::tempdir().expect("create tempdir"))
        .collect();
    let disks: Vec<PathBuf> = temp_dirs
        .iter()
        .map(|dir| dir.path().to_path_buf())
        .collect();
    let credentials = HandlerCredentials::new("minioadmin", "minioadmin");
    (
        ObjectApiHandlers::new(LocalObjectLayer::new(disks), credentials.clone()),
        temp_dirs,
        credentials,
    )
}

fn must_make_bucket(layer: &LocalObjectLayer, bucket: &str) {
    layer
        .make_bucket(bucket, MakeBucketOptions::default())
        .expect("make bucket");
}

fn put_object(layer: &LocalObjectLayer, bucket: &str, object: &str, data: &[u8]) {
    layer
        .put_object(
            bucket,
            object,
            &PutObjReader {
                data: data.to_vec(),
                declared_size: data.len() as i64,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put object");
}

fn deterministic_data(len: usize, start: usize) -> Vec<u8> {
    (0..len).map(|idx| ((start + idx) % 251) as u8).collect()
}

fn put_multipart_object(
    layer: &LocalObjectLayer,
    bucket: &str,
    object: &str,
    part_lengths: &[usize],
    user_defined: BTreeMap<String, String>,
) -> Vec<u8> {
    let upload = layer
        .new_multipart_upload(
            bucket,
            object,
            ObjectOptions {
                user_defined,
                ..ObjectOptions::default()
            },
        )
        .expect("new multipart upload");
    let mut complete_parts = Vec::new();
    let mut offset = 0usize;
    let mut full = Vec::new();
    for (idx, part_length) in part_lengths.iter().enumerate() {
        let part_data = deterministic_data(*part_length, offset);
        offset += *part_length;
        full.extend_from_slice(&part_data);
        let part = layer
            .put_object_part(
                bucket,
                object,
                &upload.upload_id,
                idx as i32 + 1,
                &PutObjReader {
                    data: part_data,
                    declared_size: *part_length as i64,
                    expected_md5: String::new(),
                    expected_sha256: String::new(),
                },
                ObjectOptions::default(),
            )
            .expect("put object part");
        complete_parts.push(minio_rust::cmd::CompletePart {
            etag: part.etag,
            part_number: idx as i32 + 1,
        });
    }
    layer
        .complete_multipart_upload(
            bucket,
            object,
            &upload.upload_id,
            &complete_parts,
            ObjectOptions::default(),
        )
        .expect("complete multipart upload");
    full
}

fn xml_tag_text(body: &[u8], tag: &str) -> String {
    let xml = std::str::from_utf8(body).expect("xml utf8");
    let doc = Document::parse(xml).expect("parse xml");
    doc.descendants()
        .find(|node| node.has_tag_name(tag))
        .and_then(|node| node.text())
        .unwrap_or_default()
        .to_string()
}

fn xml_tag_values(body: &[u8], tag: &str) -> Vec<String> {
    let xml = std::str::from_utf8(body).expect("xml utf8");
    let doc = Document::parse(xml).expect("parse xml");
    doc.descendants()
        .filter(|node| node.has_tag_name(tag))
        .filter_map(|node| node.text())
        .map(ToString::to_string)
        .collect()
}

#[test]
fn test_apihead_object_handler_line_64() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "head-bucket";
    let object = "test-object";
    let data = vec![b'a'; 6 * 1024 * 1024];
    must_make_bucket(layer, bucket);
    put_object(layer, bucket, object, &data);

    let auth_v4 = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let auth_v2 = RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key);
    let invalid = RequestAuth::signed_v4("Invalid-AccessID", &credentials.secret_key);

    let response = handlers.head_object(bucket, object, &auth_v4, &BTreeMap::new());
    assert_eq!(response.status, 200);
    assert_eq!(
        response.headers.get("content-length").map(String::as_str),
        Some(data.len().to_string().as_str())
    );

    let missing = handlers.head_object(bucket, "abcd", &auth_v4, &BTreeMap::new());
    assert_eq!(missing.status, 404);
    assert_eq!(xml_tag_text(&missing.body, "Code"), "NoSuchKey");

    let forbidden = handlers.head_object(bucket, object, &invalid, &BTreeMap::new());
    assert_eq!(forbidden.status, 403);
    assert_eq!(xml_tag_text(&forbidden.body, "Code"), "InvalidAccessKeyId");

    let response_v2 = handlers.head_object(bucket, object, &auth_v2, &BTreeMap::new());
    assert_eq!(response_v2.status, 200);
}

#[test]
fn test_apihead_object_handler_with_encryption_line_206() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "head-sse-bucket";
    must_make_bucket(layer, bucket);

    layer
        .put_object(
            bucket,
            "enc-object",
            &PutObjReader {
                data: b"encrypted payload".to_vec(),
                declared_size: "encrypted payload".len() as i64,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions {
                user_defined: BTreeMap::from([
                    (
                        "x-amz-server-side-encryption-customer-algorithm".to_string(),
                        "AES256".to_string(),
                    ),
                    (
                        "x-amz-server-side-encryption-customer-key".to_string(),
                        "Zm9v".to_string(),
                    ),
                    (
                        "x-amz-server-side-encryption-customer-key-md5".to_string(),
                        "YmFy".to_string(),
                    ),
                ]),
                ..ObjectOptions::default()
            },
        )
        .expect("put sse object");

    let auth = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);

    let without_headers = handlers.head_object(bucket, "enc-object", &auth, &BTreeMap::new());
    assert_eq!(without_headers.status, 400);
    assert_eq!(
        xml_tag_text(&without_headers.body, "Code"),
        "InvalidRequest"
    );

    let with_headers = handlers.head_object(
        bucket,
        "enc-object",
        &auth,
        &BTreeMap::from([
            (
                "x-amz-server-side-encryption-customer-algorithm".to_string(),
                "AES256".to_string(),
            ),
            (
                "x-amz-server-side-encryption-customer-key".to_string(),
                "Zm9v".to_string(),
            ),
            (
                "x-amz-server-side-encryption-customer-key-md5".to_string(),
                "YmFy".to_string(),
            ),
        ]),
    );
    assert_eq!(with_headers.status, 200);
    assert_eq!(
        with_headers
            .headers
            .get("content-length")
            .map(String::as_str),
        Some("17")
    );
}

#[test]
fn test_apiget_object_handler_line_326() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "get-bucket";
    let object = "test-object";
    let data = (0..1024usize).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    must_make_bucket(layer, bucket);
    put_object(layer, bucket, object, &data);

    let auth_v4 = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let auth_v2 = RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key);
    let invalid = RequestAuth::signed_v4("Invalid-AccessID", &credentials.secret_key);

    let full = handlers.get_object(bucket, object, &auth_v4, None);
    assert_eq!(full.status, 200);
    assert_eq!(full.body, data);

    let ranged = handlers.get_object(bucket, object, &auth_v4, Some("bytes=10-100"));
    assert_eq!(ranged.status, 206);
    assert_eq!(ranged.body, data[10..101].to_vec());

    let huge_range =
        handlers.get_object(bucket, object, &auth_v4, Some("bytes=10-1000000000000000"));
    assert_eq!(huge_range.status, 206);
    assert_eq!(huge_range.body, data[10..].to_vec());

    let invalid_range = handlers.get_object(bucket, object, &auth_v4, Some("bytes=-0"));
    assert_eq!(invalid_range.status, 416);
    assert_eq!(xml_tag_text(&invalid_range.body, "Code"), "InvalidRange");

    let missing = handlers.get_object(bucket, "abcd", &auth_v4, None);
    assert_eq!(missing.status, 404);
    assert_eq!(xml_tag_text(&missing.body, "Code"), "NoSuchKey");

    let bad_name = handlers.get_object(bucket, "../../etc", &auth_v4, None);
    assert_eq!(bad_name.status, 400);
    assert_eq!(xml_tag_text(&bad_name.body, "Code"), "InvalidObjectName");

    let forbidden = handlers.get_object(bucket, object, &invalid, None);
    assert_eq!(forbidden.status, 403);
    assert_eq!(xml_tag_text(&forbidden.body, "Code"), "InvalidAccessKeyId");

    let full_v2 = handlers.get_object(bucket, object, &auth_v2, None);
    assert_eq!(full_v2.status, 200);
    assert_eq!(full_v2.body, data);
}

#[test]
fn test_apiget_object_with_mphandler_line_638() {
    let five_mib = 5 * 1024 * 1024;
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "get-multipart-bucket";
    must_make_bucket(layer, bucket);

    let plain_single = put_multipart_object(layer, bucket, "small-0", &[11], BTreeMap::new());
    let plain_multi = put_multipart_object(layer, bucket, "mp-0", &[five_mib, 33], BTreeMap::new());
    let sse_headers = BTreeMap::from([
        (
            "x-amz-server-side-encryption-customer-algorithm".to_string(),
            "AES256".to_string(),
        ),
        (
            "x-amz-server-side-encryption-customer-key".to_string(),
            "Zm9v".to_string(),
        ),
        (
            "x-amz-server-side-encryption-customer-key-md5".to_string(),
            "YmFy".to_string(),
        ),
    ]);
    let encrypted_multi = put_multipart_object(
        layer,
        bucket,
        "enc-mp-0",
        &[five_mib, 17],
        sse_headers.clone(),
    );

    let auth_v4 = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let auth_v2 = RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key);

    let whole_single = handlers.get_object("get-multipart-bucket", "small-0", &auth_v4, None);
    assert_eq!(whole_single.status, 200);
    assert_eq!(whole_single.body, plain_single);

    let whole_multi = handlers.get_object("get-multipart-bucket", "mp-0", &auth_v4, None);
    assert_eq!(whole_multi.status, 200);
    assert_eq!(whole_multi.body, plain_multi);

    let ranged = handlers.get_object("get-multipart-bucket", "mp-0", &auth_v4, Some("bytes=1-80"));
    assert_eq!(ranged.status, 206);
    assert_eq!(ranged.body, plain_multi[1..81].to_vec());

    let suffix = handlers.get_object("get-multipart-bucket", "mp-0", &auth_v2, Some("bytes=-12"));
    assert_eq!(suffix.status, 206);
    assert_eq!(suffix.body, plain_multi[plain_multi.len() - 12..].to_vec());

    let enc_missing = handlers.get_object("get-multipart-bucket", "enc-mp-0", &auth_v4, None);
    assert_eq!(enc_missing.status, 400);
    assert_eq!(xml_tag_text(&enc_missing.body, "Code"), "InvalidRequest");

    let enc_ok = handlers.get_object_with_headers(
        "get-multipart-bucket",
        "enc-mp-0",
        &auth_v4,
        Some("bytes=5-30"),
        &sse_headers,
    );
    assert_eq!(enc_ok.status, 206);
    assert_eq!(enc_ok.body, encrypted_multi[5..31].to_vec());
}

#[test]
fn test_apiget_object_with_part_number_handler_line_834() {
    let five_mib = 5 * 1024 * 1024;
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "get-part-number-bucket";
    must_make_bucket(layer, bucket);

    let empty = Vec::new();
    put_object(layer, bucket, "nothing", &empty);
    let one_byte = deterministic_data(1, 0);
    put_object(layer, bucket, "1byte", &one_byte);
    let multipart = put_multipart_object(layer, bucket, "mp-0", &[five_mib, 5], BTreeMap::new());
    let sse_headers = BTreeMap::from([
        (
            "x-amz-server-side-encryption-customer-algorithm".to_string(),
            "AES256".to_string(),
        ),
        (
            "x-amz-server-side-encryption-customer-key".to_string(),
            "Zm9v".to_string(),
        ),
        (
            "x-amz-server-side-encryption-customer-key-md5".to_string(),
            "YmFy".to_string(),
        ),
    ]);
    let encrypted = put_multipart_object(
        layer,
        bucket,
        "enc-mp-0",
        &[five_mib, 4],
        sse_headers.clone(),
    );

    let auth = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);

    let empty_part = handlers.get_object_part_number(bucket, "nothing", 1, &auth, &BTreeMap::new());
    assert_eq!(empty_part.status, 200);
    assert!(empty_part.body.is_empty());

    let first_part = handlers.get_object_part_number(bucket, "1byte", 1, &auth, &BTreeMap::new());
    assert_eq!(first_part.status, 200);
    assert_eq!(first_part.body, one_byte);

    let mp_part1 = handlers.get_object_part_number(bucket, "mp-0", 1, &auth, &BTreeMap::new());
    assert_eq!(mp_part1.status, 206);
    assert_eq!(mp_part1.body, multipart[..five_mib].to_vec());

    let mp_part2 = handlers.get_object_part_number(bucket, "mp-0", 2, &auth, &BTreeMap::new());
    assert_eq!(mp_part2.status, 206);
    assert_eq!(mp_part2.body, multipart[five_mib..five_mib + 5].to_vec());

    let bad_part = handlers.get_object_part_number(bucket, "mp-0", 3, &auth, &BTreeMap::new());
    assert_eq!(bad_part.status, 416);
    assert_eq!(xml_tag_text(&bad_part.body, "Code"), "InvalidRange");

    let enc_missing =
        handlers.get_object_part_number(bucket, "enc-mp-0", 1, &auth, &BTreeMap::new());
    assert_eq!(enc_missing.status, 400);
    assert_eq!(xml_tag_text(&enc_missing.body, "Code"), "InvalidRequest");

    let enc_part = handlers.get_object_part_number(bucket, "enc-mp-0", 2, &auth, &sse_headers);
    assert_eq!(enc_part.status, 206);
    assert_eq!(enc_part.body, encrypted[five_mib..five_mib + 4].to_vec());
}

#[test]
fn test_apiput_object_stream_sig_v4_handler_line_971() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "put-stream-bucket";
    must_make_bucket(layer, bucket);

    let data = deterministic_data(65 * 1024, 0);
    let auth = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);

    let success = handlers.put_object_streaming(
        bucket,
        "stream-object",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth,
        &BTreeMap::from([(
            "content-encoding".to_string(),
            "aws-chunked,gzip".to_string(),
        )]),
        true,
        &data.len().to_string(),
    );
    assert_eq!(success.status, 200);
    assert_eq!(
        layer
            .get_object(bucket, "stream-object")
            .expect("stream object"),
        data
    );
    let info = layer
        .get_object_info(bucket, "stream-object")
        .expect("stream info");
    assert_eq!(
        info.user_defined
            .get("content-encoding")
            .map(String::as_str),
        Some("gzip")
    );

    let missing_date = handlers.put_object_streaming(
        bucket,
        "stream-no-date",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth,
        &BTreeMap::new(),
        false,
        "5",
    );
    assert_eq!(missing_date.status, 400);
    assert_eq!(xml_tag_text(&missing_date.body, "Code"), "AccessDenied");

    let bad_length = handlers.put_object_streaming(
        bucket,
        "stream-bad-length",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth,
        &BTreeMap::new(),
        true,
        "not-a-number",
    );
    assert_eq!(bad_length.status, 400);
    assert_eq!(xml_tag_text(&bad_length.body, "Code"), "BadRequest");

    let invalid_auth = handlers.put_object_streaming(
        bucket,
        "stream-invalid-auth",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &RequestAuth::signed_v4("Invalid-AccessID", &credentials.secret_key),
        &BTreeMap::new(),
        true,
        "5",
    );
    assert_eq!(invalid_auth.status, 403);
    assert_eq!(
        xml_tag_text(&invalid_auth.body, "Code"),
        "InvalidAccessKeyId"
    );
}

#[test]
fn test_apiput_object_handler_line_1295() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "put-handler-bucket";
    must_make_bucket(layer, bucket);

    let data = deterministic_data(6 * 1024, 7);
    let auth_v4 = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let auth_v2 = RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key);
    let invalid_auth = RequestAuth::signed_v4("Wrong-AccessID", &credentials.secret_key);

    let success = handlers.put_object(
        bucket,
        "put-v4",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v4,
        &BTreeMap::new(),
    );
    assert_eq!(success.status, 200);
    assert_eq!(layer.get_object(bucket, "put-v4").expect("put-v4"), data);

    let success_v2 = handlers.put_object(
        bucket,
        "put-v2",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v2,
        &BTreeMap::new(),
    );
    assert_eq!(success_v2.status, 200);
    assert_eq!(layer.get_object(bucket, "put-v2").expect("put-v2"), data);

    let forbidden = handlers.put_object(
        bucket,
        "bad-auth",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &invalid_auth,
        &BTreeMap::new(),
    );
    assert_eq!(forbidden.status, 403);
    assert_eq!(xml_tag_text(&forbidden.body, "Code"), "InvalidAccessKeyId");

    let copy_source = handlers.put_object(
        bucket,
        "copy-source",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v4,
        &BTreeMap::from([("x-amz-copy-source".to_string(), "somewhere".to_string())]),
    );
    assert_eq!(copy_source.status, 400);
    assert_eq!(xml_tag_text(&copy_source.body, "Code"), "InvalidArgument");

    let invalid_md5 = handlers.put_object(
        bucket,
        "invalid-md5",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v4,
        &BTreeMap::from([("content-md5".to_string(), "42".to_string())]),
    );
    assert_eq!(invalid_md5.status, 400);
    assert_eq!(xml_tag_text(&invalid_md5.body, "Code"), "InvalidDigest");

    let too_big = handlers.put_object(
        bucket,
        "too-big",
        &PutObjReader {
            data: data.clone(),
            declared_size: 5_i64 * 1024 * 1024 * 1024 * 1024 + 1,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v4,
        &BTreeMap::new(),
    );
    assert_eq!(too_big.status, 400);
    assert_eq!(xml_tag_text(&too_big.body, "Code"), "EntityTooLarge");

    let missing_length = handlers.put_object(
        bucket,
        "missing-length",
        &PutObjReader {
            data: data.clone(),
            declared_size: -1,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v4,
        &BTreeMap::new(),
    );
    assert_eq!(missing_length.status, 411);
    assert_eq!(
        xml_tag_text(&missing_length.body, "Code"),
        "MissingContentLength"
    );

    let invalid_storage_class = handlers.put_object(
        bucket,
        "invalid-storage-class",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v4,
        &BTreeMap::from([("x-amz-storage-class".to_string(), "INVALID".to_string())]),
    );
    assert_eq!(invalid_storage_class.status, 400);
    assert_eq!(
        xml_tag_text(&invalid_storage_class.body, "Code"),
        "InvalidStorageClass"
    );

    let wrong_crc32 = handlers.put_object(
        bucket,
        "wrong-crc32",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v4,
        &BTreeMap::from([(
            "x-amz-checksum-crc32".to_string(),
            BASE64_STANDARD.encode([1_u8, 2, 3, 4]),
        )]),
    );
    assert_eq!(wrong_crc32.status, 400);
    assert_eq!(
        xml_tag_text(&wrong_crc32.body, "Code"),
        "XAmzContentChecksumMismatch"
    );

    let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    let good_crc32 = BASE64_STANDARD.encode(crc32.checksum(&data).to_be_bytes());
    let good_crc32_response = handlers.put_object(
        bucket,
        "good-crc32",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v4,
        &BTreeMap::from([("x-amz-checksum-crc32".to_string(), good_crc32.clone())]),
    );
    assert_eq!(good_crc32_response.status, 200);
    assert_eq!(
        good_crc32_response
            .headers
            .get("x-amz-checksum-crc32")
            .map(String::as_str),
        Some(good_crc32.as_str())
    );

    let good_sha256 = BASE64_STANDARD.encode(Sha256::digest(&data));
    let good_sha256_response = handlers.put_object(
        bucket,
        "good-sha256",
        &PutObjReader {
            data: data.clone(),
            declared_size: data.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        &auth_v4,
        &BTreeMap::from([("x-amz-checksum-sha256".to_string(), good_sha256.clone())]),
    );
    assert_eq!(good_sha256_response.status, 200);
    assert_eq!(
        good_sha256_response
            .headers
            .get("x-amz-checksum-sha256")
            .map(String::as_str),
        Some(good_sha256.as_str())
    );
}

#[test]
fn test_apicopy_object_part_handler_sanity_line_1675() {
    let five_mib = 5 * 1024 * 1024;
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "copy-part-sanity-bucket";
    must_make_bucket(layer, bucket);

    let source_data = deterministic_data(6 * 1024 * 1024, 0);
    put_object(layer, bucket, "source-object", &source_data);

    let init = handlers.new_multipart_upload(
        bucket,
        "copied-object",
        &RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key),
    );
    assert_eq!(init.status, 200);
    let upload_id = xml_tag_text(&init.body, "UploadId");

    let auth = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let part1 = handlers.copy_object_part(
        bucket,
        "copied-object",
        &upload_id,
        "1",
        &format!("/{bucket}/source-object"),
        Some(&format!("bytes=0-{}", five_mib - 1)),
        &auth,
    );
    assert_eq!(part1.status, 200);
    let part1_etag = xml_tag_text(&part1.body, "ETag");

    let part2 = handlers.copy_object_part(
        bucket,
        "copied-object",
        &upload_id,
        "2",
        &format!("/{bucket}/source-object"),
        Some(&format!("bytes={}-{}", five_mib, source_data.len() - 1)),
        &auth,
    );
    assert_eq!(part2.status, 200);
    let part2_etag = xml_tag_text(&part2.body, "ETag");

    let completed = handlers.complete_multipart_upload(
        bucket,
        "copied-object",
        &upload_id,
        &[
            minio_rust::cmd::CompletePart {
                etag: part1_etag,
                part_number: 1,
            },
            minio_rust::cmd::CompletePart {
                etag: part2_etag,
                part_number: 2,
            },
        ],
        &auth,
    );
    assert_eq!(completed.status, 200);
    assert_eq!(
        layer
            .get_object(bucket, "copied-object")
            .expect("copied multipart object"),
        source_data
    );
}

#[test]
fn test_apicopy_object_part_handler_line_1823() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "copy-part-bucket";
    must_make_bucket(layer, bucket);
    put_object(
        layer,
        bucket,
        "source-object",
        &deterministic_data(6 * 1024, 3),
    );

    let auth = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let upload_id = xml_tag_text(
        &handlers
            .new_multipart_upload(bucket, "dest-object", &auth)
            .body,
        "UploadId",
    );

    let success = handlers.copy_object_part(
        bucket,
        "dest-object",
        &upload_id,
        "1",
        &format!("/{bucket}/source-object"),
        None,
        &auth,
    );
    assert_eq!(success.status, 200);
    assert_eq!(
        layer
            .list_object_parts(
                bucket,
                "dest-object",
                &upload_id,
                0,
                10,
                ObjectOptions::default()
            )
            .expect("list copied parts")
            .parts
            .len(),
        1
    );

    let invalid_source =
        handlers.copy_object_part(bucket, "dest-object", &upload_id, "1", "/", None, &auth);
    assert_eq!(invalid_source.status, 400);
    assert_eq!(
        xml_tag_text(&invalid_source.body, "Code"),
        "InvalidArgument"
    );

    let invalid_range = handlers.copy_object_part(
        bucket,
        "dest-object",
        &upload_id,
        "1",
        &format!("/{bucket}/source-object"),
        Some("bytes=99999-"),
        &auth,
    );
    assert_eq!(invalid_range.status, 400);
    assert_eq!(xml_tag_text(&invalid_range.body, "Code"), "InvalidArgument");

    let missing_source = handlers.copy_object_part(
        bucket,
        "dest-object",
        &upload_id,
        "1",
        &format!("/{bucket}/missing-object"),
        None,
        &auth,
    );
    assert_eq!(missing_source.status, 404);
    assert_eq!(xml_tag_text(&missing_source.body, "Code"), "NoSuchKey");

    let missing_bucket = handlers.copy_object_part(
        "missing-bucket",
        "dest-object",
        &upload_id,
        "1",
        &format!("/{bucket}/source-object"),
        None,
        &auth,
    );
    assert_eq!(missing_bucket.status, 404);
    assert_eq!(xml_tag_text(&missing_bucket.body, "Code"), "NoSuchBucket");

    let invalid_auth = handlers.copy_object_part(
        bucket,
        "dest-object",
        &upload_id,
        "1",
        &format!("/{bucket}/source-object"),
        None,
        &RequestAuth::signed_v4("Invalid-AccessID", &credentials.secret_key),
    );
    assert_eq!(invalid_auth.status, 403);
    assert_eq!(
        xml_tag_text(&invalid_auth.body, "Code"),
        "InvalidAccessKeyId"
    );

    let invalid_upload = handlers.copy_object_part(
        bucket,
        "dest-object",
        "does-not-exist",
        "1",
        &format!("/{bucket}/source-object"),
        None,
        &auth,
    );
    assert_eq!(invalid_upload.status, 404);
    assert_eq!(xml_tag_text(&invalid_upload.body, "Code"), "NoSuchUpload");
}

#[test]
fn test_apicopy_object_handler_line_2155() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "copy-object-bucket";
    must_make_bucket(layer, bucket);
    let source_data = deterministic_data(6 * 1024, 11);
    put_object(layer, bucket, "source-object", &source_data);

    let auth = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let success = handlers.copy_object(
        bucket,
        "copied-object",
        &format!("/{bucket}/source-object"),
        &auth,
        &BTreeMap::new(),
    );
    assert_eq!(success.status, 200);
    assert_eq!(
        layer
            .get_object(bucket, "copied-object")
            .expect("copied object"),
        source_data
    );

    let metadata_replace = handlers.copy_object(
        bucket,
        "metadata-copy",
        &format!("/{bucket}/source-object"),
        &auth,
        &BTreeMap::from([
            (
                "x-amz-metadata-directive".to_string(),
                "REPLACE".to_string(),
            ),
            ("content-type".to_string(), "application/json".to_string()),
        ]),
    );
    assert_eq!(metadata_replace.status, 200);
    let replaced_info = layer
        .get_object_info(bucket, "metadata-copy")
        .expect("metadata copy info");
    assert_eq!(
        replaced_info
            .user_defined
            .get("content-type")
            .map(String::as_str),
        Some("application/json")
    );

    let invalid_source =
        handlers.copy_object(bucket, "copy-bad-source", "/", &auth, &BTreeMap::new());
    assert_eq!(invalid_source.status, 400);
    assert_eq!(
        xml_tag_text(&invalid_source.body, "Code"),
        "InvalidArgument"
    );

    let invalid_dest = handlers.copy_object(
        bucket,
        "dir//bad",
        &format!("/{bucket}/source-object"),
        &auth,
        &BTreeMap::new(),
    );
    assert_eq!(invalid_dest.status, 400);
    assert_eq!(
        xml_tag_text(&invalid_dest.body, "Code"),
        "InvalidObjectName"
    );

    let same_source = handlers.copy_object(
        bucket,
        "source-object",
        &format!("/{bucket}/source-object"),
        &auth,
        &BTreeMap::new(),
    );
    assert_eq!(same_source.status, 400);
    assert_eq!(xml_tag_text(&same_source.body, "Code"), "InvalidRequest");

    let missing_source = handlers.copy_object(
        bucket,
        "copy-missing-source",
        &format!("/{bucket}/missing-object"),
        &auth,
        &BTreeMap::new(),
    );
    assert_eq!(missing_source.status, 404);
    assert_eq!(xml_tag_text(&missing_source.body, "Code"), "NoSuchKey");

    let missing_bucket = handlers.copy_object(
        "missing-bucket",
        "copy-missing-bucket",
        &format!("/{bucket}/source-object"),
        &auth,
        &BTreeMap::new(),
    );
    assert_eq!(missing_bucket.status, 404);
    assert_eq!(xml_tag_text(&missing_bucket.body, "Code"), "NoSuchBucket");

    let invalid_auth = handlers.copy_object(
        bucket,
        "copy-invalid-auth",
        &format!("/{bucket}/source-object"),
        &RequestAuth::signed_v4("Invalid-AccessID", &credentials.secret_key),
        &BTreeMap::new(),
    );
    assert_eq!(invalid_auth.status, 403);
    assert_eq!(
        xml_tag_text(&invalid_auth.body, "Code"),
        "InvalidAccessKeyId"
    );
}

#[test]
fn test_apinew_multipart_handler_line_2662() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "multipart-bucket";
    let object = "test-object-new-multipart";
    must_make_bucket(layer, bucket);

    let auth_v4 = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let auth_v2 = RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key);
    let invalid = RequestAuth::signed_v4("Invalid-AccessID", &credentials.secret_key);

    let created = handlers.new_multipart_upload(bucket, object, &auth_v4);
    assert_eq!(created.status, 200);
    let upload_id = xml_tag_text(&created.body, "UploadId");
    assert!(!upload_id.is_empty());
    let listed = layer
        .list_object_parts(bucket, object, &upload_id, 0, 1, ObjectOptions::default())
        .expect("list object parts");
    assert_eq!(listed.upload_id, upload_id);

    let forbidden = handlers.new_multipart_upload(bucket, object, &invalid);
    assert_eq!(forbidden.status, 403);
    assert_eq!(xml_tag_text(&forbidden.body, "Code"), "InvalidAccessKeyId");

    let created_v2 = handlers.new_multipart_upload(bucket, object, &auth_v2);
    assert_eq!(created_v2.status, 200);
    assert!(!xml_tag_text(&created_v2.body, "UploadId").is_empty());
}

#[test]
fn test_apinew_multipart_handler_parallel_line_2801() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let bucket = "multipart-parallel-bucket";
    let object = "test-object-new-multipart-parallel";
    must_make_bucket(handlers.layer().expect("layer"), bucket);

    let handlers = Arc::new(handlers);
    let mut threads = Vec::new();
    for _ in 0..10 {
        let handlers = Arc::clone(&handlers);
        let access_key = credentials.access_key.clone();
        let secret_key = credentials.secret_key.clone();
        let bucket = bucket.to_string();
        let object = object.to_string();
        threads.push(std::thread::spawn(move || {
            let response = handlers.new_multipart_upload(
                &bucket,
                &object,
                &RequestAuth::signed_v4(&access_key, &secret_key),
            );
            assert_eq!(response.status, 200);
            xml_tag_text(&response.body, "UploadId")
        }));
    }

    let upload_ids = threads
        .into_iter()
        .map(|thread| thread.join().expect("join upload thread"))
        .collect::<Vec<_>>();
    assert_eq!(upload_ids.len(), 10);
    for upload_id in upload_ids {
        let info = handlers
            .layer()
            .expect("layer")
            .list_object_parts(bucket, object, &upload_id, 0, 1, ObjectOptions::default())
            .expect("list object parts");
        assert_eq!(info.upload_id, upload_id);
    }
}

#[test]
fn test_apicomplete_multipart_handler_line_2864() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "complete-multipart-bucket";
    let object = "test-object-new-multipart";
    must_make_bucket(layer, bucket);

    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;
    let anon_upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;

    let valid_part = vec![b'a'; 6 * 1024 * 1024];
    let valid_md5 = get_md5_hash(&valid_part);

    for (part_number, data) in [
        (1, b"abcd".as_slice()),
        (2, b"efgh".as_slice()),
        (3, b"ijkl".as_slice()),
        (4, b"mnop".as_slice()),
    ] {
        layer
            .put_object_part(
                bucket,
                object,
                &upload_id,
                part_number,
                &PutObjReader {
                    data: data.to_vec(),
                    declared_size: data.len() as i64,
                    expected_md5: get_md5_hash(data),
                    expected_sha256: String::new(),
                },
                ObjectOptions::default(),
            )
            .expect("put small part");
    }
    for part_number in [5, 6] {
        layer
            .put_object_part(
                bucket,
                object,
                &upload_id,
                part_number,
                &PutObjReader {
                    data: valid_part.clone(),
                    declared_size: valid_part.len() as i64,
                    expected_md5: valid_md5.clone(),
                    expected_sha256: String::new(),
                },
                ObjectOptions::default(),
            )
            .expect("put large part");
    }
    for part_number in [1, 2] {
        layer
            .put_object_part(
                bucket,
                object,
                &anon_upload_id,
                part_number,
                &PutObjReader {
                    data: valid_part.clone(),
                    declared_size: valid_part.len() as i64,
                    expected_md5: valid_md5.clone(),
                    expected_sha256: String::new(),
                },
                ObjectOptions::default(),
            )
            .expect("put anon part");
    }

    let auth = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let invalid = RequestAuth::signed_v4("Invalid-AccessID", &credentials.secret_key);

    let etag_mismatch = handlers.complete_multipart_upload(
        bucket,
        object,
        &upload_id,
        &[minio_rust::cmd::CompletePart {
            etag: "abcd".to_string(),
            part_number: 1,
        }],
        &auth,
    );
    assert_eq!(etag_mismatch.status, 400);
    assert_eq!(xml_tag_text(&etag_mismatch.body, "Code"), "InvalidPart");

    let malformed = handlers.complete_multipart_upload(bucket, object, &upload_id, &[], &auth);
    assert_eq!(malformed.status, 400);
    assert_eq!(xml_tag_text(&malformed.body, "Code"), "MalformedXML");

    let missing_upload = handlers.complete_multipart_upload(
        bucket,
        object,
        "abc",
        &[minio_rust::cmd::CompletePart {
            etag: "abcd".to_string(),
            part_number: 1,
        }],
        &auth,
    );
    assert_eq!(missing_upload.status, 404);
    assert_eq!(xml_tag_text(&missing_upload.body, "Code"), "NoSuchUpload");

    let too_small = handlers.complete_multipart_upload(
        bucket,
        object,
        &upload_id,
        &[
            minio_rust::cmd::CompletePart {
                etag: get_md5_hash(b"abcd"),
                part_number: 1,
            },
            minio_rust::cmd::CompletePart {
                etag: get_md5_hash(b"efgh"),
                part_number: 2,
            },
        ],
        &auth,
    );
    assert_eq!(too_small.status, 400);
    assert_eq!(xml_tag_text(&too_small.body, "Code"), "EntityTooSmall");

    let invalid_part = handlers.complete_multipart_upload(
        bucket,
        object,
        &upload_id,
        &[minio_rust::cmd::CompletePart {
            etag: get_md5_hash(b"abcd"),
            part_number: 10,
        }],
        &auth,
    );
    assert_eq!(invalid_part.status, 400);
    assert_eq!(xml_tag_text(&invalid_part.body, "Code"), "InvalidPart");

    let unsorted = handlers.complete_multipart_upload(
        bucket,
        object,
        &upload_id,
        &[
            minio_rust::cmd::CompletePart {
                etag: valid_md5.clone(),
                part_number: 6,
            },
            minio_rust::cmd::CompletePart {
                etag: valid_md5.clone(),
                part_number: 5,
            },
        ],
        &auth,
    );
    assert_eq!(unsorted.status, 400);
    assert_eq!(xml_tag_text(&unsorted.body, "Code"), "InvalidPartOrder");

    let forbidden = handlers.complete_multipart_upload(
        bucket,
        object,
        &upload_id,
        &[
            minio_rust::cmd::CompletePart {
                etag: valid_md5.clone(),
                part_number: 5,
            },
            minio_rust::cmd::CompletePart {
                etag: valid_md5.clone(),
                part_number: 6,
            },
        ],
        &invalid,
    );
    assert_eq!(forbidden.status, 403);
    assert_eq!(xml_tag_text(&forbidden.body, "Code"), "InvalidAccessKeyId");

    let success = handlers.complete_multipart_upload(
        bucket,
        object,
        &upload_id,
        &[
            minio_rust::cmd::CompletePart {
                etag: valid_md5.clone(),
                part_number: 5,
            },
            minio_rust::cmd::CompletePart {
                etag: valid_md5.clone(),
                part_number: 6,
            },
        ],
        &auth,
    );
    assert_eq!(success.status, 200);
    assert_eq!(xml_tag_text(&success.body, "Bucket"), bucket);
    assert_eq!(xml_tag_text(&success.body, "Key"), object);
    assert!(xml_tag_text(&success.body, "ETag").contains("-2"));
}

#[test]
fn test_apiabort_multipart_handler_line_3233() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "abort-multipart-bucket";
    let object = "test-object-new-multipart";
    must_make_bucket(layer, bucket);

    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;
    let second_upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;

    let part_data = vec![b'a'; 6 * 1024 * 1024];
    let md5 = get_md5_hash(&part_data);
    layer
        .put_object_part(
            bucket,
            object,
            &upload_id,
            1,
            &PutObjReader {
                data: part_data.clone(),
                declared_size: part_data.len() as i64,
                expected_md5: md5.clone(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put object part");

    let auth = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let invalid = RequestAuth::signed_v4("Invalid-AccessID", &credentials.secret_key);

    let ok = handlers.abort_multipart_upload(bucket, object, &upload_id, &auth);
    assert_eq!(ok.status, 204);

    let missing = handlers.abort_multipart_upload(bucket, object, "nonexistent-upload-id", &auth);
    assert_eq!(missing.status, 404);
    assert_eq!(xml_tag_text(&missing.body, "Code"), "NoSuchUpload");

    let forbidden = handlers.abort_multipart_upload(bucket, object, &second_upload_id, &invalid);
    assert_eq!(forbidden.status, 403);
    assert_eq!(xml_tag_text(&forbidden.body, "Code"), "InvalidAccessKeyId");
}

#[test]
fn test_apidelete_object_handler_line_3393() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "delete-handler-bucket";
    let object = "test-object";
    must_make_bucket(layer, bucket);
    put_object(layer, bucket, object, b"payload");

    let auth_v4 = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let auth_v2 = RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key);
    let invalid = RequestAuth::signed_v4("Invalid-AccessKey", &credentials.secret_key);

    let first = handlers.delete_object(bucket, object, &auth_v4, &BTreeMap::new());
    assert_eq!(first.status, 204);

    let second = handlers.delete_object(bucket, object, &auth_v4, &BTreeMap::new());
    assert_eq!(second.status, 204);

    let forbidden = handlers.delete_object(bucket, object, &invalid, &BTreeMap::new());
    assert_eq!(forbidden.status, 403);
    assert_eq!(xml_tag_text(&forbidden.body, "Code"), "InvalidAccessKeyId");

    put_object(layer, bucket, object, b"payload");
    let via_v2 = handlers.delete_object(bucket, object, &auth_v2, &BTreeMap::new());
    assert_eq!(via_v2.status, 204);
}

#[test]
fn test_apiput_object_part_handler_streaming_line_3550() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "streaming-part-bucket";
    let object = "testobject";
    must_make_bucket(layer, bucket);

    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;

    let success = handlers.put_object_part_streaming(
        bucket,
        object,
        &upload_id,
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key),
        true,
        "5",
    );
    assert_eq!(success.status, 200);

    let missing_date = handlers.put_object_part_streaming(
        bucket,
        object,
        &upload_id,
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key),
        false,
        "5",
    );
    assert_eq!(missing_date.status, 400);
    assert_eq!(xml_tag_text(&missing_date.body, "Code"), "AccessDenied");

    let bad_length = handlers.put_object_part_streaming(
        bucket,
        object,
        &upload_id,
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key),
        true,
        "9999999999999999999999",
    );
    assert_eq!(bad_length.status, 400);
    assert_eq!(xml_tag_text(&bad_length.body, "Code"), "BadRequest");
}

#[test]
fn test_apiput_object_part_handler_line_3635() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "put-part-bucket";
    let object = "testobject";
    must_make_bucket(layer, bucket);

    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;

    let auth_v4 = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let auth_v2 = RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key);
    let invalid = RequestAuth::signed_v4("Invalid-AccessID", &credentials.secret_key);
    let bad_secret = RequestAuth::signed_v4(&credentials.access_key, "bad-secret");

    let success_v4 = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &auth_v4,
    );
    assert_eq!(success_v4.status, 200);

    let success_v2 = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "2",
        &PutObjReader {
            data: b"world".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"world"),
            expected_sha256: String::new(),
        },
        &auth_v2,
    );
    assert_eq!(success_v2.status, 200);

    let invalid_part_parse = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "9999999999999999999",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &auth_v4,
    );
    assert_eq!(invalid_part_parse.status, 400);
    assert_eq!(
        xml_tag_text(&invalid_part_parse.body, "Code"),
        "InvalidPart"
    );

    let invalid_max = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "10001",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &auth_v4,
    );
    assert_eq!(invalid_max.status, 400);
    assert_eq!(xml_tag_text(&invalid_max.body, "Code"), "InvalidMaxParts");

    let missing_length = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: -1,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &auth_v4,
    );
    assert_eq!(missing_length.status, 411);
    assert_eq!(
        xml_tag_text(&missing_length.body, "Code"),
        "MissingContentLength"
    );

    let too_large = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5 * 1024 * 1024 * 1024 * 1024 + 1,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &auth_v4,
    );
    assert_eq!(too_large.status, 400);
    assert_eq!(xml_tag_text(&too_large.body, "Code"), "EntityTooLarge");

    let bad_signature = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &bad_secret,
    );
    assert_eq!(bad_signature.status, 403);
    assert_eq!(
        xml_tag_text(&bad_signature.body, "Code"),
        "SignatureDoesNotMatch"
    );

    let bad_md5 = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: "badmd5".to_string(),
            expected_sha256: String::new(),
        },
        &auth_v4,
    );
    assert_eq!(bad_md5.status, 400);
    assert_eq!(xml_tag_text(&bad_md5.body, "Code"), "InvalidDigest");

    let missing_upload = handlers.put_object_part(
        bucket,
        object,
        "upload1",
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &auth_v4,
    );
    assert_eq!(missing_upload.status, 404);
    assert_eq!(xml_tag_text(&missing_upload.body, "Code"), "NoSuchUpload");

    let invalid_access = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "1",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &invalid,
    );
    assert_eq!(invalid_access.status, 403);
    assert_eq!(
        xml_tag_text(&invalid_access.body, "Code"),
        "InvalidAccessKeyId"
    );

    let zero_part = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "0",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &auth_v4,
    );
    assert_eq!(zero_part.status, 400);
    assert_eq!(xml_tag_text(&zero_part.body, "Code"), "InvalidPart");

    let negative_part = handlers.put_object_part(
        bucket,
        object,
        &upload_id,
        "-10",
        &PutObjReader {
            data: b"hello".to_vec(),
            declared_size: 5,
            expected_md5: get_md5_hash(b"hello"),
            expected_sha256: String::new(),
        },
        &auth_v4,
    );
    assert_eq!(negative_part.status, 400);
    assert_eq!(xml_tag_text(&negative_part.body, "Code"), "InvalidPart");
}

#[test]
fn subtest_file_scope_fmt_sprintf_min_io_s_test_d_line_3804() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "put-part-subtest-bucket";
    let object = "testobject";
    must_make_bucket(layer, bucket);
    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;

    for auth in [
        RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key),
        RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key),
    ] {
        let response = handlers.put_object_part(
            bucket,
            object,
            &upload_id,
            "1",
            &PutObjReader {
                data: b"hello".to_vec(),
                declared_size: 5,
                expected_md5: get_md5_hash(b"hello"),
                expected_sha256: String::new(),
            },
            &auth,
        );
        assert_eq!(response.status, 200);
    }
}

#[test]
fn test_apilist_object_parts_handler_pre_sign_line_3954() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "list-parts-presign-bucket";
    let object = "testobject";
    must_make_bucket(layer, bucket);

    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;
    let md5 = get_md5_hash(b"hello");
    layer
        .put_object_part(
            bucket,
            object,
            &upload_id,
            1,
            &PutObjReader {
                data: b"hello".to_vec(),
                declared_size: 5,
                expected_md5: md5,
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put object part");

    let presigned_v2 = RequestAuth::presigned_v2(&credentials.access_key, &credentials.secret_key);
    let presigned_v4 = RequestAuth::presigned_v4(&credentials.access_key, &credentials.secret_key);

    let response_v2 = handlers.list_object_parts(bucket, object, &upload_id, "", "", &presigned_v2);
    assert_eq!(response_v2.status, 200);
    assert_eq!(xml_tag_values(&response_v2.body, "PartNumber"), vec!["1"]);

    let response_v4 = handlers.list_object_parts(bucket, object, &upload_id, "", "", &presigned_v4);
    assert_eq!(response_v4.status, 200);
    assert_eq!(xml_tag_values(&response_v4.body, "PartNumber"), vec!["1"]);
}

#[test]
fn test_apilist_object_parts_handler_line_4043() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "list-parts-bucket";
    let object = "testobject";
    must_make_bucket(layer, bucket);

    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;
    for (part_number, payload) in [(1, b"hello".as_slice()), (2, b"world".as_slice())] {
        layer
            .put_object_part(
                bucket,
                object,
                &upload_id,
                part_number,
                &PutObjReader {
                    data: payload.to_vec(),
                    declared_size: payload.len() as i64,
                    expected_md5: get_md5_hash(payload),
                    expected_sha256: String::new(),
                },
                ObjectOptions::default(),
            )
            .expect("put part");
    }

    let auth_v4 = RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key);
    let auth_v2 = RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key);
    let bad_secret = RequestAuth::signed_v4(&credentials.access_key, "bad-secret");

    let bad_signature = handlers.list_object_parts(bucket, object, &upload_id, "", "", &bad_secret);
    assert_eq!(bad_signature.status, 403);
    assert_eq!(
        xml_tag_text(&bad_signature.body, "Code"),
        "SignatureDoesNotMatch"
    );

    let bad_marker = handlers.list_object_parts(bucket, object, &upload_id, "-1", "", &auth_v4);
    assert_eq!(bad_marker.status, 400);
    assert_eq!(
        xml_tag_text(&bad_marker.body, "Code"),
        "InvalidPartNumberMarker"
    );

    let bad_max_parts = handlers.list_object_parts(bucket, object, &upload_id, "", "-1", &auth_v4);
    assert_eq!(bad_max_parts.status, 400);
    assert_eq!(xml_tag_text(&bad_max_parts.body, "Code"), "InvalidMaxParts");

    let missing_upload = handlers.list_object_parts(bucket, object, "upload1", "", "", &auth_v4);
    assert_eq!(missing_upload.status, 404);
    assert_eq!(xml_tag_text(&missing_upload.body, "Code"), "NoSuchUpload");

    let response_v4 = handlers.list_object_parts(bucket, object, &upload_id, "", "1", &auth_v4);
    assert_eq!(response_v4.status, 200);
    assert_eq!(xml_tag_values(&response_v4.body, "PartNumber"), vec!["1"]);
    assert_eq!(xml_tag_text(&response_v4.body, "IsTruncated"), "true");
    assert_eq!(xml_tag_text(&response_v4.body, "NextPartNumberMarker"), "1");

    let response_v2 = handlers.list_object_parts(bucket, object, &upload_id, "1", "1000", &auth_v2);
    assert_eq!(response_v2.status, 200);
    assert_eq!(xml_tag_values(&response_v2.body, "PartNumber"), vec!["2"]);
    assert_eq!(xml_tag_text(&response_v2.body, "IsTruncated"), "false");
}

#[test]
fn subtest_file_scope_fmt_sprintf_min_io_s_test_case_d_failed_line_4136() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "list-parts-subtest-bucket";
    let object = "subtest-object";
    must_make_bucket(layer, bucket);
    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart")
        .upload_id;
    layer
        .put_object_part(
            bucket,
            object,
            &upload_id,
            1,
            &PutObjReader {
                data: b"hello".to_vec(),
                declared_size: 5,
                expected_md5: get_md5_hash(b"hello"),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put part");
    let response = handlers.list_object_parts(
        bucket,
        object,
        &upload_id,
        "",
        "",
        &RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key),
    );
    assert_eq!(response.status, 200);
    assert_eq!(xml_tag_values(&response.body, "PartNumber"), vec!["1"]);
}
