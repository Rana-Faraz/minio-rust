// Rust test snapshot derived from cmd/utils_test.go.

use std::collections::BTreeMap;

use serde::Deserialize;

use minio_rust::cmd::{
    base_ignored_errs, check_url, dump_request, get_minio_mode, is_err_ignored, is_max_object_size,
    is_max_part_id, is_min_allowed_part_size, lcp, path2_bucket_object, rest_queries,
    set_minio_mode_flags, start_profiler, to_s3_etag, RequestDumpInput, ERR_FAULTY_DISK,
    GLOBAL_MAX_OBJECT_SIZE, GLOBAL_MAX_PART_ID, GLOBAL_MINIO_MODE_DIST_ERASURE,
    GLOBAL_MINIO_MODE_ERASURE, GLOBAL_MINIO_MODE_FS, GLOBAL_MIN_PART_SIZE,
};

pub const SOURCE_FILE: &str = "cmd/utils_test.go";

#[test]
fn test_max_object_size_line_32() {
    assert!(is_max_object_size(GLOBAL_MAX_OBJECT_SIZE + 1));
    assert!(!is_max_object_size(GLOBAL_MAX_OBJECT_SIZE - 1));
}

#[test]
fn test_min_allowed_part_size_line_57() {
    assert!(is_min_allowed_part_size(GLOBAL_MIN_PART_SIZE + 1));
    assert!(!is_min_allowed_part_size(GLOBAL_MIN_PART_SIZE - 1));
}

#[test]
fn test_max_part_id_line_83() {
    assert!(!is_max_part_id(GLOBAL_MAX_PART_ID - 1));
    assert!(is_max_part_id(GLOBAL_MAX_PART_ID + 1));
}

#[test]
fn test_path2_bucket_object_name_line_109() {
    let test_cases = [
        ("/bucket/object", "bucket", "object"),
        ("/", "", ""),
        ("/bucket", "bucket", ""),
        ("/bucket/object/1/", "bucket", "object/1/"),
        ("/bucket/object/1///", "bucket", "object/1///"),
        ("/bucket/object///////", "bucket", "object///////"),
        ("/bucket////object////", "bucket", "///object////"),
        ("", "", ""),
    ];

    for (path, expected_bucket, expected_object) in test_cases {
        let (bucket, object) = path2_bucket_object(path);
        assert_eq!(bucket, expected_bucket);
        assert_eq!(object, expected_object);
    }
}

#[test]
fn test_start_profiler_line_179() {
    assert!(start_profiler("").is_err());
    assert!(start_profiler("cpu").is_ok());
}

#[test]
fn test_check_url_line_199() {
    let test_cases = [
        ("", false),
        (":", false),
        ("http://localhost/", true),
        ("http://127.0.0.1/", true),
        ("proto://myhostname/path", true),
    ];
    for (value, should_pass) in test_cases {
        let ok = check_url(value).is_ok();
        assert_eq!(ok, should_pass, "unexpected result for {value}");
    }
}

#[derive(Debug, Deserialize)]
struct DumpedRequest {
    method: String,
    #[serde(rename = "reqURI")]
    request_uri: String,
    header: BTreeMap<String, String>,
}

#[test]
fn test_dump_request_line_224() {
    let request = RequestDumpInput {
        method: "GET".to_string(),
        request_uri: "/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=USWUXHGYZQYFYFFIT3RE%2F20170529%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20170529T190139Z&X-Amz-Expires=600&X-Amz-Signature=19b58080999df54b446fc97304eb8dda60d3df1812ae97f3e8783351bfd9781d&X-Amz-SignedHeaders=host&prefix=Hello%2AWorld%2A".to_string(),
        host: "localhost:9000".to_string(),
        headers: BTreeMap::from([("content-md5".to_string(), "====test".to_string())]),
    };

    let dumped = dump_request(&request);
    let parsed: DumpedRequest =
        serde_json::from_str(&dumped.replace("%%", "%")).expect("parse dumped request");

    assert_eq!(parsed.method, "GET");
    assert_eq!(
        parsed.request_uri,
        "/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=USWUXHGYZQYFYFFIT3RE%2F20170529%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20170529T190139Z&X-Amz-Expires=600&X-Amz-Signature=19b58080999df54b446fc97304eb8dda60d3df1812ae97f3e8783351bfd9781d&X-Amz-SignedHeaders=host&prefix=Hello%2AWorld%2A"
    );
    assert_eq!(
        parsed.header,
        BTreeMap::from([
            ("content-md5".to_string(), "====test".to_string()),
            ("host".to_string(), "localhost:9000".to_string()),
        ])
    );
}

#[test]
fn test_to_s3_etag_line_271() {
    let test_cases = [
        ("\"8019e762\"", "8019e762-1"),
        (
            "5d57546eeb86b3eba68967292fba0644",
            "5d57546eeb86b3eba68967292fba0644-1",
        ),
        ("\"8019e762-1\"", "8019e762-1"),
        (
            "5d57546eeb86b3eba68967292fba0644-1",
            "5d57546eeb86b3eba68967292fba0644-1",
        ),
    ];
    for (etag, expected) in test_cases {
        assert_eq!(to_s3_etag(etag), expected);
    }
}

#[test]
fn test_ceil_frac_line_290() {
    let cases = [
        (0, 1, 0),
        (-1, 2, 0),
        (1, 2, 1),
        (1, 1, 1),
        (3, 2, 2),
        (54, 11, 5),
        (45, 11, 5),
        (-4, 3, -1),
        (4, -3, -1),
        (-4, -3, 2),
        (3, 0, 0),
    ];
    for (numerator, denominator, ceiling) in cases {
        assert_eq!(minio_rust::cmd::ceil_frac(numerator, denominator), ceiling);
    }
}

#[test]
fn test_is_err_ignored_line_315() {
    let mut ignored = base_ignored_errs();
    ignored.push("ignored error");

    assert!(!is_err_ignored(None, &ignored));
    assert!(is_err_ignored(Some("ignored error"), &ignored));
    assert!(is_err_ignored(Some(ERR_FAULTY_DISK), &ignored));
}

#[test]
fn test_queries_line_342() {
    assert_eq!(
        rest_queries(&["aaaa", "bbbb"]),
        vec!["aaaa", "{aaaa:.*}", "bbbb", "{bbbb:.*}"]
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_lcp_line_363() {
    let test_cases = [
        (vec!["", ""], ""),
        (vec!["a", "b"], ""),
        (vec!["a", "a"], "a"),
        (vec!["a/", "a/"], "a/"),
        (vec!["abcd/", ""], ""),
        (vec!["abcd/foo/", "abcd/bar/"], "abcd/"),
        (vec!["abcd/foo/bar/", "abcd/foo/bar/zoo"], "abcd/foo/bar/"),
    ];

    for (prefixes, expected) in test_cases {
        assert_eq!(lcp(&prefixes, true), expected);
    }
}

#[test]
fn test_get_minio_mode_line_385() {
    set_minio_mode_flags(true, false, false);
    assert_eq!(get_minio_mode(), GLOBAL_MINIO_MODE_DIST_ERASURE);

    set_minio_mode_flags(false, true, false);
    assert_eq!(get_minio_mode(), GLOBAL_MINIO_MODE_ERASURE);

    set_minio_mode_flags(false, false, false);
    assert_eq!(get_minio_mode(), GLOBAL_MINIO_MODE_FS);
}
