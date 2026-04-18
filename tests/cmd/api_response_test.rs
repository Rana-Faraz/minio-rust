use std::collections::BTreeMap;

use minio_rust::cmd::{
    get_object_location, get_url_scheme, headers_already_written, write_response, HeaderWriter,
    LocationRequest, TrackingResponseWriter, WrappedWriter, HTTPS_SCHEME, HTTP_SCHEME,
};

pub const SOURCE_FILE: &str = "cmd/api-response_test.go";

#[test]
fn test_object_location_line_30() {
    let cases = vec![
        (
            LocationRequest {
                host: "127.0.0.1:9000".to_string(),
                headers: [("X-Forwarded-Scheme".to_string(), HTTP_SCHEME.to_string())]
                    .into_iter()
                    .collect(),
                path: "/".to_string(),
            },
            vec![],
            "testbucket1",
            "test/1.txt",
            "http://127.0.0.1:9000/testbucket1/test/1.txt",
        ),
        (
            LocationRequest {
                host: "127.0.0.1:9000".to_string(),
                headers: [("X-Forwarded-Scheme".to_string(), HTTPS_SCHEME.to_string())]
                    .into_iter()
                    .collect(),
                path: "/".to_string(),
            },
            vec![],
            "testbucket1",
            "test/1.txt",
            "https://127.0.0.1:9000/testbucket1/test/1.txt",
        ),
        (
            LocationRequest {
                host: "s3.mybucket.org".to_string(),
                headers: [("X-Forwarded-Scheme".to_string(), HTTP_SCHEME.to_string())]
                    .into_iter()
                    .collect(),
                path: "/".to_string(),
            },
            vec![],
            "mybucket",
            "test/1.txt",
            "http://s3.mybucket.org/mybucket/test/1.txt",
        ),
        (
            LocationRequest {
                host: "mys3.mybucket.org".to_string(),
                headers: BTreeMap::new(),
                path: "/".to_string(),
            },
            vec![],
            "mybucket",
            "test/1.txt",
            "http://mys3.mybucket.org/mybucket/test/1.txt",
        ),
        (
            LocationRequest {
                host: "mybucket.mys3.bucket.org".to_string(),
                headers: BTreeMap::new(),
                path: "/".to_string(),
            },
            vec!["mys3.bucket.org".to_string()],
            "mybucket",
            "test/1.txt",
            "http://mybucket.mys3.bucket.org/test/1.txt",
        ),
        (
            LocationRequest {
                host: "mybucket.mys3.bucket.org".to_string(),
                headers: [("X-Forwarded-Scheme".to_string(), HTTPS_SCHEME.to_string())]
                    .into_iter()
                    .collect(),
                path: "/".to_string(),
            },
            vec!["mys3.bucket.org".to_string()],
            "mybucket",
            "test/1.txt",
            "https://mybucket.mys3.bucket.org/test/1.txt",
        ),
    ];

    for (request, domains, bucket, object, expected) in cases {
        assert_eq!(
            get_object_location(&request, &domains, bucket, object),
            expected
        );
    }
}

#[test]
fn subtest_test_object_location_line_107() {
    let request = LocationRequest {
        host: "mybucket.mys3.bucket.org".to_string(),
        headers: BTreeMap::new(),
        path: "/".to_string(),
    };
    assert_eq!(
        get_object_location(
            &request,
            &["mys3.bucket.org".to_string()],
            "mybucket",
            "test/1.txt"
        ),
        "http://mybucket.mys3.bucket.org/test/1.txt"
    );
}

#[test]
fn test_get_urlscheme_line_117() {
    assert_eq!(get_url_scheme(false), HTTP_SCHEME);
    assert_eq!(get_url_scheme(true), HTTPS_SCHEME);
}

#[test]
fn test_tracking_response_writer_line_130() {
    let mut writer = TrackingResponseWriter::default();
    writer.write_header(123);
    assert!(writer.header_written);
    writer.write_body(b"hello").expect("write");
    assert_eq!(writer.response.code, 123);
    assert_eq!(writer.response.body, b"hello");
    assert_eq!(writer.unwrap().code, 123);
}

#[test]
fn test_headers_already_written_line_162() {
    let mut writer = TrackingResponseWriter::default();
    assert!(!headers_already_written(&writer));
    writer.write_header(123);
    assert!(headers_already_written(&writer));
}

#[test]
fn test_headers_already_written_wrapped_line_176() {
    let writer = TrackingResponseWriter::default();
    let wrap1 = WrappedWriter { inner: writer };
    let mut wrap2 = WrappedWriter { inner: wrap1 };
    assert!(!headers_already_written(&wrap2));
    wrap2.write_header(123);
    assert!(headers_already_written(&wrap2));
}

#[test]
fn test_write_response_headers_not_written_line_192() {
    let mut writer = TrackingResponseWriter::default();
    write_response(&mut writer, 299, b"hello", "application/foo");
    assert_eq!(writer.response.code, 299);
    assert_eq!(writer.response.body, b"hello");
}

#[test]
fn test_write_response_headers_written_line_204() {
    let mut writer = TrackingResponseWriter {
        response: minio_rust::cmd::ResponseRecorder {
            code: -1,
            body: Vec::new(),
            headers: BTreeMap::new(),
        },
        header_written: true,
    };

    write_response(&mut writer, 200, b"hello", "application/foo");
    assert_eq!(writer.response.code, -1);
    assert!(writer.response.body.is_empty());
}
