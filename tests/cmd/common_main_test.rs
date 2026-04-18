use std::fs;

use tempfile::tempdir;

use minio_rust::cmd::{minio_environ_from_file, read_from_secret, EnvKv};

pub const SOURCE_FILE: &str = "cmd/common-main_test.go";

#[test]
fn test_read_from_secret_line_27() {
    let cases = [
        ("value\n", false, "value"),
        (" \t\n Hello, Gophers \n\t\r\n", false, "Hello, Gophers"),
    ];

    for (content, expected_err, expected_value) in cases {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("secret");
        fs::write(&path, content).expect("write");

        let result = read_from_secret(path.to_str().expect("utf8"));
        assert_eq!(result.is_err(), expected_err);
        if let Ok(value) = result {
            assert_eq!(value, expected_value);
        }
    }
}

#[test]
fn subtest_test_read_from_secret_line_46() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("secret");
    fs::write(&path, "  another value \n").expect("write");
    assert_eq!(
        read_from_secret(path.to_str().expect("utf8")).expect("secret"),
        "another value"
    );
}

#[test]
fn test_read_from_secret_returns_empty_for_missing_file() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("missing-secret");
    assert_eq!(
        read_from_secret(path.to_str().expect("utf8")).expect("missing secrets are ignored"),
        ""
    );
}

#[test]
fn test_minio_environ_from_file_line_69() {
    let cases = vec![
        (
            r#"
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123"#,
            false,
            vec![
                EnvKv {
                    key: "MINIO_ROOT_USER".to_string(),
                    value: "minio".to_string(),
                },
                EnvKv {
                    key: "MINIO_ROOT_PASSWORD".to_string(),
                    value: "minio123".to_string(),
                },
            ],
        ),
        (
            r#"export MINIO_ROOT_USER="minio""#,
            false,
            vec![EnvKv {
                key: "MINIO_ROOT_USER".to_string(),
                value: "minio".to_string(),
            }],
        ),
        (
            "export MINIO_ROOT_USER='minio'",
            false,
            vec![EnvKv {
                key: "MINIO_ROOT_USER".to_string(),
                value: "minio".to_string(),
            }],
        ),
        (
            r#"
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123"#,
            false,
            vec![
                EnvKv {
                    key: "MINIO_ROOT_USER".to_string(),
                    value: "minio".to_string(),
                },
                EnvKv {
                    key: "MINIO_ROOT_PASSWORD".to_string(),
                    value: "minio123".to_string(),
                },
            ],
        ),
        (
            r#"
export MINIO_ROOT_USERminio
export MINIO_ROOT_PASSWORD=minio123"#,
            true,
            vec![],
        ),
        (
            r#"
# simple comment
# MINIO_ROOT_USER=minioadmin
# MINIO_ROOT_PASSWORD=minioadmin
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123"#,
            false,
            vec![
                EnvKv {
                    key: "MINIO_ROOT_USER".to_string(),
                    value: "minio".to_string(),
                },
                EnvKv {
                    key: "MINIO_ROOT_PASSWORD".to_string(),
                    value: "minio123".to_string(),
                },
            ],
        ),
    ];

    for (content, expected_err, expected) in cases {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("env");
        fs::write(&path, content).expect("write");

        let result = minio_environ_from_file(path.to_str().expect("utf8"));
        assert_eq!(result.is_err(), expected_err);
        if let Ok(values) = result {
            assert_eq!(values, expected);
        }
    }
}

#[test]
fn subtest_test_minio_environ_from_file_line_157() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("env");
    fs::write(
        &path,
        "export MINIO_ROOT_USER=minio\nexport MINIO_ROOT_PASSWORD=minio123\n",
    )
    .expect("write");

    let values = minio_environ_from_file(path.to_str().expect("utf8")).expect("parse");
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].key, "MINIO_ROOT_USER");
    assert_eq!(values[1].key, "MINIO_ROOT_PASSWORD");
}

#[test]
fn test_minio_environ_from_file_ignores_missing_file() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("missing.env");
    let values = minio_environ_from_file(path.to_str().expect("utf8"))
        .expect("missing env file should be ignored");
    assert!(values.is_empty());
}
