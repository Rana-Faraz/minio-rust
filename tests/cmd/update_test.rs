// Rust test snapshot derived from cmd/update_test.go.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use chrono::{TimeZone, Utc};

use minio_rust::cmd::{
    download_release_url, get_download_url, get_helm_version, get_user_agent, is_dcos, is_docker,
    is_kubernetes, minio_release_url, minio_version_to_release_time, parse_release_data,
    release_tag_to_release_time, release_time_to_release_tag, COMMIT_ID, GLOBAL_MINIO_MODE_ERASURE,
    GLOBAL_MINIO_MODE_FS, KUBERNETES_DEPLOYMENT_DOC, MESOS_DEPLOYMENT_DOC, RELEASE_TAG, VERSION,
};

pub const SOURCE_FILE: &str = "cmd/update_test.go";

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn one_shot_http_server(status: &str, body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let status = status.to_string();
    let body = body.to_string();
    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });
    format!("http://{addr}/")
}

#[test]
fn test_minio_version_to_release_time_line_33() {
    let cases = [
        ("2017-09-29T19:16:56Z", true),
        ("RELEASE.2017-09-29T19-16-56Z", false),
        ("DEVELOPMENT.GOGET", false),
    ];

    for (version, is_official) in cases {
        assert_eq!(
            minio_version_to_release_time(version).is_ok(),
            is_official,
            "{version}"
        );
    }
}

#[test]
fn test_release_tag_to_nfrom_time_conversion_line_51() {
    let cases = [
        (
            Utc.with_ymd_and_hms(2017, 9, 29, 19, 16, 56).unwrap(),
            "RELEASE.2017-09-29T19-16-56Z",
            "",
        ),
        (
            Utc.with_ymd_and_hms(2017, 8, 5, 0, 0, 53).unwrap(),
            "RELEASE.2017-08-05T00-00-53Z",
            "",
        ),
        (
            Utc::now(),
            "2017-09-29T19:16:56Z",
            "2017-09-29T19:16:56Z is not a valid release tag",
        ),
        (
            Utc::now(),
            "DEVELOPMENT.GOGET",
            "DEVELOPMENT.GOGET is not a valid release tag",
        ),
        (
            Utc.with_ymd_and_hms(2017, 8, 5, 0, 0, 53).unwrap(),
            "RELEASE.2017-08-05T00-00-53Z.hotfix",
            "",
        ),
        (
            Utc.with_ymd_and_hms(2017, 8, 5, 0, 0, 53).unwrap(),
            "RELEASE.2017-08-05T00-00-53Z.hotfix.aaaa",
            "",
        ),
    ];

    for (time, tag, err_str) in cases {
        let _ = time;
        let result = release_tag_to_release_time(tag);
        match result {
            Ok(parsed) => assert_eq!(parsed, time, "{tag}"),
            Err(err) => assert_eq!(err, err_str, "{tag}"),
        }
    }
}

#[test]
fn test_download_url_line_100() {
    let _guard = env_lock().lock().unwrap();
    std::env::remove_var("KUBERNETES_SERVICE_HOST");
    std::env::remove_var("MESOS_CONTAINER_NAME");

    let version = release_time_to_release_tag(Utc::now());
    let mut durl = get_download_url(&version);
    if is_docker() {
        assert_eq!(durl, format!("podman pull quay.io/minio/minio:{version}"));
    } else if cfg!(windows) {
        assert_eq!(durl, format!("{}minio.exe", minio_release_url()));
    } else {
        assert_eq!(durl, format!("{}minio", minio_release_url()));
    }

    std::env::set_var("KUBERNETES_SERVICE_HOST", "10.11.148.5");
    durl = get_download_url(&version);
    assert_eq!(durl, KUBERNETES_DEPLOYMENT_DOC);

    std::env::set_var("MESOS_CONTAINER_NAME", "mesos-1111");
    durl = get_download_url(&version);
    assert_eq!(durl, MESOS_DEPLOYMENT_DOC);

    std::env::remove_var("KUBERNETES_SERVICE_HOST");
    std::env::remove_var("MESOS_CONTAINER_NAME");
}

#[test]
fn test_user_agent_line_133() {
    let _guard = env_lock().lock().unwrap();
    let cases = [
        (
            "",
            "",
            GLOBAL_MINIO_MODE_FS,
            format!(
                "MinIO ({}; {}; {}; source {} {} {}",
                std::env::consts::OS,
                std::env::consts::ARCH,
                GLOBAL_MINIO_MODE_FS,
                VERSION,
                RELEASE_TAG,
                COMMIT_ID
            ),
        ),
        (
            "MESOS_CONTAINER_NAME",
            "mesos-11111",
            GLOBAL_MINIO_MODE_ERASURE,
            format!(
                "MinIO ({}; {}; {}; dcos; source {} {} {} universe-{}",
                std::env::consts::OS,
                std::env::consts::ARCH,
                GLOBAL_MINIO_MODE_ERASURE,
                VERSION,
                RELEASE_TAG,
                COMMIT_ID,
                "mesos-1111"
            ),
        ),
        (
            "KUBERNETES_SERVICE_HOST",
            "10.11.148.5",
            GLOBAL_MINIO_MODE_ERASURE,
            format!(
                "MinIO ({}; {}; {}; kubernetes; source {} {} {}",
                std::env::consts::OS,
                std::env::consts::ARCH,
                GLOBAL_MINIO_MODE_ERASURE,
                VERSION,
                RELEASE_TAG,
                COMMIT_ID
            ),
        ),
    ];

    for (env_name, env_value, mode, expected) in cases {
        std::env::remove_var("MESOS_CONTAINER_NAME");
        std::env::remove_var("KUBERNETES_SERVICE_HOST");
        std::env::remove_var("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION");
        if !env_name.is_empty() {
            std::env::set_var(env_name, env_value);
            if env_name == "MESOS_CONTAINER_NAME" {
                std::env::set_var("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION", "mesos-1111");
            }
        }

        let mut expected = expected;
        if is_docker() {
            expected = expected.replace("; source", "; docker; source");
        }

        let got = get_user_agent(mode);
        assert!(
            got.contains(&expected),
            "expected substring {expected:?}, got {got:?}"
        );
    }

    std::env::remove_var("MESOS_CONTAINER_NAME");
    std::env::remove_var("KUBERNETES_SERVICE_HOST");
    std::env::remove_var("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION");
}

#[test]
fn test_is_dcos_line_182() {
    let _guard = env_lock().lock().unwrap();
    std::env::set_var("MESOS_CONTAINER_NAME", "mesos-1111");
    assert!(is_dcos());
    std::env::remove_var("MESOS_CONTAINER_NAME");
    assert!(!is_dcos());
}

#[test]
fn test_is_kubernetes_line_196() {
    let _guard = env_lock().lock().unwrap();
    std::env::set_var("KUBERNETES_SERVICE_HOST", "10.11.148.5");
    assert!(is_kubernetes());
    std::env::remove_var("KUBERNETES_SERVICE_HOST");
    assert!(!is_kubernetes());
}

#[test]
fn test_get_helm_version_line_211() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("helm-labels");
    fs::write(
        &file,
        "app=\"virtuous-rat-minio\"\nchart=\"minio-0.1.3\"\nheritage=\"Tiller\"\npod-template-hash=\"818089471\"",
    )
    .unwrap();

    let cases = [
        ("".to_string(), "".to_string()),
        ("/tmp/non-existing-file".to_string(), "".to_string()),
        (
            file.to_string_lossy().to_string(),
            "minio-0.1.3".to_string(),
        ),
    ];

    for (filename, expected) in cases {
        assert_eq!(get_helm_version(&filename), expected, "{filename}");
    }
}

#[test]
fn test_download_release_data_line_252() {
    let server1 = one_shot_http_server("200 OK", "");
    let server2 = one_shot_http_server(
        "200 OK",
        "fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z\n",
    );
    let server3 = one_shot_http_server("404 Not Found", "");

    let cases = [
        (server1.clone(), "".to_string(), None),
        (
            server2.clone(),
            "fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z\n"
                .to_string(),
            None,
        ),
        (
            server3.clone(),
            "".to_string(),
            Some(format!(
                "Error downloading URL {}. Response: 404 Not Found",
                server3
            )),
        ),
    ];

    for (url, expected, expected_err) in cases {
        let parsed = url::Url::parse(&url).unwrap();
        let result = download_release_url(&parsed, Duration::from_secs(1), "");
        match expected_err {
            None => assert_eq!(result.expect("download"), expected, "{url}"),
            Some(expected_err) => assert_eq!(result, Err(expected_err), "{url}"),
        }
    }
}

#[test]
fn test_parse_release_data_line_298() {
    let release_time = release_tag_to_release_time("RELEASE.2016-10-07T01-16-39Z").unwrap();
    let cases = [
        ("more than two fields", None, "", "", true),
        ("more than", None, "", "", true),
        ("more than.two.fields", None, "", "", true),
        ("more minio.RELEASE.fields", None, "", "", true),
        ("more minio.RELEASE.2016-10-07T01-16-39Z", None, "", "", true),
        (
            "fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z\n",
            Some(release_time),
            "fbe246edbd382902db9a4035df7dce8cb441357d",
            "minio.RELEASE.2016-10-07T01-16-39Z",
            false,
        ),
        (
            "fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z.customer-hotfix\n",
            Some(release_time),
            "fbe246edbd382902db9a4035df7dce8cb441357d",
            "minio.RELEASE.2016-10-07T01-16-39Z.customer-hotfix",
            false,
        ),
    ];

    for (data, expected_time, expected_sha, expected_info, expected_err) in cases {
        let result = parse_release_data(data);
        match result {
            Ok((sha, time, info)) => {
                assert!(!expected_err, "{data}");
                assert_eq!(hex::encode(sha), expected_sha, "{data}");
                assert_eq!(time, expected_time.unwrap(), "{data}");
                assert_eq!(info, expected_info, "{data}");
            }
            Err(_) => assert!(expected_err, "{data}"),
        }
    }
}
