use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use url::Url;

pub const VERSION: &str = "DEVELOPMENT.GOGET";
pub const RELEASE_TAG: &str = "DEVELOPMENT.GOGET";
pub const COMMIT_ID: &str = "DEVELOPMENT.GOGET";
pub const MINIO_RELEASE_TAG_TIME_LAYOUT: &str = "%Y-%m-%dT%H-%M-%SZ";
pub const MINIO_UA_NAME: &str = "MinIO";
pub const MINIO_RELEASE_BASE_URL: &str = "https://dl.min.io/server/minio/release/";
pub const KUBERNETES_DEPLOYMENT_DOC: &str =
    "https://docs.min.io/community/minio-object-store/operations/deployments/kubernetes.html";
pub const MESOS_DEPLOYMENT_DOC: &str =
    "https://docs.min.io/community/minio-object-store/operations/deployments/kubernetes.html";

pub fn minio_release_url() -> String {
    format!(
        "{}{}-{}/",
        MINIO_RELEASE_BASE_URL,
        std::env::consts::OS,
        std::env::consts::ARCH
    )
}

pub fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

pub fn minio_version_to_release_time(version: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(version)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|err| err.to_string())
}

pub fn release_time_to_release_tag(release_time: DateTime<Utc>) -> String {
    format!(
        "RELEASE.{}",
        release_time.format(MINIO_RELEASE_TAG_TIME_LAYOUT)
    )
}

pub fn release_tag_to_release_time(release_tag: &str) -> Result<DateTime<Utc>, String> {
    let fields = release_tag.split('.').collect::<Vec<_>>();
    if fields.len() < 2 || fields.len() > 4 || fields[0] != "RELEASE" {
        return Err(format!("{release_tag} is not a valid release tag"));
    }

    NaiveDateTime::parse_from_str(fields[1], MINIO_RELEASE_TAG_TIME_LAYOUT)
        .map(|time| DateTime::<Utc>::from_naive_utc_and_offset(time, Utc))
        .map_err(|_| format!("{release_tag} is not a valid release tag"))
}

pub fn is_docker() -> bool {
    ["/.dockerenv", "/run/.containerenv"]
        .iter()
        .any(|path| Path::new(path).exists())
        || env::var("MINIO_ACCESS_KEY_FILE").is_ok()
}

pub fn is_dcos() -> bool {
    env::var("MESOS_CONTAINER_NAME")
        .map(|value| !value.is_empty())
        .unwrap_or(false)
}

pub fn is_kubernetes() -> bool {
    env::var("KUBERNETES_SERVICE_HOST")
        .map(|value| !value.is_empty())
        .unwrap_or(false)
}

pub fn get_helm_version(helm_info_file_path: &str) -> String {
    if helm_info_file_path.is_empty() {
        return String::new();
    }

    let Ok(content) = fs::read_to_string(helm_info_file_path) else {
        return String::new();
    };

    for line in content.lines() {
        if let Some(value) = line.strip_prefix("chart=") {
            return value.trim_matches('"').to_string();
        }
    }

    String::new()
}

pub fn is_source_build() -> bool {
    minio_version_to_release_time(VERSION).is_err()
}

pub fn get_user_agent(mode: &str) -> String {
    let mut parts = vec![
        format!("{MINIO_UA_NAME} ({}", std::env::consts::OS),
        format!("; {}", std::env::consts::ARCH),
    ];
    if !mode.is_empty() {
        parts.push(format!("; {mode}"));
    }
    if is_dcos() {
        parts.push("; dcos".to_string());
    }
    if is_kubernetes() {
        parts.push("; kubernetes".to_string());
    }
    if is_docker() {
        parts.push("; docker".to_string());
    }
    if is_source_build() {
        parts.push("; source".to_string());
    }

    parts.push(format!(" {VERSION} {RELEASE_TAG} {COMMIT_ID}"));

    if is_dcos() {
        if let Ok(version) = env::var("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION") {
            if !version.is_empty() {
                parts.push(format!(" universe-{version}"));
            }
        }
    }

    if is_kubernetes() {
        let helm_version = get_helm_version("/podinfo/labels");
        if !helm_version.is_empty() {
            parts.push(format!(" helm-{helm_version}"));
        }
    }

    parts.join("")
}

pub fn get_download_url(release_tag: &str) -> String {
    if is_dcos() {
        return MESOS_DEPLOYMENT_DOC.to_string();
    }
    if is_kubernetes() {
        return KUBERNETES_DEPLOYMENT_DOC.to_string();
    }
    if is_docker() {
        return format!("podman pull quay.io/minio/minio:{release_tag}");
    }
    if cfg!(windows) {
        return format!("{}minio.exe", minio_release_url());
    }
    format!("{}minio", minio_release_url())
}

pub fn download_release_url(url: &Url, timeout: Duration, mode: &str) -> Result<String, String> {
    if url.scheme() != "http" {
        return Err(format!("unsupported URL scheme {}", url.scheme()));
    }

    let host = url
        .host_str()
        .ok_or_else(|| format!("Error downloading URL {}. Response: invalid host", url))?;
    let port = url.port_or_known_default().unwrap_or(80);
    let path = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_string(),
    };

    let mut stream = TcpStream::connect((host, port)).map_err(|err| err.to_string())?;
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: {}\r\nConnection: close\r\n\r\n",
        if path.is_empty() { "/" } else { &path },
        host,
        get_user_agent(mode)
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|err| err.to_string())?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|err| err.to_string())?;
    let response = String::from_utf8_lossy(&response);
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| "invalid HTTP response".to_string())?;
    let mut lines = head.lines();
    let status_line = lines.next().unwrap_or_default();
    let status = status_line.splitn(3, ' ').collect::<Vec<_>>();
    if status.len() < 3 {
        return Err("invalid HTTP response status".to_string());
    }
    if status[1] != "200" {
        return Err(format!(
            "Error downloading URL {}. Response: {} {}",
            url, status[1], status[2]
        ));
    }

    Ok(body.to_string())
}

pub fn release_info_to_release_time(release_info: &str) -> Result<DateTime<Utc>, String> {
    let fields = release_info.splitn(2, '.').collect::<Vec<_>>();
    if fields.len() != 2 {
        return Err(format!("Unknown release information `{release_info}`"));
    }
    if fields[0] != "minio" {
        return Err(format!("Unknown release `{release_info}`"));
    }
    release_tag_to_release_time(fields[1])
        .map_err(|err| format!("Unknown release tag format. {err}"))
}

pub fn parse_release_data(data: &str) -> Result<(Vec<u8>, DateTime<Utc>, String), String> {
    let fields = data.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 2 {
        return Err(format!("Unknown release data `{data}`"));
    }

    let sha256_sum = hex::decode(fields[0]).map_err(|err| err.to_string())?;
    let release_info = fields[1].to_string();
    let release_time = release_info_to_release_time(&release_info)?;
    Ok((sha256_sum, release_time, release_info))
}
