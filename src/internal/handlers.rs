use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use regex::Regex;

pub const X_FORWARDED_FOR: &str = "x-forwarded-for";
pub const X_FORWARDED_HOST: &str = "x-forwarded-host";
pub const X_FORWARDED_PORT: &str = "x-forwarded-port";
pub const X_FORWARDED_PROTO: &str = "x-forwarded-proto";
pub const X_FORWARDED_SCHEME: &str = "x-forwarded-scheme";
pub const X_REAL_IP: &str = "x-real-ip";
pub const FORWARDED: &str = "forwarded";

static ENABLE_XFF_HEADER: AtomicBool = AtomicBool::new(true);

pub type HeaderMap = HashMap<String, String>;

#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    pub headers: HeaderMap,
    pub remote_addr: String,
}

impl RequestContext {
    pub fn new(headers: impl IntoIterator<Item = (&'static str, &'static str)>) -> Self {
        Self {
            headers: headers
                .into_iter()
                .map(|(key, value)| (canonical_header(key), value.to_owned()))
                .collect(),
            remote_addr: String::new(),
        }
    }

    pub fn with_remote_addr(mut self, remote_addr: impl Into<String>) -> Self {
        self.remote_addr = remote_addr.into();
        self
    }
}

pub fn set_xff_header_enabled(enabled: bool) {
    ENABLE_XFF_HEADER.store(enabled, Ordering::SeqCst);
}

pub fn xff_header_enabled() -> bool {
    ENABLE_XFF_HEADER.load(Ordering::SeqCst)
}

pub fn get_source_scheme(request: &RequestContext) -> String {
    if let Some(proto) = header(&request.headers, X_FORWARDED_PROTO) {
        return proto.to_ascii_lowercase();
    }
    if let Some(proto) = header(&request.headers, X_FORWARDED_SCHEME) {
        return proto.to_ascii_lowercase();
    }

    let Some(forwarded) = header(&request.headers, FORWARDED) else {
        return String::new();
    };
    let Some(captures) = for_regex().captures(forwarded) else {
        return String::new();
    };
    let Some(rest) = captures.get(2).map(|match_| match_.as_str()) else {
        return String::new();
    };
    let Some(proto_match) = proto_regex().captures(rest) else {
        return String::new();
    };

    proto_match
        .get(2)
        .map(|match_| match_.as_str().to_ascii_lowercase())
        .unwrap_or_default()
}

pub fn get_source_ip_from_headers(request: &RequestContext) -> String {
    if xff_header_enabled() {
        if let Some(forwarded_for) = header(&request.headers, X_FORWARDED_FOR) {
            if let Some(first) = forwarded_for.split(", ").next() {
                if !first.is_empty() {
                    return first.to_owned();
                }
            }
        }
    }

    if let Some(real_ip) = header(&request.headers, X_REAL_IP) {
        if !real_ip.is_empty() {
            return real_ip.to_owned();
        }
    }

    let Some(forwarded) = header(&request.headers, FORWARDED) else {
        return String::new();
    };
    let Some(captures) = for_regex().captures(forwarded) else {
        return String::new();
    };
    captures
        .get(1)
        .map(|match_| match_.as_str().trim_matches('"').to_owned())
        .unwrap_or_default()
}

pub fn get_source_ip_raw(request: &RequestContext) -> String {
    let addr = {
        let from_headers = get_source_ip_from_headers(request);
        if from_headers.is_empty() {
            request.remote_addr.clone()
        } else {
            from_headers
        }
    };

    if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
        return socket_addr.ip().to_string();
    }

    if addr.starts_with('[') && addr.contains("]:") {
        if let Some((host, _)) = addr.rsplit_once("]:") {
            return host.trim_start_matches('[').to_owned();
        }
    }

    addr
}

pub fn get_source_ip(request: &RequestContext) -> String {
    let addr = get_source_ip_raw(request);
    if addr.contains(':') {
        return format!("[{addr}]");
    }
    addr
}

fn canonical_header(header: &str) -> String {
    header.to_ascii_lowercase()
}

fn header<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(&canonical_header(key)).map(String::as_str)
}

fn for_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?i)(?:for=)([^(;|,| )]+)(.*)").expect("valid for regex"))
}

fn proto_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?i)^(;|,| )+(?:proto=)(https|http)").expect("valid regex"))
}
