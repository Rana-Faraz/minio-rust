use super::*;
use crate::internal::crypto;

pub const GRID_ROUTE_PATH: &str = "/minio/grid";
pub const GRID_ROUTE_LOCK_PATH: &str = "/minio/grid/lock";
pub const USER_METADATA_KEY_PREFIXES: &[&str] = &["x-amz-meta-"];

#[derive(Debug, Clone, Default)]
pub struct GenericRequest {
    pub proto: String,
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub tls: bool,
}

pub fn guess_is_rpc_req(request: Option<&GenericRequest>) -> bool {
    let Some(request) = request else {
        return false;
    };
    request.path == "/minio/lock"
        || request.path == GRID_ROUTE_PATH
        || request.path == GRID_ROUTE_LOCK_PATH
}

pub fn is_http_header_size_too_large(headers: &BTreeMap<String, String>) -> bool {
    let mut header_size = 0usize;
    let mut user_size = 0usize;
    for key in headers.keys() {
        let lower = key.to_ascii_lowercase();
        if USER_METADATA_KEY_PREFIXES
            .iter()
            .any(|prefix| lower.starts_with(prefix))
        {
            user_size += key.len();
        } else {
            header_size += key.len();
        }
    }
    header_size > 8 * 1024 || user_size >= 2 * 1024
}

pub fn contains_reserved_metadata(headers: &BTreeMap<String, String>) -> bool {
    headers.keys().any(|key| {
        let lower = key.to_ascii_lowercase();
        if lower == crypto::META_IV.to_ascii_lowercase()
            || lower == crypto::META_ALGORITHM.to_ascii_lowercase()
            || lower == crypto::META_SEALED_KEY_SSEC.to_ascii_lowercase()
        {
            return false;
        }
        lower.starts_with(&RESERVED_METADATA_PREFIX.to_ascii_lowercase())
    })
}

pub fn validate_sse_tls_request(request: &GenericRequest, global_tls: bool) -> u16 {
    let has_sse_c = request
        .headers
        .contains_key(crypto::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM);
    let has_sse_copy = request
        .headers
        .contains_key(crypto::AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM);
    if (has_sse_c || has_sse_copy) && !global_tls {
        return 400;
    }
    200
}

pub fn has_bad_path_component(input: &str) -> bool {
    input
        .split('/')
        .any(|part| matches!(part, "." | "..") && !part.is_empty())
}
