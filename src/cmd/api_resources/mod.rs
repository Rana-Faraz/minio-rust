use std::collections::BTreeMap;

use base64::Engine;

use crate::cmd::ApiErrorCode;

pub const MAX_OBJECT_LIST: i32 = 1000;
pub const MAX_PARTS_LIST: i32 = 1000;

pub type QueryValues = BTreeMap<String, Vec<String>>;

fn get(values: &QueryValues, key: &str) -> String {
    values
        .get(key)
        .and_then(|items| items.first())
        .cloned()
        .unwrap_or_default()
}

pub fn get_list_objects_v1_args(
    values: &QueryValues,
) -> (String, String, String, i32, String, ApiErrorCode) {
    let max_keys = if get(values, "max-keys").is_empty() {
        MAX_OBJECT_LIST
    } else {
        match get(values, "max-keys").parse::<i32>() {
            Ok(value) => value,
            Err(_) => {
                return (
                    String::new(),
                    String::new(),
                    String::new(),
                    0,
                    String::new(),
                    ApiErrorCode::InvalidMaxKeys,
                );
            }
        }
    };

    (
        get(values, "prefix"),
        get(values, "marker"),
        get(values, "delimiter"),
        max_keys,
        get(values, "encoding-type"),
        ApiErrorCode::None,
    )
}

pub fn get_list_objects_v2_args(
    values: &QueryValues,
) -> (
    String,
    String,
    String,
    String,
    bool,
    i32,
    String,
    ApiErrorCode,
) {
    if values
        .get("continuation-token")
        .and_then(|items| items.first())
        .is_some_and(|value| value.is_empty())
    {
        return (
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            false,
            0,
            String::new(),
            ApiErrorCode::IncorrectContinuationToken,
        );
    }

    let max_keys = if get(values, "max-keys").is_empty() {
        MAX_OBJECT_LIST
    } else {
        match get(values, "max-keys").parse::<i32>() {
            Ok(value) => value,
            Err(_) => {
                return (
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    false,
                    0,
                    String::new(),
                    ApiErrorCode::InvalidMaxKeys,
                );
            }
        }
    };

    let token = get(values, "continuation-token");
    let decoded_token = if token.is_empty() {
        String::new()
    } else {
        match base64::engine::general_purpose::STANDARD.decode(token) {
            Ok(decoded) => String::from_utf8(decoded).unwrap_or_default(),
            Err(_) => {
                return (
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    false,
                    0,
                    String::new(),
                    ApiErrorCode::IncorrectContinuationToken,
                );
            }
        }
    };

    (
        get(values, "prefix"),
        decoded_token,
        get(values, "start-after"),
        get(values, "delimiter"),
        get(values, "fetch-owner") == "true",
        max_keys,
        get(values, "encoding-type"),
        ApiErrorCode::None,
    )
}

pub fn get_object_resources(values: &QueryValues) -> (String, i32, i32, String, ApiErrorCode) {
    let max_parts = if get(values, "max-parts").is_empty() {
        MAX_PARTS_LIST
    } else {
        match get(values, "max-parts").parse::<i32>() {
            Ok(value) => value,
            Err(_) => {
                return (
                    String::new(),
                    0,
                    0,
                    String::new(),
                    ApiErrorCode::InvalidMaxParts,
                );
            }
        }
    };

    let part_number_marker = if get(values, "part-number-marker").is_empty() {
        0
    } else {
        match get(values, "part-number-marker").parse::<i32>() {
            Ok(value) => value,
            Err(_) => {
                return (
                    String::new(),
                    0,
                    0,
                    String::new(),
                    ApiErrorCode::InvalidPartNumberMarker,
                );
            }
        }
    };

    (
        get(values, "uploadId"),
        part_number_marker,
        max_parts,
        get(values, "encoding-type"),
        ApiErrorCode::None,
    )
}
