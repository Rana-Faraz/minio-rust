use std::collections::BTreeMap;

use url::form_urlencoded::byte_serialize;

use crate::cmd::{
    parse_post_policy_form, HandlerResponse, LocalObjectLayer, ObjectOptions, PostPolicyCondition,
    PutObjReader, ERR_BUCKET_NOT_FOUND, MINIO_META_BUCKET,
};

#[derive(Debug)]
pub struct PostPolicyHandlers {
    layer: LocalObjectLayer,
}

impl PostPolicyHandlers {
    pub fn new(layer: LocalObjectLayer) -> Self {
        Self { layer }
    }

    pub fn post_policy_bucket_handler(
        &self,
        bucket: &str,
        policy_json: &str,
        form_fields: &BTreeMap<String, String>,
        file_bytes: &[u8],
    ) -> HandlerResponse {
        post_policy_bucket(&self.layer, bucket, policy_json, form_fields, file_bytes)
    }
}

pub fn post_policy_bucket(
    layer: &LocalObjectLayer,
    bucket: &str,
    policy_json: &str,
    form_fields: &BTreeMap<String, String>,
    file_bytes: &[u8],
) -> HandlerResponse {
    if bucket == MINIO_META_BUCKET {
        return HandlerResponse {
            status: 403,
            headers: BTreeMap::new(),
            body: b"reserved bucket".to_vec(),
        };
    }

    let policy = match parse_post_policy_form(policy_json) {
        Ok(policy) => policy,
        Err(err) => {
            return HandlerResponse {
                status: 400,
                headers: BTreeMap::new(),
                body: err.into_bytes(),
            };
        }
    };

    if policy.bucket().is_some_and(|value| value != bucket) {
        return HandlerResponse {
            status: 403,
            headers: BTreeMap::new(),
            body: b"bucket mismatch".to_vec(),
        };
    }

    if let Err(err) =
        validate_policy_fields(&policy.conditions, form_fields, file_bytes.len() as i64)
    {
        return HandlerResponse {
            status: 403,
            headers: BTreeMap::new(),
            body: err.into_bytes(),
        };
    }

    let key = match form_fields.get("key") {
        Some(value) if !value.is_empty() => value,
        _ => {
            return HandlerResponse {
                status: 400,
                headers: BTreeMap::new(),
                body: b"missing key".to_vec(),
            };
        }
    };

    match layer.put_object(
        bucket,
        key,
        &PutObjReader {
            data: file_bytes.to_vec(),
            declared_size: file_bytes.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        ObjectOptions::default(),
    ) {
        Ok(_) => {
            if let Some(redirect) = form_fields.get("success_action_redirect") {
                let separator = if redirect.contains('?') { '&' } else { '?' };
                let location = format!(
                    "{redirect}{separator}bucket={bucket}&key={}",
                    byte_serialize(key.as_bytes()).collect::<String>()
                );
                let mut headers = BTreeMap::new();
                headers.insert("location".to_string(), location);
                return HandlerResponse {
                    status: 303,
                    headers,
                    body: Vec::new(),
                };
            }
            HandlerResponse {
                status: 204,
                headers: BTreeMap::new(),
                body: Vec::new(),
            }
        }
        Err(err) if err == ERR_BUCKET_NOT_FOUND => HandlerResponse {
            status: 404,
            headers: BTreeMap::new(),
            body: b"bucket not found".to_vec(),
        },
        Err(err) => HandlerResponse {
            status: 500,
            headers: BTreeMap::new(),
            body: err.into_bytes(),
        },
    }
}

fn validate_policy_fields(
    conditions: &[PostPolicyCondition],
    form_fields: &BTreeMap<String, String>,
    content_length: i64,
) -> Result<(), String> {
    for condition in conditions {
        match condition {
            PostPolicyCondition::Equals { field, value } => {
                if field == "bucket" {
                    continue;
                }
                let actual = form_fields
                    .get(field)
                    .ok_or_else(|| format!("missing field: {field}"))?;
                if actual != value {
                    return Err(format!("field mismatch: {field}"));
                }
            }
            PostPolicyCondition::StartsWith { field, value } => {
                let actual = form_fields
                    .get(field)
                    .ok_or_else(|| format!("missing field: {field}"))?;
                if !actual.starts_with(value) {
                    return Err(format!("field prefix mismatch: {field}"));
                }
            }
            PostPolicyCondition::ContentLengthRange { min, max } => {
                if content_length < *min || content_length > *max {
                    return Err("content length out of range".to_string());
                }
            }
        }
    }
    Ok(())
}
