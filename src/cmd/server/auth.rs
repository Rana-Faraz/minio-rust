use super::*;

#[derive(Debug)]
pub(super) struct ExtractedAuth {
    pub(super) auth: RequestAuth,
    pub(super) body: Vec<u8>,
}

pub(super) fn extract_request_auth(
    method: &Method,
    url: &Url,
    headers: &BTreeMap<String, String>,
    body: &[u8],
    credentials: &HandlerCredentials,
) -> Result<ExtractedAuth, String> {
    let body = body.to_vec();

    if let Some((access_key, secret_key)) = extract_basic_auth(headers) {
        return Ok(ExtractedAuth {
            auth: RequestAuth::signed_v4(&access_key, &secret_key),
            body,
        });
    }

    if let (Some(access_key), Some(secret_key)) = (
        headers.get("x-minio-access-key"),
        headers.get("x-minio-secret-key"),
    ) {
        return Ok(ExtractedAuth {
            auth: RequestAuth::signed_v4(access_key, secret_key),
            body,
        });
    }

    if let Some(authorization) = headers.get("authorization") {
        if authorization.starts_with("AWS ") {
            let request = to_test_request(method, url, headers, &body)?;
            let active = Credentials::new(&credentials.access_key, &credentials.secret_key);
            let (access_key, err) = does_signature_v2_match(&request, &active);
            if err == ApiErrorCode::None {
                return Ok(ExtractedAuth {
                    auth: RequestAuth::validated(RequestAuthKind::SignedV2, &access_key),
                    body,
                });
            }
            if matches!(get_request_auth_type(&request), AuthType::SignedV2) {
                return Ok(ExtractedAuth {
                    auth: RequestAuth::anonymous(),
                    body,
                });
            }
        }

        if authorization.starts_with("AWS4-HMAC-SHA256 ") {
            let request = to_test_request(method, url, headers, &body)?;
            let (access_key, err) = does_signature_v4_match(
                &request,
                &credentials.secret_key,
                GLOBAL_MINIO_DEFAULT_REGION,
            );
            if err == ApiErrorCode::None && access_key == credentials.access_key {
                return Ok(ExtractedAuth {
                    auth: RequestAuth::validated(RequestAuthKind::SignedV4, &access_key),
                    body,
                });
            }
            if matches!(get_request_auth_type(&request), AuthType::Signed) {
                return Ok(ExtractedAuth {
                    auth: RequestAuth::anonymous(),
                    body,
                });
            }
        }
    }

    let request = to_test_request(method, url, headers, &body)?;
    if matches!(get_request_auth_type(&request), AuthType::PresignedV2) {
        let active = Credentials::new(&credentials.access_key, &credentials.secret_key);
        let now_unix = chrono::Utc::now().timestamp();
        if does_presign_v2_signature_match(&request, &active, now_unix) == ApiErrorCode::None {
            return Ok(ExtractedAuth {
                auth: RequestAuth::validated(RequestAuthKind::PresignedV2, &credentials.access_key),
                body,
            });
        }
    }
    if matches!(get_request_auth_type(&request), AuthType::Presigned) {
        let (access_key, err) = does_presigned_signature_v4_match(
            &request,
            &credentials.secret_key,
            GLOBAL_MINIO_DEFAULT_REGION,
        );
        if err == ApiErrorCode::None {
            return Ok(ExtractedAuth {
                auth: RequestAuth::validated(RequestAuthKind::PresignedV4, &access_key),
                body,
            });
        }
    }

    Ok(ExtractedAuth {
        auth: RequestAuth::anonymous(),
        body,
    })
}

fn extract_basic_auth(headers: &BTreeMap<String, String>) -> Option<(String, String)> {
    let auth = headers.get("authorization")?;
    let encoded = auth.strip_prefix("Basic ")?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.as_bytes())
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

pub(super) fn to_test_request(
    method: &Method,
    url: &Url,
    headers: &BTreeMap<String, String>,
    body: &[u8],
) -> Result<TestRequest, String> {
    Ok(TestRequest {
        method: method.as_str().to_string(),
        url: url.clone(),
        headers: headers.clone(),
        body: body.to_vec(),
    })
}
