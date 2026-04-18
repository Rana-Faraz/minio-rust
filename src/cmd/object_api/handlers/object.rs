use super::*;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use crc::{Crc, CRC_32_ISCSI, CRC_32_ISO_HDLC};
use sha1::Sha1;

const MAX_OBJECT_SIZE: i64 = 5 * 1024 * 1024 * 1024 * 1024;

fn object_resource(bucket: &str, object: &str) -> String {
    format!("/{bucket}/{object}")
}

fn trim_aws_chunked_content_encoding(value: &str) -> String {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty() && *part != "aws-chunked")
        .collect::<Vec<_>>()
        .join(",")
}

fn is_persisted_object_header(key: &str) -> bool {
    matches!(
        key,
        "cache-control"
            | "content-disposition"
            | "content-encoding"
            | "content-language"
            | "content-type"
            | "expires"
            | "x-amz-server-side-encryption"
            | "x-amz-server-side-encryption-aws-kms-key-id"
            | "x-amz-bucket-replication-status"
            | "x-minio-internal-replica"
            | "x-amz-checksum-crc32"
            | "x-amz-checksum-crc32c"
            | "x-amz-checksum-sha1"
            | "x-amz-checksum-sha256"
    ) || key.starts_with("x-amz-meta-")
}

fn is_response_object_header(key: &str) -> bool {
    is_persisted_object_header(key) && key != "x-minio-internal-replica"
}

fn has_sse_customer_headers(headers: &BTreeMap<String, String>) -> bool {
    headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
        || headers.contains_key("x-amz-server-side-encryption-customer-key")
        || headers.contains_key("x-amz-server-side-encryption-customer-key-md5")
}

fn validate_sse_headers(
    info: &ObjectInfo,
    resource: &str,
    bucket: &str,
    object: &str,
    headers: &BTreeMap<String, String>,
) -> Result<(), HandlerResponse> {
    let object_requires_sse = info
        .user_defined
        .contains_key("x-amz-server-side-encryption-customer-algorithm");
    if object_requires_sse && !has_sse_customer_headers(headers) {
        return Err(api_error_response(
            400,
            "InvalidRequest",
            "Requests specifying Server Side Encryption with Customer provided keys must provide the appropriate key.",
            resource,
            bucket,
            object,
        ));
    }
    Ok(())
}

fn validate_md5_header(
    reader: &PutObjReader,
    headers: &BTreeMap<String, String>,
    resource: &str,
    bucket: &str,
    object: &str,
) -> Result<(), HandlerResponse> {
    let Some(value) = headers.get("content-md5") else {
        return Ok(());
    };
    let decoded = BASE64_STANDARD.decode(value.as_bytes()).map_err(|_| {
        api_error_response(
            400,
            "InvalidDigest",
            "The Content-MD5 you specified was invalid.",
            resource,
            bucket,
            object,
        )
    })?;
    if decoded.len() != 16 || decoded != Md5::digest(&reader.data).to_vec() {
        return Err(api_error_response(
            400,
            "InvalidDigest",
            "The Content-MD5 you specified did not match what we received.",
            resource,
            bucket,
            object,
        ));
    }
    Ok(())
}

fn validate_checksum_headers(
    reader: &PutObjReader,
    headers: &BTreeMap<String, String>,
    resource: &str,
    bucket: &str,
    object: &str,
) -> Result<BTreeMap<String, String>, HandlerResponse> {
    let mut echoed = BTreeMap::new();
    let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    let crc32c = Crc::<u32>::new(&CRC_32_ISCSI);
    let candidates = [
        (
            "x-amz-checksum-crc32",
            crc32.checksum(&reader.data).to_be_bytes().to_vec(),
        ),
        (
            "x-amz-checksum-crc32c",
            crc32c.checksum(&reader.data).to_be_bytes().to_vec(),
        ),
        ("x-amz-checksum-sha1", Sha1::digest(&reader.data).to_vec()),
        (
            "x-amz-checksum-sha256",
            Sha256::digest(&reader.data).to_vec(),
        ),
    ];
    for (header, expected) in candidates {
        let Some(value) = headers.get(header) else {
            continue;
        };
        let decoded = BASE64_STANDARD.decode(value.as_bytes()).map_err(|_| {
            api_error_response(
                400,
                "InvalidArgument",
                "The provided checksum is malformed.",
                resource,
                bucket,
                object,
            )
        })?;
        if decoded.len() != expected.len() {
            return Err(api_error_response(
                400,
                "InvalidArgument",
                "The provided checksum is malformed.",
                resource,
                bucket,
                object,
            ));
        }
        if decoded != expected {
            return Err(api_error_response(
                400,
                "XAmzContentChecksumMismatch",
                "The provided checksum does not match what we calculated.",
                resource,
                bucket,
                object,
            ));
        }
        echoed.insert(header.to_string(), value.clone());
    }
    Ok(echoed)
}

pub(super) fn apply_bucket_default_encryption(
    layer: &LocalObjectLayer,
    bucket: &str,
    headers: &BTreeMap<String, String>,
    user_defined: &mut BTreeMap<String, String>,
) {
    if has_sse_customer_headers(headers)
        || headers.contains_key("x-amz-server-side-encryption")
        || user_defined.contains_key("x-amz-server-side-encryption")
    {
        return;
    }

    let Ok(Some(config)) = read_bucket_encryption_config(layer, bucket) else {
        return;
    };
    let algorithm = config.algo();
    if algorithm.is_empty() {
        return;
    }
    user_defined.insert(
        "x-amz-server-side-encryption".to_string(),
        algorithm.to_string(),
    );
    let key_id = config.key_id();
    if !key_id.is_empty() {
        user_defined.insert(
            "x-amz-server-side-encryption-aws-kms-key-id".to_string(),
            key_id,
        );
    }
}

pub(super) fn append_object_metadata_headers(
    response: &mut HandlerResponse,
    user_defined: &BTreeMap<String, String>,
) {
    for (key, value) in user_defined {
        if is_response_object_header(key) {
            response.headers.insert(key.clone(), value.clone());
        }
    }
}

impl ObjectApiHandlers {
    pub fn put_object(
        &self,
        bucket: &str,
        object: &str,
        reader: &PutObjReader,
        auth: &RequestAuth,
        headers: &BTreeMap<String, String>,
    ) -> HandlerResponse {
        let resource = object_resource(bucket, object);
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        if headers.contains_key("x-amz-copy-source") {
            return api_error_response(
                400,
                "InvalidArgument",
                "Copy source is not supported for PutObject.",
                &resource,
                bucket,
                object,
            );
        }
        if let Some(storage_class) = headers.get("x-amz-storage-class") {
            if storage_class != "STANDARD" {
                return api_error_response(
                    400,
                    "InvalidStorageClass",
                    "The storage class you specified is not valid.",
                    &resource,
                    bucket,
                    object,
                );
            }
        }
        if reader.declared_size < 0 {
            return api_error_response(
                411,
                "MissingContentLength",
                "You must provide the Content-Length HTTP header.",
                &resource,
                bucket,
                object,
            );
        }
        if reader.declared_size > MAX_OBJECT_SIZE {
            return api_error_response(
                400,
                "EntityTooLarge",
                "Your proposed upload exceeds the maximum allowed object size.",
                &resource,
                bucket,
                object,
            );
        }
        if let Err(response) = validate_md5_header(reader, headers, &resource, bucket, object) {
            return response;
        }
        let checksum_headers =
            match validate_checksum_headers(reader, headers, &resource, bucket, object) {
                Ok(headers) => headers,
                Err(response) => return response,
            };
        let mut user_defined = BTreeMap::new();
        for (key, value) in headers {
            if key == "content-encoding" {
                let trimmed = trim_aws_chunked_content_encoding(value);
                if !trimmed.is_empty() {
                    user_defined.insert("content-encoding".to_string(), trimmed);
                }
                continue;
            }
            if is_persisted_object_header(key) {
                user_defined.insert(key.clone(), value.clone());
            }
        }
        apply_bucket_default_encryption(layer, bucket, headers, &mut user_defined);
        match layer.put_object(
            bucket,
            object,
            reader,
            ObjectOptions {
                user_defined,
                ..ObjectOptions::default()
            },
        ) {
            Ok(info) => {
                let _ = replicate_object_for_layer(
                    layer,
                    &self.replication_targets,
                    self.replication_service(),
                    bucket,
                    object,
                    &info,
                    &reader.data,
                );
                let mut response = HandlerResponse::status_only(200);
                response
                    .headers
                    .insert("etag".to_string(), format!("\"{}\"", info.etag));
                response.headers.extend(checksum_headers);
                append_object_metadata_headers(&mut response, &info.user_defined);
                response
            }
            Err(err) if err == ERR_BUCKET_NOT_FOUND => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_OBJECT_NAME_INVALID => api_error_response(
                400,
                "InvalidObjectName",
                "The specified object name is not valid.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_BAD_DIGEST => api_error_response(
                400,
                "InvalidDigest",
                "The Content-MD5 you specified did not match what we received.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_INCOMPLETE_BODY => api_error_response(
                400,
                "IncompleteBody",
                "You did not provide the number of bytes specified by the Content-Length HTTP header.",
                &resource,
                bucket,
                object,
            ),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                object,
            ),
        }
    }

    pub fn put_object_streaming(
        &self,
        bucket: &str,
        object: &str,
        reader: &PutObjReader,
        auth: &RequestAuth,
        headers: &BTreeMap<String, String>,
        has_date_header: bool,
        decoded_content_length: &str,
    ) -> HandlerResponse {
        let resource = object_resource(bucket, object);
        if !has_date_header {
            return api_error_response(
                400,
                "AccessDenied",
                "AWS authentication requires a valid Date or x-amz-date header.",
                &resource,
                bucket,
                object,
            );
        }
        if decoded_content_length.parse::<i64>().is_err() {
            return api_error_response(
                400,
                "BadRequest",
                "400 BadRequest",
                &resource,
                bucket,
                object,
            );
        }
        self.put_object(bucket, object, reader, auth, headers)
    }

    pub fn head_object(
        &self,
        bucket: &str,
        object: &str,
        auth: &RequestAuth,
        headers: &BTreeMap<String, String>,
    ) -> HandlerResponse {
        let resource = object_resource(bucket, object);
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.get_object_info(bucket, object) {
            Ok(info) => {
                if let Err(response) =
                    validate_sse_headers(&info, &resource, bucket, object, headers)
                {
                    return response;
                }
                let mut response = HandlerResponse::status_only(200);
                response
                    .headers
                    .insert("content-length".to_string(), info.size.to_string());
                append_object_metadata_headers(&mut response, &info.user_defined);
                response
            }
            Err(err) if err == ERR_FILE_NOT_FOUND => api_error_response(
                404,
                "NoSuchKey",
                "The specified key does not exist.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_BUCKET_NOT_FOUND => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_OBJECT_NAME_INVALID => api_error_response(
                400,
                "InvalidObjectName",
                "The specified object name is not valid.",
                &resource,
                bucket,
                object,
            ),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                object,
            ),
        }
    }

    pub fn get_object(
        &self,
        bucket: &str,
        object: &str,
        auth: &RequestAuth,
        range: Option<&str>,
    ) -> HandlerResponse {
        self.get_object_with_headers(bucket, object, auth, range, &BTreeMap::new())
    }

    pub fn get_object_with_headers(
        &self,
        bucket: &str,
        object: &str,
        auth: &RequestAuth,
        range: Option<&str>,
        headers: &BTreeMap<String, String>,
    ) -> HandlerResponse {
        let resource = object_resource(bucket, object);
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        let info = match layer.get_object_info(bucket, object) {
            Ok(info) => info,
            Err(err) if err == ERR_FILE_NOT_FOUND => {
                return api_error_response(
                    404,
                    "NoSuchKey",
                    "The specified key does not exist.",
                    &resource,
                    bucket,
                    object,
                );
            }
            Err(err) if err == ERR_BUCKET_NOT_FOUND => {
                return api_error_response(
                    404,
                    "NoSuchBucket",
                    "The specified bucket does not exist.",
                    &resource,
                    bucket,
                    object,
                );
            }
            Err(err) if err == ERR_OBJECT_NAME_INVALID => {
                return api_error_response(
                    400,
                    "InvalidObjectName",
                    "The specified object name is not valid.",
                    &resource,
                    bucket,
                    object,
                );
            }
            Err(_) => {
                return api_error_response(
                    500,
                    "InternalError",
                    "We encountered an internal error, please try again.",
                    &resource,
                    bucket,
                    object,
                );
            }
        };
        if let Err(response) = validate_sse_headers(&info, &resource, bucket, object, headers) {
            return response;
        }
        match layer.get_object(bucket, object) {
            Ok(data) => {
                if let Some(range) = range {
                    match parse_request_range_spec(range)
                        .and_then(|spec| spec.get_offset_length(data.len() as i64))
                    {
                        Ok((offset, length)) => {
                            let start = offset as usize;
                            let end = (offset + length) as usize;
                            let mut response = HandlerResponse::status_only(206);
                            response
                                .headers
                                .insert("content-length".to_string(), length.to_string());
                            response.body = data[start..end].to_vec();
                            return response;
                        }
                        Err(err) if is_err_invalid_range(&err) => {
                            return api_error_response(
                                416,
                                "InvalidRange",
                                "The requested range is not satisfiable.",
                                &resource,
                                bucket,
                                object,
                            );
                        }
                        Err(_) => {
                            return api_error_response(
                                400,
                                "InvalidArgument",
                                "The request range cannot be parsed.",
                                &resource,
                                bucket,
                                object,
                            );
                        }
                    }
                }
                let mut response = HandlerResponse::status_only(200);
                response
                    .headers
                    .insert("content-length".to_string(), data.len().to_string());
                append_object_metadata_headers(&mut response, &info.user_defined);
                response.body = data;
                response
            }
            Err(err) if err == ERR_FILE_NOT_FOUND => api_error_response(
                404,
                "NoSuchKey",
                "The specified key does not exist.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_BUCKET_NOT_FOUND => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_OBJECT_NAME_INVALID => api_error_response(
                400,
                "InvalidObjectName",
                "The specified object name is not valid.",
                &resource,
                bucket,
                object,
            ),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                object,
            ),
        }
    }

    pub fn get_object_part_number(
        &self,
        bucket: &str,
        object: &str,
        part_number: i32,
        auth: &RequestAuth,
        headers: &BTreeMap<String, String>,
    ) -> HandlerResponse {
        let resource = object_resource(bucket, object);
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        let info = match layer.get_object_info(bucket, object) {
            Ok(info) => info,
            Err(err) if err == ERR_FILE_NOT_FOUND => {
                return api_error_response(
                    404,
                    "NoSuchKey",
                    "The specified key does not exist.",
                    &resource,
                    bucket,
                    object,
                );
            }
            Err(err) if err == ERR_BUCKET_NOT_FOUND => {
                return api_error_response(
                    404,
                    "NoSuchBucket",
                    "The specified bucket does not exist.",
                    &resource,
                    bucket,
                    object,
                );
            }
            Err(err) if err == ERR_OBJECT_NAME_INVALID => {
                return api_error_response(
                    400,
                    "InvalidObjectName",
                    "The specified object name is not valid.",
                    &resource,
                    bucket,
                    object,
                );
            }
            Err(_) => {
                return api_error_response(
                    500,
                    "InternalError",
                    "We encountered an internal error, please try again.",
                    &resource,
                    bucket,
                    object,
                );
            }
        };
        if let Err(response) = validate_sse_headers(&info, &resource, bucket, object, headers) {
            return response;
        }

        match layer.get_object_part(bucket, object, part_number) {
            Ok(data) => {
                let mut response =
                    HandlerResponse::status_only(if info.parts.len() > 1 { 206 } else { 200 });
                response
                    .headers
                    .insert("content-length".to_string(), data.len().to_string());
                append_object_metadata_headers(&mut response, &info.user_defined);
                response.body = data;
                response
            }
            Err(err) if err == ERR_INVALID_RANGE => api_error_response(
                416,
                "InvalidRange",
                "The requested range is not satisfiable.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_FILE_NOT_FOUND => api_error_response(
                404,
                "NoSuchKey",
                "The specified key does not exist.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_BUCKET_NOT_FOUND => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                object,
            ),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                object,
            ),
        }
    }

    pub fn delete_object(
        &self,
        bucket: &str,
        object: &str,
        auth: &RequestAuth,
        headers: &BTreeMap<String, String>,
    ) -> HandlerResponse {
        let resource = object_resource(bucket, object);
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.delete_object(bucket, object, ObjectOptions::default()) {
            Ok(_) => {
                if headers
                    .get("x-minio-internal-replica")
                    .is_none_or(|value| value != "true")
                {
                    let _ = replicate_delete_for_layer(
                        layer,
                        &self.replication_targets,
                        self.replication_service(),
                        bucket,
                        object,
                    );
                }
                HandlerResponse::status_only(204)
            }
            Err(_) => HandlerResponse::status_only(204),
        }
    }
}
