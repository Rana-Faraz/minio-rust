use super::*;

fn parse_copy_source(copy_source: &str) -> Result<(String, String), ()> {
    let raw = copy_source.trim();
    if raw.is_empty() {
        return Err(());
    }
    let without_query = raw.split('?').next().unwrap_or(raw);
    let trimmed = without_query.trim_start_matches('/');
    let mut parts = trimmed.splitn(2, '/');
    let bucket = parts.next().unwrap_or_default().trim();
    let object = parts.next().unwrap_or_default().trim();
    if bucket.is_empty() || object.is_empty() {
        return Err(());
    }
    Ok((bucket.to_string(), object.to_string()))
}

fn copy_source_version_allowed(copy_source: &str) -> bool {
    let Some((_, query)) = copy_source.split_once('?') else {
        return true;
    };
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        let key = kv.next().unwrap_or_default();
        let value = kv.next().unwrap_or_default();
        if key == "versionId" {
            return value.is_empty() || value == "null";
        }
    }
    true
}

impl ObjectApiHandlers {
    pub fn copy_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number: &str,
        copy_source: &str,
        copy_source_range: Option<&str>,
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = format!("/{bucket}/{object}?partNumber={part_number}&uploadId={upload_id}");
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };

        let part_number = match part_number.parse::<i32>() {
            Ok(value) if value >= 1 && value <= 99_999 => value,
            _ => 1,
        };
        if !copy_source_version_allowed(copy_source) {
            return api_error_response(
                404,
                "NoSuchKey",
                "The specified key does not exist.",
                &resource,
                bucket,
                object,
            );
        }
        let (src_bucket, src_object) = match parse_copy_source(copy_source) {
            Ok(parsed) => parsed,
            Err(()) => {
                return api_error_response(
                    400,
                    "InvalidArgument",
                    "The copy source is malformed.",
                    &resource,
                    bucket,
                    object,
                );
            }
        };
        let source = match layer.get_object(&src_bucket, &src_object) {
            Ok(data) => data,
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
            Err(_) => {
                return api_error_response(
                    400,
                    "InvalidArgument",
                    "The copy source is malformed.",
                    &resource,
                    bucket,
                    object,
                );
            }
        };

        let payload = if let Some(range) = copy_source_range {
            if range.is_empty() {
                return api_error_response(
                    400,
                    "InvalidArgument",
                    "The requested copy range is not satisfiable.",
                    &resource,
                    bucket,
                    object,
                );
            }
            let spec = match parse_request_range_spec(range) {
                Ok(spec) => spec,
                Err(_) => {
                    return api_error_response(
                        400,
                        "InvalidArgument",
                        "The requested copy range is not satisfiable.",
                        &resource,
                        bucket,
                        object,
                    );
                }
            };
            let (offset, length) = match spec.get_offset_length(source.len() as i64) {
                Ok(result) => result,
                Err(_) => {
                    return api_error_response(
                        400,
                        "InvalidArgument",
                        "The requested copy range is not satisfiable.",
                        &resource,
                        bucket,
                        object,
                    );
                }
            };
            source[offset as usize..(offset + length) as usize].to_vec()
        } else {
            source
        };

        match layer.put_object_part(
            bucket,
            object,
            upload_id,
            part_number,
            &PutObjReader {
                declared_size: payload.len() as i64,
                data: payload,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        ) {
            Ok(part) => HandlerResponse::xml(
                200,
                format!(
                    "<CopyPartResult><ETag>{}</ETag></CopyPartResult>",
                    xml_escape(&part.etag),
                ),
            ),
            Err(err) if err.contains(ERR_INVALID_UPLOAD_ID) => api_error_response(
                404,
                "NoSuchUpload",
                "The specified multipart upload does not exist.",
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

    pub fn copy_object(
        &self,
        bucket: &str,
        object: &str,
        copy_source: &str,
        auth: &RequestAuth,
        headers: &BTreeMap<String, String>,
    ) -> HandlerResponse {
        let resource = format!("/{bucket}/{object}");
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        let (src_bucket, src_object) = match parse_copy_source(copy_source) {
            Ok(parsed) => parsed,
            Err(()) => {
                return api_error_response(
                    400,
                    "InvalidArgument",
                    "The copy source is malformed.",
                    &resource,
                    bucket,
                    object,
                );
            }
        };
        if !copy_source_version_allowed(copy_source) {
            return api_error_response(
                404,
                "NoSuchKey",
                "The specified key does not exist.",
                &resource,
                bucket,
                object,
            );
        }
        if src_bucket == bucket
            && src_object == object
            && headers
                .get("x-amz-metadata-directive")
                .is_none_or(|value| value != "REPLACE")
        {
            return api_error_response(
                400,
                "InvalidRequest",
                "This copy request is illegal because it is trying to copy an object to itself.",
                &resource,
                bucket,
                object,
            );
        }

        let source_info = match layer.get_object_info(&src_bucket, &src_object) {
            Ok(info) => info,
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
            Err(_) => {
                return api_error_response(
                    400,
                    "InvalidArgument",
                    "The copy source is malformed.",
                    &resource,
                    bucket,
                    object,
                );
            }
        };
        let source_data = match layer.get_object(&src_bucket, &src_object) {
            Ok(data) => data,
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

        let mut user_defined = source_info.user_defined.clone();
        if headers
            .get("x-amz-metadata-directive")
            .is_some_and(|value| value == "REPLACE")
        {
            user_defined.clear();
            if let Some(content_type) = headers.get("content-type") {
                user_defined.insert("content-type".to_string(), content_type.clone());
            }
        }

        match layer.put_object(
            bucket,
            object,
            &PutObjReader {
                data: source_data.clone(),
                declared_size: source_info.size,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
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
                    &source_data,
                );
                HandlerResponse::xml(
                    200,
                    format!(
                        "<CopyObjectResult><ETag>{}</ETag></CopyObjectResult>",
                        xml_escape(&info.etag),
                    ),
                )
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
}
