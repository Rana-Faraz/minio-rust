use super::object::{append_object_metadata_headers, apply_bucket_default_encryption};
use super::*;

const MAX_PART_NUMBER: i32 = 10_000;
const MAX_REQUEST_SIZE: i64 = 5 * 1024 * 1024 * 1024 * 1024;

fn multipart_resource(bucket: &str, object: &str, upload_id: Option<&str>) -> String {
    match upload_id {
        Some(upload_id) => format!("/{bucket}/{object}?uploadId={upload_id}"),
        None => format!("/{bucket}/{object}?uploads"),
    }
}

impl ObjectApiHandlers {
    pub fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number: &str,
        reader: &PutObjReader,
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = multipart_resource(bucket, object, Some(upload_id));
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };

        let part_number = match part_number.parse::<i32>() {
            Ok(value) if value >= 1 && value <= MAX_PART_NUMBER => value,
            Ok(value) if value > MAX_PART_NUMBER => {
                return api_error_response(
                    400,
                    "InvalidMaxParts",
                    "Part number must be an integer between 1 and 10000, inclusive.",
                    &resource,
                    bucket,
                    object,
                );
            }
            _ => {
                return api_error_response(
                    400,
                    "InvalidPart",
                    "One or more of the specified parts could not be found.",
                    &resource,
                    bucket,
                    object,
                );
            }
        };

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
        if reader.declared_size > MAX_REQUEST_SIZE {
            return api_error_response(
                400,
                "EntityTooLarge",
                "Your proposed upload exceeds the maximum allowed object size.",
                &resource,
                bucket,
                object,
            );
        }

        match layer.put_object_part(
            bucket,
            object,
            upload_id,
            part_number,
            reader,
            ObjectOptions::default(),
        ) {
            Ok(result) => {
                let mut response = HandlerResponse::status_only(200);
                response
                    .headers
                    .insert("etag".to_string(), format!("\"{}\"", result.etag));
                response
            }
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

    pub fn put_object_part_streaming(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number: &str,
        reader: &PutObjReader,
        auth: &RequestAuth,
        has_date_header: bool,
        decoded_content_length: &str,
    ) -> HandlerResponse {
        let resource = multipart_resource(bucket, object, Some(upload_id));
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
        self.put_object_part(bucket, object, upload_id, part_number, reader, auth)
    }

    pub fn new_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = multipart_resource(bucket, object, None);
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        let mut user_defined = BTreeMap::new();
        apply_bucket_default_encryption(layer, bucket, &BTreeMap::new(), &mut user_defined);
        match layer.new_multipart_upload(
            bucket,
            object,
            ObjectOptions {
                user_defined,
                ..ObjectOptions::default()
            },
        ) {
            Ok(result) => HandlerResponse::xml(
                200,
                format!(
                    "<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
                    xml_escape(bucket),
                    xml_escape(object),
                    xml_escape(&result.upload_id),
                ),
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

    pub fn abort_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = multipart_resource(bucket, object, Some(upload_id));
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.abort_multipart_upload(bucket, object, upload_id, ObjectOptions::default()) {
            Ok(()) => HandlerResponse::status_only(204),
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

    pub fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        parts: &[CompletePart],
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = multipart_resource(bucket, object, Some(upload_id));
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };

        if parts.is_empty() {
            return api_error_response(
                400,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
                &resource,
                bucket,
                object,
            );
        }
        if parts
            .windows(2)
            .any(|pair| pair[0].part_number >= pair[1].part_number)
        {
            return api_error_response(
                400,
                "InvalidPartOrder",
                "The list of parts was not in ascending order. Parts must be ordered by part number.",
                &resource,
                bucket,
                object,
            );
        }

        match layer.complete_multipart_upload(
            bucket,
            object,
            upload_id,
            parts,
            ObjectOptions::default(),
        ) {
            Ok(result) => {
                if let Ok(data) = layer.get_object(bucket, object) {
                    let _ = replicate_object_for_layer(
                        layer,
                        &self.replication_targets,
                        self.replication_service(),
                        bucket,
                        object,
                        &result,
                        &data,
                    );
                }
                let mut response = HandlerResponse::xml(
                    200,
                    format!(
                    "<CompleteMultipartUploadResult><Location>{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>\"{}\"</ETag></CompleteMultipartUploadResult>",
                    xml_escape(&format!("/{bucket}/{object}")),
                    xml_escape(bucket),
                    xml_escape(object),
                    xml_escape(&result.etag),
                    ),
                );
                append_object_metadata_headers(&mut response, &result.user_defined);
                response
            }
            Err(err) if err.contains(ERR_INVALID_UPLOAD_ID) => api_error_response(
                404,
                "NoSuchUpload",
                "The specified multipart upload does not exist.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_INVALID_PART => api_error_response(
                400,
                "InvalidPart",
                "One or more of the specified parts could not be found.",
                &resource,
                bucket,
                object,
            ),
            Err(err) if err == ERR_PART_TOO_SMALL => api_error_response(
                400,
                "EntityTooSmall",
                "Your proposed upload is smaller than the minimum allowed object size.",
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

    pub fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: &str,
        max_parts: &str,
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = multipart_resource(bucket, object, Some(upload_id));
        if let Err(response) = self.authorize(auth, &resource, bucket, object) {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };

        let part_number_marker = if part_number_marker.is_empty() {
            0
        } else {
            match part_number_marker.parse::<i32>() {
                Ok(value) if value >= 0 => value,
                _ => {
                    return api_error_response(
                        400,
                        "InvalidPartNumberMarker",
                        "Part number marker must be a non-negative integer.",
                        &resource,
                        bucket,
                        object,
                    );
                }
            }
        };
        let max_parts = if max_parts.is_empty() {
            1000
        } else {
            match max_parts.parse::<i32>() {
                Ok(value) if value >= 0 => value,
                _ => {
                    return api_error_response(
                        400,
                        "InvalidMaxParts",
                        "Argument max-parts must be an integer between 0 and 2147483647.",
                        &resource,
                        bucket,
                        object,
                    );
                }
            }
        };

        match layer.list_object_parts(
            bucket,
            object,
            upload_id,
            part_number_marker,
            max_parts,
            ObjectOptions::default(),
        ) {
            Ok(result) => {
                let parts_xml = result
                    .parts
                    .iter()
                    .map(|part| {
                        format!(
                            "<Part><PartNumber>{}</PartNumber><ETag>\"{}\"</ETag><Size>{}</Size></Part>",
                            part.part_number,
                            xml_escape(&part.etag),
                            part.size
                        )
                    })
                    .collect::<String>();
                HandlerResponse::xml(
                    200,
                    format!(
                        "<ListPartsResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId><PartNumberMarker>{}</PartNumberMarker><NextPartNumberMarker>{}</NextPartNumberMarker><MaxParts>{}</MaxParts><IsTruncated>{}</IsTruncated>{}</ListPartsResult>",
                        xml_escape(&result.bucket),
                        xml_escape(&result.object),
                        xml_escape(&result.upload_id),
                        result.part_number_marker,
                        result.next_part_number_marker,
                        result.max_parts,
                        result.is_truncated,
                        parts_xml,
                    ),
                )
            }
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
