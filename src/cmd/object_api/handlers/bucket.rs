use super::*;

fn bucket_resource(bucket: &str, suffix: &str) -> String {
    if suffix.is_empty() {
        format!("/{bucket}/")
    } else {
        format!("/{bucket}/?{suffix}")
    }
}

fn list_buckets_resource() -> &'static str {
    "/"
}

fn render_list_buckets(buckets: &[BucketInfo]) -> String {
    let mut xml = String::from("<ListAllMyBucketsResult><Buckets>");
    for bucket in buckets {
        xml.push_str(&format!(
            "<Bucket><Name>{}</Name></Bucket>",
            xml_escape(&bucket.name)
        ));
    }
    xml.push_str("</Buckets></ListAllMyBucketsResult>");
    xml
}

fn render_list_multipart_uploads(bucket: &str, result: &ListMultipartsInfo) -> String {
    let mut xml = String::from("<ListMultipartUploadsResult>");
    xml.push_str(&format!(
        "<Bucket>{}</Bucket><KeyMarker>{}</KeyMarker><UploadIdMarker>{}</UploadIdMarker><NextKeyMarker>{}</NextKeyMarker><NextUploadIdMarker>{}</NextUploadIdMarker><Delimiter>{}</Delimiter><Prefix>{}</Prefix><MaxUploads>{}</MaxUploads><IsTruncated>{}</IsTruncated>",
        xml_escape(bucket),
        xml_escape(&result.key_marker),
        xml_escape(&result.upload_id_marker),
        xml_escape(&result.next_key_marker),
        xml_escape(&result.next_upload_id_marker),
        xml_escape(&result.delimiter),
        xml_escape(&result.prefix),
        result.max_uploads.max(0),
        result.is_truncated,
    ));
    for upload in &result.uploads {
        xml.push_str(&format!(
            "<Upload><Key>{}</Key><UploadId>{}</UploadId></Upload>",
            xml_escape(&upload.object),
            xml_escape(&upload.upload_id)
        ));
    }
    xml.push_str("</ListMultipartUploadsResult>");
    xml
}

fn format_last_modified(unix_seconds: i64) -> String {
    DateTime::<Utc>::from_timestamp(unix_seconds, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch"))
        .format("%Y-%m-%dT%H:%M:%S.000Z")
        .to_string()
}

fn render_list_objects_common(xml: &mut String, prefixes: &[String], objects: &[ObjectInfo]) {
    for prefix in prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(prefix)
        ));
    }
    for object in objects {
        xml.push_str("<Contents>");
        xml.push_str(&format!("<Key>{}</Key>", xml_escape(&object.name)));
        xml.push_str(&format!(
            "<LastModified>{}</LastModified>",
            xml_escape(&format_last_modified(object.mod_time))
        ));
        xml.push_str(&format!(
            "<ETag>&quot;{}&quot;</ETag>",
            xml_escape(&object.etag)
        ));
        xml.push_str(&format!("<Size>{}</Size>", object.size.max(0)));
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("</Contents>");
    }
}

fn render_list_objects_v1(
    bucket: &str,
    prefix: &str,
    marker: &str,
    delimiter: &str,
    max_keys: i32,
    info: &ListObjectsInfo,
) -> String {
    let mut xml = String::from("<ListBucketResult>");
    xml.push_str(&format!("<Name>{}</Name>", xml_escape(bucket)));
    xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(prefix)));
    xml.push_str(&format!("<Marker>{}</Marker>", xml_escape(marker)));
    xml.push_str(&format!("<MaxKeys>{}</MaxKeys>", max_keys.max(0)));
    xml.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(delimiter)));
    xml.push_str(&format!("<IsTruncated>{}</IsTruncated>", info.is_truncated));
    if info.is_truncated {
        xml.push_str(&format!(
            "<NextMarker>{}</NextMarker>",
            xml_escape(&info.next_marker)
        ));
    }
    render_list_objects_common(&mut xml, &info.prefixes, &info.objects);
    xml.push_str("</ListBucketResult>");
    xml
}

fn render_list_objects_v2(
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    max_keys: i32,
    continuation_token: &str,
    start_after: &str,
    fetch_owner: bool,
    info: &ListObjectsInfo,
) -> String {
    let mut xml = String::from("<ListBucketResult>");
    xml.push_str(&format!("<Name>{}</Name>", xml_escape(bucket)));
    xml.push_str("<KeyCount>");
    xml.push_str(&(info.objects.len() + info.prefixes.len()).to_string());
    xml.push_str("</KeyCount>");
    xml.push_str(&format!("<MaxKeys>{}</MaxKeys>", max_keys.max(0)));
    xml.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(delimiter)));
    xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(prefix)));
    if !continuation_token.is_empty() {
        xml.push_str(&format!(
            "<ContinuationToken>{}</ContinuationToken>",
            xml_escape(continuation_token)
        ));
    }
    if !start_after.is_empty() {
        xml.push_str(&format!(
            "<StartAfter>{}</StartAfter>",
            xml_escape(start_after)
        ));
    }
    xml.push_str(&format!("<FetchOwner>{}</FetchOwner>", fetch_owner));
    xml.push_str(&format!("<IsTruncated>{}</IsTruncated>", info.is_truncated));
    if !info.next_continuation_token.is_empty() {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(&info.next_continuation_token)
        ));
    }
    render_list_objects_common(&mut xml, &info.prefixes, &info.objects);
    xml.push_str("</ListBucketResult>");
    xml
}

fn render_delete_result(
    deleted: &[String],
    errors: &[(String, String, String)],
    quiet: bool,
) -> String {
    let mut xml = String::from("<DeleteResult>");
    if !quiet {
        for name in deleted {
            xml.push_str(&format!(
                "<Deleted><Key>{}</Key></Deleted>",
                xml_escape(name)
            ));
        }
    }
    for (code, message, key) in errors {
        xml.push_str(&format!(
            "<Error><Code>{}</Code><Message>{}</Message><Key>{}</Key></Error>",
            xml_escape(code),
            xml_escape(message),
            xml_escape(key)
        ));
    }
    xml.push_str("</DeleteResult>");
    xml
}

impl ObjectApiHandlers {
    pub fn make_bucket(&self, bucket: &str, auth: &RequestAuth) -> HandlerResponse {
        let resource = bucket_resource(bucket, "");
        if let Err(response) = self.authorize(auth, &resource, bucket, "") {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.make_bucket(bucket, MakeBucketOptions::default()) {
            Ok(()) => HandlerResponse::status_only(200),
            Err(err) if err == ERR_BUCKET_NAME_INVALID => api_error_response(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_VOLUME_EXISTS => api_error_response(
                409,
                "BucketAlreadyOwnedByYou",
                "Your previous request to create the named bucket succeeded and you already own it.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_ERASURE_WRITE_QUORUM => api_error_response(
                503,
                "SlowDown",
                "Please reduce your request rate.",
                &resource,
                bucket,
                "",
            ),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                "",
            ),
        }
    }

    pub fn remove_bucket(&self, bucket: &str, auth: &RequestAuth) -> HandlerResponse {
        let resource = bucket_resource(bucket, "");
        if let Err(response) = self.authorize(auth, &resource, bucket, "") {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.remove_bucket(bucket) {
            Ok(()) => HandlerResponse::status_only(204),
            Err(err) if err == ERR_BUCKET_NAME_INVALID => api_error_response(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_BUCKET_NOT_FOUND => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_VOLUME_NOT_EMPTY => api_error_response(
                409,
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty.",
                &resource,
                bucket,
                "",
            ),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                "",
            ),
        }
    }

    pub fn get_bucket_location(&self, bucket: &str, auth: &RequestAuth) -> HandlerResponse {
        let resource = bucket_resource(bucket, "location");
        if let Err(response) = self.authorize(auth, &resource, bucket, "") {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.bucket_exists(bucket) {
            Ok(true) => HandlerResponse::xml(
                200,
                r#"<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>"#.to_string(),
            ),
            Ok(false) => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_BUCKET_NOT_FOUND => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_BUCKET_NAME_INVALID => api_error_response(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid.",
                &resource,
                bucket,
                "",
            ),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                "",
            ),
        }
    }

    pub fn head_bucket(&self, bucket: &str, auth: &RequestAuth) -> HandlerResponse {
        let resource = bucket_resource(bucket, "");
        if let Err(response) = self.authorize(auth, &resource, bucket, "") {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.bucket_exists(bucket) {
            Ok(true) => HandlerResponse::status_only(200),
            Ok(false) => HandlerResponse::status_only(404),
            Err(err) if err == ERR_BUCKET_NOT_FOUND => HandlerResponse::status_only(404),
            Err(err) if err == ERR_BUCKET_NAME_INVALID => HandlerResponse::status_only(400),
            Err(_) => HandlerResponse::status_only(500),
        }
    }

    pub fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: &str,
        delimiter: &str,
        max_uploads: &str,
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = bucket_resource(bucket, "uploads");
        if let Err(response) = self.authorize(auth, &resource, bucket, "") {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };

        let max_uploads = match max_uploads.parse::<i32>() {
            Ok(value) => value,
            Err(_) => {
                return api_error_response(
                    400,
                    "InvalidArgument",
                    "Argument max-uploads must be an integer between 0 and 2147483647.",
                    &resource,
                    bucket,
                    "",
                )
            }
        };
        if max_uploads < 0 {
            return api_error_response(
                400,
                "InvalidArgument",
                "Argument max-uploads must be an integer between 0 and 2147483647.",
                &resource,
                bucket,
                "",
            );
        }
        if !prefix.is_empty() && !key_marker.is_empty() && !key_marker.starts_with(prefix) {
            return api_error_response(
                501,
                "NotImplemented",
                "A header you provided implies functionality that is not implemented.",
                &resource,
                bucket,
                "",
            );
        }

        match layer.list_multipart_uploads(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            delimiter,
            max_uploads,
        ) {
            Ok(result) => HandlerResponse::xml(200, render_list_multipart_uploads(bucket, &result)),
            Err(err) if err == ERR_BUCKET_NAME_INVALID => api_error_response(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_BUCKET_NOT_FOUND => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                "",
            ),
            Err(err)
                if err.contains("invalid combination") || err.contains("malformed upload id") =>
            {
                api_error_response(
                    501,
                    "NotImplemented",
                    "A header you provided implies functionality that is not implemented.",
                    &resource,
                    bucket,
                    "",
                )
            }
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                "",
            ),
        }
    }

    pub fn list_buckets(&self, auth: &RequestAuth) -> HandlerResponse {
        let resource = list_buckets_resource();
        if let Err(response) = self.authorize(auth, resource, "", "") {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.list_buckets(BucketOptions::default()) {
            Ok(buckets) => HandlerResponse::xml(200, render_list_buckets(&buckets)),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                resource,
                "",
                "",
            ),
        }
    }

    pub fn list_objects_v1(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        delimiter: &str,
        max_keys: i32,
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = bucket_resource(bucket, "");
        if let Err(response) = self.authorize(auth, &resource, bucket, "") {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.list_objects(bucket, prefix, marker, delimiter, max_keys) {
            Ok(info) => HandlerResponse::xml(
                200,
                render_list_objects_v1(bucket, prefix, marker, delimiter, max_keys, &info),
            ),
            Err(err) if err == ERR_BUCKET_NAME_INVALID => api_error_response(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_BUCKET_NOT_FOUND => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                "",
            ),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                "",
            ),
        }
    }

    pub fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        delimiter: &str,
        max_keys: i32,
        start_after: &str,
        fetch_owner: bool,
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = bucket_resource(bucket, "");
        if let Err(response) = self.authorize(auth, &resource, bucket, "") {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        match layer.list_objects_v2(
            bucket,
            prefix,
            continuation_token,
            delimiter,
            max_keys,
            fetch_owner,
            start_after,
        ) {
            Ok(info) => HandlerResponse::xml(
                200,
                render_list_objects_v2(
                    bucket,
                    prefix,
                    delimiter,
                    max_keys,
                    continuation_token,
                    start_after,
                    fetch_owner,
                    &info,
                ),
            ),
            Err(err) if err == ERR_BUCKET_NAME_INVALID => api_error_response(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_BUCKET_NOT_FOUND => api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                "",
            ),
            Err(err) if err == ERR_INVALID_ARGUMENT => api_error_response(
                400,
                "InvalidArgument",
                "The continuation token provided is incorrect.",
                &resource,
                bucket,
                "",
            ),
            Err(_) => api_error_response(
                500,
                "InternalError",
                "We encountered an internal error, please try again.",
                &resource,
                bucket,
                "",
            ),
        }
    }

    pub fn delete_multiple_objects(
        &self,
        bucket: &str,
        objects: &[String],
        quiet: bool,
        auth: &RequestAuth,
    ) -> HandlerResponse {
        let resource = bucket_resource(bucket, "delete");
        if let Err(response) = self.authorize(auth, &resource, bucket, "") {
            return response;
        }
        let Ok(layer) = self.require_layer() else {
            return self.require_layer().unwrap_err();
        };
        if !is_valid_bucket_name(bucket) {
            return api_error_response(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid.",
                &resource,
                bucket,
                "",
            );
        }
        if !layer.bucket_exists(bucket).unwrap_or(false) {
            return api_error_response(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                &resource,
                bucket,
                "",
            );
        }

        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for object in objects {
            match layer.delete_object(bucket, object, ObjectOptions::default()) {
                Ok(_) => deleted.push(object.clone()),
                Err(err) if err == ERR_FILE_NOT_FOUND => deleted.push(object.clone()),
                Err(err) if err == ERR_OBJECT_NAME_INVALID => errors.push((
                    "InvalidObjectName".to_string(),
                    "The specified object name is not valid.".to_string(),
                    object.clone(),
                )),
                Err(_) => errors.push((
                    "AccessDenied".to_string(),
                    "Access Denied.".to_string(),
                    object.clone(),
                )),
            }
        }

        HandlerResponse::xml(200, render_delete_result(&deleted, &errors, quiet))
    }
}
