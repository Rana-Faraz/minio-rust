use super::auth::{extract_request_auth, to_test_request};
use super::metrics::{is_public_metrics_request, route_health_request, route_metrics_request};
use super::state::{maybe_record_notification, now_ms, ServerAdminState};
use super::xml::{parse_complete_multipart_upload, parse_delete_objects, parse_multipart_form};
use super::*;

pub(super) fn route_request(
    method: &Method,
    raw_url: &str,
    request_headers: &[Header],
    body: &[u8],
    handlers: &ObjectApiHandlers,
    credentials: &HandlerCredentials,
    admin_state: &ServerAdminState,
) -> Result<HandlerResponse, String> {
    let headers = normalized_headers(request_headers);
    let host = headers
        .get("host")
        .cloned()
        .unwrap_or_else(|| DEFAULT_ADDRESS.to_string());
    let url = Url::parse(&format!("http://{host}{raw_url}")).map_err(|err| err.to_string())?;
    let query = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<BTreeMap<_, _>>();

    if let Some(response) = route_health_request(&url, handlers) {
        return Ok(response);
    }
    if is_public_metrics_request(&url) {
        if let Some(response) = route_metrics_request(&url, admin_state) {
            return Ok(response);
        }
    }

    let auth = extract_request_auth(method, &url, &headers, body, credentials)?;
    let body = auth.body;
    let auth = auth.auth;

    if url.path().starts_with("/minio/admin/v3/") {
        let request = to_test_request(method, &url, &headers, &body)?;
        return Ok(route_admin_request(&request, admin_state));
    }
    if let Some(response) = route_metrics_request(&url, admin_state) {
        return Ok(response);
    }
    if url.path().starts_with("/minio/kms/v1/") {
        let request = to_test_request(method, &url, &headers, &body)?;
        return Ok(route_kms_request(&request, admin_state));
    }
    if url.path().starts_with("/minio/sts/v1/") {
        let request = to_test_request(method, &url, &headers, &body)?;
        return Ok(route_sts_request(&request, &auth, admin_state));
    }

    let (bucket, object) = split_bucket_object(url.path())?;
    let mut notification_object = object.clone();

    let response = match (method.clone(), bucket.as_deref(), object.as_deref()) {
        (Method::Get, None, None) => handlers.list_buckets(&auth),
        (Method::Put, Some(bucket), None) if query.contains_key("policy") => {
            put_bucket_policy_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
                &String::from_utf8_lossy(&body),
            )
        }
        (Method::Get, Some(bucket), None) if query.contains_key("policy") => {
            get_bucket_policy_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Delete, Some(bucket), None) if query.contains_key("policy") => {
            delete_bucket_policy_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Put, Some(bucket), None) if query.contains_key("lifecycle") => {
            put_bucket_lifecycle_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
                &String::from_utf8_lossy(&body),
            )
        }
        (Method::Get, Some(bucket), None) if query.contains_key("lifecycle") => {
            get_bucket_lifecycle_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Delete, Some(bucket), None) if query.contains_key("lifecycle") => {
            delete_bucket_lifecycle_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Put, Some(bucket), None) if query.contains_key("versioning") => {
            put_bucket_versioning_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
                &String::from_utf8_lossy(&body),
            )
        }
        (Method::Get, Some(bucket), None) if query.contains_key("versioning") => {
            get_bucket_versioning_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Put, Some(bucket), None) if query.contains_key("encryption") => {
            put_bucket_encryption_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
                &String::from_utf8_lossy(&body),
            )
        }
        (Method::Get, Some(bucket), None) if query.contains_key("encryption") => {
            get_bucket_encryption_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Delete, Some(bucket), None) if query.contains_key("encryption") => {
            delete_bucket_encryption_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Put, Some(bucket), None) if query.contains_key("replication") => {
            put_bucket_replication_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
                &String::from_utf8_lossy(&body),
            )
        }
        (Method::Get, Some(bucket), None) if query.contains_key("replication") => {
            get_bucket_replication_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Delete, Some(bucket), None) if query.contains_key("replication") => {
            delete_bucket_replication_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Put, Some(bucket), None) if query.contains_key("notification") => {
            put_bucket_notification_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
                &String::from_utf8_lossy(&body),
            )
        }
        (Method::Get, Some(bucket), None) if query.contains_key("notification") => {
            get_bucket_notification_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Delete, Some(bucket), None) if query.contains_key("notification") => {
            delete_bucket_notification_for_layer(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                credentials,
                bucket,
                &auth,
            )
        }
        (Method::Post, Some(bucket), None)
            if headers
                .get("content-type")
                .is_some_and(|value| value.starts_with("multipart/form-data")) =>
        {
            let (fields, file_bytes) = parse_multipart_form(
                &body,
                headers
                    .get("content-type")
                    .map(String::as_str)
                    .unwrap_or_default(),
            )?;
            notification_object = fields.get("key").cloned();
            let policy = fields.get("policy").cloned().unwrap_or_default();
            post_policy_bucket(
                handlers
                    .layer()
                    .ok_or_else(|| "server not initialized".to_string())?,
                bucket,
                &policy,
                &fields,
                &file_bytes,
            )
        }
        (Method::Put, Some(bucket), None) => handlers.make_bucket(bucket, &auth),
        (Method::Head, Some(bucket), None) => handlers.head_bucket(bucket, &auth),
        (Method::Delete, Some(bucket), None) => handlers.remove_bucket(bucket, &auth),
        (Method::Get, Some(bucket), None) if query.contains_key("location") => {
            handlers.get_bucket_location(bucket, &auth)
        }
        (Method::Get, Some(bucket), None) if query.contains_key("uploads") => handlers
            .list_multipart_uploads(
                bucket,
                query.get("prefix").map(String::as_str).unwrap_or_default(),
                query
                    .get("key-marker")
                    .map(String::as_str)
                    .unwrap_or_default(),
                query
                    .get("upload-id-marker")
                    .map(String::as_str)
                    .unwrap_or_default(),
                query
                    .get("delimiter")
                    .map(String::as_str)
                    .unwrap_or_default(),
                query
                    .get("max-uploads")
                    .map(String::as_str)
                    .unwrap_or("1000"),
                &auth,
            ),
        (Method::Get, Some(bucket), None)
            if query.get("list-type").map(String::as_str) == Some("2") =>
        {
            handlers.list_objects_v2(
                bucket,
                query.get("prefix").map(String::as_str).unwrap_or_default(),
                query
                    .get("continuation-token")
                    .map(String::as_str)
                    .unwrap_or_default(),
                query
                    .get("delimiter")
                    .map(String::as_str)
                    .unwrap_or_default(),
                parse_i32(query.get("max-keys"), 1000),
                query
                    .get("start-after")
                    .map(String::as_str)
                    .unwrap_or_default(),
                query
                    .get("fetch-owner")
                    .is_some_and(|value| value == "true"),
                &auth,
            )
        }
        (Method::Get, Some(bucket), None) => handlers.list_objects_v1(
            bucket,
            query.get("prefix").map(String::as_str).unwrap_or_default(),
            query.get("marker").map(String::as_str).unwrap_or_default(),
            query
                .get("delimiter")
                .map(String::as_str)
                .unwrap_or_default(),
            parse_i32(query.get("max-keys"), 1000),
            &auth,
        ),
        (Method::Post, Some(bucket), None) if query.contains_key("delete") => {
            let (objects, quiet) = parse_delete_objects(&body).map_err(|err| err.to_string())?;
            handlers.delete_multiple_objects(bucket, &objects, quiet, &auth)
        }
        (Method::Post, Some(bucket), Some(object)) if query.contains_key("uploads") => {
            handlers.new_multipart_upload(bucket, object, &auth)
        }
        (Method::Delete, Some(bucket), Some(object)) if query.contains_key("uploadId") => handlers
            .abort_multipart_upload(
                bucket,
                object,
                query
                    .get("uploadId")
                    .map(String::as_str)
                    .unwrap_or_default(),
                &auth,
            ),
        (Method::Get, Some(bucket), Some(object)) if query.contains_key("uploadId") => handlers
            .list_object_parts(
                bucket,
                object,
                query
                    .get("uploadId")
                    .map(String::as_str)
                    .unwrap_or_default(),
                query
                    .get("part-number-marker")
                    .map(String::as_str)
                    .unwrap_or_default(),
                query.get("max-parts").map(String::as_str).unwrap_or("1000"),
                &auth,
            ),
        (Method::Post, Some(bucket), Some(object)) if query.contains_key("uploadId") => {
            let parts = parse_complete_multipart_upload(&body).map_err(|err| err.to_string())?;
            handlers.complete_multipart_upload(
                bucket,
                object,
                query
                    .get("uploadId")
                    .map(String::as_str)
                    .unwrap_or_default(),
                &parts,
                &auth,
            )
        }
        (Method::Put, Some(bucket), Some(object))
            if query.contains_key("uploadId") && query.contains_key("partNumber") =>
        {
            let upload_id = query
                .get("uploadId")
                .map(String::as_str)
                .unwrap_or_default();
            let part_number = query
                .get("partNumber")
                .map(String::as_str)
                .unwrap_or_default();
            if let Some(copy_source) = headers.get("x-amz-copy-source") {
                handlers.copy_object_part(
                    bucket,
                    object,
                    upload_id,
                    part_number,
                    copy_source,
                    headers.get("x-amz-copy-source-range").map(String::as_str),
                    &auth,
                )
            } else {
                let reader = request_body_reader(&body, &headers);
                if headers.get("x-amz-content-sha256").map(String::as_str)
                    == Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                {
                    handlers.put_object_part_streaming(
                        bucket,
                        object,
                        upload_id,
                        part_number,
                        &reader,
                        &auth,
                        headers.contains_key("x-amz-date") || headers.contains_key("date"),
                        headers
                            .get("x-amz-decoded-content-length")
                            .map(String::as_str)
                            .unwrap_or_default(),
                    )
                } else {
                    handlers.put_object_part(bucket, object, upload_id, part_number, &reader, &auth)
                }
            }
        }
        (Method::Put, Some(bucket), Some(object)) if headers.contains_key("x-amz-copy-source") => {
            handlers.copy_object(
                bucket,
                object,
                headers
                    .get("x-amz-copy-source")
                    .map(String::as_str)
                    .unwrap_or_default(),
                &auth,
                &headers,
            )
        }
        (Method::Put, Some(bucket), Some(object))
            if query.contains_key("versionId")
                && headers
                    .get("x-minio-internal-replica")
                    .is_some_and(|value| value == "true") =>
        {
            route_replica_put_object_with_version(
                handlers,
                bucket,
                object,
                query
                    .get("versionId")
                    .map(String::as_str)
                    .unwrap_or_default(),
                &body,
                &headers,
            )
        }
        (Method::Put, Some(bucket), Some(object)) => {
            let reader = request_body_reader(&body, &headers);
            if headers.get("x-amz-content-sha256").map(String::as_str)
                == Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
            {
                handlers.put_object_streaming(
                    bucket,
                    object,
                    &reader,
                    &auth,
                    &headers,
                    headers.contains_key("x-amz-date") || headers.contains_key("date"),
                    headers
                        .get("x-amz-decoded-content-length")
                        .map(String::as_str)
                        .unwrap_or_default(),
                )
            } else {
                handlers.put_object(bucket, object, &reader, &auth, &headers)
            }
        }
        (Method::Head, Some(bucket), Some(object)) => {
            handlers.head_object(bucket, object, &auth, &headers)
        }
        (Method::Get, Some(bucket), Some(object)) if query.contains_key("partNumber") => handlers
            .get_object_part_number(
                bucket,
                object,
                parse_i32(query.get("partNumber"), 0),
                &auth,
                &headers,
            ),
        (Method::Get, Some(bucket), Some(object)) => handlers.get_object_with_headers(
            bucket,
            object,
            &auth,
            headers.get("range").map(String::as_str),
            &headers,
        ),
        (Method::Delete, Some(bucket), Some(object)) => route_delete_object(
            handlers,
            admin_state,
            bucket,
            object,
            &auth,
            &headers,
            query.get("versionId").map(String::as_str),
        ),
        _ => method_not_allowed(),
    };

    let mut response = response;
    if response.status < 300 {
        append_current_version_header(
            &mut response,
            handlers.layer(),
            method,
            bucket.as_deref(),
            object.as_deref(),
        );
    }

    maybe_record_notification(
        admin_state,
        handlers.layer(),
        method,
        &query,
        bucket.as_deref(),
        notification_object.as_deref(),
        &headers,
        &response,
    );

    Ok(response)
}

fn route_replica_put_object_with_version(
    handlers: &ObjectApiHandlers,
    bucket: &str,
    object: &str,
    version_id: &str,
    body: &[u8],
    headers: &BTreeMap<String, String>,
) -> HandlerResponse {
    let Some(layer) = handlers.layer() else {
        return HandlerResponse {
            status: 500,
            ..HandlerResponse::default()
        };
    };
    let mut user_defined = BTreeMap::new();
    for (key, value) in headers {
        let should_copy = matches!(
            key.as_str(),
            "content-type"
                | "content-encoding"
                | "x-amz-server-side-encryption"
                | "x-amz-server-side-encryption-aws-kms-key-id"
                | "x-amz-bucket-replication-status"
                | "x-minio-internal-replica"
                | "x-amz-checksum-crc32"
                | "x-amz-checksum-crc32c"
                | "x-amz-checksum-sha1"
                | "x-amz-checksum-sha256"
        ) || key.starts_with("x-amz-meta-");
        if should_copy {
            user_defined.insert(key.to_string(), value.clone());
        }
    }
    match layer.put_object(
        bucket,
        object,
        &request_body_reader(body, headers),
        ObjectOptions {
            user_defined,
            versioned: true,
            version_id: version_id.to_string(),
            ..ObjectOptions::default()
        },
    ) {
        Ok(info) => {
            let mut response = HandlerResponse {
                status: 200,
                ..HandlerResponse::default()
            };
            if !info.version_id.is_empty() {
                response
                    .headers
                    .insert("x-amz-version-id".to_string(), info.version_id);
            }
            response
        }
        Err(_) => HandlerResponse {
            status: 500,
            ..HandlerResponse::default()
        },
    }
}

fn route_delete_object(
    handlers: &ObjectApiHandlers,
    admin_state: &ServerAdminState,
    bucket: &str,
    object: &str,
    auth: &RequestAuth,
    headers: &BTreeMap<String, String>,
    version_id: Option<&str>,
) -> HandlerResponse {
    let resource = format!("/{bucket}/{object}");
    if let Err(response) = handlers.authorize(auth, &resource, bucket, object) {
        return response;
    }
    let Some(layer) = handlers.layer() else {
        return HandlerResponse {
            status: 500,
            ..HandlerResponse::default()
        };
    };

    let mut opts = ObjectOptions::default();
    if let Some(version_id) = version_id.filter(|value| !value.is_empty()) {
        opts.versioned = true;
        opts.version_id = version_id.to_string();
    }
    if let Some(marker_version) = headers
        .get("x-minio-internal-delete-marker-version-id")
        .filter(|value| !value.is_empty())
    {
        opts.versioned = true;
        opts.user_defined.insert(
            "x-minio-internal-delete-marker-version-id".to_string(),
            marker_version.clone(),
        );
    }

    match layer.delete_object(bucket, object, opts) {
        Ok(info) => {
            if headers
                .get("x-minio-internal-replica")
                .is_none_or(|value| value != "true")
            {
                let _ = replicate_delete_info_for_layer(
                    layer,
                    &admin_state.replication_targets,
                    Some(&admin_state.replication_service),
                    bucket,
                    object,
                    &info,
                );
            }
            let mut response = HandlerResponse {
                status: 204,
                ..HandlerResponse::default()
            };
            if !info.version_id.is_empty() {
                response
                    .headers
                    .insert("x-amz-version-id".to_string(), info.version_id.clone());
            }
            if info.delete_marker {
                response
                    .headers
                    .insert("x-amz-delete-marker".to_string(), "true".to_string());
            }
            response
        }
        Err(_) => HandlerResponse {
            status: 204,
            ..HandlerResponse::default()
        },
    }
}

fn append_current_version_header(
    response: &mut HandlerResponse,
    layer: Option<&LocalObjectLayer>,
    method: &Method,
    bucket: Option<&str>,
    object: Option<&str>,
) {
    if !matches!(method, Method::Get | Method::Head) {
        return;
    }
    if response.headers.contains_key("x-amz-version-id") {
        return;
    }
    let (Some(layer), Some(bucket), Some(object)) = (layer, bucket, object) else {
        return;
    };
    let Ok(info) = layer.get_object_info(bucket, object) else {
        return;
    };
    if !info.content_type.is_empty() && !response.headers.contains_key("content-type") {
        response
            .headers
            .insert("content-type".to_string(), info.content_type.clone());
    }
    if !info.version_id.is_empty() {
        response
            .headers
            .insert("x-amz-version-id".to_string(), info.version_id.clone());
    }
    for (key, value) in &info.user_defined {
        let should_expose = key == "content-encoding"
            || key == "x-amz-server-side-encryption"
            || key == "x-amz-server-side-encryption-aws-kms-key-id"
            || key == "x-amz-bucket-replication-status"
            || key.starts_with("x-amz-meta-")
            || key.starts_with("x-amz-checksum-");
        if should_expose && !response.headers.contains_key(key) {
            response.headers.insert(key.clone(), value.clone());
        }
    }
}

fn route_admin_request(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    match req.url.path() {
        "/minio/admin/v3/service" => match admin_state.handlers.restart_handler(req) {
            Ok(result) => json_response(200, &serde_json::json!({ "action": result.action })),
            Err(code) => admin_api_error_response(code),
        },
        "/minio/admin/v3/info" => route_admin_server_info(req, admin_state),
        "/minio/admin/v3/storageinfo" => route_admin_storage_info(req, admin_state),
        "/minio/admin/v3/kms/status" => route_admin_kms_status(req, admin_state),
        "/minio/admin/v3/kms/key/status" => route_admin_kms_key_status(req, admin_state),
        "/minio/admin/v3/kms/key/create" => route_admin_kms_key_create(req, admin_state),
        "/minio/admin/v3/policy/add" => route_admin_add_canned_policy(req, admin_state),
        "/minio/admin/v3/policy/list" => route_admin_list_canned_policies(req, admin_state),
        "/minio/admin/v3/policy/attach" => route_admin_attach_policy(req, admin_state),
        "/minio/admin/v3/policy/detach" => route_admin_detach_policy(req, admin_state),
        "/minio/admin/v3/policy/remove" => route_admin_remove_canned_policy(req, admin_state),
        "/minio/admin/v3/service-account/add" => route_admin_add_service_account(req, admin_state),
        "/minio/admin/v3/service-account/list" => {
            route_admin_list_service_accounts(req, admin_state)
        }
        "/minio/admin/v3/service-account/update" => {
            route_admin_update_service_account(req, admin_state)
        }
        "/minio/admin/v3/service-account/remove" => {
            route_admin_remove_service_account(req, admin_state)
        }
        "/minio/admin/v3/sts/revoke" => route_admin_revoke_sts_session(req, admin_state),
        "/minio/admin/v3/add-user" => route_admin_add_user(req, admin_state),
        "/minio/admin/v3/list-users" => route_admin_list_users(req, admin_state),
        "/minio/admin/v3/iam/export" => route_admin_export_iam(req, admin_state),
        "/minio/admin/v3/iam/import" => route_admin_import_iam(req, admin_state),
        "/minio/admin/v3/idp/openid/add" => route_admin_add_openid_provider(req, admin_state),
        "/minio/admin/v3/idp/ldap/config" => route_admin_set_ldap_config(req, admin_state),
        "/minio/admin/v3/notifications" => route_admin_list_notifications(req, admin_state),
        "/minio/admin/v3/queue-deliveries" => route_admin_list_queue_deliveries(req, admin_state),
        "/minio/admin/v3/elasticsearch-deliveries" => {
            route_admin_list_elasticsearch_deliveries(req, admin_state)
        }
        "/minio/admin/v3/redis-deliveries" => route_admin_list_redis_deliveries(req, admin_state),
        "/minio/admin/v3/mysql-deliveries" => route_admin_list_mysql_deliveries(req, admin_state),
        "/minio/admin/v3/postgresql-deliveries" => {
            route_admin_list_postgresql_deliveries(req, admin_state)
        }
        "/minio/admin/v3/amqp-deliveries" => route_admin_list_amqp_deliveries(req, admin_state),
        "/minio/admin/v3/mqtt-deliveries" => route_admin_list_mqtt_deliveries(req, admin_state),
        "/minio/admin/v3/kafka-deliveries" => route_admin_list_kafka_deliveries(req, admin_state),
        "/minio/admin/v3/nats-deliveries" => route_admin_list_nats_deliveries(req, admin_state),
        "/minio/admin/v3/nsq-deliveries" => route_admin_list_nsq_deliveries(req, admin_state),
        "/minio/admin/v3/replication/resync" => route_admin_replication_resync(req, admin_state),
        "/minio/admin/v3/replication/status" => route_admin_replication_status(req, admin_state),
        "/minio/admin/v3/webhook-deliveries" => {
            route_admin_list_webhook_deliveries(req, admin_state)
        }
        "/minio/admin/v3/set-user-status" => route_admin_set_user_status(req, admin_state),
        "/minio/admin/v3/remove-user" => route_admin_remove_user(req, admin_state),
        path if path.starts_with("/minio/admin/v3/heal") => route_admin_heal(req, admin_state),
        _ => HandlerResponse {
            status: 404,
            headers: BTreeMap::new(),
            body: b"not found".to_vec(),
        },
    }
}

fn route_sts_request(
    req: &TestRequest,
    auth: &RequestAuth,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    match req.url.path() {
        "/minio/sts/v1/assume-role/root" => route_sts_assume_role_root(req, auth, admin_state),
        "/minio/sts/v1/assume-role/user" => route_sts_assume_role_user(req, admin_state),
        "/minio/sts/v1/assume-role/openid" => route_sts_assume_role_openid(req, admin_state),
        "/minio/sts/v1/assume-role/ldap" => route_sts_assume_role_ldap(req, admin_state),
        _ => HandlerResponse {
            status: 404,
            headers: BTreeMap::new(),
            body: b"not found".to_vec(),
        },
    }
}

fn route_kms_request(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    match req.url.path() {
        "/minio/kms/v1/status" => route_kms_status(req, admin_state),
        "/minio/kms/v1/metrics" => route_kms_metrics(req, admin_state),
        "/minio/kms/v1/key/status" => route_kms_key_status(req, admin_state),
        "/minio/kms/v1/key/list" => route_kms_list_keys(req, admin_state),
        "/minio/kms/v1/key/create" => route_kms_key_create(req, admin_state),
        "/minio/kms/v1/key/generate" => route_kms_key_generate(req, admin_state),
        "/minio/kms/v1/key/decrypt" => route_kms_key_decrypt(req, admin_state),
        "/minio/kms/v1/version" => route_kms_version(req, admin_state),
        "/minio/kms/v1/apis" => route_kms_apis(req, admin_state),
        _ => HandlerResponse {
            status: 404,
            headers: BTreeMap::new(),
            body: b"not found".to_vec(),
        },
    }
}

fn ensure_kms_auth(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> Result<(), HandlerResponse> {
    if ensure_admin_auth(req, &admin_state.active).is_ok() {
        return Ok(());
    }
    if let Some(token) = admin_state.kms.api_key_auth_token() {
        if req
            .header("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|value| value == token)
        {
            return Ok(());
        }
    }
    Err(admin_api_error_response(ApiErrorCode::AccessDenied))
}

fn kms_status_value(admin_state: &ServerAdminState, bucket: Option<String>) -> serde_json::Value {
    let mut value = serde_json::to_value(admin_state.kms.status())
        .unwrap_or_else(|_| serde_json::json!({ "configured": false }));
    if let Some(bucket) = bucket {
        let bucket_key = read_bucket_encryption_config(&admin_state.layer, &bucket)
            .ok()
            .and_then(|config| config)
            .and_then(|config| admin_state.kms.resolve_bucket_default_key(Some(&config)));
        value["bucket"] = serde_json::Value::String(bucket);
        value["bucketKey"] = serde_json::to_value(bucket_key).unwrap_or(serde_json::Value::Null);
    }
    value
}

fn route_kms_status(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_kms_auth(req, admin_state) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    json_response(
        200,
        &kms_status_value(admin_state, req.query_value("bucket")),
    )
}

fn route_kms_version(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_kms_auth(req, admin_state) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    json_response(
        200,
        &serde_json::json!({
            "version": "minio-rust-kms/v1",
            "backend": admin_state.kms.status().backend,
        }),
    )
}

fn route_kms_apis(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_kms_auth(req, admin_state) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    json_response(
        200,
        &serde_json::json!({
            "endpoints": [
                { "method": "GET", "path": "/minio/kms/v1/status" },
                { "method": "GET", "path": "/minio/kms/v1/metrics" },
                { "method": "GET", "path": "/minio/kms/v1/version" },
                { "method": "GET", "path": "/minio/kms/v1/apis" },
                { "method": "GET", "path": "/minio/kms/v1/key/list" },
                { "method": "GET", "path": "/minio/kms/v1/key/status" },
                { "method": "POST", "path": "/minio/kms/v1/key/create" },
                { "method": "POST", "path": "/minio/kms/v1/key/generate" },
                { "method": "POST", "path": "/minio/kms/v1/key/decrypt" },
            ]
        }),
    )
}

fn route_kms_metrics(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_kms_auth(req, admin_state) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    json_response(
        200,
        &serde_json::to_value(admin_state.kms.metrics()).unwrap_or(serde_json::Value::Null),
    )
}

fn route_kms_list_keys(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_kms_auth(req, admin_state) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let pattern = req.query_value("pattern");
    json_response(
        200,
        &serde_json::to_value(admin_state.kms.list_keys(pattern.as_deref()))
            .unwrap_or(serde_json::Value::Null),
    )
}

fn route_kms_key_status(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_kms_auth(req, admin_state) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let key_id = req.query_value("key-id");
    match admin_state.kms.key_status(key_id.as_deref()) {
        Ok(status) => json_response(
            200,
            &serde_json::to_value(status).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => json_response(400, &serde_json::json!({ "error": error })),
    }
}

fn route_kms_key_create(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_kms_auth(req, admin_state) {
        return response;
    }
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let key_id = req.query_value("key-id").unwrap_or_default();
    match admin_state.kms.create_key(&key_id) {
        Ok(status) => json_response(
            200,
            &serde_json::to_value(status).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => json_response(400, &serde_json::json!({ "error": error })),
    }
}

fn route_kms_key_generate(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_kms_auth(req, admin_state) {
        return response;
    }
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let body: crate::cmd::kms_service::KmsGenerateKeyRequestBody =
        match serde_json::from_slice(&req.body) {
            Ok(body) => body,
            Err(_) => return json_response(400, &serde_json::json!({ "error": "malformed json" })),
        };
    match admin_state.kms.generate_data_key(
        (!body.key_id.is_empty()).then_some(body.key_id.as_str()),
        crate::internal::kms::Context(body.associated_data),
    ) {
        Ok(dek) => json_response(
            200,
            &serde_json::to_value(crate::cmd::kms_service::KmsDekJson {
                key_id: dek.key_id,
                version: dek.version,
                plaintext: dek.plaintext.as_ref().map(|value| {
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, value)
                }),
                ciphertext: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &dek.ciphertext,
                ),
            })
            .unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => json_response(400, &serde_json::json!({ "error": error })),
    }
}

fn route_kms_key_decrypt(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_kms_auth(req, admin_state) {
        return response;
    }
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let body: crate::cmd::kms_service::KmsDecryptKeyRequestBody =
        match serde_json::from_slice(&req.body) {
            Ok(body) => body,
            Err(_) => return json_response(400, &serde_json::json!({ "error": "malformed json" })),
        };
    let ciphertext =
        match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, body.ciphertext) {
            Ok(ciphertext) => ciphertext,
            Err(_) => {
                return json_response(400, &serde_json::json!({ "error": "invalid ciphertext" }))
            }
        };
    match admin_state.kms.decrypt_data_key(
        &body.key_id,
        body.version,
        &ciphertext,
        crate::internal::kms::Context(body.associated_data),
    ) {
        Ok(plaintext) => json_response(
            200,
            &serde_json::to_value(crate::cmd::kms_service::KmsDecryptKeyResponse {
                plaintext: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    plaintext,
                ),
            })
            .unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => json_response(400, &serde_json::json!({ "error": error })),
    }
}

fn admin_api_error_response(code: ApiErrorCode) -> HandlerResponse {
    let (status, body) = match code {
        ApiErrorCode::AccessDenied
        | ApiErrorCode::SignatureDoesNotMatch
        | ApiErrorCode::InvalidAccessKeyID => (403, "access denied"),
        ApiErrorCode::InvalidQueryParams => (400, "invalid query parameters"),
        _ => (500, "internal error"),
    };
    HandlerResponse {
        status,
        headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: serde_json::json!({ "error": body })
            .to_string()
            .into_bytes(),
    }
}

fn json_response(status: u16, value: &serde_json::Value) -> HandlerResponse {
    HandlerResponse {
        status,
        headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: value.to_string().into_bytes(),
    }
}

#[derive(Debug, Deserialize)]
struct AdminAddUserBody {
    #[serde(rename = "secretKey")]
    secret_key: String,
    #[serde(default)]
    status: String,
}

fn route_admin_add_user(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "PUT" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let access_key = req.query_value("accessKey").unwrap_or_default();
    if access_key.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let body: AdminAddUserBody = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(_) => return json_response(400, &serde_json::json!({ "error": "malformed json" })),
    };
    let status = match parse_admin_account_status(&body.status) {
        Some(status) => status,
        None => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    match admin_state
        .users
        .lock()
        .expect("admin users lock")
        .set_user(&access_key, &body.secret_key, status)
    {
        Ok(()) => {
            let mut identity = admin_state.identity.lock().expect("identity lock");
            let _ = identity.set_user(
                &access_key,
                &body.secret_key,
                IdentityAccountStatus::from(status),
            );
            identity.add_sts_user(StsUser {
                username: access_key.clone(),
                secret_key: body.secret_key.clone(),
                policies: BTreeSet::new(),
                groups: BTreeSet::new(),
                enabled: status == AccountStatus::Enabled,
            });
            json_response(
                200,
                &serde_json::json!({
                    "accessKey": access_key,
                    "status": admin_account_status_name(status),
                }),
            )
        }
        Err(_) => admin_api_error_response(ApiErrorCode::InternalError),
    }
}

fn route_admin_list_users(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let users = admin_state
        .users
        .lock()
        .expect("admin users lock")
        .list_users();
    let json = users
        .into_iter()
        .map(|(access_key, info)| {
            (
                access_key,
                serde_json::json!({ "status": admin_account_status_name(info.status) }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json_response(200, &serde_json::Value::Object(json))
}

fn route_admin_set_user_status(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "PUT" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let access_key = req.query_value("accessKey").unwrap_or_default();
    let status = match req
        .query_value("status")
        .and_then(|value| parse_admin_account_status(&value))
    {
        Some(status) => status,
        None => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    match admin_state
        .users
        .lock()
        .expect("admin users lock")
        .set_user_status(&access_key, status)
    {
        Ok(()) => {
            let mut identity = admin_state.identity.lock().expect("identity lock");
            let _ = identity.set_user_status(&access_key, IdentityAccountStatus::from(status));
            if let Some(user) = identity.list_users().get(&access_key).cloned() {
                identity.add_sts_user(StsUser {
                    username: user.access_key,
                    secret_key: user.secret_key,
                    policies: BTreeSet::new(),
                    groups: BTreeSet::new(),
                    enabled: status == AccountStatus::Enabled,
                });
            }
            json_response(
                200,
                &serde_json::json!({
                    "accessKey": access_key,
                    "status": admin_account_status_name(status),
                }),
            )
        }
        Err(_) => json_response(404, &serde_json::json!({ "error": "no such user" })),
    }
}

fn route_admin_list_notifications(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let records = admin_state
        .notifications
        .lock()
        .expect("notifications lock");
    let values = records
        .iter()
        .filter(|record| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &record.bucket == bucket)
        })
        .map(|record| {
            serde_json::json!({
                "event": record.event,
                "bucket": record.bucket,
                "object": record.object,
                "targets": record.targets,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_queue_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .queue_deliveries
        .lock()
        .expect("queue deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "target_id": delivery.target_id,
                "event": delivery.event,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "version_id": delivery.version_id,
                "payload": delivery.payload,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_nats_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .nats_deliveries
        .lock()
        .expect("nats deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "address": delivery.address,
                "subject": delivery.subject,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_elasticsearch_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .elasticsearch_deliveries
        .lock()
        .expect("elasticsearch deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "endpoint": delivery.endpoint,
                "index": delivery.index,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_redis_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .redis_deliveries
        .lock()
        .expect("redis deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "address": delivery.address,
                "key": delivery.key,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_mysql_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .mysql_deliveries
        .lock()
        .expect("mysql deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "address": delivery.address,
                "database": delivery.database,
                "table": delivery.table,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_postgresql_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .postgresql_deliveries
        .lock()
        .expect("postgresql deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "address": delivery.address,
                "database": delivery.database,
                "table": delivery.table,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_amqp_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .amqp_deliveries
        .lock()
        .expect("amqp deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "url": delivery.url,
                "exchange": delivery.exchange,
                "routingKey": delivery.routing_key,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_mqtt_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .mqtt_deliveries
        .lock()
        .expect("mqtt deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "broker": delivery.broker,
                "topic": delivery.topic,
                "qos": delivery.qos,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_kafka_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .kafka_deliveries
        .lock()
        .expect("kafka deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "brokers": delivery.brokers,
                "topic": delivery.topic,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_list_nsq_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let bucket_filter = req.query_value("bucket");
    let deliveries = admin_state
        .nsq_deliveries
        .lock()
        .expect("nsq deliveries lock");
    let values = deliveries
        .iter()
        .filter(|delivery| {
            bucket_filter
                .as_ref()
                .is_none_or(|bucket| &delivery.bucket == bucket)
        })
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "address": delivery.address,
                "topic": delivery.topic,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_admin_kms_status(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    json_response(
        200,
        &kms_status_value(admin_state, req.query_value("bucket")),
    )
}

fn route_admin_kms_key_status(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let key_id = req.query_value("key-id");
    match admin_state.kms.key_status(key_id.as_deref()) {
        Ok(status) => json_response(
            200,
            &serde_json::to_value(status).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => json_response(400, &serde_json::json!({ "error": error })),
    }
}

fn route_admin_kms_key_create(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let key_id = req.query_value("key-id").unwrap_or_default();
    match admin_state.kms.create_key(&key_id) {
        Ok(status) => json_response(
            200,
            &serde_json::to_value(status).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => json_response(400, &serde_json::json!({ "error": error })),
    }
}

fn route_admin_add_canned_policy(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "PUT" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let name = req.query_value("name").unwrap_or_default();
    if name.is_empty() || req.body.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    match admin_state
        .identity
        .lock()
        .expect("identity lock")
        .add_canned_policy(&name, &req.body)
    {
        Ok(()) => json_response(200, &serde_json::json!({ "name": name })),
        Err(_) => admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    }
}

fn route_admin_list_canned_policies(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let policies = admin_state
        .identity
        .lock()
        .expect("identity lock")
        .list_canned_policies();
    json_response(200, &serde_json::json!({ "policies": policies }))
}

fn route_admin_attach_policy(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "PUT" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let access_key = req.query_value("accessKey").unwrap_or_default();
    let policy = req.query_value("policy").unwrap_or_default();
    if access_key.is_empty() || policy.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    match admin_state
        .identity
        .lock()
        .expect("identity lock")
        .attach_policy(&access_key, &policy)
    {
        Ok(()) => json_response(
            200,
            &serde_json::json!({ "accessKey": access_key, "policy": policy }),
        ),
        Err(_) => admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    }
}

fn route_admin_detach_policy(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "PUT" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let access_key = req.query_value("accessKey").unwrap_or_default();
    let policy = req.query_value("policy").unwrap_or_default();
    if access_key.is_empty() || policy.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    match admin_state
        .identity
        .lock()
        .expect("identity lock")
        .detach_policy(&access_key, &policy)
    {
        Ok(()) => json_response(
            200,
            &serde_json::json!({ "accessKey": access_key, "policy": policy }),
        ),
        Err(_) => admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    }
}

fn route_admin_remove_canned_policy(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "DELETE" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let name = req.query_value("name").unwrap_or_default();
    if name.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    match admin_state
        .identity
        .lock()
        .expect("identity lock")
        .remove_canned_policy(&name)
    {
        Ok(()) => HandlerResponse {
            status: 204,
            ..HandlerResponse::default()
        },
        Err(_) => admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    }
}

#[derive(Debug, Deserialize)]
struct AdminAddServiceAccountBody {
    #[serde(default)]
    session_policy_json: Option<serde_json::Value>,
    #[serde(default)]
    access_key: Option<String>,
    #[serde(default)]
    secret_key: Option<String>,
}

fn route_admin_add_service_account(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "PUT" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let target_user = req.query_value("targetUser").unwrap_or_default();
    if target_user.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let body = match serde_json::from_slice::<AdminAddServiceAccountBody>(&req.body) {
        Ok(value) => value,
        Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    let session_policy = match body.session_policy_json {
        Some(value) => match serde_json::to_vec(&value) {
            Ok(bytes) => Some(bytes),
            Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
        },
        None => None,
    };
    let mut identity = admin_state.identity.lock().expect("identity lock");
    let actor_access = identity.root_access_key().to_string();
    let actor_secret = identity.root_secret_key().to_string();
    match identity.add_service_account(
        &actor_access,
        &actor_secret,
        &target_user,
        session_policy.as_deref(),
        body.access_key.as_deref(),
        body.secret_key.as_deref(),
    ) {
        Ok(record) => json_response(200, &serde_json::to_value(record).unwrap_or_default()),
        Err(_) => admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    }
}

fn route_admin_list_service_accounts(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let target_user = req.query_value("targetUser");
    let accounts = admin_state
        .identity
        .lock()
        .expect("identity lock")
        .list_service_accounts(target_user.as_deref());
    json_response(200, &serde_json::json!({ "accounts": accounts }))
}

#[derive(Debug, Deserialize)]
struct AdminUpdateServiceAccountBody {
    #[serde(default)]
    secret_key: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

fn route_admin_update_service_account(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let access_key = req.query_value("accessKey").unwrap_or_default();
    if access_key.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let body = match serde_json::from_slice::<AdminUpdateServiceAccountBody>(&req.body) {
        Ok(value) => value,
        Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    let status = match body.status {
        Some(value) => match parse_admin_account_status(&value) {
            Some(status) => Some(IdentityAccountStatus::from(status)),
            None => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
        },
        None => None,
    };
    match admin_state
        .identity
        .lock()
        .expect("identity lock")
        .update_service_account(&access_key, body.secret_key.as_deref(), status)
    {
        Ok(()) => json_response(200, &serde_json::json!({ "accessKey": access_key })),
        Err(_) => admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    }
}

fn route_admin_remove_service_account(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "DELETE" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let access_key = req.query_value("accessKey").unwrap_or_default();
    if access_key.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    match admin_state
        .identity
        .lock()
        .expect("identity lock")
        .delete_service_account(&access_key)
    {
        Ok(()) => HandlerResponse {
            status: 204,
            ..HandlerResponse::default()
        },
        Err(_) => admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    }
}

fn route_admin_revoke_sts_session(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let access_key = req.query_value("accessKey").unwrap_or_default();
    if access_key.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    admin_state
        .identity
        .lock()
        .expect("identity lock")
        .revoke_session(&access_key);
    json_response(
        200,
        &serde_json::json!({ "accessKey": access_key, "revoked": true }),
    )
}

fn route_admin_export_iam(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let exported = admin_state.sts.lock().expect("sts lock").export_iam();
    match serde_json::to_value(exported) {
        Ok(value) => json_response(200, &value),
        Err(_) => admin_api_error_response(ApiErrorCode::InternalError),
    }
}

fn route_admin_import_iam(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let imported = match serde_json::from_slice::<ExportedIam>(&req.body) {
        Ok(value) => value,
        Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    admin_state
        .sts
        .lock()
        .expect("sts lock")
        .import_iam(imported.clone());
    json_response(
        200,
        &serde_json::json!({
            "users": imported.users.len(),
            "groups": imported.groups.len(),
            "policies": imported.policies.len(),
        }),
    )
}

fn route_admin_add_openid_provider(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "PUT" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let provider = match serde_json::from_slice::<OpenIdProvider>(&req.body) {
        Ok(value) => value,
        Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    let mut sts = admin_state.sts.lock().expect("sts lock");
    sts.add_openid_provider(provider.clone());
    if sts.validate_openid_configs().is_err() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    admin_state
        .identity
        .lock()
        .expect("identity lock")
        .add_openid_provider(provider.clone());
    json_response(
        200,
        &serde_json::json!({
            "provider": provider.name,
            "claimName": provider.claim_name,
        }),
    )
}

fn route_admin_set_ldap_config(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "PUT" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let config = match serde_json::from_slice::<LdapConfig>(&req.body) {
        Ok(value) => value,
        Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    admin_state
        .sts
        .lock()
        .expect("sts lock")
        .set_ldap_config(config.clone());
    admin_state
        .identity
        .lock()
        .expect("identity lock")
        .set_ldap_config(config.clone());
    json_response(
        200,
        &serde_json::json!({
            "normalizedBaseDn": config.normalized_base_dn,
        }),
    )
}

fn route_admin_replication_status(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }

    let bucket_filter = req.query_value("bucket");
    let objects = collect_replication_objects(&admin_state.layer, bucket_filter.as_deref());
    let now = now_ms();
    let queue = admin_state.replication_service.snapshot(now);
    let resync_targets = {
        let mut tracked = admin_state
            .resync_targets
            .lock()
            .expect("resync targets lock");
        let summaries =
            build_resync_target_summaries(&tracked, &queue.queue, bucket_filter.as_deref(), now);
        apply_resync_target_summaries(&mut tracked, &summaries);
        let _ = persist_resync_target_records(admin_state, bucket_filter.as_deref(), &tracked);
        summaries
    };
    let mut payload = build_replication_admin_status_payload_from_runtime(
        &objects,
        &[ReplicationRuntimeNodeSnapshot {
            node_name: "local".to_string(),
            uptime: now.saturating_sub(admin_state.started_at_ms),
            queue: queue.queue,
        }],
        now,
    );
    payload.resync_targets = resync_targets;
    match serde_json::to_value(&payload) {
        Ok(value) => json_response(200, &value),
        Err(_) => admin_api_error_response(ApiErrorCode::InternalError),
    }
}

fn route_admin_server_info(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" || req.query_value("info").is_none() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }

    let now = now_ms();
    let region = {
        let site = current_site();
        if site.region().is_empty() {
            crate::cmd::GLOBAL_MINIO_DEFAULT_REGION.to_string()
        } else {
            site.region().to_string()
        }
    };
    let kms_metrics = admin_state.kms.metrics();
    let queue = admin_state.replication_service.snapshot(now);
    let disks = admin_state
        .layer
        .disk_statuses()
        .into_iter()
        .map(|(path, online)| {
            serde_json::json!({
                "path": path,
                "online": online,
                "state": if online { "online" } else { "offline" },
            })
        })
        .collect::<Vec<_>>();

    json_response(
        200,
        &serde_json::json!({
            "mode": if admin_state.layer.has_write_quorum() && admin_state.layer.has_read_quorum() {
                "online"
            } else {
                "degraded"
            },
            "region": region,
            "startedAt": admin_state.started_at_ms,
            "uptime": now.saturating_sub(admin_state.started_at_ms),
            "servers": [{
                "endpoint": "local",
                "state": if admin_state.layer.has_write_quorum() && admin_state.layer.has_read_quorum() {
                    "online"
                } else {
                    "degraded"
                },
                "disks": disks,
            }],
            "buckets": {
                "count": admin_state.layer.bucket_count(),
            },
            "objects": {
                "count": admin_state.layer.visible_object_count(),
            },
            "backend": {
                "type": "LocalObjectLayer",
                "totalDisks": admin_state.layer.total_disk_count(),
                "onlineDisks": admin_state.layer.online_disk_count(),
                "offlineDisks": admin_state.layer.offline_disk_count(),
                "writeQuorum": admin_state.layer.has_write_quorum(),
                "readQuorum": admin_state.layer.has_read_quorum(),
            },
            "notifications": {
                "targets": admin_state.notification_targets.list().len(),
            },
            "kms": {
                "configured": kms_metrics.configured,
                "online": kms_metrics.online,
                "backend": kms_metrics.backend,
                "defaultKey": kms_metrics.default_key,
            },
            "replication": {
                "pending": queue.stats.queued.saturating_add(queue.stats.waiting_retry),
                "queued": queue.stats.queued,
                "waitingRetry": queue.stats.waiting_retry,
                "inFlight": queue.stats.in_flight,
                "failed": queue.stats.failed,
                "succeeded": queue.stats.succeeded,
                "completed": queue.stats.total_completed,
            },
        }),
    )
}

fn route_admin_storage_info(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }

    let disks = admin_state
        .layer
        .disk_statuses()
        .into_iter()
        .map(|(path, online)| {
            serde_json::json!({
                "path": path,
                "online": online,
                "state": if online { "ok" } else { "offline" },
            })
        })
        .collect::<Vec<_>>();

    json_response(
        200,
        &serde_json::json!({
            "backend": {
                "type": "LocalObjectLayer",
                "totalDisks": admin_state.layer.total_disk_count(),
                "onlineDisks": admin_state.layer.online_disk_count(),
                "offlineDisks": admin_state.layer.offline_disk_count(),
                "writeQuorum": admin_state.layer.has_write_quorum(),
                "readQuorum": admin_state.layer.has_read_quorum(),
            },
            "usage": {
                "buckets": admin_state.layer.bucket_count(),
                "objects": admin_state.layer.visible_object_count(),
            },
            "disks": disks,
        }),
    )
}

fn route_admin_replication_resync(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }

    let Some(bucket) = req.query_value("bucket").filter(|value| !value.is_empty()) else {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    };
    let target_arn = req.query_value("arn").filter(|value| !value.is_empty());
    let resync_before_date = now_ms();
    let resync_id = format!("resync-{resync_before_date}");

    match enqueue_bucket_replication_resync_for_layer(
        &admin_state.layer,
        &admin_state.replication_service,
        &bucket,
        target_arn.as_deref(),
        &resync_id,
        resync_before_date,
    ) {
        Ok(enqueued) => {
            let now = now_ms();
            let total_enqueued = enqueued
                .iter()
                .map(|summary| summary.scheduled_count)
                .sum::<u64>();
            {
                let mut tracked = admin_state
                    .resync_targets
                    .lock()
                    .expect("resync targets lock");
                for summary in &enqueued {
                    tracked.insert(
                        format!("{}\u{1f}{}", bucket, summary.target),
                        BucketReplicationResyncTargetRecord {
                            arn: summary.target.clone(),
                            resync_id: resync_id.clone(),
                            resync_before_date,
                            start_time: now,
                            last_updated: now,
                            status: "PENDING".to_string(),
                            scheduled_count: summary.scheduled_count,
                            scheduled_bytes: summary.scheduled_bytes,
                        },
                    );
                }
                let _ = persist_resync_target_records(admin_state, Some(&bucket), &tracked);
            }
            json_response(
                200,
                &serde_json::json!({
                    "bucket": bucket,
                    "arn": target_arn,
                    "resyncId": resync_id,
                    "resyncBeforeDate": resync_before_date,
                    "enqueued": total_enqueued,
                    "targets": enqueued.iter().map(|summary| serde_json::json!({
                        "target": summary.target,
                        "scheduledCount": summary.scheduled_count,
                        "scheduledBytes": summary.scheduled_bytes,
                    })).collect::<Vec<_>>(),
                    "status": "PENDING",
                }),
            )
        }
        Err(error) if error.contains("not found") => {
            json_response(404, &serde_json::json!({ "error": error }))
        }
        Err(error) => json_response(500, &serde_json::json!({ "error": error })),
    }
}

fn build_resync_target_summaries(
    tracked: &BTreeMap<String, BucketReplicationResyncTargetRecord>,
    queue: &ReplicationQueue,
    bucket_filter: Option<&str>,
    now_ms: i64,
) -> Vec<ReplicationResyncTargetStatusSummary> {
    let mut summaries = Vec::new();
    for (key, state) in tracked.iter().filter(|(key, _)| {
        let bucket = key.split('\u{1f}').next().unwrap_or_default();
        bucket_filter.map(|filter| filter == bucket).unwrap_or(true)
    }) {
        let bucket = key.split('\u{1f}').next().unwrap_or_default().to_string();
        let mut summary = ReplicationResyncTargetStatusSummary {
            bucket: bucket.clone(),
            arn: state.arn.clone(),
            resync_id: state.resync_id.clone(),
            resync_before_date: state.resync_before_date,
            start_time: state.start_time,
            last_updated: state.last_updated,
            ..ReplicationResyncTargetStatusSummary::default()
        };
        let mut queue_scheduled_count = 0u64;
        let mut queue_scheduled_bytes = 0u64;

        for entry in queue.entries.values() {
            if entry.bucket != bucket || entry.target_arn != state.arn {
                continue;
            }
            let Some(metadata) = &entry.metadata else {
                continue;
            };
            if metadata
                .get("resync-id")
                .is_none_or(|resync_id| resync_id != &state.resync_id)
            {
                continue;
            }

            summary.last_updated = summary.last_updated.max(entry.updated_at);
            queue_scheduled_count = queue_scheduled_count.saturating_add(1);
            queue_scheduled_bytes = queue_scheduled_bytes.saturating_add(entry.payload_size);
            match entry.status {
                ReplicationQueueStatus::Queued if entry.retry.next_attempt_at > now_ms => {
                    summary.pending_count = summary.pending_count.saturating_add(1);
                    summary.pending_bytes =
                        summary.pending_bytes.saturating_add(entry.payload_size);
                }
                ReplicationQueueStatus::Queued | ReplicationQueueStatus::InFlight => {
                    summary.pending_count = summary.pending_count.saturating_add(1);
                    summary.pending_bytes =
                        summary.pending_bytes.saturating_add(entry.payload_size);
                }
                ReplicationQueueStatus::Succeeded => {
                    summary.completed_count = summary.completed_count.saturating_add(1);
                    summary.completed_bytes =
                        summary.completed_bytes.saturating_add(entry.payload_size);
                }
                ReplicationQueueStatus::Failed => {
                    summary.failed_count = summary.failed_count.saturating_add(1);
                    summary.failed_bytes = summary.failed_bytes.saturating_add(entry.payload_size);
                }
            }
        }
        summary.scheduled_count = state.scheduled_count.max(queue_scheduled_count);
        summary.scheduled_bytes = state.scheduled_bytes.max(queue_scheduled_bytes);

        summary.status = if summary.pending_count > 0 {
            "PENDING".to_string()
        } else if summary.failed_count > 0 && summary.completed_count == 0 {
            "FAILED".to_string()
        } else if summary.pending_count == 0
            && summary.completed_count == 0
            && summary.failed_count == 0
        {
            state.status.clone()
        } else {
            "COMPLETED".to_string()
        };
        summaries.push(summary);
    }
    summaries
}

fn apply_resync_target_summaries(
    tracked: &mut BTreeMap<String, BucketReplicationResyncTargetRecord>,
    summaries: &[ReplicationResyncTargetStatusSummary],
) {
    for summary in summaries {
        if let Some(record) = tracked.get_mut(&format!("{}\u{1f}{}", summary.bucket, summary.arn)) {
            record.status = summary.status.clone();
            record.last_updated = summary.last_updated;
            record.scheduled_count = summary.scheduled_count;
            record.scheduled_bytes = summary.scheduled_bytes;
        }
    }
}

fn persist_resync_target_records(
    admin_state: &ServerAdminState,
    bucket_filter: Option<&str>,
    tracked: &BTreeMap<String, BucketReplicationResyncTargetRecord>,
) -> Result<(), String> {
    let mut per_bucket =
        BTreeMap::<String, BTreeMap<String, BucketReplicationResyncTargetRecord>>::new();
    for (key, record) in tracked {
        let mut parts = key.split('\u{1f}');
        let bucket = parts.next().unwrap_or_default();
        let arn = parts.next().unwrap_or_default();
        if bucket_filter.is_some_and(|filter| filter != bucket) {
            continue;
        }
        per_bucket
            .entry(bucket.to_string())
            .or_default()
            .insert(arn.to_string(), record.clone());
    }

    if let Some(bucket) = bucket_filter {
        let records = per_bucket.remove(bucket).unwrap_or_default();
        write_bucket_replication_resync_state(&admin_state.layer, bucket, &records)?;
        return Ok(());
    }

    for bucket in admin_state
        .layer
        .list_buckets(BucketOptions::default())?
        .into_iter()
        .map(|bucket| bucket.name)
    {
        let records = per_bucket.remove(&bucket).unwrap_or_default();
        write_bucket_replication_resync_state(&admin_state.layer, &bucket, &records)?;
    }
    Ok(())
}

pub(super) fn rehydrate_replication_resync_queue(
    admin_state: &ServerAdminState,
) -> Result<(), String> {
    let tracked = admin_state
        .resync_targets
        .lock()
        .expect("resync targets lock")
        .clone();
    for (key, record) in tracked {
        if matches!(record.status.as_str(), "COMPLETED" | "FAILED") {
            continue;
        }
        let bucket = key.split('\u{1f}').next().unwrap_or_default().to_string();
        let _ = enqueue_bucket_replication_resync_for_layer(
            &admin_state.layer,
            &admin_state.replication_service,
            &bucket,
            Some(&record.arn),
            &record.resync_id,
            record.resync_before_date,
        )?;
    }
    Ok(())
}

fn route_admin_list_webhook_deliveries(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "GET" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let deliveries = admin_state
        .webhook_deliveries
        .lock()
        .expect("webhook deliveries lock");
    let values = deliveries
        .iter()
        .map(|delivery| {
            serde_json::json!({
                "targetId": delivery.target_id,
                "endpoint": delivery.endpoint,
                "bucket": delivery.bucket,
                "object": delivery.object,
                "event": delivery.event,
                "delivered": delivery.delivered,
                "error": delivery.error,
            })
        })
        .collect::<Vec<_>>();
    json_response(200, &serde_json::json!({ "records": values }))
}

fn route_sts_assume_role_root(
    req: &TestRequest,
    auth: &RequestAuth,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    if auth.access_key != admin_state.active.access_key
        || !(auth.prevalidated || auth.secret_key == admin_state.active.secret_key)
    {
        return admin_api_error_response(ApiErrorCode::AccessDenied);
    }

    let session_policy = if req.body.is_empty() {
        None
    } else {
        match serde_json::from_slice::<StsPolicy>(&req.body) {
            Ok(value) => Some(value),
            Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
        }
    };
    match admin_state
        .sts
        .lock()
        .expect("sts lock")
        .assume_role_for_root(
            &admin_state.active.access_key,
            &admin_state.active.secret_key,
            session_policy,
        ) {
        Ok(creds) => match serde_json::to_value(creds) {
            Ok(value) => json_response(200, &value),
            Err(_) => admin_api_error_response(ApiErrorCode::InternalError),
        },
        Err(_) => admin_api_error_response(ApiErrorCode::AccessDenied),
    }
}

fn route_sts_assume_role_user(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    #[derive(Deserialize)]
    struct UserReq {
        username: String,
        secret_key: String,
        #[serde(default)]
        session_policy: Option<StsPolicy>,
        #[serde(default)]
        tags: BTreeMap<String, String>,
    }

    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let payload = match serde_json::from_slice::<UserReq>(&req.body) {
        Ok(value) => value,
        Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    match admin_state
        .identity
        .lock()
        .expect("identity lock")
        .assume_role_for_user(
            &payload.username,
            &payload.secret_key,
            payload.session_policy,
            payload.tags,
        ) {
        Ok(creds) => match serde_json::to_value(creds) {
            Ok(value) => json_response(200, &value),
            Err(_) => admin_api_error_response(ApiErrorCode::InternalError),
        },
        Err(_) => admin_api_error_response(ApiErrorCode::AccessDenied),
    }
}

fn route_sts_assume_role_openid(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    #[derive(Deserialize)]
    struct OpenIdReq {
        provider_name: String,
        claims: OpenIdClaims,
        requested_role: Option<String>,
    }

    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let payload = match serde_json::from_slice::<OpenIdReq>(&req.body) {
        Ok(value) => value,
        Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    match admin_state
        .sts
        .lock()
        .expect("sts lock")
        .assume_role_with_openid(
            &payload.provider_name,
            &payload.claims,
            payload.requested_role.as_deref(),
        ) {
        Ok(creds) => match serde_json::to_value(creds) {
            Ok(value) => json_response(200, &value),
            Err(_) => admin_api_error_response(ApiErrorCode::InternalError),
        },
        Err(_) => admin_api_error_response(ApiErrorCode::AccessDenied),
    }
}

fn route_sts_assume_role_ldap(
    req: &TestRequest,
    admin_state: &ServerAdminState,
) -> HandlerResponse {
    #[derive(Deserialize)]
    struct LdapReq {
        username: String,
        dn: String,
        #[serde(default)]
        group_dns: Vec<String>,
    }

    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let payload = match serde_json::from_slice::<LdapReq>(&req.body) {
        Ok(value) => value,
        Err(_) => return admin_api_error_response(ApiErrorCode::InvalidQueryParams),
    };
    match admin_state
        .sts
        .lock()
        .expect("sts lock")
        .assume_role_with_ldap(&payload.username, &payload.dn, &payload.group_dns)
    {
        Ok(creds) => match serde_json::to_value(creds) {
            Ok(value) => json_response(200, &value),
            Err(_) => admin_api_error_response(ApiErrorCode::InternalError),
        },
        Err(_) => admin_api_error_response(ApiErrorCode::AccessDenied),
    }
}

fn collect_replication_objects(
    layer: &LocalObjectLayer,
    bucket_filter: Option<&str>,
) -> Vec<ReplicationObjectInfo> {
    let bucket_names = match bucket_filter {
        Some(bucket) => vec![bucket.to_string()],
        None => layer
            .list_buckets(BucketOptions::default())
            .map(|buckets| buckets.into_iter().map(|bucket| bucket.name).collect())
            .unwrap_or_default(),
    };

    let mut objects = Vec::new();
    for bucket in bucket_names {
        let Ok(entries) = layer.all_object_versions(&bucket) else {
            continue;
        };
        objects.extend(entries.into_iter().map(|info| {
            let status = info
                .user_defined
                .get("x-amz-bucket-replication-status")
                .cloned()
                .unwrap_or_default();
            ReplicationObjectInfo {
                bucket: info.bucket,
                name: info.name,
                size: info.size,
                delete_marker: info.delete_marker,
                version_id: info.version_id,
                mod_time: info.mod_time,
                replication_status: status,
                replication_status_internal: String::new(),
                user_defined: info.user_defined,
            }
        }));
    }
    objects
}

fn route_admin_remove_user(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "DELETE" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    let access_key = req.query_value("accessKey").unwrap_or_default();
    if access_key.is_empty() {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }
    match admin_state
        .users
        .lock()
        .expect("admin users lock")
        .remove_user(&access_key)
    {
        Ok(()) => {
            let _ = admin_state
                .identity
                .lock()
                .expect("identity lock")
                .remove_user(&access_key);
            HandlerResponse {
                status: 204,
                ..HandlerResponse::default()
            }
        }
        Err(_) => admin_api_error_response(ApiErrorCode::InternalError),
    }
}

fn route_admin_heal(req: &TestRequest, admin_state: &ServerAdminState) -> HandlerResponse {
    if let Err(response) = ensure_admin_auth(req, &admin_state.active) {
        return response;
    }
    if req.method != "POST" {
        return admin_api_error_response(ApiErrorCode::InvalidQueryParams);
    }

    let mut vars = BTreeMap::new();
    let suffix = req
        .url
        .path()
        .trim_start_matches("/minio/admin/v3/heal")
        .trim_start_matches('/');
    if !suffix.is_empty() {
        let mut parts = suffix.splitn(2, '/');
        if let Some(bucket) = parts.next().filter(|part| !part.is_empty()) {
            vars.insert(MGMT_BUCKET.to_string(), bucket.to_string());
        }
        if let Some(prefix) = parts.next().filter(|part| !part.is_empty()) {
            vars.insert(MGMT_PREFIX.to_string(), prefix.to_string());
        }
    }

    let mut q_params = BTreeMap::<String, Vec<String>>::new();
    for (key, value) in req.url.query_pairs() {
        q_params
            .entry(key.into_owned())
            .or_default()
            .push(value.into_owned());
    }

    let (params, err) = extract_heal_init_params(&vars, &q_params, &req.body);
    if err != AdminApiErrorCode::None {
        return admin_mgmt_error_response(err);
    }

    json_response(
        200,
        &serde_json::json!({
            "bucket": params.bucket,
            "prefix": params.obj_prefix,
            "clientToken": params.client_token,
            "forceStart": params.force_start,
            "forceStop": params.force_stop,
            "recursive": params.hs.recursive,
            "dryRun": params.hs.dry_run,
            "remove": params.hs.remove,
            "scanMode": params.hs.scan_mode,
        }),
    )
}

fn ensure_admin_auth(req: &TestRequest, active: &Credentials) -> Result<(), HandlerResponse> {
    let (_, err) = check_admin_request_auth(req, active);
    if err == ApiErrorCode::None {
        Ok(())
    } else {
        Err(admin_api_error_response(err))
    }
}

fn parse_admin_account_status(value: &str) -> Option<AccountStatus> {
    if value.is_empty() || value.eq_ignore_ascii_case("enabled") {
        Some(AccountStatus::Enabled)
    } else if value.eq_ignore_ascii_case("disabled") {
        Some(AccountStatus::Disabled)
    } else {
        None
    }
}

fn admin_account_status_name(status: AccountStatus) -> &'static str {
    match status {
        AccountStatus::Enabled => "enabled",
        AccountStatus::Disabled => "disabled",
    }
}

fn admin_mgmt_error_response(code: AdminApiErrorCode) -> HandlerResponse {
    let (status, body) = match code {
        AdminApiErrorCode::Api(api) => return admin_api_error_response(api),
        AdminApiErrorCode::InvalidRequest | AdminApiErrorCode::RequestBodyParse => {
            (400, "invalid admin request")
        }
        AdminApiErrorCode::HealMissingBucket => (400, "missing heal bucket"),
        AdminApiErrorCode::AdminConfigNoQuorum => (503, "admin config quorum unavailable"),
        AdminApiErrorCode::None => (200, "ok"),
    };
    json_response(status, &serde_json::json!({ "error": body }))
}

fn normalized_headers(headers: &[Header]) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|header| {
            (
                header.field.as_str().to_string().to_ascii_lowercase(),
                header.value.as_str().to_string(),
            )
        })
        .collect()
}

fn split_bucket_object(path: &str) -> Result<(Option<String>, Option<String>), String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok((None, None));
    }

    let mut parts = trimmed.splitn(2, '/');
    let bucket = percent_decode_str(parts.next().unwrap_or_default())
        .decode_utf8_lossy()
        .to_string();
    if bucket.is_empty() {
        return Err("bucket path is empty".to_string());
    }
    let object = parts
        .next()
        .map(|part| percent_decode_str(part).decode_utf8_lossy().to_string())
        .filter(|part| !part.is_empty());
    Ok((Some(bucket), object))
}

fn parse_i32(value: Option<&String>, default: i32) -> i32 {
    value
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(default)
}

fn request_body_reader(body: &[u8], headers: &BTreeMap<String, String>) -> PutObjReader {
    let declared_size = headers
        .get("content-length")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(body.len() as i64);
    PutObjReader {
        data: body.to_vec(),
        declared_size,
        ..PutObjReader::default()
    }
}

fn method_not_allowed() -> HandlerResponse {
    HandlerResponse {
        status: 405,
        headers: BTreeMap::from([(
            "content-type".to_string(),
            "application/xml".to_string(),
        )]),
        body: b"<Error><Code>MethodNotAllowed</Code><Message>The specified method is not allowed against this resource.</Message></Error>".to_vec(),
    }
}
