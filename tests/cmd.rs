#![allow(dead_code)]

#[path = "support/mod.rs"]
mod support;

#[path = "cmd/admin_handlers_users_race_test.rs"]
mod cmd_admin_handlers_users_race_test;

#[path = "cmd/admin_handlers_users_test.rs"]
mod cmd_admin_handlers_users_test;

#[path = "cmd/admin_handlers_test.rs"]
mod cmd_admin_handlers_test;

#[path = "cmd/api_errors_test.rs"]
mod cmd_api_errors_test;

#[path = "cmd/api_headers_test.rs"]
mod cmd_api_headers_test;

#[path = "cmd/api_resources_test.rs"]
mod cmd_api_resources_test;

#[path = "cmd/api_response_test.rs"]
mod cmd_api_response_test;

#[path = "cmd/api_utils_test.rs"]
mod cmd_api_utils_test;

#[path = "cmd/auth_handler_test.rs"]
mod cmd_auth_handler_test;

#[path = "cmd/background_newdisks_heal_ops_gen_test.rs"]
mod cmd_background_newdisks_heal_ops_gen_test;

#[path = "cmd/batch_expire_gen_test.rs"]
mod cmd_batch_expire_gen_test;

#[path = "cmd/batch_expire_test.rs"]
mod cmd_batch_expire_test;

#[path = "cmd/batch_handlers_gen_test.rs"]
mod cmd_batch_handlers_gen_test;

#[path = "cmd/batch_handlers_test.rs"]
mod cmd_batch_handlers_test;

#[path = "cmd/batch_job_common_types_gen_test.rs"]
mod cmd_batch_job_common_types_gen_test;

#[path = "cmd/batch_job_common_types_test.rs"]
mod cmd_batch_job_common_types_test;

#[path = "cmd/batch_replicate_gen_test.rs"]
mod cmd_batch_replicate_gen_test;

#[path = "cmd/batch_replicate_test.rs"]
mod cmd_batch_replicate_test;

#[path = "cmd/batch_rotate_gen_test.rs"]
mod cmd_batch_rotate_gen_test;

#[path = "cmd/benchmark_utils_test.rs"]
mod cmd_benchmark_utils_test;

#[path = "cmd/bitrot_test.rs"]
mod cmd_bitrot_test;

#[path = "cmd/bootstrap_peer_server_gen_test.rs"]
mod cmd_bootstrap_peer_server_gen_test;

#[path = "cmd/bucket_encryption_test.rs"]
mod cmd_bucket_encryption_test;

#[path = "cmd/bucket_handlers_test.rs"]
mod cmd_bucket_handlers_test;

#[path = "cmd/bucket_lifecycle_handlers_test.rs"]
mod cmd_bucket_lifecycle_handlers_test;

#[path = "cmd/bucket_lifecycle_test.rs"]
mod cmd_bucket_lifecycle_test;

#[path = "cmd/bucket_metadata_gen_test.rs"]
mod cmd_bucket_metadata_gen_test;

#[path = "cmd/bucket_policy_handlers_test.rs"]
mod cmd_bucket_policy_handlers_test;

#[path = "cmd/bucket_replication_metrics_gen_test.rs"]
mod cmd_bucket_replication_metrics_gen_test;

#[path = "cmd/bucket_replication_utils_gen_test.rs"]
mod cmd_bucket_replication_utils_gen_test;

#[path = "cmd/bucket_replication_utils_test.rs"]
mod cmd_bucket_replication_utils_test;

#[path = "cmd/bucket_replication_test.rs"]
mod cmd_bucket_replication_test;

#[path = "cmd/remote_replication_transport_test.rs"]
mod cmd_remote_replication_transport_test;

#[path = "cmd/replication_queue_store_test.rs"]
mod cmd_replication_queue_store_test;

#[path = "cmd/bucket_stats_gen_test.rs"]
mod cmd_bucket_stats_gen_test;

#[path = "cmd/common_main_test.rs"]
mod cmd_common_main_test;

#[path = "cmd/config_current_test.rs"]
mod cmd_config_current_test;

#[path = "cmd/config_encrypted_test.rs"]
mod cmd_config_encrypted_test;

#[path = "cmd/copy_part_range_test.rs"]
mod cmd_copy_part_range_test;

#[path = "cmd/crossdomain_xml_handler_test.rs"]
mod cmd_crossdomain_xml_handler_test;

#[path = "cmd/data_scanner_test.rs"]
mod cmd_data_scanner_test;

#[path = "cmd/data_usage_cache_gen_test.rs"]
mod cmd_data_usage_cache_gen_test;

#[path = "cmd/data_usage_cache_test.rs"]
mod cmd_data_usage_cache_test;

#[path = "cmd/data_usage_test.rs"]
mod cmd_data_usage_test;

#[path = "cmd/dummy_data_generator_test.rs"]
mod cmd_dummy_data_generator_test;

#[path = "cmd/dynamic_timeouts_test.rs"]
mod cmd_dynamic_timeouts_test;

#[path = "cmd/encryption_v1_test.rs"]
mod cmd_encryption_v1_test;

#[path = "cmd/endpoint_ellipses_test.rs"]
mod cmd_endpoint_ellipses_test;

#[path = "cmd/endpoint_contrib_test.rs"]
mod cmd_endpoint_contrib_test;

#[path = "cmd/endpoint_test.rs"]
mod cmd_endpoint_test;

#[path = "cmd/erasure_decode_test.rs"]
mod cmd_erasure_decode_test;

#[path = "cmd/erasure_encode_test.rs"]
mod cmd_erasure_encode_test;

#[path = "cmd/erasure_heal_test.rs"]
mod cmd_erasure_heal_test;

#[path = "cmd/erasure_healing_common_test.rs"]
mod cmd_erasure_healing_common_test;

#[path = "cmd/erasure_healing_test.rs"]
mod cmd_erasure_healing_test;

#[path = "cmd/erasure_metadata_utils_test.rs"]
mod cmd_erasure_metadata_utils_test;

#[path = "cmd/erasure_metadata_test.rs"]
mod cmd_erasure_metadata_test;

#[path = "cmd/erasure_multipart_conditional_test.rs"]
mod cmd_erasure_multipart_conditional_test;

#[path = "cmd/erasure_object_conditional_test.rs"]
mod cmd_erasure_object_conditional_test;

#[path = "cmd/erasure_object_test.rs"]
mod cmd_erasure_object_test;

#[path = "cmd/erasure_server_pool_decom_gen_test.rs"]
mod cmd_erasure_server_pool_decom_gen_test;

#[path = "cmd/erasure_server_pool_decom_test.rs"]
mod cmd_erasure_server_pool_decom_test;

#[path = "cmd/erasure_server_pool_rebalance_gen_test.rs"]
mod cmd_erasure_server_pool_rebalance_gen_test;

#[path = "cmd/erasure_sets_test.rs"]
mod cmd_erasure_sets_test;

#[path = "cmd/erasure_test.rs"]
mod cmd_erasure_test;

#[path = "cmd/format_erasure_test.rs"]
mod cmd_format_erasure_test;

#[path = "cmd/generic_handlers_test.rs"]
mod cmd_generic_handlers_test;

#[path = "cmd/handler_utils_test.rs"]
mod cmd_handler_utils_test;

#[path = "cmd/http_tracer_test.rs"]
mod cmd_http_tracer_test;

#[path = "cmd/httprange_test.rs"]
mod cmd_httprange_test;

#[path = "cmd/iam_etcd_store_test.rs"]
mod cmd_iam_etcd_store_test;

#[path = "cmd/iam_object_store_test.rs"]
mod cmd_iam_object_store_test;

#[path = "cmd/jwt_test.rs"]
mod cmd_jwt_test;

#[path = "cmd/kms_handlers_test.rs"]
mod cmd_kms_handlers_test;

#[path = "cmd/kms_service_test.rs"]
mod cmd_kms_service_test;

#[path = "cmd/last_minute_gen_test.rs"]
mod cmd_last_minute_gen_test;

#[path = "cmd/leak_detect_test.rs"]
mod cmd_leak_detect_test;

#[path = "cmd/local_locker_gen_test.rs"]
mod cmd_local_locker_gen_test;

#[path = "cmd/local_locker_test.rs"]
mod cmd_local_locker_test;

#[path = "cmd/lock_rest_client_test.rs"]
mod cmd_lock_rest_client_test;

#[path = "cmd/lock_rest_server_common_test.rs"]
mod cmd_lock_rest_server_common_test;

#[path = "cmd/metacache_bucket_test.rs"]
mod cmd_metacache_bucket_test;

#[path = "cmd/metacache_entries_test.rs"]
mod cmd_metacache_entries_test;

#[path = "cmd/metacache_set_gen_test.rs"]
mod cmd_metacache_set_gen_test;

#[path = "cmd/metacache_stream_test.rs"]
mod cmd_metacache_stream_test;

#[path = "cmd/metacache_walk_gen_test.rs"]
mod cmd_metacache_walk_gen_test;

#[path = "cmd/metacache_gen_test.rs"]
mod cmd_metacache_gen_test;

#[path = "cmd/metacache_test.rs"]
mod cmd_metacache_test;

#[path = "cmd/metrics_v2_gen_test.rs"]
mod cmd_metrics_v2_gen_test;

#[path = "cmd/metrics_v2_test.rs"]
mod cmd_metrics_v2_test;

#[path = "cmd/mrf_gen_test.rs"]
mod cmd_mrf_gen_test;

#[path = "cmd/namespace_lock_test.rs"]
mod cmd_namespace_lock_test;

#[path = "cmd/naughty_disk_test.rs"]
mod cmd_naughty_disk_test;

#[path = "cmd/net_test.rs"]
mod cmd_net_test;

#[path = "cmd/object_api_deleteobject_test.rs"]
mod cmd_object_api_deleteobject_test;

#[path = "cmd/object_api_getobjectinfo_test.rs"]
mod cmd_object_api_getobjectinfo_test;

#[path = "cmd/object_api_listobjects_test.rs"]
mod cmd_object_api_listobjects_test;

#[path = "cmd/object_api_multipart_test.rs"]
mod cmd_object_api_multipart_test;

#[path = "cmd/object_api_options_test.rs"]
mod cmd_object_api_options_test;

#[path = "cmd/object_api_putobject_test.rs"]
mod cmd_object_api_putobject_test;

#[path = "cmd/object_api_utils_test.rs"]
mod cmd_object_api_utils_test;

#[path = "cmd/object_handlers_common_test.rs"]
mod cmd_object_handlers_common_test;

#[path = "cmd/object_handlers_test.rs"]
mod cmd_object_handlers_test;

#[path = "cmd/object_lambda_handlers_test.rs"]
mod cmd_object_lambda_handlers_test;

#[path = "cmd/object_api_suite_test.rs"]
mod cmd_object_api_suite_test;

#[path = "cmd/os_readdir_test.rs"]
mod cmd_os_readdir_test;

#[path = "cmd/os_reliable_test.rs"]
mod cmd_os_reliable_test;

#[path = "cmd/policy_test.rs"]
mod cmd_policy_test;

#[path = "cmd/post_policy_test.rs"]
mod cmd_post_policy_test;

#[path = "cmd/postpolicyform_test.rs"]
mod cmd_postpolicyform_test;

#[path = "cmd/server_main_test.rs"]
mod cmd_server_main_test;

#[path = "cmd/server_startup_msg_test.rs"]
mod cmd_server_startup_msg_test;

#[path = "cmd/server_test.rs"]
mod cmd_server_test;

#[path = "cmd/sftp_server_test.rs"]
mod cmd_sftp_server_test;

#[path = "cmd/signature_v2_test.rs"]
mod cmd_signature_v2_test;

#[path = "cmd/signature_v4_parser_test.rs"]
mod cmd_signature_v4_parser_test;

#[path = "cmd/signature_v4_utils_test.rs"]
mod cmd_signature_v4_utils_test;

#[path = "cmd/signature_v4_test.rs"]
mod cmd_signature_v4_test;

#[path = "cmd/site_replication_metrics_gen_test.rs"]
mod cmd_site_replication_metrics_gen_test;

#[path = "cmd/site_replication_utils_gen_test.rs"]
mod cmd_site_replication_utils_gen_test;

#[path = "cmd/site_replication_test.rs"]
mod cmd_site_replication_test;

#[path = "cmd/storage_datatypes_gen_test.rs"]
mod cmd_storage_datatypes_gen_test;

#[path = "cmd/storage_datatypes_test.rs"]
mod cmd_storage_datatypes_test;

#[path = "cmd/storage_rest_common_gen_test.rs"]
mod cmd_storage_rest_common_gen_test;

#[path = "cmd/storage_rest_test.rs"]
mod cmd_storage_rest_test;

#[path = "cmd/streaming_signature_v4_test.rs"]
mod cmd_streaming_signature_v4_test;

#[path = "cmd/sts_handlers_test.rs"]
mod cmd_sts_handlers_test;

#[path = "cmd/test_utils_test.rs"]
mod cmd_test_utils_test;

#[path = "cmd/tier_last_day_stats_gen_test.rs"]
mod cmd_tier_last_day_stats_gen_test;

#[path = "cmd/tier_gen_test.rs"]
mod cmd_tier_gen_test;

#[path = "cmd/tier_test.rs"]
mod cmd_tier_test;

#[path = "cmd/update_notifier_test.rs"]
mod cmd_update_notifier_test;

#[path = "cmd/update_test.rs"]
mod cmd_update_test;

#[path = "cmd/url_test.rs"]
mod cmd_url_test;

#[path = "cmd/utils_test.rs"]
mod cmd_utils_test;

#[path = "cmd/version_test.rs"]
mod cmd_version_test;

#[path = "cmd/xl_storage_errors_test.rs"]
mod cmd_xl_storage_errors_test;

#[path = "cmd/xl_storage_format_utils_test.rs"]
mod cmd_xl_storage_format_utils_test;

#[path = "cmd/xl_storage_format_v1_gen_test.rs"]
mod cmd_xl_storage_format_v1_gen_test;

#[path = "cmd/xl_storage_format_v2_gen_test.rs"]
mod cmd_xl_storage_format_v2_gen_test;

#[path = "cmd/xl_storage_format_v2_test.rs"]
mod cmd_xl_storage_format_v2_test;

#[path = "cmd/xl_storage_format_test.rs"]
mod cmd_xl_storage_format_test;

#[path = "cmd/xl_storage_free_version_test.rs"]
mod cmd_xl_storage_free_version_test;

#[path = "cmd/xl_storage_test.rs"]
mod cmd_xl_storage_test;

#[path = "cmd/xl_storage_unix_test.rs"]
mod cmd_xl_storage_unix_test;

#[path = "cmd/xl_storage_windows_test.rs"]
mod cmd_xl_storage_windows_test;
