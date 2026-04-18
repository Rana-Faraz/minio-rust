#![allow(dead_code)]

#[path = "support/mod.rs"]
mod support;

#[path = "internal/amztime/iso8601_time_test.rs"]
mod internal_amztime_iso8601_time_test;

#[path = "internal/amztime/parse_test.rs"]
mod internal_amztime_parse_test;

#[path = "internal/arn/arn_test.rs"]
mod internal_arn_arn_test;

#[path = "internal/auth/credentials_test.rs"]
mod internal_auth_credentials_test;

#[path = "internal/bpool/bpool_test.rs"]
mod internal_bpool_bpool_test;

#[path = "internal/bucket/bandwidth/monitor_gen_test.rs"]
mod internal_bucket_bandwidth_monitor_gen_test;

#[path = "internal/bucket/bandwidth/monitor_test.rs"]
mod internal_bucket_bandwidth_monitor_test;

#[path = "internal/bucket/encryption/bucket_sse_config_test.rs"]
mod internal_bucket_encryption_bucket_sse_config_test;

#[path = "internal/bucket/lifecycle/delmarker_expiration_test.rs"]
mod internal_bucket_lifecycle_delmarker_expiration_test;

#[path = "internal/bucket/lifecycle/evaluator_test.rs"]
mod internal_bucket_lifecycle_evaluator_test;

#[path = "internal/bucket/lifecycle/expiration_test.rs"]
mod internal_bucket_lifecycle_expiration_test;

#[path = "internal/bucket/lifecycle/filter_test.rs"]
mod internal_bucket_lifecycle_filter_test;

#[path = "internal/bucket/lifecycle/lifecycle_test.rs"]
mod internal_bucket_lifecycle_lifecycle_test;

#[path = "internal/bucket/lifecycle/noncurrentversion_test.rs"]
mod internal_bucket_lifecycle_noncurrentversion_test;

#[path = "internal/bucket/lifecycle/rule_test.rs"]
mod internal_bucket_lifecycle_rule_test;

#[path = "internal/bucket/lifecycle/transition_test.rs"]
mod internal_bucket_lifecycle_transition_test;

#[path = "internal/bucket/object/lock/lock_test.rs"]
mod internal_bucket_object_lock_lock_test;

#[path = "internal/bucket/replication/datatypes_gen_test.rs"]
mod internal_bucket_replication_datatypes_gen_test;

#[path = "internal/bucket/replication/replication_test.rs"]
mod internal_bucket_replication_replication_test;

#[path = "internal/bucket/replication/rule_test.rs"]
mod internal_bucket_replication_rule_test;

#[path = "internal/bucket/versioning/versioning_test.rs"]
mod internal_bucket_versioning_versioning_test;

#[path = "internal/cachevalue/cache_test.rs"]
mod internal_cachevalue_cache_test;

#[path = "internal/config/bool_flag_test.rs"]
mod internal_config_bool_flag_test;

#[path = "internal/config/certs_test.rs"]
mod internal_config_certs_test;

#[path = "internal/config/compress/compress_test.rs"]
mod internal_config_compress_compress_test;

#[path = "internal/config/config_test.rs"]
mod internal_config_config_test;

#[path = "internal/config/crypto_test.rs"]
mod internal_config_crypto_test;

#[path = "internal/config/dns/etcd_dns_test.rs"]
mod internal_config_dns_etcd_dns_test;

#[path = "internal/config/etcd/etcd_test.rs"]
mod internal_config_etcd_etcd_test;

#[path = "internal/config/identity/openid/jwks_test.rs"]
mod internal_config_identity_openid_jwks_test;

#[path = "internal/config/identity/openid/jwt_test.rs"]
mod internal_config_identity_openid_jwt_test;

#[path = "internal/config/lambda/event/arn_test.rs"]
mod internal_config_lambda_event_arn_test;

#[path = "internal/config/lambda/event/targetid_test.rs"]
mod internal_config_lambda_event_targetid_test;

#[path = "internal/config/lambda/event/targetidset_test.rs"]
mod internal_config_lambda_event_targetidset_test;

#[path = "internal/config/storageclass/storage_class_test.rs"]
mod internal_config_storageclass_storage_class_test;

#[path = "internal/crypto/header_test.rs"]
mod internal_crypto_header_test;

#[path = "internal/crypto/key_test.rs"]
mod internal_crypto_key_test;

#[path = "internal/crypto/metadata_test.rs"]
mod internal_crypto_metadata_test;

#[path = "internal/crypto/sse_test.rs"]
mod internal_crypto_sse_test;

#[path = "internal/deadlineconn/deadlineconn_test.rs"]
mod internal_deadlineconn_deadlineconn_test;

#[path = "internal/disk/disk_test.rs"]
mod internal_disk_disk_test;

#[path = "internal/disk/stat_test.rs"]
mod internal_disk_stat_test;

#[path = "internal/dsync/drwmutex_test.rs"]
mod internal_dsync_drwmutex_test;

#[path = "internal/dsync/dsync_client_test.rs"]
mod internal_dsync_dsync_client_test;

#[path = "internal/dsync/dsync_server_test.rs"]
mod internal_dsync_dsync_server_test;

#[path = "internal/dsync/dsync_test.rs"]
mod internal_dsync_dsync_test;

#[path = "internal/dsync/lock_args_gen_test.rs"]
mod internal_dsync_lock_args_gen_test;

#[path = "internal/etag/etag_test.rs"]
mod internal_etag_etag_test;

#[path = "internal/event/arn_test.rs"]
mod internal_event_arn_test;

#[path = "internal/event/config_test.rs"]
mod internal_event_config_test;

#[path = "internal/event/name_test.rs"]
mod internal_event_name_test;

#[path = "internal/event/rules_test.rs"]
mod internal_event_rules_test;

#[path = "internal/event/rulesmap_test.rs"]
mod internal_event_rulesmap_test;

#[path = "internal/event/target/mysql_test.rs"]
mod internal_event_target_mysql_test;

#[path = "internal/event/target/nats_contrib_test.rs"]
mod internal_event_target_nats_contrib_test;

#[path = "internal/event/target/nats_tls_contrib_test.rs"]
mod internal_event_target_nats_tls_contrib_test;

#[path = "internal/event/target/nsq_test.rs"]
mod internal_event_target_nsq_test;

#[path = "internal/event/target/postgresql_test.rs"]
mod internal_event_target_postgresql_test;

#[path = "internal/event/targetid_test.rs"]
mod internal_event_targetid_test;

#[path = "internal/event/targetidset_test.rs"]
mod internal_event_targetidset_test;

#[path = "internal/event/targetlist_test.rs"]
mod internal_event_targetlist_test;

#[path = "internal/grid/benchmark_test.rs"]
mod internal_grid_benchmark_test;

#[path = "internal/grid/connection_test.rs"]
mod internal_grid_connection_test;

#[path = "internal/grid/grid_test.rs"]
mod internal_grid_grid_test;

#[path = "internal/grid/grid_types_msgp_test.rs"]
mod internal_grid_grid_types_msgp_test;

#[path = "internal/grid/grid_types_test.rs"]
mod internal_grid_grid_types_test;

#[path = "internal/grid/msg_gen_test.rs"]
mod internal_grid_msg_gen_test;

#[path = "internal/grid/types_test.rs"]
mod internal_grid_types_test;

#[path = "internal/handlers/proxy_test.rs"]
mod internal_handlers_proxy_test;

#[path = "internal/hash/checksum_test.rs"]
mod internal_hash_checksum_test;

#[path = "internal/hash/reader_test.rs"]
mod internal_hash_reader_test;

#[path = "internal/http/check_port_test.rs"]
mod internal_http_check_port_test;

#[path = "internal/http/listener_test.rs"]
mod internal_http_listener_test;

#[path = "internal/http/server_test.rs"]
mod internal_http_server_test;

#[path = "internal/ioutil/ioutil_test.rs"]
mod internal_ioutil_ioutil_test;

#[path = "internal/jwt/parser_test.rs"]
mod internal_jwt_parser_test;

#[path = "internal/kms/config_test.rs"]
mod internal_kms_config_test;

#[path = "internal/kms/dek_test.rs"]
mod internal_kms_dek_test;

#[path = "internal/kms/secret_key_test.rs"]
mod internal_kms_secret_key_test;

#[path = "internal/lock/lock_test.rs"]
mod internal_lock_lock_test;

#[path = "internal/lock/lock_windows_test.rs"]
mod internal_lock_lock_windows_test;

#[path = "internal/lsync/lrwmutex_test.rs"]
mod internal_lsync_lrwmutex_test;

#[path = "internal/mountinfo/mountinfo_linux_test.rs"]
mod internal_mountinfo_mountinfo_linux_test;

#[path = "internal/pubsub/pubsub_test.rs"]
mod internal_pubsub_pubsub_test;

#[path = "internal/rest/client_test.rs"]
mod internal_rest_client_test;

#[path = "internal/ringbuffer/ring_buffer_benchmark_test.rs"]
mod internal_ringbuffer_ring_buffer_benchmark_test;

#[path = "internal/ringbuffer/ring_buffer_test.rs"]
mod internal_ringbuffer_ring_buffer_test;

#[path = "internal/s3select/csv/reader_contrib_test.rs"]
mod internal_s3select_csv_reader_contrib_test;

#[path = "internal/s3select/json/preader_test.rs"]
mod internal_s3select_json_preader_test;

#[path = "internal/s3select/json/reader_test.rs"]
mod internal_s3select_json_reader_test;

#[path = "internal/s3select/jstream/decoder_test.rs"]
mod internal_s3select_jstream_decoder_test;

#[path = "internal/s3select/jstream/scanner_test.rs"]
mod internal_s3select_jstream_scanner_test;

#[path = "internal/s3select/select_benchmark_test.rs"]
mod internal_s3select_select_benchmark_test;

#[path = "internal/s3select/select_test.rs"]
mod internal_s3select_select_test;

#[path = "internal/s3select/simdj/reader_amd64_test.rs"]
mod internal_s3select_simdj_reader_amd64_test;

#[path = "internal/s3select/sql/jsonpath_test.rs"]
mod internal_s3select_sql_jsonpath_test;

#[path = "internal/s3select/sql/parser_test.rs"]
mod internal_s3select_sql_parser_test;

#[path = "internal/s3select/sql/stringfuncs_contrib_test.rs"]
mod internal_s3select_sql_stringfuncs_contrib_test;

#[path = "internal/s3select/sql/stringfuncs_test.rs"]
mod internal_s3select_sql_stringfuncs_test;

#[path = "internal/s3select/sql/timestampfuncs_test.rs"]
mod internal_s3select_sql_timestampfuncs_test;

#[path = "internal/s3select/sql/value_test.rs"]
mod internal_s3select_sql_value_test;

#[path = "internal/store/batch_test.rs"]
mod internal_store_batch_test;

#[path = "internal/store/queuestore_test.rs"]
mod internal_store_queuestore_test;

#[path = "internal/store/store_test.rs"]
mod internal_store_store_test;
