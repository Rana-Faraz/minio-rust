use std::collections::BTreeMap;
use std::env;

use crate::internal::event::TargetId;

use super::super::backends::{
    AmqpArgs, AmqpNotificationTarget, KafkaArgs, KafkaNotificationTarget, KafkaProducerArgs,
    KafkaSaslArgs, KafkaTlsArgs, MqttArgs, MqttNotificationTarget,
};
use super::super::core::NotificationTarget;
use super::super::support::{
    is_enabled, parse_duration_millis, parse_duration_secs, validate_amqp_args,
    validate_kafka_args, validate_mqtt_args,
};

pub fn load_amqp_targets_from_env() -> Result<Vec<AmqpNotificationTarget>, String> {
    let mut enabled = BTreeMap::<String, bool>::new();
    let mut urls = BTreeMap::<String, String>::new();
    let mut exchanges = BTreeMap::<String, String>::new();
    let mut routing_keys = BTreeMap::<String, String>::new();
    let mut exchange_types = BTreeMap::<String, String>::new();
    let mut delivery_modes = BTreeMap::<String, u8>::new();
    let mut mandatory = BTreeMap::<String, bool>::new();
    let mut immediate = BTreeMap::<String, bool>::new();
    let mut durable = BTreeMap::<String, bool>::new();
    let mut internal = BTreeMap::<String, bool>::new();
    let mut no_wait = BTreeMap::<String, bool>::new();
    let mut auto_deleted = BTreeMap::<String, bool>::new();
    let mut publisher_confirms = BTreeMap::<String, bool>::new();
    let mut queue_dirs = BTreeMap::<String, String>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_AMQP_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_URL_") {
            urls.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_AMQP_URL" {
            urls.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_EXCHANGE_") {
            exchanges.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_AMQP_EXCHANGE" {
            exchanges.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_ROUTING_KEY_") {
            routing_keys.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_AMQP_ROUTING_KEY" {
            routing_keys.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_EXCHANGE_TYPE_") {
            exchange_types.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_AMQP_EXCHANGE_TYPE" {
            exchange_types.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_DELIVERY_MODE_") {
            delivery_modes.insert(
                name.to_string(),
                value
                    .parse::<u8>()
                    .map_err(|_| format!("invalid AMQP delivery mode for target {name}"))?,
            );
        } else if key == "MINIO_NOTIFY_AMQP_DELIVERY_MODE" {
            delivery_modes.insert(
                "1".to_string(),
                value
                    .parse::<u8>()
                    .map_err(|_| "invalid AMQP delivery mode for target 1".to_string())?,
            );
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_MANDATORY_") {
            mandatory.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_AMQP_MANDATORY" {
            mandatory.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_IMMEDIATE_") {
            immediate.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_AMQP_IMMEDIATE" {
            immediate.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_DURABLE_") {
            durable.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_AMQP_DURABLE" {
            durable.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_INTERNAL_") {
            internal.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_AMQP_INTERNAL" {
            internal.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_NO_WAIT_") {
            no_wait.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_AMQP_NO_WAIT" {
            no_wait.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_AUTO_DELETED_") {
            auto_deleted.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_AMQP_AUTO_DELETED" {
            auto_deleted.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_PUBLISHING_CONFIRMS_") {
            publisher_confirms.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_AMQP_PUBLISHING_CONFIRMS" {
            publisher_confirms.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_AMQP_QUEUE_DIR_") {
            queue_dirs.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_AMQP_QUEUE_DIR" {
            queue_dirs.insert("1".to_string(), value);
        }
    }

    let mut targets = Vec::new();
    for (name, is_enabled) in enabled {
        if !is_enabled {
            continue;
        }
        let url = urls
            .remove(&name)
            .ok_or_else(|| format!("missing AMQP url for target {name}"))?;
        let args = AmqpArgs {
            enable: true,
            url,
            exchange: exchanges.remove(&name).unwrap_or_default(),
            routing_key: routing_keys.remove(&name).unwrap_or_default(),
            exchange_type: exchange_types.remove(&name).unwrap_or_default(),
            delivery_mode: delivery_modes.remove(&name).unwrap_or(0),
            mandatory: mandatory.remove(&name).unwrap_or(false),
            immediate: immediate.remove(&name).unwrap_or(false),
            durable: durable.remove(&name).unwrap_or(false),
            internal: internal.remove(&name).unwrap_or(false),
            no_wait: no_wait.remove(&name).unwrap_or(false),
            auto_deleted: auto_deleted.remove(&name).unwrap_or(false),
            publisher_confirms: publisher_confirms.remove(&name).unwrap_or(false),
            queue_dir: queue_dirs.remove(&name).unwrap_or_default(),
        };
        validate_amqp_args(&args)?;
        targets.push(AmqpNotificationTarget {
            target_id: TargetId::new(name.clone(), "amqp"),
            args,
        });
    }
    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    Ok(targets)
}

pub fn load_mqtt_targets_from_env() -> Result<Vec<MqttNotificationTarget>, String> {
    let mut enabled = BTreeMap::<String, bool>::new();
    let mut brokers = BTreeMap::<String, String>::new();
    let mut topics = BTreeMap::<String, String>::new();
    let mut qos_values = BTreeMap::<String, u8>::new();
    let mut usernames = BTreeMap::<String, String>::new();
    let mut passwords = BTreeMap::<String, String>::new();
    let mut reconnect_values = BTreeMap::<String, u64>::new();
    let mut keep_alive_values = BTreeMap::<String, u64>::new();
    let mut queue_dirs = BTreeMap::<String, String>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MQTT_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_MQTT_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MQTT_BROKER_") {
            brokers.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MQTT_BROKER" {
            brokers.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MQTT_TOPIC_") {
            topics.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MQTT_TOPIC" {
            topics.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MQTT_QOS_") {
            qos_values.insert(
                name.to_string(),
                value
                    .parse::<u8>()
                    .map_err(|_| format!("invalid MQTT qos for target {name}"))?,
            );
        } else if key == "MINIO_NOTIFY_MQTT_QOS" {
            qos_values.insert(
                "1".to_string(),
                value
                    .parse::<u8>()
                    .map_err(|_| "invalid MQTT qos for target 1".to_string())?,
            );
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MQTT_USERNAME_") {
            usernames.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MQTT_USERNAME" {
            usernames.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MQTT_PASSWORD_") {
            passwords.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MQTT_PASSWORD" {
            passwords.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MQTT_RECONNECT_INTERVAL_") {
            reconnect_values.insert(
                name.to_string(),
                parse_duration_secs(&value).map_err(|err| {
                    format!("invalid MQTT reconnect interval for target {name}: {err}")
                })?,
            );
        } else if key == "MINIO_NOTIFY_MQTT_RECONNECT_INTERVAL" {
            reconnect_values.insert(
                "1".to_string(),
                parse_duration_secs(&value).map_err(|err| {
                    format!("invalid MQTT reconnect interval for target 1: {err}")
                })?,
            );
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MQTT_KEEP_ALIVE_INTERVAL_") {
            keep_alive_values.insert(
                name.to_string(),
                parse_duration_secs(&value)
                    .map_err(|err| format!("invalid MQTT keep alive for target {name}: {err}"))?,
            );
        } else if key == "MINIO_NOTIFY_MQTT_KEEP_ALIVE_INTERVAL" {
            keep_alive_values.insert(
                "1".to_string(),
                parse_duration_secs(&value)
                    .map_err(|err| format!("invalid MQTT keep alive for target 1: {err}"))?,
            );
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MQTT_QUEUE_DIR_") {
            queue_dirs.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MQTT_QUEUE_DIR" {
            queue_dirs.insert("1".to_string(), value);
        }
    }

    let mut targets = Vec::new();
    for (name, is_enabled) in enabled {
        if !is_enabled {
            continue;
        }
        let broker = brokers
            .remove(&name)
            .ok_or_else(|| format!("missing MQTT broker for target {name}"))?;
        let topic = topics
            .remove(&name)
            .ok_or_else(|| format!("missing MQTT topic for target {name}"))?;
        let args = MqttArgs {
            enable: true,
            broker,
            topic,
            qos: qos_values.remove(&name).unwrap_or(0),
            username: usernames.remove(&name).unwrap_or_default(),
            password: passwords.remove(&name).unwrap_or_default(),
            reconnect_interval_secs: reconnect_values.remove(&name).unwrap_or(0),
            keep_alive_secs: keep_alive_values.remove(&name).unwrap_or(0),
            queue_dir: queue_dirs.remove(&name).unwrap_or_default(),
        };
        validate_mqtt_args(&args)?;
        targets.push(MqttNotificationTarget {
            target_id: TargetId::new(name.clone(), "mqtt"),
            args,
        });
    }
    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    Ok(targets)
}

pub fn load_kafka_targets_from_env() -> Result<Vec<KafkaNotificationTarget>, String> {
    let mut enabled = BTreeMap::<String, bool>::new();
    let mut brokers = BTreeMap::<String, String>::new();
    let mut topics = BTreeMap::<String, String>::new();
    let mut queue_dirs = BTreeMap::<String, String>::new();
    let mut versions = BTreeMap::<String, String>::new();
    let mut batch_sizes = BTreeMap::<String, u32>::new();
    let mut batch_timeouts = BTreeMap::<String, u64>::new();
    let mut tls_enable = BTreeMap::<String, bool>::new();
    let mut tls_skip_verify = BTreeMap::<String, bool>::new();
    let mut tls_client_cert = BTreeMap::<String, String>::new();
    let mut tls_client_key = BTreeMap::<String, String>::new();
    let mut sasl_enable = BTreeMap::<String, bool>::new();
    let mut sasl_usernames = BTreeMap::<String, String>::new();
    let mut sasl_passwords = BTreeMap::<String, String>::new();
    let mut sasl_mechanisms = BTreeMap::<String, String>::new();
    let mut compression = BTreeMap::<String, String>::new();
    let mut compression_levels = BTreeMap::<String, i32>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_KAFKA_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_BROKERS_") {
            brokers.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_BROKERS" {
            brokers.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_TOPIC_") {
            topics.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_TOPIC" {
            topics.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_QUEUE_DIR_") {
            queue_dirs.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_QUEUE_DIR" {
            queue_dirs.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_VERSION_") {
            versions.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_VERSION" {
            versions.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_BATCH_SIZE_") {
            batch_sizes.insert(
                name.to_string(),
                value
                    .parse::<u32>()
                    .map_err(|_| format!("invalid Kafka batch size for target {name}"))?,
            );
        } else if key == "MINIO_NOTIFY_KAFKA_BATCH_SIZE" {
            batch_sizes.insert(
                "1".to_string(),
                value
                    .parse::<u32>()
                    .map_err(|_| "invalid Kafka batch size for target 1".to_string())?,
            );
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_BATCH_COMMIT_TIMEOUT_") {
            batch_timeouts.insert(
                name.to_string(),
                parse_duration_millis(&value).map_err(|err| {
                    format!("invalid Kafka batch timeout for target {name}: {err}")
                })?,
            );
        } else if key == "MINIO_NOTIFY_KAFKA_BATCH_COMMIT_TIMEOUT" {
            batch_timeouts.insert(
                "1".to_string(),
                parse_duration_millis(&value)
                    .map_err(|err| format!("invalid Kafka batch timeout for target 1: {err}"))?,
            );
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_TLS_SKIP_VERIFY_") {
            tls_skip_verify.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_KAFKA_TLS_SKIP_VERIFY" {
            tls_skip_verify.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_TLS_") {
            tls_enable.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_KAFKA_TLS" {
            tls_enable.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_CLIENT_TLS_CERT_") {
            tls_client_cert.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_CLIENT_TLS_CERT" {
            tls_client_cert.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_CLIENT_TLS_KEY_") {
            tls_client_key.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_CLIENT_TLS_KEY" {
            tls_client_key.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_SASL_USERNAME_") {
            sasl_usernames.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_SASL_USERNAME" {
            sasl_usernames.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_SASL_PASSWORD_") {
            sasl_passwords.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_SASL_PASSWORD" {
            sasl_passwords.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_SASL_MECHANISM_") {
            sasl_mechanisms.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_SASL_MECHANISM" {
            sasl_mechanisms.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_KAFKA_SASL_") {
            sasl_enable.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_KAFKA_SASL" {
            sasl_enable.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) =
            key.strip_prefix("MINIO_NOTIFY_KAFKA_PRODUCER_COMPRESSION_CODEC_")
        {
            compression.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_KAFKA_PRODUCER_COMPRESSION_CODEC" {
            compression.insert("1".to_string(), value);
        } else if let Some(name) =
            key.strip_prefix("MINIO_NOTIFY_KAFKA_PRODUCER_COMPRESSION_LEVEL_")
        {
            compression_levels.insert(
                name.to_string(),
                value
                    .parse::<i32>()
                    .map_err(|_| format!("invalid Kafka compression level for target {name}"))?,
            );
        } else if key == "MINIO_NOTIFY_KAFKA_PRODUCER_COMPRESSION_LEVEL" {
            compression_levels.insert(
                "1".to_string(),
                value
                    .parse::<i32>()
                    .map_err(|_| "invalid Kafka compression level for target 1".to_string())?,
            );
        }
    }

    let mut targets = Vec::new();
    for (name, is_enabled) in enabled {
        if !is_enabled {
            continue;
        }
        let broker_value = brokers
            .remove(&name)
            .ok_or_else(|| format!("missing Kafka brokers for target {name}"))?;
        let topic = topics
            .remove(&name)
            .ok_or_else(|| format!("missing Kafka topic for target {name}"))?;
        let args = KafkaArgs {
            enable: true,
            brokers: broker_value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect(),
            topic,
            queue_dir: queue_dirs.remove(&name).unwrap_or_default(),
            version: versions.remove(&name).unwrap_or_default(),
            batch_size: batch_sizes.remove(&name).unwrap_or(0),
            batch_commit_timeout_ms: batch_timeouts.remove(&name).unwrap_or(0),
            tls: KafkaTlsArgs {
                enable: tls_enable.remove(&name).unwrap_or(false),
                skip_verify: tls_skip_verify.remove(&name).unwrap_or(false),
                client_tls_cert: tls_client_cert.remove(&name).unwrap_or_default(),
                client_tls_key: tls_client_key.remove(&name).unwrap_or_default(),
            },
            sasl: KafkaSaslArgs {
                enable: sasl_enable.remove(&name).unwrap_or(false),
                username: sasl_usernames.remove(&name).unwrap_or_default(),
                password: sasl_passwords.remove(&name).unwrap_or_default(),
                mechanism: sasl_mechanisms.remove(&name).unwrap_or_default(),
            },
            producer: KafkaProducerArgs {
                compression: compression.remove(&name).unwrap_or_default(),
                compression_level: compression_levels.remove(&name).unwrap_or_default(),
            },
        };
        validate_kafka_args(&args)?;
        targets.push(KafkaNotificationTarget {
            target_id: TargetId::new(name.clone(), "kafka"),
            args,
        });
    }
    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    Ok(targets)
}
