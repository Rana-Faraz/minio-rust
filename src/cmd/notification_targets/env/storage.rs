use std::collections::BTreeMap;
use std::env;

use crate::internal::event::TargetId;

use super::super::backends::{
    ElasticsearchArgs, ElasticsearchNotificationTarget, RedisArgs, RedisNotificationTarget,
};
use super::super::core::NotificationTarget;
use super::super::support::is_enabled;

pub fn load_elasticsearch_targets_from_env() -> Result<Vec<ElasticsearchNotificationTarget>, String>
{
    let mut enabled = BTreeMap::<String, bool>::new();
    let mut endpoints = BTreeMap::<String, String>::new();
    let mut indices = BTreeMap::<String, String>::new();
    let mut usernames = BTreeMap::<String, String>::new();
    let mut passwords = BTreeMap::<String, String>::new();
    let mut formats = BTreeMap::<String, String>::new();
    let mut queue_dirs = BTreeMap::<String, String>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_ELASTICSEARCH_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_ELASTICSEARCH_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_ELASTICSEARCH_URL_") {
            endpoints.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_ELASTICSEARCH_URL" {
            endpoints.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_ELASTICSEARCH_INDEX_") {
            indices.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_ELASTICSEARCH_INDEX" {
            indices.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_ELASTICSEARCH_USERNAME_") {
            usernames.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_ELASTICSEARCH_USERNAME" {
            usernames.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_ELASTICSEARCH_PASSWORD_") {
            passwords.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_ELASTICSEARCH_PASSWORD" {
            passwords.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_ELASTICSEARCH_FORMAT_") {
            formats.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_ELASTICSEARCH_FORMAT" {
            formats.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_ELASTICSEARCH_QUEUE_DIR_") {
            queue_dirs.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_ELASTICSEARCH_QUEUE_DIR" {
            queue_dirs.insert("1".to_string(), value);
        }
    }

    let mut targets = Vec::new();
    for (name, is_enabled) in enabled {
        if !is_enabled {
            continue;
        }
        let endpoint = endpoints
            .remove(&name)
            .ok_or_else(|| format!("missing Elasticsearch url for target {name}"))?;
        let index = indices
            .remove(&name)
            .ok_or_else(|| format!("missing Elasticsearch index for target {name}"))?;
        targets.push(ElasticsearchNotificationTarget {
            target_id: TargetId::new(name.clone(), "elasticsearch"),
            args: ElasticsearchArgs {
                enable: true,
                endpoint,
                index,
                username: usernames.remove(&name).unwrap_or_default(),
                password: passwords.remove(&name).unwrap_or_default(),
                format: formats.remove(&name).unwrap_or_default(),
                queue_dir: queue_dirs.remove(&name).unwrap_or_default(),
            },
        });
    }

    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    Ok(targets)
}

pub fn load_redis_targets_from_env() -> Result<Vec<RedisNotificationTarget>, String> {
    let mut enabled = BTreeMap::<String, bool>::new();
    let mut addresses = BTreeMap::<String, String>::new();
    let mut keys = BTreeMap::<String, String>::new();
    let mut users = BTreeMap::<String, String>::new();
    let mut passwords = BTreeMap::<String, String>::new();
    let mut formats = BTreeMap::<String, String>::new();
    let mut queue_dirs = BTreeMap::<String, String>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_REDIS_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_REDIS_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_REDIS_ADDRESS_") {
            addresses.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_REDIS_ADDRESS" {
            addresses.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_REDIS_KEY_") {
            keys.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_REDIS_KEY" {
            keys.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_REDIS_USER_") {
            users.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_REDIS_USER" {
            users.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_REDIS_PASSWORD_") {
            passwords.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_REDIS_PASSWORD" {
            passwords.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_REDIS_FORMAT_") {
            formats.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_REDIS_FORMAT" {
            formats.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_REDIS_QUEUE_DIR_") {
            queue_dirs.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_REDIS_QUEUE_DIR" {
            queue_dirs.insert("1".to_string(), value);
        }
    }

    let mut targets = Vec::new();
    for (name, is_enabled) in enabled {
        if !is_enabled {
            continue;
        }
        let address = addresses
            .remove(&name)
            .ok_or_else(|| format!("missing Redis address for target {name}"))?;
        let key = keys
            .remove(&name)
            .ok_or_else(|| format!("missing Redis key for target {name}"))?;
        targets.push(RedisNotificationTarget {
            target_id: TargetId::new(name.clone(), "redis"),
            args: RedisArgs {
                enable: true,
                address,
                key,
                user: users.remove(&name).unwrap_or_default(),
                password: passwords.remove(&name).unwrap_or_default(),
                format: formats.remove(&name).unwrap_or_default(),
                queue_dir: queue_dirs.remove(&name).unwrap_or_default(),
            },
        });
    }

    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    Ok(targets)
}
