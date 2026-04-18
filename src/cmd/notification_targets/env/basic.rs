use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;

use crate::internal::event::target::{Host, NatsArgs, NsqArgs, NsqTlsArgs};
use crate::internal::event::TargetId;

use super::super::backends::{
    InMemoryQueueTarget, NatsNotificationTarget, NsqNotificationTarget, WebhookNotificationTarget,
};
use super::super::core::NotificationTarget;
use super::super::support::{is_enabled, parse_host};

pub fn load_webhook_targets_from_env() -> Vec<WebhookNotificationTarget> {
    let mut endpoints = BTreeMap::<String, String>::new();
    let mut auth_tokens = BTreeMap::<String, String>::new();
    let mut enabled = BTreeMap::<String, bool>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_WEBHOOK_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_WEBHOOK_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_WEBHOOK_ENDPOINT_") {
            endpoints.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_WEBHOOK_ENDPOINT" {
            endpoints.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN_") {
            auth_tokens.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN" {
            auth_tokens.insert("1".to_string(), value);
        }
    }

    let mut targets = Vec::new();
    for (name, endpoint) in endpoints {
        if !enabled.get(&name).copied().unwrap_or(false) || endpoint.is_empty() {
            continue;
        }
        targets.push(WebhookNotificationTarget {
            target_id: TargetId::new(name.clone(), "webhook"),
            endpoint,
            auth_token: auth_tokens.remove(&name).unwrap_or_default(),
        });
    }
    targets.sort_by(|left, right| left.target_id.to_string().cmp(&right.target_id.to_string()));
    targets
}

pub fn load_queue_targets_from_env() -> Vec<Arc<InMemoryQueueTarget>> {
    let mut enabled = BTreeMap::<String, bool>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_QUEUE_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_QUEUE_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        }
    }

    let mut targets = Vec::new();
    for (name, is_enabled) in enabled {
        if !is_enabled {
            continue;
        }
        targets.push(Arc::new(InMemoryQueueTarget::new(TargetId::new(
            name.clone(),
            "queue",
        ))));
    }
    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    targets
}

pub fn load_nats_targets_from_env() -> Result<Vec<NatsNotificationTarget>, String> {
    let mut enabled = BTreeMap::<String, bool>::new();
    let mut addresses = BTreeMap::<String, String>::new();
    let mut subjects = BTreeMap::<String, String>::new();
    let mut usernames = BTreeMap::<String, String>::new();
    let mut passwords = BTreeMap::<String, String>::new();
    let mut tokens = BTreeMap::<String, String>::new();
    let mut nkey_seeds = BTreeMap::<String, String>::new();
    let mut secure = BTreeMap::<String, bool>::new();
    let mut tls_skip_verify = BTreeMap::<String, bool>::new();
    let mut cert_authorities = BTreeMap::<String, String>::new();
    let mut client_certs = BTreeMap::<String, String>::new();
    let mut client_keys = BTreeMap::<String, String>::new();
    let mut handshake_first = BTreeMap::<String, bool>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_NATS_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_ADDRESS_") {
            addresses.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NATS_ADDRESS" {
            addresses.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_SUBJECT_") {
            subjects.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NATS_SUBJECT" {
            subjects.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_USERNAME_") {
            usernames.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NATS_USERNAME" {
            usernames.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_PASSWORD_") {
            passwords.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NATS_PASSWORD" {
            passwords.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_TOKEN_") {
            tokens.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NATS_TOKEN" {
            tokens.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_NKEY_SEED_") {
            nkey_seeds.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NATS_NKEY_SEED" {
            nkey_seeds.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_SECURE_") {
            secure.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_NATS_SECURE" {
            secure.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY_") {
            tls_skip_verify.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY" {
            tls_skip_verify.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_CERT_AUTHORITY_") {
            cert_authorities.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NATS_CERT_AUTHORITY" {
            cert_authorities.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_CLIENT_CERT_") {
            client_certs.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NATS_CLIENT_CERT" {
            client_certs.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_CLIENT_KEY_") {
            client_keys.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NATS_CLIENT_KEY" {
            client_keys.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NATS_TLS_HANDSHAKE_FIRST_") {
            handshake_first.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_NATS_TLS_HANDSHAKE_FIRST" {
            handshake_first.insert("1".to_string(), is_enabled(&value));
        }
    }

    let mut targets = Vec::new();
    for (name, is_enabled) in enabled {
        if !is_enabled {
            continue;
        }
        let address = addresses
            .remove(&name)
            .ok_or_else(|| format!("missing NATS address for target {name}"))?;
        let subject = subjects
            .remove(&name)
            .ok_or_else(|| format!("missing NATS subject for target {name}"))?;
        let host: Host = parse_host(&address)?;
        targets.push(NatsNotificationTarget {
            target_id: TargetId::new(name.clone(), "nats"),
            args: NatsArgs {
                enable: true,
                address: host,
                subject,
                username: usernames.remove(&name).unwrap_or_default(),
                password: passwords.remove(&name).unwrap_or_default(),
                token: tokens.remove(&name).unwrap_or_default(),
                nkey_seed: nkey_seeds.remove(&name).unwrap_or_default(),
                secure: secure.remove(&name).unwrap_or(false),
                tls_skip_verify: tls_skip_verify.remove(&name).unwrap_or(false),
                cert_authority: cert_authorities.remove(&name).unwrap_or_default(),
                client_cert: client_certs.remove(&name).unwrap_or_default(),
                client_key: client_keys.remove(&name).unwrap_or_default(),
                tls_handshake_first: handshake_first.remove(&name).unwrap_or(false),
            },
        });
    }

    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    Ok(targets)
}

pub fn load_nsq_targets_from_env() -> Result<Vec<NsqNotificationTarget>, String> {
    let mut enabled = BTreeMap::<String, bool>::new();
    let mut addresses = BTreeMap::<String, String>::new();
    let mut topics = BTreeMap::<String, String>::new();
    let mut tls = BTreeMap::<String, bool>::new();
    let mut tls_skip_verify = BTreeMap::<String, bool>::new();
    let mut queue_dirs = BTreeMap::<String, String>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NSQ_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_NSQ_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NSQ_NSQD_ADDRESS_") {
            addresses.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NSQ_NSQD_ADDRESS" {
            addresses.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NSQ_TOPIC_") {
            topics.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NSQ_TOPIC" {
            topics.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NSQ_TLS_ENABLE_") {
            tls.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_NSQ_TLS_ENABLE" {
            tls.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NSQ_TLS_SKIP_VERIFY_") {
            tls_skip_verify.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_NSQ_TLS_SKIP_VERIFY" {
            tls_skip_verify.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_NSQ_QUEUE_DIR_") {
            queue_dirs.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_NSQ_QUEUE_DIR" {
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
            .ok_or_else(|| format!("missing NSQ address for target {name}"))?;
        let topic = topics
            .remove(&name)
            .ok_or_else(|| format!("missing NSQ topic for target {name}"))?;
        let host = parse_host(&address)?;
        targets.push(NsqNotificationTarget {
            target_id: TargetId::new(name.clone(), "nsq"),
            args: NsqArgs {
                enable: true,
                nsqd_address: host,
                topic,
                tls: NsqTlsArgs {
                    enable: tls.remove(&name).unwrap_or(false),
                    skip_verify: tls_skip_verify.remove(&name).unwrap_or(false),
                },
                queue_dir: queue_dirs.remove(&name).unwrap_or_default(),
            },
        });
    }

    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    Ok(targets)
}
