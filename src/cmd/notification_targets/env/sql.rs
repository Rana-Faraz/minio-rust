use std::collections::BTreeMap;
use std::env;

use crate::internal::event::TargetId;

use super::super::backends::{
    MySqlArgs, MySqlNotificationTarget, PostgreSqlArgs, PostgreSqlNotificationTarget,
};
use super::super::core::NotificationTarget;
use super::super::support::{is_enabled, validate_mysql_args, validate_postgresql_args};

pub fn load_mysql_targets_from_env() -> Result<Vec<MySqlNotificationTarget>, String> {
    let mut enabled = BTreeMap::<String, bool>::new();
    let mut formats = BTreeMap::<String, String>::new();
    let mut dsn_strings = BTreeMap::<String, String>::new();
    let mut tables = BTreeMap::<String, String>::new();
    let mut hosts = BTreeMap::<String, String>::new();
    let mut ports = BTreeMap::<String, String>::new();
    let mut usernames = BTreeMap::<String, String>::new();
    let mut passwords = BTreeMap::<String, String>::new();
    let mut databases = BTreeMap::<String, String>::new();
    let mut queue_dirs = BTreeMap::<String, String>::new();
    let mut max_open_connections = BTreeMap::<String, usize>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_MYSQL_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_FORMAT_") {
            formats.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MYSQL_FORMAT" {
            formats.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_DSN_STRING_") {
            dsn_strings.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MYSQL_DSN_STRING" {
            dsn_strings.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_TABLE_") {
            tables.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MYSQL_TABLE" {
            tables.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_HOST_") {
            hosts.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MYSQL_HOST" {
            hosts.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_PORT_") {
            ports.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MYSQL_PORT" {
            ports.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_USERNAME_") {
            usernames.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MYSQL_USERNAME" {
            usernames.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_PASSWORD_") {
            passwords.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MYSQL_PASSWORD" {
            passwords.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_DATABASE_") {
            databases.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MYSQL_DATABASE" {
            databases.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_QUEUE_DIR_") {
            queue_dirs.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_MYSQL_QUEUE_DIR" {
            queue_dirs.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_MYSQL_MAX_OPEN_CONNECTIONS_") {
            max_open_connections.insert(
                name.to_string(),
                value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid MySQL max open connections for target {name}"))?,
            );
        } else if key == "MINIO_NOTIFY_MYSQL_MAX_OPEN_CONNECTIONS" {
            max_open_connections.insert(
                "1".to_string(),
                value
                    .parse::<usize>()
                    .map_err(|_| "invalid MySQL max open connections for target 1".to_string())?,
            );
        }
    }

    let mut targets = Vec::new();
    for (name, is_enabled) in enabled {
        if !is_enabled {
            continue;
        }
        let table = tables
            .remove(&name)
            .ok_or_else(|| format!("missing MySQL table for target {name}"))?;
        let args = MySqlArgs {
            enable: true,
            format: formats
                .remove(&name)
                .unwrap_or_else(|| "namespace".to_string()),
            dsn_string: dsn_strings.remove(&name).unwrap_or_default(),
            table,
            host: hosts.remove(&name).unwrap_or_default(),
            port: ports.remove(&name).unwrap_or_default(),
            username: usernames.remove(&name).unwrap_or_default(),
            password: passwords.remove(&name).unwrap_or_default(),
            database: databases.remove(&name).unwrap_or_default(),
            queue_dir: queue_dirs.remove(&name).unwrap_or_default(),
            max_open_connections: max_open_connections.remove(&name).unwrap_or(2),
        };
        validate_mysql_args(&args)?;
        targets.push(MySqlNotificationTarget {
            target_id: TargetId::new(name.clone(), "mysql"),
            args,
        });
    }

    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    Ok(targets)
}

pub fn load_postgresql_targets_from_env() -> Result<Vec<PostgreSqlNotificationTarget>, String> {
    let mut enabled = BTreeMap::<String, bool>::new();
    let mut formats = BTreeMap::<String, String>::new();
    let mut connection_strings = BTreeMap::<String, String>::new();
    let mut tables = BTreeMap::<String, String>::new();
    let mut hosts = BTreeMap::<String, String>::new();
    let mut ports = BTreeMap::<String, String>::new();
    let mut usernames = BTreeMap::<String, String>::new();
    let mut passwords = BTreeMap::<String, String>::new();
    let mut databases = BTreeMap::<String, String>::new();
    let mut queue_dirs = BTreeMap::<String, String>::new();
    let mut max_open_connections = BTreeMap::<String, usize>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_ENABLE_") {
            enabled.insert(name.to_string(), is_enabled(&value));
        } else if key == "MINIO_NOTIFY_POSTGRES_ENABLE" {
            enabled.insert("1".to_string(), is_enabled(&value));
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_FORMAT_") {
            formats.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_POSTGRES_FORMAT" {
            formats.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_CONNECTION_STRING_") {
            connection_strings.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_POSTGRES_CONNECTION_STRING" {
            connection_strings.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_TABLE_") {
            tables.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_POSTGRES_TABLE" {
            tables.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_HOST_") {
            hosts.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_POSTGRES_HOST" {
            hosts.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_PORT_") {
            ports.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_POSTGRES_PORT" {
            ports.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_USERNAME_") {
            usernames.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_POSTGRES_USERNAME" {
            usernames.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_PASSWORD_") {
            passwords.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_POSTGRES_PASSWORD" {
            passwords.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_DATABASE_") {
            databases.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_POSTGRES_DATABASE" {
            databases.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_QUEUE_DIR_") {
            queue_dirs.insert(name.to_string(), value);
        } else if key == "MINIO_NOTIFY_POSTGRES_QUEUE_DIR" {
            queue_dirs.insert("1".to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_NOTIFY_POSTGRES_MAX_OPEN_CONNECTIONS_") {
            max_open_connections.insert(
                name.to_string(),
                value.parse::<usize>().map_err(|_| {
                    format!("invalid PostgreSQL max open connections for target {name}")
                })?,
            );
        } else if key == "MINIO_NOTIFY_POSTGRES_MAX_OPEN_CONNECTIONS" {
            max_open_connections.insert(
                "1".to_string(),
                value.parse::<usize>().map_err(|_| {
                    "invalid PostgreSQL max open connections for target 1".to_string()
                })?,
            );
        }
    }

    let mut targets = Vec::new();
    for (name, is_enabled) in enabled {
        if !is_enabled {
            continue;
        }
        let table = tables
            .remove(&name)
            .ok_or_else(|| format!("missing PostgreSQL table for target {name}"))?;
        let args = PostgreSqlArgs {
            enable: true,
            format: formats
                .remove(&name)
                .unwrap_or_else(|| "namespace".to_string()),
            connection_string: connection_strings.remove(&name).unwrap_or_default(),
            table,
            host: hosts.remove(&name).unwrap_or_default(),
            port: ports.remove(&name).unwrap_or_default(),
            username: usernames.remove(&name).unwrap_or_default(),
            password: passwords.remove(&name).unwrap_or_default(),
            database: databases.remove(&name).unwrap_or_default(),
            queue_dir: queue_dirs.remove(&name).unwrap_or_default(),
            max_open_connections: max_open_connections.remove(&name).unwrap_or(2),
        };
        validate_postgresql_args(&args)?;
        targets.push(PostgreSqlNotificationTarget {
            target_id: TargetId::new(name.clone(), "postgresql"),
            args,
        });
    }

    targets.sort_by(|left, right| left.id().to_string().cmp(&right.id().to_string()));
    Ok(targets)
}
