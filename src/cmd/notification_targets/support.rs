use std::io::{Read, Write};
use std::time::Duration;

use amiquip::{Channel, Result as AmqpResult};
use kafka::producer::Compression as KafkaCompression;
use postgres::NoTls;
use rumqttc::QoS;
use url::Url;

use crate::internal::event::target::{
    validate_psql_table_name, Host, NatsArgs, NatsConnection, NsqConnection,
};

use super::backends::{AmqpArgs, KafkaArgs, MqttArgs, MySqlArgs, PostgreSqlArgs};
use super::core::NotificationEvent;

pub(super) fn is_enabled(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "on" | "true" | "yes" | "enabled"
    )
}

pub(super) fn parse_host(value: &str) -> Result<Host, String> {
    let (name, port) = value
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid host:port value {value}"))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| format!("invalid port in {value}"))?;
    Ok(Host::new(name, port))
}

pub(super) fn build_nats_connect_frame(args: &NatsArgs) -> Result<String, String> {
    let mut object = serde_json::Map::new();
    object.insert("verbose".to_string(), serde_json::Value::Bool(false));
    object.insert("pedantic".to_string(), serde_json::Value::Bool(false));
    object.insert(
        "tls_required".to_string(),
        serde_json::Value::Bool(args.secure),
    );
    object.insert(
        "name".to_string(),
        serde_json::Value::String("minio-rust".to_string()),
    );
    object.insert(
        "lang".to_string(),
        serde_json::Value::String("rust".to_string()),
    );
    object.insert(
        "version".to_string(),
        serde_json::Value::String(env!("CARGO_PKG_VERSION").to_string()),
    );
    if !args.username.is_empty() {
        object.insert(
            "user".to_string(),
            serde_json::Value::String(args.username.clone()),
        );
    }
    if !args.password.is_empty() {
        object.insert(
            "pass".to_string(),
            serde_json::Value::String(args.password.clone()),
        );
    }
    if !args.token.is_empty() {
        object.insert(
            "auth_token".to_string(),
            serde_json::Value::String(args.token.clone()),
        );
    }
    let json = serde_json::to_string(&object).map_err(|err| err.to_string())?;
    Ok(format!("CONNECT {json}\r\n"))
}

pub(super) fn write_nats_frame(
    connection: &mut NatsConnection,
    bytes: &[u8],
) -> std::io::Result<()> {
    match connection {
        NatsConnection::Plain(stream) => {
            stream.write_all(bytes)?;
            stream.flush()
        }
        NatsConnection::Tls(stream) => {
            stream.write_all(bytes)?;
            stream.flush()
        }
    }
}

pub(super) fn prepare_nsq_connection(connection: &mut NsqConnection) -> std::io::Result<()> {
    let timeout = Some(Duration::from_millis(250));
    match connection {
        NsqConnection::Plain(stream) => {
            stream.set_read_timeout(timeout)?;
            stream.set_write_timeout(timeout)
        }
        NsqConnection::Tls(stream) => {
            stream.sock.set_read_timeout(timeout)?;
            stream.sock.set_write_timeout(timeout)
        }
    }
}

pub(super) fn write_nsq_frame(connection: &mut NsqConnection, bytes: &[u8]) -> std::io::Result<()> {
    match connection {
        NsqConnection::Plain(stream) => {
            stream.write_all(bytes)?;
            stream.flush()
        }
        NsqConnection::Tls(stream) => {
            stream.write_all(bytes)?;
            stream.flush()
        }
    }
}

pub(super) fn prepare_nats_connection(connection: &mut NatsConnection) -> std::io::Result<()> {
    let timeout = Some(Duration::from_millis(250));
    match connection {
        NatsConnection::Plain(stream) => {
            stream.set_read_timeout(timeout)?;
            stream.set_write_timeout(timeout)
        }
        NatsConnection::Tls(stream) => {
            stream.sock.set_read_timeout(timeout)?;
            stream.sock.set_write_timeout(timeout)
        }
    }
}

pub(super) fn redis_command(parts: &[&str]) -> String {
    let mut out = format!("*{}\r\n", parts.len());
    for part in parts {
        out.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    out
}

pub(super) fn read_redis_line(stream: &mut std::net::TcpStream) -> std::io::Result<Option<String>> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => {
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if line.is_empty() {
                    return Ok(None);
                }
                break;
            }
            Err(error) => return Err(error),
        }
    }
    if line.is_empty() {
        return Ok(None);
    }
    Ok(Some(String::from_utf8_lossy(&line).to_string()))
}

pub(super) fn read_nats_line(connection: &mut NatsConnection) -> std::io::Result<Option<String>> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let read = match connection {
            NatsConnection::Plain(stream) => stream.read(&mut byte),
            NatsConnection::Tls(stream) => stream.read(&mut byte),
        };
        match read {
            Ok(0) => break,
            Ok(_) => {
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if line.is_empty() {
                    return Ok(None);
                }
                break;
            }
            Err(error) => return Err(error),
        }
    }

    if line.is_empty() {
        return Ok(None);
    }
    Ok(Some(String::from_utf8_lossy(&line).to_string()))
}

pub(super) fn validate_mysql_args(args: &MySqlArgs) -> Result<(), String> {
    if !args.enable {
        return Ok(());
    }
    if args.table.trim().is_empty() {
        return Err("MySQL table unspecified".to_string());
    }
    validate_mysql_table_name(&args.table)?;
    if !args.dsn_string.is_empty() {
        mysql::Opts::from_url(&args.dsn_string)
            .map_err(|err| format!("invalid MySQL DSN: {err}"))?;
    } else {
        if args.host.trim().is_empty() {
            return Err("MySQL host unspecified".to_string());
        }
        if args.database.trim().is_empty() {
            return Err("MySQL database unspecified".to_string());
        }
        mysql_port(args)
            .parse::<u16>()
            .map_err(|_| "invalid MySQL port".to_string())?;
    }
    if !args.queue_dir.is_empty() && !args.queue_dir.starts_with('/') {
        return Err("MySQL queueDir path should be absolute".to_string());
    }
    Ok(())
}

pub(super) fn validate_postgresql_args(args: &PostgreSqlArgs) -> Result<(), String> {
    if !args.enable {
        return Ok(());
    }
    if args.table.trim().is_empty() {
        return Err("PostgreSQL table unspecified".to_string());
    }
    validate_psql_table_name(&args.table).map_err(|err| err.to_string())?;
    if args.connection_string.is_empty() {
        if args.host.trim().is_empty() {
            return Err("PostgreSQL host unspecified".to_string());
        }
        if args.database.trim().is_empty() {
            return Err("PostgreSQL database unspecified".to_string());
        }
        postgresql_port(args)
            .parse::<u16>()
            .map_err(|_| "invalid PostgreSQL port".to_string())?;
    }
    if !args.queue_dir.is_empty() && !args.queue_dir.starts_with('/') {
        return Err("PostgreSQL queueDir path should be absolute".to_string());
    }
    Ok(())
}

pub(super) fn validate_amqp_args(args: &AmqpArgs) -> Result<(), String> {
    if !args.enable {
        return Ok(());
    }
    Url::parse(&args.url).map_err(|err| format!("invalid AMQP url: {err}"))?;
    if !args.queue_dir.is_empty() && !args.queue_dir.starts_with('/') {
        return Err("AMQP queueDir path should be absolute".to_string());
    }
    Ok(())
}

pub(super) fn validate_mqtt_args(args: &MqttArgs) -> Result<(), String> {
    if !args.enable {
        return Ok(());
    }
    let broker = Url::parse(&args.broker).map_err(|err| format!("invalid MQTT broker: {err}"))?;
    match broker.scheme() {
        "ws" | "wss" | "tcp" | "ssl" | "tls" | "tcps" | "mqtt" => {}
        _ => return Err("unknown protocol in broker address".to_string()),
    }
    if args.topic.trim().is_empty() {
        return Err("MQTT topic unspecified".to_string());
    }
    if args.queue_dir != "" && !args.queue_dir.starts_with('/') {
        return Err("MQTT queueDir path should be absolute".to_string());
    }
    if args.queue_dir != "" && args.qos == 0 {
        return Err("MQTT qos should be 1 or 2 if queueDir is set".to_string());
    }
    if args.qos > 2 {
        return Err("MQTT qos must be 0, 1, or 2".to_string());
    }
    Ok(())
}

fn validate_mysql_table_name(name: &str) -> Result<(), String> {
    if name
        .chars()
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '$')
    {
        Ok(())
    } else {
        Err("invalid MySQL table".to_string())
    }
}

pub(super) fn mysql_format(args: &MySqlArgs) -> &str {
    if args.format.trim().is_empty() {
        "namespace"
    } else {
        args.format.trim()
    }
}

pub(super) fn postgresql_format(args: &PostgreSqlArgs) -> &str {
    if args.format.trim().is_empty() {
        "namespace"
    } else {
        args.format.trim()
    }
}

pub(super) fn mysql_port(args: &MySqlArgs) -> &str {
    if args.port.trim().is_empty() {
        "3306"
    } else {
        args.port.trim()
    }
}

pub(super) fn postgresql_port(args: &PostgreSqlArgs) -> &str {
    if args.port.trim().is_empty() {
        "5432"
    } else {
        args.port.trim()
    }
}

pub(super) fn mysql_options(args: &MySqlArgs) -> Result<mysql::Opts, String> {
    if !args.dsn_string.is_empty() {
        return mysql::Opts::from_url(&args.dsn_string).map_err(|err| err.to_string());
    }
    let builder = mysql::OptsBuilder::new()
        .ip_or_hostname(Some(args.host.clone()))
        .tcp_port(
            mysql_port(args)
                .parse::<u16>()
                .map_err(|_| "invalid MySQL port".to_string())?,
        )
        .user((!args.username.is_empty()).then_some(args.username.clone()))
        .pass((!args.password.is_empty()).then_some(args.password.clone()))
        .db_name(Some(args.database.clone()))
        .stmt_cache_size(Some(args.max_open_connections.max(1)))
        .tcp_connect_timeout(Some(Duration::from_millis(250)))
        .read_timeout(Some(Duration::from_millis(250)))
        .write_timeout(Some(Duration::from_millis(250)));
    Ok(mysql::Opts::from(builder))
}

pub(super) fn postgresql_client(args: &PostgreSqlArgs) -> Result<postgres::Client, String> {
    if !args.connection_string.is_empty() {
        return postgres::Client::connect(&args.connection_string, NoTls)
            .map_err(|err| err.to_string());
    }
    let mut config = postgres::Config::new();
    config.host(&args.host);
    config.port(
        postgresql_port(args)
            .parse::<u16>()
            .map_err(|_| "invalid PostgreSQL port".to_string())?,
    );
    config.user(if args.username.is_empty() {
        "postgres"
    } else {
        &args.username
    });
    if !args.password.is_empty() {
        config.password(&args.password);
    }
    config.dbname(&args.database);
    config.connect_timeout(Duration::from_millis(250));
    config.connect(NoTls).map_err(|err| err.to_string())
}

pub(crate) fn mysql_update_row(table: &str) -> String {
    format!(
        "INSERT INTO {table} (key_name, value) VALUES (?, CAST(? AS JSON)) ON DUPLICATE KEY UPDATE value=VALUES(value);"
    )
}

pub(crate) fn mysql_delete_row(table: &str) -> String {
    format!("DELETE FROM {table} WHERE key_hash = SHA2(?, 256);")
}

pub(crate) fn mysql_insert_row(table: &str) -> String {
    format!("INSERT INTO {table} (event_time, event_data) VALUES (?, CAST(? AS JSON));")
}

pub(crate) fn postgresql_update_row(table: &str) -> String {
    format!(
        "INSERT INTO {table} (key, value) VALUES ($1, CAST($2 AS JSONB)) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;"
    )
}

pub(crate) fn postgresql_delete_row(table: &str) -> String {
    format!("DELETE FROM {table} WHERE key = $1;")
}

pub(crate) fn postgresql_insert_row(table: &str) -> String {
    format!("INSERT INTO {table} (event_time, event_data) VALUES ($1, CAST($2 AS JSONB));")
}

pub(super) fn is_remove_event(event: &NotificationEvent) -> bool {
    event.event_name.to_string() == crate::internal::event::Name::ObjectRemovedDelete.to_string()
}

pub(super) fn mqtt_keep_alive(args: &MqttArgs) -> u64 {
    if args.keep_alive_secs == 0 {
        5
    } else {
        args.keep_alive_secs
    }
}

pub(super) fn mqtt_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

pub(super) fn validate_kafka_args(args: &KafkaArgs) -> Result<(), String> {
    if args.brokers.is_empty() {
        return Err("Kafka brokers unspecified".to_string());
    }
    if args.topic.trim().is_empty() {
        return Err("Kafka topic unspecified".to_string());
    }
    if !args.queue_dir.is_empty() && !args.queue_dir.starts_with('/') {
        return Err("Kafka queueDir path should be absolute".to_string());
    }
    if args.sasl.enable {
        if args.sasl.username.trim().is_empty() {
            return Err("Kafka SASL username unspecified".to_string());
        }
        if args.sasl.password.trim().is_empty() {
            return Err("Kafka SASL password unspecified".to_string());
        }
        let mechanism = args.sasl.mechanism.trim().to_ascii_uppercase();
        match mechanism.as_str() {
            "" | "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" => {}
            _ => return Err(format!("unsupported Kafka SASL mechanism {mechanism}")),
        }
    }
    Ok(())
}

pub(super) fn kafka_compression(value: &str) -> KafkaCompression {
    match value.trim().to_ascii_lowercase().as_str() {
        "gzip" => KafkaCompression::GZIP,
        "snappy" => KafkaCompression::SNAPPY,
        _ => KafkaCompression::NONE,
    }
}

pub(super) fn kafka_ack_timeout(args: &KafkaArgs) -> Duration {
    let millis = if args.batch_commit_timeout_ms == 0 {
        1_000
    } else {
        args.batch_commit_timeout_ms
    };
    Duration::from_millis(millis)
}

pub(super) fn declare_amqp_exchange(channel: &Channel, args: &AmqpArgs) -> AmqpResult<()> {
    if args.exchange.is_empty() {
        return Ok(());
    }
    let exchange_type = match args.exchange_type.as_str() {
        "" | "direct" => amiquip::ExchangeType::Direct,
        "fanout" => amiquip::ExchangeType::Fanout,
        "topic" => amiquip::ExchangeType::Topic,
        "headers" => amiquip::ExchangeType::Headers,
        _ => amiquip::ExchangeType::Direct,
    };
    channel.exchange_declare(
        exchange_type,
        &args.exchange,
        amiquip::ExchangeDeclareOptions {
            durable: args.durable,
            auto_delete: args.auto_deleted,
            internal: args.internal,
            arguments: amiquip::FieldTable::new(),
        },
    )?;
    Ok(())
}

pub(super) fn now_millis_for_mqtt() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

pub(super) fn parse_duration_secs(value: &str) -> Result<u64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(0);
    }
    if let Some(number) = value.strip_suffix('s') {
        return number
            .parse::<u64>()
            .map_err(|_| format!("invalid seconds duration {value}"));
    }
    if let Some(number) = value.strip_suffix('m') {
        return number
            .parse::<u64>()
            .map(|minutes| minutes * 60)
            .map_err(|_| format!("invalid minutes duration {value}"));
    }
    if let Some(number) = value.strip_suffix('h') {
        return number
            .parse::<u64>()
            .map(|hours| hours * 3600)
            .map_err(|_| format!("invalid hours duration {value}"));
    }
    value
        .parse::<u64>()
        .map_err(|_| format!("invalid duration {value}"))
}

pub(super) fn parse_duration_millis(value: &str) -> Result<u64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(0);
    }
    if let Some(number) = value.strip_suffix("ms") {
        return number
            .parse::<u64>()
            .map_err(|_| format!("invalid milliseconds duration {value}"));
    }
    if let Some(number) = value.strip_suffix('s') {
        return number
            .parse::<u64>()
            .map(|seconds| seconds * 1_000)
            .map_err(|_| format!("invalid seconds duration {value}"));
    }
    if let Some(number) = value.strip_suffix('m') {
        return number
            .parse::<u64>()
            .map(|minutes| minutes * 60_000)
            .map_err(|_| format!("invalid minutes duration {value}"));
    }
    value
        .parse::<u64>()
        .map_err(|_| format!("invalid duration {value}"))
}
