use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amiquip::{AmqpProperties, Connection as AmqpConnection, Publish};
use base64::Engine;
use chrono::Utc;
use kafka::producer::{
    Producer as KafkaProducer, Record as KafkaProducerRecord, RequiredAcks as KafkaRequiredAcks,
};
use mysql::prelude::Queryable;
use rumqttc::{Client as MqttClient, MqttOptions};
use url::Url;

use crate::internal::event::target::{NatsArgs, NsqArgs};
use crate::internal::event::TargetId;

use super::core::{NotificationDeliveryReceipt, NotificationEvent, NotificationTarget};
use super::support::{
    build_nats_connect_frame, declare_amqp_exchange, is_remove_event, kafka_ack_timeout,
    kafka_compression, mqtt_keep_alive, mqtt_qos, mysql_delete_row, mysql_format, mysql_insert_row,
    mysql_options, mysql_port, mysql_update_row, now_millis_for_mqtt, postgresql_client,
    postgresql_delete_row, postgresql_format, postgresql_insert_row, postgresql_port,
    postgresql_update_row, prepare_nats_connection, prepare_nsq_connection, read_nats_line,
    read_redis_line, redis_command, validate_amqp_args, validate_kafka_args, validate_mqtt_args,
    validate_mysql_args, validate_postgresql_args, write_nats_frame, write_nsq_frame,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookNotificationTarget {
    pub target_id: TargetId,
    pub endpoint: String,
    pub auth_token: String,
}

impl NotificationTarget for WebhookNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "webhook"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        let payload = event.to_payload();
        let request = ureq::post(&self.endpoint).set("Content-Type", "application/json");
        let request = if self.auth_token.is_empty() {
            request
        } else if self.auth_token.split_whitespace().count() == 2 {
            request.set("Authorization", &self.auth_token)
        } else {
            request.set("Authorization", &format!("Bearer {}", self.auth_token))
        };

        let response = request
            .send_string(&payload.to_string())
            .map_err(|err| err.to_string())?;
        let mut detail = BTreeMap::new();
        detail.insert("endpoint".to_string(), self.endpoint.clone());
        detail.insert("status".to_string(), response.status().to_string());
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedNotification {
    pub target_id: String,
    pub event: NotificationEvent,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryQueueTarget {
    target_id: TargetId,
    entries: Arc<Mutex<Vec<QueuedNotification>>>,
}

impl InMemoryQueueTarget {
    pub fn new(target_id: TargetId) -> Self {
        Self {
            target_id,
            entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn drain(&self) -> Vec<QueuedNotification> {
        let mut entries = self.entries.lock().expect("queue target mutex poisoned");
        std::mem::take(&mut *entries)
    }

    pub fn snapshot(&self) -> Vec<QueuedNotification> {
        self.entries
            .lock()
            .expect("queue target mutex poisoned")
            .clone()
    }
}

impl NotificationTarget for InMemoryQueueTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "queue"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        let payload = event.to_payload();
        let mut entries = self.entries.lock().expect("queue target mutex poisoned");
        entries.push(QueuedNotification {
            target_id: self.target_id.to_string(),
            event: event.clone(),
            payload,
        });
        let mut detail = BTreeMap::new();
        detail.insert("queued".to_string(), entries.len().to_string());
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsNotificationTarget {
    pub target_id: TargetId,
    pub args: NatsArgs,
}

impl NotificationTarget for NatsNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "nats"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        let payload = event.to_payload().to_string();
        let mut connection = self.args.connect_nats().map_err(|err| err.to_string())?;
        prepare_nats_connection(&mut connection).map_err(|err| err.to_string())?;
        let _ = read_nats_line(&mut connection).map_err(|err| err.to_string())?;
        let connect = build_nats_connect_frame(&self.args)?;
        let publish = format!(
            "PUB {} {}\r\n{}\r\n",
            self.args.subject,
            payload.len(),
            payload
        );
        write_nats_frame(&mut connection, connect.as_bytes()).map_err(|err| err.to_string())?;
        write_nats_frame(&mut connection, publish.as_bytes()).map_err(|err| err.to_string())?;
        connection.close();

        let mut detail = BTreeMap::new();
        detail.insert(
            "address".to_string(),
            format!("{}:{}", self.args.address.name, self.args.address.port),
        );
        detail.insert("subject".to_string(), self.args.subject.clone());
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NsqNotificationTarget {
    pub target_id: TargetId,
    pub args: NsqArgs,
}

impl NotificationTarget for NsqNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "nsq"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        let payload = event.to_payload().to_string();
        let mut connection = self.args.connect_nsq().map_err(|err| err.to_string())?;
        prepare_nsq_connection(&mut connection).map_err(|err| err.to_string())?;
        write_nsq_frame(&mut connection, b"  V2").map_err(|err| err.to_string())?;
        let command = format!("PUB {}\n", self.args.topic);
        write_nsq_frame(&mut connection, command.as_bytes()).map_err(|err| err.to_string())?;
        write_nsq_frame(&mut connection, &(payload.len() as u32).to_be_bytes())
            .map_err(|err| err.to_string())?;
        write_nsq_frame(&mut connection, payload.as_bytes()).map_err(|err| err.to_string())?;
        connection.close();

        let mut detail = BTreeMap::new();
        detail.insert(
            "address".to_string(),
            format!(
                "{}:{}",
                self.args.nsqd_address.name, self.args.nsqd_address.port
            ),
        );
        detail.insert("topic".to_string(), self.args.topic.clone());
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ElasticsearchArgs {
    pub enable: bool,
    pub endpoint: String,
    pub index: String,
    pub username: String,
    pub password: String,
    pub format: String,
    pub queue_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElasticsearchNotificationTarget {
    pub target_id: TargetId,
    pub args: ElasticsearchArgs,
}

impl NotificationTarget for ElasticsearchNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "elasticsearch"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        let payload = event.to_payload().to_string();
        let endpoint = self.args.endpoint.trim_end_matches('/');
        let url = format!("{}/{}/_doc", endpoint, self.args.index);
        let request = ureq::post(&url).set("Content-Type", "application/json");
        let request = if !self.args.username.is_empty() || !self.args.password.is_empty() {
            let auth = base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", self.args.username, self.args.password));
            request.set("Authorization", &format!("Basic {auth}"))
        } else {
            request
        };
        let response = request
            .send_string(&payload)
            .map_err(|err: ureq::Error| err.to_string())?;
        let mut detail = BTreeMap::new();
        detail.insert("endpoint".to_string(), self.args.endpoint.clone());
        detail.insert("index".to_string(), self.args.index.clone());
        detail.insert("status".to_string(), response.status().to_string());
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisArgs {
    pub enable: bool,
    pub address: String,
    pub key: String,
    pub user: String,
    pub password: String,
    pub format: String,
    pub queue_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisNotificationTarget {
    pub target_id: TargetId,
    pub args: RedisArgs,
}

impl NotificationTarget for RedisNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "redis"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        let payload = event.to_payload().to_string();
        let mut stream =
            std::net::TcpStream::connect(&self.args.address).map_err(|err| err.to_string())?;
        stream
            .set_read_timeout(Some(Duration::from_millis(250)))
            .map_err(|err| err.to_string())?;
        stream
            .set_write_timeout(Some(Duration::from_millis(250)))
            .map_err(|err| err.to_string())?;
        if !self.args.password.is_empty() || !self.args.user.is_empty() {
            let auth = if !self.args.user.is_empty() {
                redis_command(&["AUTH", &self.args.user, &self.args.password])
            } else {
                redis_command(&["AUTH", &self.args.password])
            };
            stream
                .write_all(auth.as_bytes())
                .map_err(|err| err.to_string())?;
            stream.flush().map_err(|err| err.to_string())?;
            let _ = read_redis_line(&mut stream).map_err(|err| err.to_string())?;
        }
        let command = redis_command(&["RPUSH", &self.args.key, &payload]);
        stream
            .write_all(command.as_bytes())
            .and_then(|_| stream.flush())
            .map_err(|err| err.to_string())?;
        let _ = read_redis_line(&mut stream).map_err(|err| err.to_string())?;
        let mut detail = BTreeMap::new();
        detail.insert("address".to_string(), self.args.address.clone());
        detail.insert("key".to_string(), self.args.key.clone());
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MySqlArgs {
    pub enable: bool,
    pub format: String,
    pub dsn_string: String,
    pub table: String,
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub database: String,
    pub queue_dir: String,
    pub max_open_connections: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlNotificationTarget {
    pub target_id: TargetId,
    pub args: MySqlArgs,
}

impl NotificationTarget for MySqlNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "mysql"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        validate_mysql_args(&self.args)?;
        let options = mysql_options(&self.args)?;
        let pool = mysql::Pool::new(options).map_err(|err| err.to_string())?;
        let mut conn = pool.get_conn().map_err(|err| err.to_string())?;
        let payload = event.to_payload().to_string();

        if mysql_format(&self.args) == "namespace" {
            let key = event.key();
            if is_remove_event(event) {
                conn.exec_drop(mysql_delete_row(&self.args.table), (key,))
                    .map_err(|err| err.to_string())?;
            } else {
                conn.exec_drop(mysql_update_row(&self.args.table), (key, payload))
                    .map_err(|err| err.to_string())?;
            }
        } else {
            conn.exec_drop(
                mysql_insert_row(&self.args.table),
                (Utc::now().naive_utc(), payload),
            )
            .map_err(|err| err.to_string())?;
        }

        let mut detail = BTreeMap::new();
        detail.insert("table".to_string(), self.args.table.clone());
        detail.insert("database".to_string(), self.args.database.clone());
        if !self.args.host.is_empty() {
            detail.insert(
                "address".to_string(),
                format!("{}:{}", self.args.host, mysql_port(&self.args)),
            );
        }
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PostgreSqlArgs {
    pub enable: bool,
    pub format: String,
    pub connection_string: String,
    pub table: String,
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub database: String,
    pub queue_dir: String,
    pub max_open_connections: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgreSqlNotificationTarget {
    pub target_id: TargetId,
    pub args: PostgreSqlArgs,
}

impl NotificationTarget for PostgreSqlNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "postgresql"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        validate_postgresql_args(&self.args)?;
        let mut client = postgresql_client(&self.args)?;
        let payload = event.to_payload().to_string();

        if postgresql_format(&self.args) == "namespace" {
            let key = event.key();
            if is_remove_event(event) {
                client
                    .execute(postgresql_delete_row(&self.args.table).as_str(), &[&key])
                    .map_err(|err| err.to_string())?;
            } else {
                client
                    .execute(
                        postgresql_update_row(&self.args.table).as_str(),
                        &[&key, &payload],
                    )
                    .map_err(|err| err.to_string())?;
            }
        } else {
            client
                .execute(
                    postgresql_insert_row(&self.args.table).as_str(),
                    &[&Utc::now(), &payload],
                )
                .map_err(|err| err.to_string())?;
        }

        let mut detail = BTreeMap::new();
        detail.insert("table".to_string(), self.args.table.clone());
        detail.insert("database".to_string(), self.args.database.clone());
        if !self.args.host.is_empty() {
            detail.insert(
                "address".to_string(),
                format!("{}:{}", self.args.host, postgresql_port(&self.args)),
            );
        }
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AmqpArgs {
    pub enable: bool,
    pub url: String,
    pub exchange: String,
    pub routing_key: String,
    pub exchange_type: String,
    pub delivery_mode: u8,
    pub mandatory: bool,
    pub immediate: bool,
    pub durable: bool,
    pub internal: bool,
    pub no_wait: bool,
    pub auto_deleted: bool,
    pub publisher_confirms: bool,
    pub queue_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmqpNotificationTarget {
    pub target_id: TargetId,
    pub args: AmqpArgs,
}

impl NotificationTarget for AmqpNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "amqp"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        validate_amqp_args(&self.args)?;
        let mut connection =
            AmqpConnection::insecure_open(&self.args.url).map_err(|err| err.to_string())?;
        let channel = connection
            .open_channel(None)
            .map_err(|err| err.to_string())?;
        declare_amqp_exchange(&channel, &self.args).map_err(|err| err.to_string())?;
        let payload = event.to_payload().to_string();
        let properties = if self.args.delivery_mode == 0 {
            AmqpProperties::default()
        } else {
            AmqpProperties::default().with_delivery_mode(self.args.delivery_mode)
        };
        channel
            .basic_publish(
                self.args.exchange.clone(),
                Publish::with_properties(payload.as_bytes(), &self.args.routing_key, properties),
            )
            .map_err(|err| err.to_string())?;
        let _ = connection.close();

        let mut detail = BTreeMap::new();
        detail.insert("url".to_string(), self.args.url.clone());
        detail.insert("exchange".to_string(), self.args.exchange.clone());
        detail.insert("routingKey".to_string(), self.args.routing_key.clone());
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MqttArgs {
    pub enable: bool,
    pub broker: String,
    pub topic: String,
    pub qos: u8,
    pub username: String,
    pub password: String,
    pub reconnect_interval_secs: u64,
    pub keep_alive_secs: u64,
    pub queue_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttNotificationTarget {
    pub target_id: TargetId,
    pub args: MqttArgs,
}

impl NotificationTarget for MqttNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "mqtt"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        validate_mqtt_args(&self.args)?;
        let broker = Url::parse(&self.args.broker).map_err(|err| err.to_string())?;
        let host = broker
            .host_str()
            .ok_or_else(|| "MQTT broker host missing".to_string())?;
        let port = broker
            .port_or_known_default()
            .ok_or_else(|| "MQTT broker port missing".to_string())?;
        let client_id = format!("minio-rust-{}", now_millis_for_mqtt());
        let mut options = MqttOptions::new(client_id, host, port);
        options.set_keep_alive(Duration::from_secs(mqtt_keep_alive(&self.args)));
        if !self.args.username.is_empty() || !self.args.password.is_empty() {
            options.set_credentials(self.args.username.clone(), self.args.password.clone());
        }
        let (client, mut connection) = MqttClient::new(options, 10);
        let pump = std::thread::spawn(move || for _ in connection.iter().take(4) {});
        client
            .publish(
                self.args.topic.clone(),
                mqtt_qos(self.args.qos),
                false,
                event.to_payload().to_string().into_bytes(),
            )
            .map_err(|err| err.to_string())?;
        let _ = client.disconnect();
        let _ = pump.join();

        let mut detail = BTreeMap::new();
        detail.insert("broker".to_string(), self.args.broker.clone());
        detail.insert("topic".to_string(), self.args.topic.clone());
        detail.insert("qos".to_string(), self.args.qos.to_string());
        Ok(NotificationDeliveryReceipt { detail })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KafkaTlsArgs {
    pub enable: bool,
    pub skip_verify: bool,
    pub client_tls_cert: String,
    pub client_tls_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KafkaSaslArgs {
    pub enable: bool,
    pub username: String,
    pub password: String,
    pub mechanism: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KafkaProducerArgs {
    pub compression: String,
    pub compression_level: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KafkaArgs {
    pub enable: bool,
    pub brokers: Vec<String>,
    pub topic: String,
    pub queue_dir: String,
    pub version: String,
    pub batch_size: u32,
    pub batch_commit_timeout_ms: u64,
    pub tls: KafkaTlsArgs,
    pub sasl: KafkaSaslArgs,
    pub producer: KafkaProducerArgs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaNotificationTarget {
    pub target_id: TargetId,
    pub args: KafkaArgs,
}

impl NotificationTarget for KafkaNotificationTarget {
    fn id(&self) -> TargetId {
        self.target_id.clone()
    }

    fn kind(&self) -> &'static str {
        "kafka"
    }

    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String> {
        validate_kafka_args(&self.args)?;
        if self.args.sasl.enable {
            return Err("Kafka SASL is not implemented".to_string());
        }
        if self.args.tls.enable {
            return Err("Kafka TLS is not implemented".to_string());
        }

        let payload = event.to_payload().to_string();
        let key = event.key();
        let mut producer = KafkaProducer::from_hosts(self.args.brokers.clone())
            .with_ack_timeout(kafka_ack_timeout(&self.args))
            .with_required_acks(KafkaRequiredAcks::One)
            .with_compression(kafka_compression(&self.args.producer.compression))
            .with_client_id("minio-rust".to_string())
            .create()
            .map_err(|err| err.to_string())?;
        producer
            .send(&KafkaProducerRecord::from_key_value(
                &self.args.topic,
                key.as_bytes(),
                payload.as_bytes(),
            ))
            .map_err(|err| err.to_string())?;

        let mut detail = BTreeMap::new();
        detail.insert("brokers".to_string(), self.args.brokers.join(","));
        detail.insert("topic".to_string(), self.args.topic.clone());
        Ok(NotificationDeliveryReceipt { detail })
    }
}
