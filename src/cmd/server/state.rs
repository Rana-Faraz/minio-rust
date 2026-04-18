use super::*;

pub(super) const REPLICATION_QUEUE_FILE: &str = ".minio.sys/replication-queue.json";
pub(super) const NOTIFICATION_HISTORY_FILE: &str = ".minio.sys/notification-history.json";

#[derive(Debug)]
pub struct ServerHandle {
    pub(super) address: String,
    pub(super) console_address: Option<String>,
    pub(super) root_user: String,
    pub(super) root_password: String,
    pub(super) shutdown: Arc<AtomicBool>,
    pub(super) replication_worker: Option<ReplicationWorker>,
    pub(super) joins: Vec<JoinHandle<Result<(), String>>>,
}

impl ServerHandle {
    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn console_address(&self) -> Option<&str> {
        self.console_address.as_deref()
    }

    pub fn shutdown(mut self) -> Result<(), String> {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(worker) = self.replication_worker.take() {
            let _ = worker.join();
        }
        for join in self.joins.drain(..) {
            join.join()
                .map_err(|_| "server thread panicked".to_string())??;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct ServerAdminState {
    pub(super) handlers: AdminHandlers,
    pub(super) users: Mutex<AdminUsers>,
    pub(super) sts: Mutex<StsService>,
    pub(super) identity: Mutex<AdminIdentityApi>,
    pub(super) kms: KmsServiceFacade,
    pub(super) active: Credentials,
    pub(super) layer: Arc<LocalObjectLayer>,
    pub(super) replication_service: ReplicationService,
    pub(super) started_at_ms: i64,
    pub(super) notifications: Mutex<Vec<NotificationRecord>>,
    pub(super) resync_targets: Arc<Mutex<BTreeMap<String, BucketReplicationResyncTargetRecord>>>,
    pub(super) replication_targets: BTreeMap<String, ReplicationRemoteTarget>,
    pub(super) notification_targets: NotificationTargetRegistry,
    pub(super) queue_deliveries: Mutex<Vec<QueueDeliveryRecord>>,
    pub(super) webhook_deliveries: Mutex<Vec<WebhookDeliveryRecord>>,
    pub(super) elasticsearch_deliveries: Mutex<Vec<ElasticsearchDeliveryRecord>>,
    pub(super) redis_deliveries: Mutex<Vec<RedisDeliveryRecord>>,
    pub(super) mysql_deliveries: Mutex<Vec<MySqlDeliveryRecord>>,
    pub(super) postgresql_deliveries: Mutex<Vec<PostgreSqlDeliveryRecord>>,
    pub(super) amqp_deliveries: Mutex<Vec<AmqpDeliveryRecord>>,
    pub(super) mqtt_deliveries: Mutex<Vec<MqttDeliveryRecord>>,
    pub(super) kafka_deliveries: Mutex<Vec<KafkaDeliveryRecord>>,
    pub(super) nats_deliveries: Mutex<Vec<NatsDeliveryRecord>>,
    pub(super) nsq_deliveries: Mutex<Vec<NsqDeliveryRecord>>,
}

impl ServerAdminState {
    pub(super) fn new(
        active: Credentials,
        layer: Arc<LocalObjectLayer>,
        replication_service: ReplicationService,
        kms: KmsServiceFacade,
        notification_targets: NotificationTargetRegistry,
        replication_targets: BTreeMap<String, ReplicationRemoteTarget>,
    ) -> Self {
        Self {
            handlers: AdminHandlers::new(active.clone()),
            users: Mutex::new(AdminUsers::new(false)),
            sts: Mutex::new(StsService::new(&active.access_key, &active.secret_key)),
            identity: Mutex::new(AdminIdentityApi::new(false)),
            kms,
            active,
            layer,
            replication_service,
            started_at_ms: now_ms(),
            notifications: Mutex::new(Vec::new()),
            resync_targets: Arc::new(Mutex::new(BTreeMap::new())),
            replication_targets,
            notification_targets,
            queue_deliveries: Mutex::new(Vec::new()),
            webhook_deliveries: Mutex::new(Vec::new()),
            elasticsearch_deliveries: Mutex::new(Vec::new()),
            redis_deliveries: Mutex::new(Vec::new()),
            mysql_deliveries: Mutex::new(Vec::new()),
            postgresql_deliveries: Mutex::new(Vec::new()),
            amqp_deliveries: Mutex::new(Vec::new()),
            mqtt_deliveries: Mutex::new(Vec::new()),
            kafka_deliveries: Mutex::new(Vec::new()),
            nats_deliveries: Mutex::new(Vec::new()),
            nsq_deliveries: Mutex::new(Vec::new()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct NotificationRecord {
    pub(super) event: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) targets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct QueueDeliveryRecord {
    pub(super) target_id: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) version_id: String,
    pub(super) event: String,
    pub(super) payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct WebhookDeliveryRecord {
    pub(super) target_id: String,
    pub(super) endpoint: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ElasticsearchDeliveryRecord {
    pub(super) target_id: String,
    pub(super) endpoint: String,
    pub(super) index: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RedisDeliveryRecord {
    pub(super) target_id: String,
    pub(super) address: String,
    pub(super) key: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MySqlDeliveryRecord {
    pub(super) target_id: String,
    pub(super) address: String,
    pub(super) database: String,
    pub(super) table: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PostgreSqlDeliveryRecord {
    pub(super) target_id: String,
    pub(super) address: String,
    pub(super) database: String,
    pub(super) table: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct AmqpDeliveryRecord {
    pub(super) target_id: String,
    pub(super) url: String,
    pub(super) exchange: String,
    pub(super) routing_key: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MqttDeliveryRecord {
    pub(super) target_id: String,
    pub(super) broker: String,
    pub(super) topic: String,
    pub(super) qos: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct KafkaDeliveryRecord {
    pub(super) target_id: String,
    pub(super) brokers: String,
    pub(super) topic: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct NatsDeliveryRecord {
    pub(super) target_id: String,
    pub(super) address: String,
    pub(super) subject: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct NsqDeliveryRecord {
    pub(super) target_id: String,
    pub(super) address: String,
    pub(super) topic: String,
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) event: String,
    pub(super) delivered: bool,
    pub(super) error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub(super) struct NotificationHistoryState {
    pub(super) notifications: Vec<NotificationRecord>,
    pub(super) queue_deliveries: Vec<QueueDeliveryRecord>,
    pub(super) webhook_deliveries: Vec<WebhookDeliveryRecord>,
    pub(super) elasticsearch_deliveries: Vec<ElasticsearchDeliveryRecord>,
    pub(super) redis_deliveries: Vec<RedisDeliveryRecord>,
    pub(super) mysql_deliveries: Vec<MySqlDeliveryRecord>,
    pub(super) postgresql_deliveries: Vec<PostgreSqlDeliveryRecord>,
    pub(super) amqp_deliveries: Vec<AmqpDeliveryRecord>,
    pub(super) mqtt_deliveries: Vec<MqttDeliveryRecord>,
    pub(super) kafka_deliveries: Vec<KafkaDeliveryRecord>,
    pub(super) nats_deliveries: Vec<NatsDeliveryRecord>,
    pub(super) nsq_deliveries: Vec<NsqDeliveryRecord>,
}

pub(super) fn replication_queue_path(layer: &LocalObjectLayer) -> PathBuf {
    layer
        .disk_paths()
        .first()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(REPLICATION_QUEUE_FILE)
}

fn notification_history_path(layer: &LocalObjectLayer) -> PathBuf {
    layer
        .disk_paths()
        .first()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(NOTIFICATION_HISTORY_FILE)
}

pub(super) fn load_notification_history_state(
    layer: &LocalObjectLayer,
) -> Result<NotificationHistoryState, String> {
    let path = notification_history_path(layer);
    if !path.exists() {
        return Ok(NotificationHistoryState::default());
    }
    let bytes = fs::read(path).map_err(|err| err.to_string())?;
    serde_json::from_slice(&bytes).map_err(|err| err.to_string())
}

fn persist_notification_history_state(admin_state: &ServerAdminState) -> Result<(), String> {
    let state = NotificationHistoryState {
        notifications: admin_state
            .notifications
            .lock()
            .expect("notifications lock")
            .clone(),
        queue_deliveries: admin_state
            .queue_deliveries
            .lock()
            .expect("queue deliveries lock")
            .clone(),
        webhook_deliveries: admin_state
            .webhook_deliveries
            .lock()
            .expect("webhook deliveries lock")
            .clone(),
        elasticsearch_deliveries: admin_state
            .elasticsearch_deliveries
            .lock()
            .expect("elasticsearch deliveries lock")
            .clone(),
        redis_deliveries: admin_state
            .redis_deliveries
            .lock()
            .expect("redis deliveries lock")
            .clone(),
        mysql_deliveries: admin_state
            .mysql_deliveries
            .lock()
            .expect("mysql deliveries lock")
            .clone(),
        postgresql_deliveries: admin_state
            .postgresql_deliveries
            .lock()
            .expect("postgresql deliveries lock")
            .clone(),
        amqp_deliveries: admin_state
            .amqp_deliveries
            .lock()
            .expect("amqp deliveries lock")
            .clone(),
        mqtt_deliveries: admin_state
            .mqtt_deliveries
            .lock()
            .expect("mqtt deliveries lock")
            .clone(),
        kafka_deliveries: admin_state
            .kafka_deliveries
            .lock()
            .expect("kafka deliveries lock")
            .clone(),
        nats_deliveries: admin_state
            .nats_deliveries
            .lock()
            .expect("nats deliveries lock")
            .clone(),
        nsq_deliveries: admin_state
            .nsq_deliveries
            .lock()
            .expect("nsq deliveries lock")
            .clone(),
    };
    let path = notification_history_path(&admin_state.layer);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let bytes = serde_json::to_vec_pretty(&state).map_err(|err| err.to_string())?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).map_err(|err| err.to_string())?;
    fs::rename(&tmp, path).map_err(|err| err.to_string())?;
    Ok(())
}

pub(super) fn startup_message(handle: &ServerHandle) -> String {
    let api_endpoints = vec![format!("http://{}", handle.address())];
    let console_endpoints = handle
        .console_address()
        .map(|address| vec![format!("http://{address}")])
        .unwrap_or_else(|| vec!["http://127.0.0.1:9001".to_string()]);
    print_startup_message(
        &api_endpoints,
        &console_endpoints,
        "local",
        &handle.root_user,
        &handle.root_password,
    )
}

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

pub(super) fn maybe_record_notification(
    admin_state: &ServerAdminState,
    layer: Option<&LocalObjectLayer>,
    method: &Method,
    query: &BTreeMap<String, String>,
    bucket: Option<&str>,
    object: Option<&str>,
    headers: &BTreeMap<String, String>,
    response: &HandlerResponse,
) {
    if response.status >= 300 {
        return;
    }
    let Some(layer) = layer else {
        return;
    };
    let (Some(bucket), Some(object)) = (bucket, object) else {
        return;
    };

    let event = if *method == Method::Put && headers.contains_key("x-amz-copy-source") {
        Some(crate::internal::event::Name::ObjectCreatedCopy)
    } else if *method == Method::Put {
        Some(crate::internal::event::Name::ObjectCreatedPut)
    } else if *method == Method::Delete {
        Some(crate::internal::event::Name::ObjectRemovedDelete)
    } else if *method == Method::Post && query.contains_key("uploadId") {
        Some(crate::internal::event::Name::ObjectCreatedCompleteMultipartUpload)
    } else if *method == Method::Post {
        Some(crate::internal::event::Name::ObjectCreatedPost)
    } else {
        None
    };
    let Some(event) = event else {
        return;
    };

    let Ok(Some(config)) = read_bucket_notification_config(layer, bucket) else {
        return;
    };
    let configured_targets = config
        .to_rules_map()
        .match_object(event, object)
        .to_vec()
        .into_iter()
        .map(|target| target.to_string())
        .collect::<Vec<_>>();
    if configured_targets.is_empty() {
        return;
    }
    let notification_event = NotificationEvent::new(event, bucket, object);
    let Ok(report) = admin_state
        .notification_targets
        .dispatch(&config, &notification_event)
    else {
        {
            admin_state
                .notifications
                .lock()
                .expect("notifications lock")
                .push(NotificationRecord {
                    event: event.to_string(),
                    bucket: bucket.to_string(),
                    object: object.to_string(),
                    targets: configured_targets,
                });
        }
        let _ = persist_notification_history_state(admin_state);
        return;
    };
    record_queue_deliveries(admin_state, &report);
    record_webhook_deliveries(admin_state, &report);
    record_elasticsearch_deliveries(admin_state, &report);
    record_redis_deliveries(admin_state, &report);
    record_mysql_deliveries(admin_state, &report);
    record_postgresql_deliveries(admin_state, &report);
    record_amqp_deliveries(admin_state, &report);
    record_mqtt_deliveries(admin_state, &report);
    record_kafka_deliveries(admin_state, &report);
    record_nats_deliveries(admin_state, &report);
    record_nsq_deliveries(admin_state, &report);

    {
        admin_state
            .notifications
            .lock()
            .expect("notifications lock")
            .push(NotificationRecord {
                event: event.to_string(),
                bucket: bucket.to_string(),
                object: object.to_string(),
                targets: configured_targets,
            });
    }
    let _ = persist_notification_history_state(admin_state);
}

fn record_queue_deliveries(admin_state: &ServerAdminState, report: &NotificationDispatchReport) {
    let mut deliveries = admin_state
        .queue_deliveries
        .lock()
        .expect("queue deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "queue").then(|| QueueDeliveryRecord {
            target_id: delivery.target_id.clone(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            version_id: String::new(),
            event: delivery.event.clone(),
            payload: serde_json::json!({
                "EventName": delivery.event,
                "Key": format!("{}/{}", delivery.bucket, delivery.object),
            }),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_webhook_deliveries(admin_state: &ServerAdminState, report: &NotificationDispatchReport) {
    let mut deliveries = admin_state
        .webhook_deliveries
        .lock()
        .expect("webhook deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "webhook").then(|| WebhookDeliveryRecord {
            target_id: delivery.target_id.clone(),
            endpoint: delivery.detail.get("endpoint").cloned().unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_elasticsearch_deliveries(
    admin_state: &ServerAdminState,
    report: &NotificationDispatchReport,
) {
    let mut deliveries = admin_state
        .elasticsearch_deliveries
        .lock()
        .expect("elasticsearch deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "elasticsearch").then(|| ElasticsearchDeliveryRecord {
            target_id: delivery.target_id.clone(),
            endpoint: delivery.detail.get("endpoint").cloned().unwrap_or_default(),
            index: delivery.detail.get("index").cloned().unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_redis_deliveries(admin_state: &ServerAdminState, report: &NotificationDispatchReport) {
    let mut deliveries = admin_state
        .redis_deliveries
        .lock()
        .expect("redis deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "redis").then(|| RedisDeliveryRecord {
            target_id: delivery.target_id.clone(),
            address: delivery.detail.get("address").cloned().unwrap_or_default(),
            key: delivery.detail.get("key").cloned().unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_mysql_deliveries(admin_state: &ServerAdminState, report: &NotificationDispatchReport) {
    let mut deliveries = admin_state
        .mysql_deliveries
        .lock()
        .expect("mysql deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "mysql").then(|| MySqlDeliveryRecord {
            target_id: delivery.target_id.clone(),
            address: delivery.detail.get("address").cloned().unwrap_or_default(),
            database: delivery.detail.get("database").cloned().unwrap_or_default(),
            table: delivery.detail.get("table").cloned().unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_postgresql_deliveries(
    admin_state: &ServerAdminState,
    report: &NotificationDispatchReport,
) {
    let mut deliveries = admin_state
        .postgresql_deliveries
        .lock()
        .expect("postgresql deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "postgresql").then(|| PostgreSqlDeliveryRecord {
            target_id: delivery.target_id.clone(),
            address: delivery.detail.get("address").cloned().unwrap_or_default(),
            database: delivery.detail.get("database").cloned().unwrap_or_default(),
            table: delivery.detail.get("table").cloned().unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_amqp_deliveries(admin_state: &ServerAdminState, report: &NotificationDispatchReport) {
    let mut deliveries = admin_state
        .amqp_deliveries
        .lock()
        .expect("amqp deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "amqp").then(|| AmqpDeliveryRecord {
            target_id: delivery.target_id.clone(),
            url: delivery.detail.get("url").cloned().unwrap_or_default(),
            exchange: delivery.detail.get("exchange").cloned().unwrap_or_default(),
            routing_key: delivery
                .detail
                .get("routingKey")
                .cloned()
                .unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_mqtt_deliveries(admin_state: &ServerAdminState, report: &NotificationDispatchReport) {
    let mut deliveries = admin_state
        .mqtt_deliveries
        .lock()
        .expect("mqtt deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "mqtt").then(|| MqttDeliveryRecord {
            target_id: delivery.target_id.clone(),
            broker: delivery.detail.get("broker").cloned().unwrap_or_default(),
            topic: delivery.detail.get("topic").cloned().unwrap_or_default(),
            qos: delivery.detail.get("qos").cloned().unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_kafka_deliveries(admin_state: &ServerAdminState, report: &NotificationDispatchReport) {
    let mut deliveries = admin_state
        .kafka_deliveries
        .lock()
        .expect("kafka deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "kafka").then(|| KafkaDeliveryRecord {
            target_id: delivery.target_id.clone(),
            brokers: delivery.detail.get("brokers").cloned().unwrap_or_default(),
            topic: delivery.detail.get("topic").cloned().unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_nats_deliveries(admin_state: &ServerAdminState, report: &NotificationDispatchReport) {
    let mut deliveries = admin_state
        .nats_deliveries
        .lock()
        .expect("nats deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "nats").then(|| NatsDeliveryRecord {
            target_id: delivery.target_id.clone(),
            address: delivery.detail.get("address").cloned().unwrap_or_default(),
            subject: delivery.detail.get("subject").cloned().unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}

fn record_nsq_deliveries(admin_state: &ServerAdminState, report: &NotificationDispatchReport) {
    let mut deliveries = admin_state
        .nsq_deliveries
        .lock()
        .expect("nsq deliveries lock");
    deliveries.extend(report.deliveries.iter().filter_map(|delivery| {
        (delivery.target_kind == "nsq").then(|| NsqDeliveryRecord {
            target_id: delivery.target_id.clone(),
            address: delivery.detail.get("address").cloned().unwrap_or_default(),
            topic: delivery.detail.get("topic").cloned().unwrap_or_default(),
            bucket: delivery.bucket.clone(),
            object: delivery.object.clone(),
            event: delivery.event.clone(),
            delivered: delivery.delivered,
            error: delivery.error.clone(),
        })
    }));
    drop(deliveries);
    let _ = persist_notification_history_state(admin_state);
}
