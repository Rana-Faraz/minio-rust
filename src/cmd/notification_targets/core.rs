use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use crate::internal::event::{Config as EventConfig, Name, Target, TargetId, TargetList};

use super::backends::{
    AmqpNotificationTarget, ElasticsearchNotificationTarget, InMemoryQueueTarget,
    KafkaNotificationTarget, MqttNotificationTarget, MySqlNotificationTarget,
    NatsNotificationTarget, NsqNotificationTarget, PostgreSqlNotificationTarget,
    RedisNotificationTarget,
};
use super::env::{
    load_amqp_targets_from_env, load_elasticsearch_targets_from_env, load_kafka_targets_from_env,
    load_mqtt_targets_from_env, load_mysql_targets_from_env, load_nats_targets_from_env,
    load_nsq_targets_from_env, load_postgresql_targets_from_env, load_queue_targets_from_env,
    load_redis_targets_from_env, load_webhook_targets_from_env,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationEvent {
    pub event_name: Name,
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub metadata: BTreeMap<String, String>,
}

impl NotificationEvent {
    pub fn new(event_name: Name, bucket: impl Into<String>, object: impl Into<String>) -> Self {
        Self {
            event_name,
            bucket: bucket.into(),
            object: object.into(),
            version_id: String::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn key(&self) -> String {
        format!("{}/{}", self.bucket, self.object)
    }

    pub fn to_payload(&self) -> serde_json::Value {
        let mut object = serde_json::json!({
            "key": self.object,
        });
        if !self.version_id.is_empty() {
            object["versionId"] = serde_json::Value::String(self.version_id.clone());
        }
        if !self.metadata.is_empty() {
            object["userMetadata"] = serde_json::to_value(&self.metadata)
                .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));
        }

        serde_json::json!({
            "EventName": self.event_name.to_string(),
            "Key": self.key(),
            "Records": [{
                "eventName": self.event_name.to_string(),
                "s3": {
                    "bucket": { "name": self.bucket },
                    "object": object,
                }
            }]
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NotificationDeliveryReceipt {
    pub detail: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NotificationDeliveryRecord {
    pub target_id: String,
    pub target_kind: String,
    pub event: String,
    pub bucket: String,
    pub object: String,
    pub delivered: bool,
    pub error: String,
    pub detail: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NotificationDispatchReport {
    pub matched_targets: Vec<String>,
    pub deliveries: Vec<NotificationDeliveryRecord>,
}

pub trait NotificationTarget: Send + Sync {
    fn id(&self) -> TargetId;
    fn kind(&self) -> &'static str;
    fn deliver(&self, event: &NotificationEvent) -> Result<NotificationDeliveryReceipt, String>;
}

#[derive(Default)]
pub struct NotificationTargetRegistry {
    region: String,
    targets: HashMap<TargetId, Arc<dyn NotificationTarget>>,
}

impl fmt::Debug for NotificationTargetRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotificationTargetRegistry")
            .field("region", &self.region)
            .field("targets", &self.list())
            .finish()
    }
}

impl NotificationTargetRegistry {
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            targets: HashMap::new(),
        }
    }

    pub fn register<T: NotificationTarget + 'static>(&mut self, target: T) -> Result<(), String> {
        let id = target.id();
        if self.targets.contains_key(&id) {
            return Err(format!("target {} already exists", id));
        }
        self.targets.insert(id, Arc::new(target));
        Ok(())
    }

    pub fn register_shared(&mut self, target: Arc<dyn NotificationTarget>) -> Result<(), String> {
        let id = target.id();
        if self.targets.contains_key(&id) {
            return Err(format!("target {} already exists", id));
        }
        self.targets.insert(id, target);
        Ok(())
    }

    pub fn register_webhooks_from_env(&mut self) -> Result<usize, String> {
        let mut registered = 0usize;
        for target in load_webhook_targets_from_env() {
            self.register(target)?;
            registered += 1;
        }
        Ok(registered)
    }

    pub fn register_queues_from_env(&mut self) -> Result<Vec<Arc<InMemoryQueueTarget>>, String> {
        let mut registered = Vec::new();
        for target in load_queue_targets_from_env() {
            self.register_shared(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn register_nats_from_env(&mut self) -> Result<Vec<NatsNotificationTarget>, String> {
        let mut registered = Vec::new();
        for target in load_nats_targets_from_env()? {
            self.register(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn register_nsq_from_env(&mut self) -> Result<Vec<NsqNotificationTarget>, String> {
        let mut registered = Vec::new();
        for target in load_nsq_targets_from_env()? {
            self.register(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn register_elasticsearch_from_env(
        &mut self,
    ) -> Result<Vec<ElasticsearchNotificationTarget>, String> {
        let mut registered = Vec::new();
        for target in load_elasticsearch_targets_from_env()? {
            self.register(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn register_redis_from_env(&mut self) -> Result<Vec<RedisNotificationTarget>, String> {
        let mut registered = Vec::new();
        for target in load_redis_targets_from_env()? {
            self.register(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn register_mysql_from_env(&mut self) -> Result<Vec<MySqlNotificationTarget>, String> {
        let mut registered = Vec::new();
        for target in load_mysql_targets_from_env()? {
            self.register(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn register_postgresql_from_env(
        &mut self,
    ) -> Result<Vec<PostgreSqlNotificationTarget>, String> {
        let mut registered = Vec::new();
        for target in load_postgresql_targets_from_env()? {
            self.register(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn register_amqp_from_env(&mut self) -> Result<Vec<AmqpNotificationTarget>, String> {
        let mut registered = Vec::new();
        for target in load_amqp_targets_from_env()? {
            self.register(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn register_mqtt_from_env(&mut self) -> Result<Vec<MqttNotificationTarget>, String> {
        let mut registered = Vec::new();
        for target in load_mqtt_targets_from_env()? {
            self.register(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn register_kafka_from_env(&mut self) -> Result<Vec<KafkaNotificationTarget>, String> {
        let mut registered = Vec::new();
        for target in load_kafka_targets_from_env()? {
            self.register(target.clone())?;
            registered.push(target);
        }
        Ok(registered)
    }

    pub fn exists(&self, id: &TargetId) -> bool {
        self.targets.contains_key(id)
    }

    pub fn list(&self) -> Vec<TargetId> {
        let mut ids = self.targets.keys().cloned().collect::<Vec<_>>();
        ids.sort_by(|left, right| left.to_string().cmp(&right.to_string()));
        ids
    }

    pub fn validate_config(&self, config: &EventConfig) -> Result<(), String> {
        let mut target_list = TargetList::new();
        for target_id in self.targets.keys() {
            target_list
                .add(RegisteredTargetPresence(target_id.clone()))
                .map_err(|err| err.to_string())?;
        }
        config
            .validate(&self.region, Some(&target_list))
            .map_err(|err| err.to_string())
    }

    pub fn matching_target_ids(
        &self,
        config: &EventConfig,
        event: &NotificationEvent,
    ) -> Vec<TargetId> {
        let matches = config
            .to_rules_map()
            .match_object(event.event_name, &event.object);
        let mut ids = matches
            .to_vec()
            .into_iter()
            .filter(|target_id| self.targets.contains_key(target_id))
            .collect::<Vec<_>>();
        ids.sort_by(|left, right| left.to_string().cmp(&right.to_string()));
        ids
    }

    pub fn dispatch(
        &self,
        config: &EventConfig,
        event: &NotificationEvent,
    ) -> Result<NotificationDispatchReport, String> {
        let mut report = NotificationDispatchReport::default();
        let matched = self.matching_target_ids(config, event);
        report.matched_targets = matched.iter().map(ToString::to_string).collect();

        for target_id in matched {
            let Some(target) = self.targets.get(&target_id) else {
                continue;
            };
            match target.deliver(event) {
                Ok(receipt) => report.deliveries.push(NotificationDeliveryRecord {
                    target_id: target_id.to_string(),
                    target_kind: target.kind().to_string(),
                    event: event.event_name.to_string(),
                    bucket: event.bucket.clone(),
                    object: event.object.clone(),
                    delivered: true,
                    error: String::new(),
                    detail: receipt.detail,
                }),
                Err(error) => report.deliveries.push(NotificationDeliveryRecord {
                    target_id: target_id.to_string(),
                    target_kind: target.kind().to_string(),
                    event: event.event_name.to_string(),
                    bucket: event.bucket.clone(),
                    object: event.object.clone(),
                    delivered: false,
                    error,
                    detail: BTreeMap::new(),
                }),
            }
        }

        Ok(report)
    }

    pub fn dispatch_xml(
        &self,
        xml: &[u8],
        event: &NotificationEvent,
    ) -> Result<NotificationDispatchReport, String> {
        let config = EventConfig::unmarshal_xml(xml).map_err(|err| err.to_string())?;
        self.dispatch(&config, event)
    }
}

#[derive(Clone)]
struct RegisteredTargetPresence(TargetId);

impl Target for RegisteredTargetPresence {
    fn id(&self) -> TargetId {
        self.0.clone()
    }
}
