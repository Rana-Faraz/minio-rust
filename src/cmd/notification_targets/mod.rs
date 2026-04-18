mod backends;
mod core;
mod env;
mod support;

pub use backends::{
    AmqpArgs, AmqpNotificationTarget, ElasticsearchArgs, ElasticsearchNotificationTarget,
    InMemoryQueueTarget, KafkaArgs, KafkaNotificationTarget, KafkaProducerArgs, KafkaSaslArgs,
    KafkaTlsArgs, MqttArgs, MqttNotificationTarget, MySqlArgs, MySqlNotificationTarget,
    NatsNotificationTarget, NsqNotificationTarget, PostgreSqlArgs, PostgreSqlNotificationTarget,
    QueuedNotification, RedisArgs, RedisNotificationTarget, WebhookNotificationTarget,
};
pub use core::{
    NotificationDeliveryReceipt, NotificationDeliveryRecord, NotificationDispatchReport,
    NotificationEvent, NotificationTarget, NotificationTargetRegistry,
};
pub use env::{
    load_amqp_targets_from_env, load_elasticsearch_targets_from_env, load_kafka_targets_from_env,
    load_mqtt_targets_from_env, load_mysql_targets_from_env, load_nats_targets_from_env,
    load_nsq_targets_from_env, load_postgresql_targets_from_env, load_queue_targets_from_env,
    load_redis_targets_from_env, load_webhook_targets_from_env,
};

pub(crate) use support::{
    mysql_delete_row, mysql_insert_row, mysql_update_row, postgresql_delete_row,
    postgresql_insert_row, postgresql_update_row,
};

#[cfg(test)]
#[path = "../../../tests/cmd/notification_targets_test.rs"]
mod notification_targets_test;
