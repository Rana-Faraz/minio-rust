use super::state::{now_ms, ServerAdminState};
use super::*;

const PROMETHEUS_AUTH_TYPE_ENV: &str = "MINIO_PROMETHEUS_AUTH_TYPE";

pub(super) fn route_health_request(
    url: &Url,
    handlers: &ObjectApiHandlers,
) -> Option<HandlerResponse> {
    let text_response = |status: u16, body: &'static [u8]| HandlerResponse {
        status,
        headers: BTreeMap::from([(
            "content-type".to_string(),
            "text/plain; charset=utf-8".to_string(),
        )]),
        body: body.to_vec(),
    };

    match url.path() {
        "/minio/health/live" | "/minio/health/ready" => Some(text_response(200, b"OK")),
        "/minio/health/cluster" => {
            let Some(layer) = handlers.layer() else {
                return Some(text_response(503, b"UNAVAILABLE"));
            };
            if url
                .query_pairs()
                .any(|(key, value)| key == "maintenance" && value.eq_ignore_ascii_case("true"))
            {
                return Some(if layer.can_maintain_quorum_after_one_offline() {
                    text_response(200, b"OK")
                } else {
                    text_response(412, b"PRECONDITION FAILED")
                });
            }
            Some(if layer.has_write_quorum() {
                text_response(200, b"OK")
            } else {
                text_response(503, b"UNAVAILABLE")
            })
        }
        "/minio/health/cluster/read" => {
            let Some(layer) = handlers.layer() else {
                return Some(text_response(503, b"UNAVAILABLE"));
            };
            Some(if layer.has_read_quorum() {
                text_response(200, b"OK")
            } else {
                text_response(503, b"UNAVAILABLE")
            })
        }
        _ => None,
    }
}

pub(super) fn is_public_metrics_request(url: &Url) -> bool {
    let auth_type = std::env::var(PROMETHEUS_AUTH_TYPE_ENV)
        .unwrap_or_else(|_| "jwt".to_string())
        .to_ascii_lowercase();
    auth_type == "public" && url.path().starts_with("/minio/v2/metrics/")
}

pub(super) fn route_metrics_request(
    url: &Url,
    admin_state: &ServerAdminState,
) -> Option<HandlerResponse> {
    let kms_metrics = admin_state.kms.metrics();
    let layer = &admin_state.layer;
    let queue = admin_state.replication_service.snapshot(now_ms());
    let metrics = match url.path() {
        "/minio/v2/metrics/cluster" => {
            cluster_prometheus_metrics(admin_state, layer, &kms_metrics, &queue)
        }
        "/minio/v2/metrics/node" => node_prometheus_metrics(admin_state, layer, &kms_metrics),
        "/minio/v2/metrics/bucket" => bucket_prometheus_metrics(layer),
        "/minio/v2/metrics/resource" => resource_prometheus_metrics(layer),
        _ => return None,
    };
    let body = render_prometheus_metrics(&metrics);

    Some(HandlerResponse {
        status: 200,
        headers: BTreeMap::from([(
            "content-type".to_string(),
            "text/plain; version=0.0.4; charset=utf-8".to_string(),
        )]),
        body: body.into_bytes(),
    })
}

#[derive(Debug, Clone)]
struct PrometheusSample {
    name: &'static str,
    help: &'static str,
    value: f64,
    labels: BTreeMap<String, String>,
}

fn prometheus_metric(name: &'static str, help: &'static str, value: f64) -> PrometheusSample {
    PrometheusSample {
        name,
        help,
        value,
        labels: BTreeMap::new(),
    }
}

fn prometheus_labeled_metric(
    name: &'static str,
    help: &'static str,
    value: f64,
    labels: BTreeMap<String, String>,
) -> PrometheusSample {
    PrometheusSample {
        name,
        help,
        value,
        labels,
    }
}

fn cluster_prometheus_metrics(
    admin_state: &ServerAdminState,
    layer: &LocalObjectLayer,
    kms_metrics: &KmsServiceMetrics,
    queue: &ReplicationServiceSnapshot,
) -> Vec<PrometheusSample> {
    vec![
        prometheus_metric(
            "minio_cluster_health_status",
            "Cluster health status based on write quorum.",
            layer.has_write_quorum() as i32 as f64,
        ),
        prometheus_metric(
            "minio_cluster_nodes_online_total",
            "Number of online nodes in the cluster.",
            1.0,
        ),
        prometheus_metric(
            "minio_cluster_nodes_offline_total",
            "Number of offline nodes in the cluster.",
            0.0,
        ),
        prometheus_metric(
            "minio_cluster_drive_online_total",
            "Number of online drives in the cluster.",
            layer.online_disk_count() as f64,
        ),
        prometheus_metric(
            "minio_cluster_drive_offline_total",
            "Number of offline drives in the cluster.",
            layer.offline_disk_count() as f64,
        ),
        prometheus_metric(
            "minio_cluster_bucket_total",
            "Total number of buckets in the cluster.",
            layer.bucket_count() as f64,
        ),
        prometheus_metric(
            "minio_cluster_usage_object_total",
            "Total number of visible objects in the cluster.",
            layer.visible_object_count() as f64,
        ),
        prometheus_metric(
            "minio_cluster_kms_online",
            "Whether the configured KMS backend is online.",
            kms_metrics.online as i32 as f64,
        ),
        prometheus_metric(
            "minio_cluster_kms_configured",
            "Whether a KMS backend is configured.",
            kms_metrics.configured as i32 as f64,
        ),
        prometheus_metric(
            "minio_cluster_notification_targets_total",
            "Total configured notification targets.",
            admin_state.notification_targets.list().len() as f64,
        ),
        prometheus_metric(
            "minio_cluster_replication_queue_current_count",
            "Current replication queue entries.",
            queue.stats.queued as f64,
        ),
        prometheus_metric(
            "minio_cluster_replication_queue_failed_total",
            "Total permanently failed replication entries.",
            queue.stats.total_failed as f64,
        ),
    ]
}

fn node_prometheus_metrics(
    _admin_state: &ServerAdminState,
    layer: &LocalObjectLayer,
    kms_metrics: &KmsServiceMetrics,
) -> Vec<PrometheusSample> {
    vec![
        prometheus_metric(
            "minio_node_drive_online_total",
            "Number of online drives on this node.",
            layer.online_disk_count() as f64,
        ),
        prometheus_metric(
            "minio_node_drive_offline_total",
            "Number of offline drives on this node.",
            layer.offline_disk_count() as f64,
        ),
        prometheus_metric(
            "minio_node_bucket_total",
            "Total number of buckets visible from this node.",
            layer.bucket_count() as f64,
        ),
        prometheus_metric(
            "minio_node_usage_object_total",
            "Total number of visible objects on this node.",
            layer.visible_object_count() as f64,
        ),
        prometheus_metric(
            "minio_node_kms_online",
            "Whether the configured KMS backend is online for this node.",
            kms_metrics.online as i32 as f64,
        ),
    ]
}

fn bucket_prometheus_metrics(layer: &LocalObjectLayer) -> Vec<PrometheusSample> {
    let mut metrics = Vec::new();
    for (bucket, object_count) in layer.bucket_object_counts() {
        metrics.push(prometheus_labeled_metric(
            "minio_bucket_usage_object_total",
            "Total number of visible objects in the bucket.",
            object_count as f64,
            BTreeMap::from([("bucket".to_string(), bucket)]),
        ));
    }
    if metrics.is_empty() {
        metrics.push(prometheus_metric(
            "minio_bucket_usage_object_total",
            "Total number of visible objects in the bucket.",
            0.0,
        ));
    }
    metrics
}

fn resource_prometheus_metrics(layer: &LocalObjectLayer) -> Vec<PrometheusSample> {
    let mut metrics = Vec::new();
    for (disk, online) in layer.disk_statuses() {
        metrics.push(prometheus_labeled_metric(
            "minio_node_drive_status",
            "Drive online status for this node.",
            online as i32 as f64,
            BTreeMap::from([("disk".to_string(), disk)]),
        ));
    }
    metrics.push(prometheus_metric(
        "minio_node_drive_total",
        "Total number of drives configured on this node.",
        layer.total_disk_count() as f64,
    ));
    metrics
}

fn render_prometheus_metrics(metrics: &[PrometheusSample]) -> String {
    let mut output = String::new();
    let mut described = BTreeSet::new();
    for metric in metrics {
        if described.insert(metric.name) {
            output.push_str("# HELP ");
            output.push_str(metric.name);
            output.push(' ');
            output.push_str(metric.help);
            output.push('\n');
            output.push_str("# TYPE ");
            output.push_str(metric.name);
            output.push_str(" gauge\n");
        }
        output.push_str(metric.name);
        if !metric.labels.is_empty() {
            output.push('{');
            let mut first = true;
            for (label, label_value) in &metric.labels {
                if !first {
                    output.push(',');
                }
                first = false;
                output.push_str(label);
                output.push_str("=\"");
                output.push_str(&label_value.replace('\\', "\\\\").replace('"', "\\\""));
                output.push('"');
            }
            output.push('}');
        }
        output.push(' ');
        output.push_str(&format!("{}\n", metric.value));
    }
    output
}
