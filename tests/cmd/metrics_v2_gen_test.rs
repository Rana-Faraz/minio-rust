use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;

use minio_rust::cmd::{MetricDescription, MetricV2, MetricsGroupOpts, MetricsGroupV2};

pub const SOURCE_FILE: &str = "cmd/metrics-v2_gen_test.go";

trait CmdCodec: Default + Clone + PartialEq + Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
    fn msgsize(&self) -> usize;
}

macro_rules! impl_cmd_codec {
    ($ty:ty) => {
        impl CmdCodec for $ty {
            fn marshal_msg(&self) -> Result<Vec<u8>, String> {
                <$ty>::marshal_msg(self)
            }
            fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
                <$ty>::unmarshal_msg(self, bytes)
            }
            fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String> {
                <$ty>::encode(self, writer)
            }
            fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String> {
                <$ty>::decode(self, reader)
            }
            fn msgsize(&self) -> usize {
                <$ty>::msgsize(self)
            }
        }
    };
}

impl_cmd_codec!(MetricDescription);
impl_cmd_codec!(MetricV2);
impl_cmd_codec!(MetricsGroupOpts);
impl_cmd_codec!(MetricsGroupV2);

fn assert_roundtrip<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = T::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

fn assert_encode_decode<T: CmdCodec>(value: T) {
    let mut buf = Cursor::new(Vec::new());
    value.encode(&mut buf).expect("encode");
    assert!(value.msgsize() >= buf.get_ref().len());
    buf.set_position(0);
    let mut decoded = T::default();
    decoded.decode(&mut buf).expect("decode");
    assert_eq!(decoded, value);
}

fn exercise_benchmark_smoke<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    assert!(value.msgsize() >= bytes.len());
    for _ in 0..100 {
        let mut decoded = T::default();
        let left = decoded.unmarshal_msg(&bytes).expect("decode");
        assert!(left.is_empty());
    }
}

fn sample_metric_description() -> MetricDescription {
    MetricDescription {
        namespace: "cluster".to_string(),
        subsystem: "storage".to_string(),
        name: "free_bytes".to_string(),
        help: "Free bytes in the cluster".to_string(),
        metric_type: "gauge".to_string(),
    }
}

fn sample_metric_v2() -> MetricV2 {
    MetricV2 {
        description: sample_metric_description(),
        static_labels: Some(BTreeMap::from([(
            "server".to_string(),
            "node-a".to_string(),
        )])),
        value: 42.5,
        variable_labels: Some(BTreeMap::from([(
            "bucket".to_string(),
            "photos".to_string(),
        )])),
        histogram_bucket_label: "le".to_string(),
        histogram: Some(BTreeMap::from([
            ("0.5".to_string(), 4),
            ("1.0".to_string(), 7),
        ])),
    }
}

fn sample_metrics_group_opts() -> MetricsGroupOpts {
    MetricsGroupOpts {
        depend_global_object_api: true,
        depend_global_auth_n_plugin: true,
        depend_global_site_replication_sys: false,
        depend_global_notification_sys: true,
        depend_global_kms: false,
        bucket_only: true,
        depend_global_lambda_target_list: false,
        depend_global_iam_sys: true,
        depend_global_lock_server: false,
        depend_global_is_dist_erasure: true,
        depend_global_background_heal_state: true,
        depend_bucket_target_sys: false,
    }
}

fn sample_metrics_group_v2() -> MetricsGroupV2 {
    MetricsGroupV2 {
        cache_interval_nanos: 30_000_000_000,
        metrics_group_opts: sample_metrics_group_opts(),
    }
}

#[test]
fn test_marshal_unmarshal_metric_description_line_11() {
    assert_roundtrip(sample_metric_description());
}

#[test]
fn benchmark_marshal_msg_metric_description_line_34() {
    exercise_benchmark_smoke(sample_metric_description());
}

#[test]
fn benchmark_append_msg_metric_description_line_43() {
    assert_encode_decode(sample_metric_description());
}

#[test]
fn benchmark_unmarshal_metric_description_line_55() {
    exercise_benchmark_smoke(sample_metric_description());
}

#[test]
fn test_marshal_unmarshal_metric_v2_line_69() {
    assert_roundtrip(sample_metric_v2());
}

#[test]
fn benchmark_marshal_msg_metric_v2_line_92() {
    exercise_benchmark_smoke(sample_metric_v2());
}

#[test]
fn benchmark_append_msg_metric_v2_line_101() {
    assert_encode_decode(sample_metric_v2());
}

#[test]
fn benchmark_unmarshal_metric_v2_line_113() {
    exercise_benchmark_smoke(sample_metric_v2());
}

#[test]
fn test_marshal_unmarshal_metrics_group_opts_line_127() {
    assert_roundtrip(sample_metrics_group_opts());
}

#[test]
fn benchmark_marshal_msg_metrics_group_opts_line_150() {
    exercise_benchmark_smoke(sample_metrics_group_opts());
}

#[test]
fn benchmark_append_msg_metrics_group_opts_line_159() {
    assert_encode_decode(sample_metrics_group_opts());
}

#[test]
fn benchmark_unmarshal_metrics_group_opts_line_171() {
    exercise_benchmark_smoke(sample_metrics_group_opts());
}

#[test]
fn test_marshal_unmarshal_metrics_group_v2_line_185() {
    assert_roundtrip(sample_metrics_group_v2());
}

#[test]
fn benchmark_marshal_msg_metrics_group_v2_line_208() {
    exercise_benchmark_smoke(sample_metrics_group_v2());
}

#[test]
fn benchmark_append_msg_metrics_group_v2_line_217() {
    assert_encode_decode(sample_metrics_group_v2());
}

#[test]
fn benchmark_unmarshal_metrics_group_v2_line_229() {
    exercise_benchmark_smoke(sample_metrics_group_v2());
}
