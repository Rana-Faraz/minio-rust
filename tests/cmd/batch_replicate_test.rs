use minio_rust::cmd::BatchJobRequest;

pub const SOURCE_FILE: &str = "cmd/batch-replicate_test.go";

#[test]
fn test_parse_batch_job_replicate_line_27() {
    let replicate_yaml = r#"
replicate:
  apiVersion: v1
  source:
    type: minio
    bucket: mytest
    prefix: object-prefix1
    snowball:
      disable: true
  target:
    type: minio
    bucket: mytest
    prefix: stage
    endpoint: "http://127.0.0.1:9001"
    credentials:
      accessKey: minioadmin
      secretKey: minioadmin
  flags:
    filter:
      newerThan: "7d10h31s"
      olderThan: "7d"
      tags:
         - key: "name"
           value: "pick*"
      metadata:
         - key: "content-type"
           value: "image/*"
"#;

    let job: BatchJobRequest = serde_yaml::from_str(replicate_yaml).expect("parse yaml");
    let replicate = job.replicate.expect("replicate config");
    assert_eq!(replicate.source.bucket, "mytest");
    assert_eq!(
        replicate.source.prefix.f(),
        vec!["object-prefix1".to_string()]
    );

    let multi_prefix_yaml = r#"
replicate:
  apiVersion: v1
  source:
    type: minio
    bucket: mytest
    prefix:
      - object-prefix1
      - object-prefix2
    snowball:
      disable: true
  target:
    type: minio
    bucket: mytest
    prefix: stage
    endpoint: "http://127.0.0.1:9001"
    credentials:
      accessKey: minioadmin
      secretKey: minioadmin
  flags:
    filter:
      newerThan: "7d10h31s"
      olderThan: "7d"
      tags:
         - key: "name"
           value: "pick*"
      metadata:
         - key: "content-type"
           value: "image/*"
"#;

    let multi: BatchJobRequest = serde_yaml::from_str(multi_prefix_yaml).expect("parse yaml");
    let replicate = multi.replicate.expect("replicate config");
    assert_eq!(
        replicate.source.prefix.f(),
        vec!["object-prefix1".to_string(), "object-prefix2".to_string()]
    );
}
