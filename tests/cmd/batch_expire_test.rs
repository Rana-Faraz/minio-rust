use minio_rust::cmd::BatchJobRequest;

pub const SOURCE_FILE: &str = "cmd/batch-expire_test.go";

#[test]
fn test_parse_batch_job_expire_line_27() {
    let expire_yaml = r#"
expire:
  apiVersion: v1
  bucket: mybucket
  prefix: myprefix
  rules:
    - type: object
      name: NAME
      olderThan: 7d10h
      createdBefore: "2006-01-02T15:04:05.00Z"
      tags:
        - key: name
          value: pick*
      metadata:
        - key: content-type
          value: image/*
      size:
        lessThan: "10MiB"
        greaterThan: 1MiB
      purge:
    - type: deleted
      name: NAME
      olderThan: 10h
      createdBefore: "2006-01-02T15:04:05.00Z"
      purge:
  notify:
    endpoint: https://notify.endpoint
    token: Bearer xxxxx
  retry:
    attempts: 10
    delay: 500ms
"#;
    let job: BatchJobRequest = serde_yaml::from_str(expire_yaml).expect("parse expire yaml");
    assert_eq!(
        job.expire.expect("expire job").prefix.f(),
        vec!["myprefix".to_string()]
    );

    let multi_prefix_yaml = r#"
expire:
  apiVersion: v1
  bucket: mybucket
  prefix:
    - myprefix
    - myprefix1
  rules:
    - type: object
      name: NAME
      olderThan: 7d10h
      createdBefore: "2006-01-02T15:04:05.00Z"
      tags:
        - key: name
          value: pick*
      metadata:
        - key: content-type
          value: image/*
      size:
        lessThan: "10MiB"
        greaterThan: 1MiB
      purge:
    - type: deleted
      name: NAME
      olderThan: 10h
      createdBefore: "2006-01-02T15:04:05.00Z"
      purge:
  notify:
    endpoint: https://notify.endpoint
    token: Bearer xxxxx
  retry:
    attempts: 10
    delay: 500ms
"#;
    let job: BatchJobRequest =
        serde_yaml::from_str(multi_prefix_yaml).expect("parse multi-prefix yaml");
    assert_eq!(
        job.expire.expect("expire job").prefix.f(),
        vec!["myprefix".to_string(), "myprefix1".to_string()]
    );
}
