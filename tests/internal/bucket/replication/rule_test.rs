use minio_rust::internal::bucket::replication::{parse_config, ObjectOpts, ReplicationType};

#[test]
fn test_metadata_replicate_line_26() {
    let cases = [
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination><SourceSelectionCriteria><ReplicaModifications><Status>Enabled</Status></ReplicaModifications></SourceSelectionCriteria></Rule></ReplicationConfiguration>",
            ObjectOpts {
                name: "c1test".to_owned(),
                op_type: ReplicationType::Object,
                replica: false,
                ..ObjectOpts::default()
            },
            true,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination><SourceSelectionCriteria><ReplicaModifications><Status>Disabled</Status></ReplicaModifications></SourceSelectionCriteria></Rule></ReplicationConfiguration>",
            ObjectOpts {
                name: "c2test".to_owned(),
                op_type: ReplicationType::Object,
                replica: true,
                ..ObjectOpts::default()
            },
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination><SourceSelectionCriteria><ReplicaModifications><Status>Disabled</Status></ReplicaModifications></SourceSelectionCriteria></Rule></ReplicationConfiguration>",
            ObjectOpts {
                name: "c2test".to_owned(),
                op_type: ReplicationType::Object,
                replica: false,
                ..ObjectOpts::default()
            },
            true,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination><SourceSelectionCriteria><ReplicaModifications><Status>Enabled</Status></ReplicaModifications></SourceSelectionCriteria></Rule></ReplicationConfiguration>",
            ObjectOpts {
                name: "c2test".to_owned(),
                op_type: ReplicationType::Metadata,
                replica: true,
                ..ObjectOpts::default()
            },
            true,
        ),
    ];

    for (xml, opts, expected) in cases {
        let config = parse_config(xml).expect("replication config should parse");
        assert_eq!(config.rules[0].metadata_replicate(&opts), expected);
    }
}

#[test]
fn subtest_test_metadata_replicate_fmt_sprintf_test_d_line_60() {
    test_metadata_replicate_line_26();
}
