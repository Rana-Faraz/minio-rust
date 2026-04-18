use std::io::Cursor;

use minio_rust::internal::bucket::encryption::{
    parse_bucket_sse_config, BucketSseConfig, EncryptionAction, Rule, AES256, AWS_KMS, XML_NS,
};

#[test]
fn parse_bucket_sse_config_matches_reference_cases() {
    let actual_aes256_no_ns_config = BucketSseConfig {
        xmlns: String::new(),
        rules: vec![Rule {
            default_encryption_action: EncryptionAction {
                algorithm: AES256.to_owned(),
                master_key_id: String::new(),
            },
        }],
    };

    let actual_aes256_config = BucketSseConfig {
        xmlns: XML_NS.to_owned(),
        rules: vec![Rule {
            default_encryption_action: EncryptionAction {
                algorithm: AES256.to_owned(),
                master_key_id: String::new(),
            },
        }],
    };

    let actual_kms_config = BucketSseConfig {
        xmlns: XML_NS.to_owned(),
        rules: vec![Rule {
            default_encryption_action: EncryptionAction {
                algorithm: AWS_KMS.to_owned(),
                master_key_id: "arn:aws:kms:my-minio-key".to_owned(),
            },
        }],
    };

    let test_cases = [
        (
            r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#,
            None,
            true,
            Some(actual_aes256_config.clone()),
            "",
        ),
        (
            r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>arn:aws:kms:my-minio-key</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#,
            None,
            true,
            Some(actual_kms_config.clone()),
            "my-minio-key",
        ),
        (
            r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#,
            Some("only one server-side encryption rule is allowed at a time"),
            false,
            None,
            "",
        ),
        (
            r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm><KMSMasterKeyID>arn:aws:kms:us-east-1:1234/5678example</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#,
            Some("MasterKeyID is allowed with aws:kms only"),
            false,
            None,
            "",
        ),
        (
            r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#,
            Some("MasterKeyID is missing with aws:kms"),
            false,
            None,
            "",
        ),
        (
            r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>InvalidAlgorithm</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#,
            Some("Unknown SSE algorithm"),
            false,
            None,
            "",
        ),
        (
            r#"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#,
            None,
            true,
            Some(actual_aes256_no_ns_config.clone()),
            "",
        ),
        (
            r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID> arn:aws:kms:my-minio-key </KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#,
            Some("MasterKeyID contains unsupported characters"),
            false,
            None,
            "",
        ),
    ];

    for (index, (input_xml, expected_err, should_pass, expected_config, key_id)) in
        test_cases.into_iter().enumerate()
    {
        let result = parse_bucket_sse_config(Cursor::new(input_xml));

        if should_pass {
            let config = result.unwrap_or_else(|err| {
                panic!("test case {} should succeed but got {}", index + 1, err)
            });
            if !key_id.is_empty() {
                assert_eq!(config.key_id(), key_id, "test case {}", index + 1);
            }

            let expected_xml = expected_config
                .expect("successful case should have expected config")
                .to_xml()
                .expect("xml serialization should succeed");
            assert_eq!(expected_xml, input_xml, "test case {}", index + 1);
        } else {
            let err = result.expect_err("test case should fail");
            assert_eq!(
                err.to_string(),
                expected_err.expect("failing case should have expected error"),
                "test case {}",
                index + 1
            );
        }
    }
}
