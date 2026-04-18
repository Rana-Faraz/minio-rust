use std::io::Cursor;

use minio_rust::cmd::validate_bucket_sse_config;

pub const SOURCE_FILE: &str = "cmd/bucket-encryption_test.go";

#[test]
fn test_validate_bucket_sseconfig_line_25() {
    let cases = [
        (
            r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Rule>
            <ApplyServerSideEncryptionByDefault>
            <SSEAlgorithm>AES256</SSEAlgorithm>
            </ApplyServerSideEncryptionByDefault>
            </Rule>
            </ServerSideEncryptionConfiguration>"#,
            true,
        ),
        (
            r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Rule>
            <ApplyServerSideEncryptionByDefault>
            <SSEAlgorithm>aws:kms</SSEAlgorithm>
            <KMSMasterKeyID>my-key</KMSMasterKeyID>
            </ApplyServerSideEncryptionByDefault>
            </Rule>
            </ServerSideEncryptionConfiguration>"#,
            true,
        ),
    ];

    for (xml, should_pass) in cases {
        let result = validate_bucket_sse_config(Cursor::new(xml.as_bytes()));
        assert_eq!(result.is_ok(), should_pass);
    }
}
