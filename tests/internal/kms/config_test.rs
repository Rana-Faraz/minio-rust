use std::collections::HashMap;

use minio_rust::internal::kms;

pub const SOURCE_FILE: &str = "internal/kms/config_test.go";

#[test]
fn test_is_present() {
    let test_cases = [
        (HashMap::new(), false, false),
        (
            HashMap::from([(
                kms::ENV_KMS_SECRET_KEY.to_owned(),
                "minioy-default-key:6jEQjjMh8iPq8/gqgb4eMDIZFOtPACIsr9kO+vx8JFs=".to_owned(),
            )]),
            true,
            false,
        ),
        (
            HashMap::from([
                (
                    kms::ENV_KMS_ENDPOINT.to_owned(),
                    "https://127.0.0.1:7373".to_owned(),
                ),
                (kms::ENV_KMS_DEFAULT_KEY.to_owned(), "minio-key".to_owned()),
                (kms::ENV_KMS_ENCLAVE.to_owned(), "demo".to_owned()),
                (
                    kms::ENV_KMS_API_KEY.to_owned(),
                    "k1:MBDtmC9ZAf3Wi4-oGglgKx_6T1jwJfct1IC15HOxetg".to_owned(),
                ),
            ]),
            true,
            false,
        ),
        (
            HashMap::from([
                (
                    kms::ENV_KES_ENDPOINT.to_owned(),
                    "https://127.0.0.1:7373".to_owned(),
                ),
                (kms::ENV_KES_DEFAULT_KEY.to_owned(), "minio-key".to_owned()),
                (
                    kms::ENV_KES_API_KEY.to_owned(),
                    "kes:v1:AGtR4PvKXNjz+/MlBX2Djg0qxwS3C4OjoDzsuFSQr82e".to_owned(),
                ),
            ]),
            true,
            false,
        ),
        (
            HashMap::from([
                (
                    kms::ENV_KES_ENDPOINT.to_owned(),
                    "https://127.0.0.1:7373".to_owned(),
                ),
                (kms::ENV_KES_DEFAULT_KEY.to_owned(), "minio-key".to_owned()),
                (
                    kms::ENV_KES_CLIENT_KEY.to_owned(),
                    "/tmp/client.key".to_owned(),
                ),
                (
                    kms::ENV_KES_CLIENT_CERT.to_owned(),
                    "/tmp/client.crt".to_owned(),
                ),
            ]),
            true,
            false,
        ),
        (
            HashMap::from([
                (
                    kms::ENV_KMS_ENDPOINT.to_owned(),
                    "https://127.0.0.1:7373".to_owned(),
                ),
                (
                    kms::ENV_KES_ENDPOINT.to_owned(),
                    "https://127.0.0.1:7373".to_owned(),
                ),
            ]),
            false,
            true,
        ),
        (
            HashMap::from([
                (
                    kms::ENV_KMS_ENDPOINT.to_owned(),
                    "https://127.0.0.1:7373".to_owned(),
                ),
                (
                    kms::ENV_KMS_SECRET_KEY.to_owned(),
                    "minioy-default-key:6jEQjjMh8iPq8/gqgb4eMDIZFOtPACIsr9kO+vx8JFs=".to_owned(),
                ),
            ]),
            false,
            true,
        ),
        (
            HashMap::from([
                (kms::ENV_KMS_ENCLAVE.to_owned(), "foo".to_owned()),
                (
                    kms::ENV_KES_SERVER_CA.to_owned(),
                    "/etc/minio/certs".to_owned(),
                ),
            ]),
            false,
            true,
        ),
    ];

    for (idx, (env, expected, should_fail)) in test_cases.into_iter().enumerate() {
        let result = kms::is_present_in(&env);
        match (result, should_fail) {
            (Ok(value), false) => assert_eq!(value, expected, "case {}", idx),
            (Err(_), true) => {}
            (Ok(value), true) => panic!("case {idx} should fail but returned {value}"),
            (Err(err), false) => panic!("case {idx} failed unexpectedly: {err}"),
        }
    }
}
