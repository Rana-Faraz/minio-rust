use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use minio_rust::internal::kms::{self, Context, DecryptRequest, GenerateKeyRequest};

pub const SOURCE_FILE: &str = "internal/kms/secret-key_test.go";

#[test]
fn test_single_key_roundtrip() {
    let kms = kms::parse_secret_key("my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=")
        .expect("kms must initialize");

    let key = kms
        .generate_key(&GenerateKeyRequest {
            name: "my-key".to_owned(),
            associated_data: Context::default(),
        })
        .expect("key generation should succeed");
    let plaintext = kms
        .decrypt(&DecryptRequest {
            name: key.key_id.clone(),
            ciphertext: key.ciphertext.clone(),
            ..DecryptRequest::default()
        })
        .expect("key decryption should succeed");

    assert_eq!(key.plaintext.as_deref(), Some(plaintext.as_slice()));
}

#[test]
fn test_decrypt_key() {
    let kms = kms::parse_secret_key("my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=")
        .expect("kms must initialize");

    let test_cases = [
        (
            "my-key",
            "zmS7NrG765UZ0ZN85oPjybelxqVvpz01vxsSpOISy2M=",
            r#"{"aead":"ChaCha20Poly1305","iv":"JbI+vwvYww1lCb5VpkAFuQ==","nonce":"ARjIjJxBSD541Gz8","bytes":"KCbEc2sA0TLvA7aWTWa23AdccVfJMpOxwgG8hm+4PaNrxYfy1xFWZg2gEenVrOgv"}"#,
            Context::default(),
        ),
        (
            "my-key",
            "UnPWsZgVI+T4L9WGNzFlP1PsP1Z6hn2Fx8ISeZfDGnA=",
            r#"{"aead":"ChaCha20Poly1305","iv":"r4+yfiVbVIYR0Z2I9Fq+6g==","nonce":"2YpwGwE59GcVraI3","bytes":"k/svMglOU7/Kgwv73heG38NWW575XLcFp3SaxQHDMjJGYyRI3Fiygu2OeutGPXNL"}"#,
            Context::from([("key", "value")]),
        ),
    ];

    for (idx, (key_id, plaintext, ciphertext, context)) in test_cases.into_iter().enumerate() {
        let expected = BASE64_STANDARD
            .decode(plaintext)
            .unwrap_or_else(|err| panic!("case {idx} plaintext base64 invalid: {err}"));
        let actual = kms
            .decrypt(&DecryptRequest {
                name: key_id.to_owned(),
                ciphertext: ciphertext.as_bytes().to_vec(),
                associated_data: context,
                ..DecryptRequest::default()
            })
            .unwrap_or_else(|err| panic!("case {idx} decrypt failed: {err}"));
        assert_eq!(actual, expected, "case {}", idx);
    }
}
