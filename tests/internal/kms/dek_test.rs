use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use minio_rust::internal::kms::Dek;

pub const SOURCE_FILE: &str = "internal/kms/dek_test.go";

#[test]
fn test_encode_decode_dek() {
    let test_cases = [
        Dek::default(),
        Dek {
            plaintext: None,
            ciphertext: decode_b64("eyJhZWFkIjoiQUVTLTI1Ni1HQ00tSE1BQy1TSEEtMjU2IiwiaXYiOiJ3NmhLUFVNZXVtejZ5UlVZL29pTFVBPT0iLCJub25jZSI6IktMSEU3UE1jRGo2N2UweHkiLCJieXRlcyI6Ik1wUkhjQWJaTzZ1Sm5lUGJGcnpKTkxZOG9pdkxwTmlUcTNLZ0hWdWNGYkR2Y0RlbEh1c1lYT29zblJWVTZoSXIifQ=="),
            ..Dek::default()
        },
        Dek {
            plaintext: Some(decode_b64("GM2UvLXp/X8lzqq0mibFC0LayDCGlmTHQhYLj7qAy7Q=")),
            ciphertext: decode_b64("eyJhZWFkIjoiQUVTLTI1Ni1HQ00tSE1BQy1TSEEtMjU2IiwiaXYiOiJ3NmhLUFVNZXVtejZ5UlVZL29pTFVBPT0iLCJub25jZSI6IktMSEU3UE1jRGo2N2UweHkiLCJieXRlcyI6Ik1wUkhjQWJaTzZ1Sm5lUGJGcnpKTkxZOG9pdkxwTmlUcTNLZ0hWdWNGYkR2Y0RlbEh1c1lYT29zblJWVTZoSXIifQ=="),
            ..Dek::default()
        },
        Dek {
            version: 3,
            plaintext: Some(decode_b64("GM2UvLXp/X8lzqq0mibFC0LayDCGlmTHQhYLj7qAy7Q=")),
            ciphertext: decode_b64("eyJhZWFkIjoiQUVTLTI1Ni1HQ00tSE1BQy1TSEEtMjU2IiwiaXYiOiJ3NmhLUFVNZXVtejZ5UlVZL29pTFVBPT0iLCJub25jZSI6IktMSEU3UE1jRGo2N2UweHkiLCJieXRlcyI6Ik1wUkhjQWJaTzZ1Sm5lUGJGcnpKTkxZOG9pdkxwTmlUcTNLZ0hWdWNGYkR2Y0RlbEh1c1lYT29zblJWVTZoSXIifQ=="),
            ..Dek::default()
        },
    ];

    for (idx, key) in test_cases.into_iter().enumerate() {
        let text = key
            .marshal_text()
            .unwrap_or_else(|err| panic!("case {idx} marshal failed: {err}"));
        let mut decoded = Dek::default();
        decoded
            .unmarshal_text(&text)
            .unwrap_or_else(|err| panic!("case {idx} unmarshal failed: {err}"));
        assert!(decoded.plaintext.is_none(), "case {}", idx);
        assert_eq!(decoded.ciphertext, key.ciphertext, "case {}", idx);
    }
}

fn decode_b64(value: &str) -> Vec<u8> {
    BASE64_STANDARD.decode(value).expect("valid base64 fixture")
}
