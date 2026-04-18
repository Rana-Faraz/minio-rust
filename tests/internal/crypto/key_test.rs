use std::io::{self, Read};

use hex::encode as hex_encode;
use minio_rust::internal::crypto::{ObjectKey, ERR_SECRET_KEY_MISMATCH, INSECURE_SEAL_ALGORITHM};

pub const SOURCE_FILE: &str = "internal/crypto/key_test.go";

#[test]
fn test_generate_key() {
    for (idx, (random, should_pass)) in [
        (None, true),
        (Some(FixedReader::new(32)), true),
        (Some(FixedReader::new(32)), true),
        (Some(FixedReader::new(31)), false),
    ]
    .into_iter()
    .enumerate()
    {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut reader = random;
            let key = ObjectKey::generate(
                [0u8; 32].as_slice(),
                reader.as_mut().map(|r| r as &mut dyn Read),
            );
            assert_ne!(key.0, [0u8; 32]);
        }));
        assert_eq!(result.is_ok(), should_pass, "case {}", idx);
    }
}

#[test]
fn test_generate_iv() {
    for (idx, (random, should_pass)) in [
        (None, true),
        (Some(FixedReader::new(32)), true),
        (Some(FixedReader::new(32)), true),
        (Some(FixedReader::new(31)), false),
    ]
    .into_iter()
    .enumerate()
    {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut reader = random;
            let iv = ObjectKey::generate_iv(reader.as_mut().map(|r| r as &mut dyn Read));
            assert_ne!(iv, [0u8; 32]);
        }));
        assert_eq!(result.is_ok(), should_pass, "case {}", idx);
    }
}

#[test]
fn test_seal_unseal_key() {
    let cases = [
        (
            [0u8; 32], [0u8; 32], "SSE-C", "bucket", "object", [0u8; 32], "SSE-C", "bucket",
            "object", true,
        ),
        (
            [0u8; 32], [0u8; 32], "SSE-C", "bucket", "object", [1u8; 32], "SSE-C", "bucket",
            "object", false,
        ),
        (
            [0u8; 32], [0u8; 32], "SSE-S3", "bucket", "object", [0u8; 32], "SSE-C", "bucket",
            "object", false,
        ),
        (
            [0u8; 32], [0u8; 32], "SSE-C", "bucket", "object", [0u8; 32], "SSE-C", "Bucket",
            "object", false,
        ),
        (
            [0u8; 32], [0u8; 32], "SSE-C", "bucket", "object", [0u8; 32], "SSE-C", "bucket",
            "Object", false,
        ),
    ];

    for (idx, case) in cases.into_iter().enumerate() {
        let key = ObjectKey::generate(&case.0, None);
        let sealed = key.seal(&case.0, case.1, case.2, case.3, case.4);
        let mut unsealed = ObjectKey::default();
        let result = unsealed.unseal(&case.5, &sealed, case.6, case.7, case.8);
        assert_eq!(result.is_ok(), case.9, "case {}", idx);
    }

    let key = ObjectKey::generate(&[0u8; 32], None);
    let mut sealed = key.seal(&[0u8; 32], [0u8; 32], "SSE-S3", "bucket", "object");
    sealed.algorithm = INSECURE_SEAL_ALGORITHM.to_owned();
    let mut out = ObjectKey::default();
    let err = out
        .unseal(&[0u8; 32], &sealed, "SSE-S3", "bucket", "object")
        .expect_err("legacy algorithm should fail");
    assert_ne!(err, ERR_SECRET_KEY_MISMATCH);
}

#[test]
fn test_derive_part_key() {
    let key = ObjectKey([0u8; 32]);
    let cases = [
        (
            0u32,
            "aa7855e13839dd767cd5da7c1ff5036540c9264b7a803029315e55375287b4af",
        ),
        (
            1u32,
            "a3e7181c6eed030fd52f79537c56c4d07da92e56d374ff1dd2043350785b37d8",
        ),
        (
            10000u32,
            "f86e65c396ed52d204ee44bd1a0bbd86eb8b01b7354e67a3b3ae0e34dd5bd115",
        ),
    ];

    for (idx, (part_id, expected)) in cases.into_iter().enumerate() {
        let derived = key.derive_part_key(part_id);
        assert_eq!(hex_encode(derived), expected, "case {}", idx);
    }
}

#[test]
fn test_seal_etag() {
    let mut raw = [0u8; 32];
    for (i, byte) in raw.iter_mut().enumerate() {
        *byte = i as u8;
    }
    let key = ObjectKey(raw);
    for (idx, etag) in [
        "",
        "90682b8e8cc7609c",
        "90682b8e8cc7609c4671e1d64c73fc30",
        "90682b8e8cc7609c4671e1d64c73fc307fb3104f",
    ]
    .into_iter()
    .enumerate()
    {
        let bytes = hex::decode(etag).expect("valid etag hex");
        let sealed = key.seal_etag(&bytes);
        let unsealed = key.unseal_etag(&sealed).expect("etag should unseal");
        assert_eq!(unsealed, bytes, "case {}", idx);
    }
}

#[derive(Clone)]
struct FixedReader {
    remaining: usize,
    next: u8,
}

impl FixedReader {
    fn new(remaining: usize) -> Self {
        Self { remaining, next: 1 }
    }
}

impl Read for FixedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let n = self.remaining.min(buf.len());
        for slot in &mut buf[..n] {
            *slot = self.next;
            self.next = self.next.wrapping_add(1);
        }
        self.remaining -= n;
        Ok(n)
    }
}
