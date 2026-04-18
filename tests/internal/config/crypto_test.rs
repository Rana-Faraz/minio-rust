use std::io::Read;

use minio_rust::internal::config::crypto::{decrypt, encrypt};
use minio_rust::internal::kms::{new_builtin, Context};

#[test]
fn encrypt_decrypt_matches_reference_cases() {
    let key = hex::decode("ddedadb867afa3f73bd33c25499a723ed7f9f51172ee7b1b679e08dc795debcc")
        .expect("master key hex should decode");
    let kms = new_builtin("my-key", &key).expect("builtin kms should be created");

    let cases = [
        (Vec::new(), Context::default()),
        (vec![1], Context::default()),
        (vec![1], Context::from([("key", "value")])),
        (
            vec![0u8; 1 << 20],
            Context::from([("key", "value"), ("a", "b")]),
        ),
        (vec![0u8; 1024], Context::from([("key", "value")])),
        (vec![0u8; 512 * 1024], Context::from([("key", "value")])),
        (
            vec![0u8; 10 * 1024 * 1024],
            Context::from([("key", "value")]),
        ),
    ];

    for (index, (input, context)) in cases.into_iter().enumerate() {
        let ciphertext = encrypt(&kms, std::io::Cursor::new(&input), context.clone())
            .unwrap_or_else(|err| panic!("case {} encrypt failed: {}", index + 1, err));
        let plaintext = decrypt(&kms, ciphertext, context)
            .unwrap_or_else(|err| panic!("case {} decrypt failed: {}", index + 1, err));
        let mut plaintext = plaintext;
        let mut output = Vec::new();
        plaintext
            .read_to_end(&mut output)
            .unwrap_or_else(|err| panic!("case {} plaintext read failed: {}", index + 1, err));
        assert_eq!(output, input, "case {}", index + 1);
    }
}
