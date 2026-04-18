use std::io::Cursor;

use minio_rust::cmd::{decrypt_data, encrypt_data, Credentials};

pub const SOURCE_FILE: &str = "cmd/config-encrypted_test.go";

#[test]
fn test_decrypt_data_line_28() {
    let cred1 = Credentials::new("minio", "minio123");
    let cred2 = Credentials::new("minio", "minio1234");
    let data = b"config data";

    let edata1 = encrypt_data(&cred1, data).expect("encrypt 1");
    let edata2 = encrypt_data(&cred2, data).expect("encrypt 2");

    let tests = [
        (edata1, cred1, true),
        (edata2, cred2, true),
        (data.to_vec(), Credentials::new("minio", "minio123"), false),
    ];

    for (edata, cred, success) in tests {
        let result = decrypt_data(&cred, Cursor::new(edata));
        match (result, success) {
            (Ok(ddata), true) => assert_eq!(ddata, data),
            (Err(_), false) => {}
            (Ok(_), false) => panic!("expected failure, saw success"),
            (Err(err), true) => panic!("expected success, saw failure {err}"),
        }
    }
}
