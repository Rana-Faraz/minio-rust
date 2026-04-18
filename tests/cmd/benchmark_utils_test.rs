use minio_rust::cmd::{generate_bytes_data, get_random_byte};

pub const SOURCE_FILE: &str = "cmd/benchmark-utils_test.go";

#[test]
fn missing_extracted_entries() {
    let random = get_random_byte();
    assert!(random.is_ascii_alphabetic());

    let bytes = generate_bytes_data(256);
    assert_eq!(bytes.len(), 256);
    assert!(bytes.iter().all(|byte| byte.is_ascii_alphabetic()));
    assert!(bytes.windows(2).all(|pair| pair[0] == pair[1]));
}
