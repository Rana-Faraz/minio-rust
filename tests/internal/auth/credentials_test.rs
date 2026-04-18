use minio_rust::internal::auth::{
    self, create_credentials, exp_to_int64, get_new_credentials, is_access_key_valid,
    is_secret_key_valid, Credentials, Error, ACCESS_KEY_MIN_LEN, ALPHA_NUMERIC_TABLE,
    SECRET_KEY_MIN_LEN,
};
use serde_json::Number;
use std::time::Duration;

pub const SOURCE_FILE: &str = "internal/auth/credentials_test.go";

#[test]
fn exp_to_int64_matches_reference_cases() {
    assert!(exp_to_int64("").is_err());
    assert!(exp_to_int64("-1").is_err());
    assert_eq!(
        exp_to_int64("1574812326").expect("string parse"),
        1_574_812_326
    );
    assert_eq!(
        exp_to_int64(1_574_812_326_f64).expect("f64 parse"),
        1_574_812_326
    );
    assert_eq!(
        exp_to_int64(1_574_812_326_i64).expect("i64 parse"),
        1_574_812_326
    );
    assert_eq!(
        exp_to_int64(1_574_812_326_i32).expect("i32 parse"),
        1_574_812_326
    );
    assert_eq!(
        exp_to_int64(1_574_812_326_u32).expect("u32 parse"),
        1_574_812_326
    );
    assert_eq!(
        exp_to_int64(1_574_812_326_u64).expect("u64 parse"),
        1_574_812_326
    );
    assert_eq!(
        exp_to_int64(Number::from(1_574_812_326_i64)).expect("json number parse"),
        1_574_812_326
    );
    assert!(exp_to_int64(Duration::from_secs(3 * 60)).is_ok());
}

#[test]
fn access_key_validation_matches_reference_cases() {
    assert!(is_access_key_valid(
        &ALPHA_NUMERIC_TABLE[..ACCESS_KEY_MIN_LEN]
    ));
    assert!(is_access_key_valid(
        &ALPHA_NUMERIC_TABLE[..ACCESS_KEY_MIN_LEN + 1]
    ));
    assert!(!is_access_key_valid(
        &ALPHA_NUMERIC_TABLE[..ACCESS_KEY_MIN_LEN - 1]
    ));
}

#[test]
fn secret_key_validation_matches_reference_cases() {
    let long_secret = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    assert!(is_secret_key_valid(&long_secret[..SECRET_KEY_MIN_LEN]));
    assert!(is_secret_key_valid(&long_secret[..SECRET_KEY_MIN_LEN + 1]));
    assert!(!is_secret_key_valid(&long_secret[..SECRET_KEY_MIN_LEN - 1]));
}

#[test]
fn get_new_credentials_returns_valid_credentials() {
    let credentials = get_new_credentials().expect("expected fresh credentials");
    assert!(credentials.is_valid());
    assert_eq!(credentials.access_key.len(), auth::ACCESS_KEY_MAX_LEN);
    assert_eq!(credentials.secret_key.len(), auth::SECRET_KEY_MAX_LEN);
}

#[test]
fn create_credentials_matches_reference_cases() {
    let valid = create_credentials(&ALPHA_NUMERIC_TABLE[..ACCESS_KEY_MIN_LEN], "abcdefgh")
        .expect("expected valid credentials");
    assert!(valid.is_valid());

    assert_eq!(
        create_credentials(&ALPHA_NUMERIC_TABLE[..ACCESS_KEY_MIN_LEN - 1], "abcdefgh")
            .expect_err("short access key must fail"),
        Error::InvalidAccessKeyLength
    );
    assert_eq!(
        create_credentials(&ALPHA_NUMERIC_TABLE[..ACCESS_KEY_MIN_LEN], "abcdefg")
            .expect_err("short secret key must fail"),
        Error::InvalidSecretKeyLength
    );
}

#[test]
fn credentials_equal_matches_reference_cases() {
    let first = get_new_credentials().expect("first credentials");
    let second = get_new_credentials().expect("second credentials");

    assert!(first.equal(&first));
    assert!(!first.equal(&Credentials {
        access_key: String::new(),
        secret_key: String::new(),
        session_token: String::new(),
        expiration: None,
        status: auth::AccountStatus::On,
    }));
    assert!(!first.equal(&second));
    assert!(!first.equal(&Credentials {
        access_key: "myuser".to_owned(),
        secret_key: first.secret_key.clone(),
        session_token: String::new(),
        expiration: None,
        status: auth::AccountStatus::On,
    }));
    assert!(!first.equal(&Credentials {
        access_key: first.access_key.clone(),
        secret_key: "mypassword".to_owned(),
        session_token: String::new(),
        expiration: None,
        status: auth::AccountStatus::On,
    }));
}
