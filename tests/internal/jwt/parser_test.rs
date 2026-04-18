use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha512;

use minio_rust::internal::jwt::{
    parse_with_claims, parse_with_standard_claims, MapClaims, StandardClaims, ValidationError,
    ValidationErrorKind,
};

pub const SOURCE_FILE: &str = "internal/jwt/parser_test.go";

fn default_key_func(_: &MapClaims) -> Result<Vec<u8>, ValidationError> {
    Ok(b"HelloSecret".to_vec())
}

fn empty_key_func(_: &MapClaims) -> Result<Vec<u8>, ValidationError> {
    Ok(Vec::new())
}

fn error_key_func(_: &MapClaims) -> Result<Vec<u8>, ValidationError> {
    Err(ValidationError {
        kind: ValidationErrorKind::Unverifiable,
        message: "error loading key".to_owned(),
    })
}

fn sign_hs512(claims: serde_json::Value) -> String {
    let header = json!({"typ":"JWT","alg":"HS512"});
    let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).expect("header should encode"));
    let payload =
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("payload should encode"));
    let signing_input = format!("{header}.{payload}");
    let mut mac =
        <Hmac<Sha512> as Mac>::new_from_slice(b"HelloSecret").expect("hmac should initialize");
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signing_input}.{signature}")
}

#[test]
fn parser_parse_matches_reference_cases() {
    let now = chrono::Utc::now().timestamp();
    let basic_token = sign_hs512(json!({
        "foo":"bar",
        "sub":"test",
        "accessKey":"test"
    }));
    let expired_token = sign_hs512(json!({
        "foo":"bar",
        "sub":"test",
        "accessKey":"test",
        "exp": now - 100
    }));
    let nbf_token = sign_hs512(json!({
        "foo":"bar",
        "sub":"test",
        "accessKey":"test",
        "nbf": now + 100
    }));
    let expired_nbf_token = sign_hs512(json!({
        "foo":"bar",
        "sub":"test",
        "accessKey":"test",
        "nbf": now + 100,
        "exp": now - 100
    }));
    let standard_token = sign_hs512(json!({
        "sub":"test",
        "accessKey":"test",
        "exp": now + 10
    }));

    let tampered = format!("{}x", basic_token);
    let rsa_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJmb28iOiJiYXIifQ.EhkiHkoESI_cG3NPigFrxEk9Z60_oXrOT2vGm9Pn6RDgYNovYORQmmA0zs1AoAOf09ly2Nx2YAg6ABqAYga1AcMFkJljwxTT5fYphTuqpWdy4BELeSYJx5Ty2gmr8e7RonuUztrdD5WfPqLKMm1Ozp_T6zALpRmwTIW0QPnaBXaQD90FplAg46Iy1UlDKr-Eupy0i5SLch5Q-p2ZpaL_5fnTIUDlxC3pWhJTyx_71qDI-mAA_5lE_VdroOeflG56sSmDxopPEG3bFlSu1eowyBfxtu0_CuVd-M42RU75Zc4Gsj6uV77MBtbMrf4_7M_NUTSgoIF3fRqxrj0NzihIBg";

    let map_cases = [
        (
            "basic",
            basic_token.as_str(),
            Some(default_key_func as fn(&MapClaims) -> _),
            true,
        ),
        (
            "basic expired",
            expired_token.as_str(),
            Some(default_key_func as fn(&MapClaims) -> _),
            false,
        ),
        (
            "basic nbf",
            nbf_token.as_str(),
            Some(default_key_func as fn(&MapClaims) -> _),
            false,
        ),
        (
            "expired and nbf",
            expired_nbf_token.as_str(),
            Some(default_key_func as fn(&MapClaims) -> _),
            false,
        ),
        (
            "basic invalid",
            tampered.as_str(),
            Some(default_key_func as fn(&MapClaims) -> _),
            false,
        ),
        ("basic nokeyfunc", rsa_token, None, false),
        (
            "basic nokey",
            rsa_token,
            Some(empty_key_func as fn(&MapClaims) -> _),
            false,
        ),
        (
            "basic errorkey",
            rsa_token,
            Some(error_key_func as fn(&MapClaims) -> _),
            false,
        ),
    ];

    for (name, token, key_fn, valid) in map_cases {
        let mut claims = MapClaims::new();
        let err = parse_with_claims(token, &mut claims, key_fn).err();
        assert_eq!(err.is_none(), valid, "case {name}");
        if !valid {
            assert!(err.is_some(), "invalid case {name} should fail");
        }
    }

    let mut standard_claims = StandardClaims::new();
    let standard_result =
        parse_with_standard_claims(&standard_token, &mut standard_claims, b"HelloSecret");
    assert!(standard_result.is_ok());
}

#[test]
fn parser_parse_subcases_match_reference_cases() {
    let now = chrono::Utc::now().timestamp();
    let token = sign_hs512(json!({
        "foo":"bar",
        "sub":"test",
        "accessKey":"test",
        "exp": now + 60
    }));

    let mut claims = MapClaims::new();
    parse_with_claims(&token, &mut claims, Some(default_key_func))
        .expect("valid token should parse");
    assert_eq!(claims.access_key, "test");
    assert_eq!(claims.lookup("foo").as_deref(), Some("bar"));
}
