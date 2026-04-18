use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::{json, Map, Value};
use sha2::Sha256;

use minio_rust::internal::config::identity::openid::jwt::{
    dummy_role_arn, get_default_expiration, update_claims_expiry, Config, DiscoveryDoc, Provider,
    ProviderCfg, PublicKeys, KEYCLOAK_ADMIN_URL, KEYCLOAK_REALM, VENDOR,
};
use minio_rust::internal::jwt::{parse_with_claims, MapClaims};

pub const SOURCE_FILE: &str = "internal/config/identity/openid/jwt_test.go";

fn sign_hs256(claims: Value, kid: &str, secret: &[u8]) -> String {
    let header = json!({
        "typ": "JWT",
        "alg": "HS256",
        "kid": kid,
    });
    let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).expect("header should encode"));
    let payload =
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("payload should encode"));
    let signing_input = format!("{header}.{payload}");
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("hmac should initialize");
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signing_input}.{signature}")
}

fn static_key_func(_: &MapClaims) -> Result<Vec<u8>, minio_rust::internal::jwt::ValidationError> {
    Ok(b"HelloSecret".to_vec())
}

#[test]
fn update_claims_expiry_matches_reference_cases() {
    let cases = [
        ("empty", update_claims_expiry("", ""), true),
        ("negative string", update_claims_expiry("0", "-1"), true),
        (
            "negative with duration",
            update_claims_expiry("900", "-1"),
            true,
        ),
        ("string", update_claims_expiry("900", "1574812326"), false),
        ("i64", update_claims_expiry("900", 1_574_812_326_i64), false),
        ("i32", update_claims_expiry("900", 1_574_812_326_i32), false),
        ("u32", update_claims_expiry("900", 1_574_812_326_u32), false),
        ("u64", update_claims_expiry("900", 1_574_812_326_u64), false),
        (
            "json number",
            update_claims_expiry("900", serde_json::Number::from(1_574_812_326_i64)),
            false,
        ),
        (
            "float",
            update_claims_expiry("900", 1_574_812_326_f64),
            false,
        ),
        (
            "duration",
            update_claims_expiry("900", Duration::from_secs(180)),
            false,
        ),
    ];

    for (name, result, expected_failure) in cases {
        assert_eq!(result.is_err(), expected_failure, "case {name}");
    }
}

#[test]
fn jwt_hmac_type_matches_reference_case() {
    let secret = b"WNGvKVyyNmXq0TraSvjaDN9CtpFgx35IXtGEffMCPR0";
    let client_id = "76b95ae5-33ef-4283-97b7-d2a85dc2d8f4";
    let token = sign_hs256(
        json!({
            "exp": 253428928061_i64,
            "aud": client_id,
            "sub": "test-user",
            "accessKey": "test-user"
        }),
        client_id,
        secret,
    );

    let mut pub_keys = PublicKeys::default();
    pub_keys.add_hmac(client_id, secret.to_vec());
    assert_eq!(pub_keys.len(), 1);

    let provider = ProviderCfg {
        client_id: client_id.to_owned(),
        client_secret: String::from_utf8(secret.to_vec()).expect("secret should be utf8"),
        jwks_url: Some("http://127.0.0.1/jwks".to_owned()),
        ..ProviderCfg::default()
    };

    let role_arn = dummy_role_arn();
    let cfg = Config {
        enabled: true,
        pub_keys,
        arn_provider_cfgs_map: HashMap::from([(role_arn.clone(), provider.clone())]),
        provider_cfgs: HashMap::from([("1".to_owned(), provider)]),
    };

    let mut claims = Map::new();
    cfg.validate(&role_arn, &token, "", "", &mut claims)
        .expect("valid HMAC JWT should validate");
    assert_eq!(claims.get("aud").and_then(Value::as_str), Some(client_id));
}

#[test]
fn jwt_invalid_token_matches_reference_case() {
    let jsonkey = r#"{"keys":
       [
         {"kty":"RSA",
          "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
          "e":"AQAB",
          "alg":"RS256",
          "kid":"2011-04-29"}
       ]
     }"#;

    let mut pub_keys = PublicKeys::default();
    pub_keys
        .parse_and_add(std::io::Cursor::new(jsonkey.as_bytes()))
        .expect("jwks should parse");
    assert_eq!(pub_keys.len(), 1);

    let role_arn = dummy_role_arn();
    let provider = ProviderCfg {
        jwks_url: Some("http://127.0.0.1:8443".to_owned()),
        ..ProviderCfg::default()
    };
    let cfg = Config {
        enabled: true,
        pub_keys,
        arn_provider_cfgs_map: HashMap::from([(role_arn.clone(), provider.clone())]),
        provider_cfgs: HashMap::from([("1".to_owned(), provider)]),
    };

    let mut claims = Map::new();
    let err = cfg.validate(&role_arn, "invalid", "", "", &mut claims);
    assert!(err.is_err(), "invalid token should fail validation");
}

#[test]
fn default_expiry_duration_matches_reference_cases() {
    let cases = [
        ("", Some(Duration::from_secs(3600))),
        ("9", None),
        ("31536001", None),
        ("800", None),
        ("901", Some(Duration::from_secs(901))),
    ];

    for (input, expected) in cases {
        let result = get_default_expiration(input);
        match expected {
            Some(expected) => assert_eq!(
                result.expect("duration should parse"),
                expected,
                "input {input}"
            ),
            None => assert!(result.is_err(), "input {input} should fail"),
        }
    }
}

#[test]
fn exp_correct_matches_reference_case() {
    let secret = b"HelloSecret";
    let updated_exp =
        update_claims_expiry("3600", Duration::from_secs(60)).expect("expiry should be updated");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be after unix epoch")
        .as_secs() as i64;
    assert!(
        updated_exp >= now + 3500,
        "updated expiry should be extended"
    );

    let token = sign_hs256(
        json!({
            "sub": "test-access",
            "accessKey": "test-access",
            "exp": updated_exp
        }),
        "ignored-kid",
        secret,
    );

    let mut claims = MapClaims::new();
    parse_with_claims(&token, &mut claims, Some(static_key_func))
        .expect("token with updated expiry should parse");
    assert_eq!(claims.lookup("accessKey").as_deref(), Some("test-access"));
}

#[test]
fn keycloak_provider_initialization_matches_reference_case() {
    let mut provider = ProviderCfg {
        discovery_doc: DiscoveryDoc {
            token_endpoint: "http://keycloak.test/token/endpoint".to_owned(),
            ..DiscoveryDoc::default()
        },
        ..ProviderCfg::default()
    };

    let kvs = HashMap::from([
        (VENDOR.to_owned(), "keycloak".to_owned()),
        (KEYCLOAK_REALM.to_owned(), "TestRealm".to_owned()),
        (
            KEYCLOAK_ADMIN_URL.to_owned(),
            "http://keycloak.test/auth/admin".to_owned(),
        ),
    ]);

    provider
        .initialize_provider(|param| kvs.get(param).cloned().unwrap_or_default())
        .expect("keycloak provider should initialize");

    match provider.provider {
        Some(Provider::Keycloak(keycloak)) => {
            assert_eq!(keycloak.realm, "TestRealm");
            assert_eq!(keycloak.admin_url, "http://keycloak.test/auth/admin");
            assert_eq!(
                keycloak.token_endpoint,
                "http://keycloak.test/token/endpoint"
            );
        }
        other => panic!("expected keycloak provider, got {other:?}"),
    }
}
