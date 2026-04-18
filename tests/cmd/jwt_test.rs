use minio_rust::cmd::{
    authenticate_node, get_token_string, metrics_request_authenticate, new_cached_auth_token,
    new_test_request, ERR_AUTHENTICATION, ERR_NO_AUTH_TOKEN,
};
use minio_rust::internal::jwt::{
    parse_with_claims_and_key, parse_with_standard_claims, MapClaims, StandardClaims,
};

pub const SOURCE_FILE: &str = "cmd/jwt_test.go";

fn credentials() -> (&'static str, &'static str) {
    ("minioadmin", "miniosecret")
}

#[test]
fn test_web_request_authenticate_line_39() {
    let (access_key, secret_key) = credentials();
    let token = get_token_string(access_key, secret_key).expect("token");

    let mut valid = new_test_request(
        "GET",
        "http://127.0.0.1:9000/minio/prometheus/metrics",
        0,
        None,
    )
    .expect("request");
    valid.set_header("Authorization", &token);

    let missing = new_test_request(
        "GET",
        "http://127.0.0.1:9000/minio/prometheus/metrics",
        0,
        None,
    )
    .expect("request");

    let mut invalid = new_test_request(
        "GET",
        "http://127.0.0.1:9000/minio/prometheus/metrics",
        0,
        None,
    )
    .expect("request");
    invalid.set_header("Authorization", "invalid-token");

    let cases = [
        (valid, Ok(access_key.to_string())),
        (missing, Err(ERR_NO_AUTH_TOKEN)),
        (invalid, Err(ERR_AUTHENTICATION)),
    ];

    for (request, expected) in cases {
        let result =
            metrics_request_authenticate(&request, secret_key).map(|claims| claims.access_key);
        assert_eq!(result, expected);
    }
}

#[test]
fn benchmark_parse_jwtstandard_claims_line_96() {
    let (access_key, secret_key) = credentials();
    let token = authenticate_node(access_key, secret_key).expect("token");

    for _ in 0..100 {
        let mut claims = StandardClaims::new();
        parse_with_standard_claims(&token, &mut claims, secret_key.as_bytes()).expect("parse");
        assert_eq!(claims.subject, access_key);
    }
}

#[test]
fn benchmark_parse_jwtmap_claims_line_127() {
    let (access_key, secret_key) = credentials();
    let token = authenticate_node(access_key, secret_key).expect("token");

    for _ in 0..100 {
        let mut claims = MapClaims::new();
        parse_with_claims_and_key(&token, &mut claims, secret_key.as_bytes()).expect("parse");
        assert_eq!(claims.access_key, access_key);
    }
}

#[test]
fn benchmark_authenticate_node_line_160() {
    let (access_key, secret_key) = credentials();
    let token = authenticate_node(access_key, secret_key).expect("token");

    let mut claims = MapClaims::new();
    parse_with_claims_and_key(&token, &mut claims, secret_key.as_bytes()).expect("parse");
    assert_eq!(claims.access_key, access_key);
}

#[test]
fn subbenchmark_benchmark_authenticate_node_uncached_line_174() {
    let (access_key, secret_key) = credentials();

    for _ in 0..100 {
        let token = authenticate_node(access_key, secret_key).expect("token");
        let mut claims = StandardClaims::new();
        parse_with_standard_claims(&token, &mut claims, secret_key.as_bytes()).expect("parse");
        assert_eq!(claims.subject, access_key);
    }
}

#[test]
fn subbenchmark_benchmark_authenticate_node_cached_line_182() {
    let (access_key, secret_key) = credentials();
    let mut cached = new_cached_auth_token(access_key, secret_key);
    let first = cached.token().expect("token");

    for _ in 0..100 {
        let token = cached.token().expect("cached token");
        assert_eq!(token, first);

        let mut claims = StandardClaims::new();
        parse_with_standard_claims(&token, &mut claims, secret_key.as_bytes()).expect("parse");
        assert_eq!(claims.subject, access_key);
    }
}
