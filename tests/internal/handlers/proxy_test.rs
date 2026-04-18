use minio_rust::internal::handlers::{
    self, RequestContext, FORWARDED, X_FORWARDED_FOR, X_FORWARDED_PROTO, X_FORWARDED_SCHEME,
    X_REAL_IP,
};

pub const SOURCE_FILE: &str = "internal/handlers/proxy_test.go";

#[test]
fn test_get_scheme() {
    let headers = [
        (X_FORWARDED_PROTO, "https", "https"),
        (X_FORWARDED_PROTO, "http", "http"),
        (X_FORWARDED_PROTO, "HTTP", "http"),
        (X_FORWARDED_SCHEME, "https", "https"),
        (X_FORWARDED_SCHEME, "http", "http"),
        (X_FORWARDED_SCHEME, "HTTP", "http"),
        (FORWARDED, r#"For="[2001:db8:cafe::17]:4711"#, ""),
        (
            FORWARDED,
            "for=192.0.2.43, for=198.51.100.17;proto=https",
            "",
        ),
        (
            FORWARDED,
            "for=172.32.10.15; proto=https;by=127.0.0.1;",
            "https",
        ),
        (
            FORWARDED,
            "for=192.0.2.60;proto=http;by=203.0.113.43",
            "http",
        ),
    ];

    for (key, value, expected) in headers {
        let request = RequestContext::new([(key, value)]);
        assert_eq!(
            handlers::get_source_scheme(&request),
            expected,
            "{key}: {value}"
        );
    }
}

#[test]
fn test_get_source_ip() {
    let headers = [
        (X_FORWARDED_FOR, "8.8.8.8", "8.8.8.8"),
        (X_FORWARDED_FOR, "8.8.8.8, 8.8.4.4", "8.8.8.8"),
        (X_FORWARDED_FOR, "", ""),
        (X_REAL_IP, "8.8.8.8", "8.8.8.8"),
        (X_REAL_IP, "[2001:db8:cafe::17]:4711", "[2001:db8:cafe::17]"),
        (X_REAL_IP, "", ""),
        (FORWARDED, r#"for="_gazonk""#, "_gazonk"),
        (
            FORWARDED,
            r#"For="[2001:db8:cafe::17]:4711"#,
            "[2001:db8:cafe::17]",
        ),
        (
            FORWARDED,
            "for=192.0.2.60;proto=http;by=203.0.113.43",
            "192.0.2.60",
        ),
        (FORWARDED, "for=192.0.2.43, for=198.51.100.17", "192.0.2.43"),
        (
            FORWARDED,
            r#"for="workstation.local",for=198.51.100.17"#,
            "workstation.local",
        ),
    ];

    for (key, value, expected) in headers {
        let request = RequestContext::new([(key, value)]);
        assert_eq!(
            handlers::get_source_ip(&request),
            expected,
            "{key}: {value}"
        );
    }
}

#[test]
fn test_xff_disabled() {
    let request = RequestContext::new([(X_FORWARDED_FOR, "8.8.8.8"), (X_REAL_IP, "1.1.1.1")]);

    assert_eq!(handlers::get_source_ip(&request), "8.8.8.8");

    let previous = handlers::xff_header_enabled();
    handlers::set_xff_header_enabled(false);
    assert_eq!(handlers::get_source_ip(&request), "1.1.1.1");
    handlers::set_xff_header_enabled(previous);
}
