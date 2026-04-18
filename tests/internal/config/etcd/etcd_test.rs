use minio_rust::internal::config::etcd::parse_endpoints;

#[test]
fn parse_endpoints_matches_reference_cases() {
    let cases = [
        (
            "https://localhost:2379,http://localhost:2380",
            None,
            false,
            false,
        ),
        (",,,", None, false, false),
        ("", None, false, false),
        ("ftp://localhost:2379", None, false, false),
        ("http://localhost:2379000", None, false, false),
        (
            "https://localhost:2379,https://localhost:2380",
            Some(vec![
                "https://localhost:2379".to_owned(),
                "https://localhost:2380".to_owned(),
            ]),
            true,
            true,
        ),
        (
            "http://localhost:2379",
            Some(vec!["http://localhost:2379".to_owned()]),
            false,
            true,
        ),
    ];

    for (input, expected_endpoints, expected_secure, success) in cases {
        let result = parse_endpoints(input);
        if success {
            let (endpoints, secure) =
                result.unwrap_or_else(|err| panic!("expected success for {input}, got {err}"));
            assert_eq!(Some(endpoints), expected_endpoints, "input {input}");
            assert_eq!(secure, expected_secure, "input {input}");
        } else if result.is_ok() {
            panic!("expected failure for {input} but succeeded");
        }
    }
}
