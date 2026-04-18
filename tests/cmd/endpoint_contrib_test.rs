use std::collections::BTreeSet;

use minio_rust::cmd::update_domain_ips;

pub const SOURCE_FILE: &str = "cmd/endpoint_contrib_test.go";

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| (*item).to_string()).collect()
}

#[test]
fn test_update_domain_ips_line_25() {
    let cases = [
        (set(&[]), set(&[])),
        (set(&["localhost"]), set(&[])),
        (set(&["localhost", "10.0.0.1"]), set(&["10.0.0.1:9000"])),
        (
            set(&["localhost:9001", "10.0.0.1"]),
            set(&["10.0.0.1:9000"]),
        ),
        (
            set(&["localhost", "10.0.0.1:9001"]),
            set(&["10.0.0.1:9001"]),
        ),
        (
            set(&["localhost:9000", "10.0.0.1:9001"]),
            set(&["10.0.0.1:9001"]),
        ),
        (
            set(&["10.0.0.1", "10.0.0.2"]),
            set(&["10.0.0.1:9000", "10.0.0.2:9000"]),
        ),
        (
            set(&["10.0.0.1:9001", "10.0.0.2"]),
            set(&["10.0.0.1:9001", "10.0.0.2:9000"]),
        ),
        (
            set(&["10.0.0.1", "10.0.0.2:9002"]),
            set(&["10.0.0.1:9000", "10.0.0.2:9002"]),
        ),
        (
            set(&["10.0.0.1:9001", "10.0.0.2:9002"]),
            set(&["10.0.0.1:9001", "10.0.0.2:9002"]),
        ),
    ];

    for (endpoints, expected) in cases {
        assert_eq!(update_domain_ips("9000", &endpoints), expected);
    }
}
