// Rust test snapshot derived from cmd/net_test.go.

use std::collections::BTreeSet;

use minio_rust::cmd::{
    check_local_server_addr, extract_host_port, get_api_endpoints, get_host_ip, is_host_ip,
    must_get_local_ip4, must_split_host_port, same_local_addrs, set_api_endpoint_globals, sort_ips,
};

pub const SOURCE_FILE: &str = "cmd/net_test.go";

#[test]
fn test_must_split_host_port_line_29() {
    let cases = [
        (":54321", "", "54321"),
        ("server:54321", "server", "54321"),
        (":0", "", "0"),
        ("server:https", "server", "443"),
        ("server:http", "server", "80"),
    ];

    for (input, expected_host, expected_port) in cases {
        let (host, port) = must_split_host_port(input);
        assert_eq!(host, expected_host, "{input}: host mismatch");
        assert_eq!(port, expected_port, "{input}: port mismatch");
    }
}

#[test]
fn test_sort_ips_line_54() {
    let cases = [
        (
            vec!["127.0.0.1", "10.0.0.13"],
            vec!["10.0.0.13", "127.0.0.1"],
        ),
        (
            vec!["127.0.0.1", "172.0.21.1", "192.168.1.106"],
            vec!["192.168.1.106", "172.0.21.1", "127.0.0.1"],
        ),
        (
            vec!["127.0.0.1", "192.168.1.106"],
            vec!["192.168.1.106", "127.0.0.1"],
        ),
        (vec!["hostname"], vec!["hostname"]),
        (vec!["127.0.0.1"], vec!["127.0.0.1"]),
        (
            vec!["hostname", "127.0.0.1", "192.168.1.106"],
            vec!["hostname", "192.168.1.106", "127.0.0.1"],
        ),
        (
            vec![
                "hostname1",
                "10.0.0.13",
                "hostname2",
                "127.0.0.1",
                "192.168.1.106",
            ],
            vec![
                "hostname1",
                "hostname2",
                "192.168.1.106",
                "10.0.0.13",
                "127.0.0.1",
            ],
        ),
        (
            vec!["127.0.0.1", "10.0.0.1", "192.168.0.1"],
            vec!["10.0.0.1", "192.168.0.1", "127.0.0.1"],
        ),
    ];

    for (input, expected) in cases {
        let sorted = sort_ips(&input.into_iter().map(str::to_string).collect::<Vec<_>>());
        assert_eq!(
            sorted,
            expected.into_iter().map(str::to_string).collect::<Vec<_>>()
        );
    }
}

#[test]
fn test_must_get_local_ip4_line_112() {
    let ips = must_get_local_ip4();
    assert!(
        !ips.intersection(&BTreeSet::from(["127.0.0.1".to_string()]))
            .collect::<Vec<_>>()
            .is_empty(),
        "localhost IPv4 should always be present, got {ips:?}"
    );
}

#[test]
fn test_get_host_ip_line_127() {
    let ips = get_host_ip("localhost").expect("resolve localhost");
    assert!(
        ips.contains("127.0.0.1"),
        "expected localhost resolution to include 127.0.0.1, got {ips:?}"
    );
}

#[test]
fn test_get_apiendpoints_line_164() {
    let original = get_api_endpoints();
    let _ = original;

    let cases = [
        ("", "80", "http://127.0.0.1:80"),
        ("127.0.0.1", "80", "http://127.0.0.1:80"),
        ("localhost", "80", "http://localhost:80"),
    ];

    for (host, port, expected) in cases {
        set_api_endpoint_globals(host, port, false, "");
        let endpoints = get_api_endpoints();
        assert!(
            endpoints.contains(&expected.to_string()),
            "expected endpoint {expected} in {endpoints:?}"
        );
    }
}

#[test]
fn test_check_local_server_addr_line_188() {
    let cases = [
        (":54321", None),
        ("localhost:54321", None),
        ("0.0.0.0:9000", None),
        (":0", None),
        ("localhost", None),
        ("", Some("invalid argument")),
        (
            "example.org:54321",
            Some("host in server address should be this server"),
        ),
        (":-10", Some("port must be between 0 to 65535")),
    ];

    for (input, expected_err) in cases {
        let result = check_local_server_addr(input);
        match expected_err {
            None => assert!(result.is_ok(), "{input}: {result:?}"),
            Some(expected) => assert_eq!(result, Err(expected.to_string()), "{input}"),
        }
    }
}

#[test]
fn subtest_test_check_local_server_addr_line_204() {
    let got = check_local_server_addr("localhost:54321");
    assert!(got.is_ok(), "localhost should be accepted");
}

#[test]
fn test_extract_host_port_line_220() {
    let cases = [
        ("", "", "", Some("unable to process empty address")),
        ("localhost:9000", "localhost", "9000", None),
        ("http://:9000/", "", "9000", None),
        ("http://8.8.8.8:9000/", "8.8.8.8", "9000", None),
        ("https://facebook.com:9000/", "facebook.com", "9000", None),
    ];

    for (input, expected_host, expected_port, expected_err) in cases {
        let result = extract_host_port(input);
        match expected_err {
            None => {
                let (host, port) = result.expect("extract host/port");
                assert_eq!(host, expected_host, "{input}: host mismatch");
                assert_eq!(port, expected_port, "{input}: port mismatch");
            }
            Some(expected) => assert_eq!(result, Err(expected.to_string()), "{input}"),
        }
    }
}

#[test]
fn test_same_local_addrs_line_258() {
    let cases = [
        ("", "", false, Some("unable to process empty address")),
        (":9000", ":9000", true, None),
        ("localhost:9000", ":9000", true, None),
        ("localhost:9000", "http://localhost:9000", true, None),
        ("http://localhost:9000", ":9000", true, None),
        ("http://localhost:9000", "http://localhost:9000", true, None),
        ("http://8.8.8.8:9000", "http://localhost:9000", false, None),
    ];

    for (left, right, expected_same, expected_err) in cases {
        let result = same_local_addrs(left, right);
        match expected_err {
            None => assert_eq!(result, Ok(expected_same), "{left} vs {right}"),
            Some(expected) => assert_eq!(result, Err(expected.to_string()), "{left} vs {right}"),
        }
    }
}

#[test]
fn subtest_test_same_local_addrs_line_275() {
    assert_eq!(
        same_local_addrs("localhost:9000", ":9000"),
        Ok(true),
        "localhost and wildcard local addr should match"
    );
}

#[test]
fn test_is_host_ip_line_297() {
    let cases = [
        ("localhost", false),
        ("localhost:9000", false),
        ("example.com", false),
        ("http://192.168.1.0", false),
        ("http://192.168.1.0:9000", false),
        ("192.168.1.0", true),
        ("[2001:3984:3989::20%eth0]:9000", true),
    ];

    for (input, expected) in cases {
        assert_eq!(is_host_ip(input), expected, "{input}");
    }
}
