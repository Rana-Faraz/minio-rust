use tempfile::tempdir;

use minio_rust::cmd::{
    create_server_endpoints, get_local_peer, get_remote_peers, merge_disks_layout_from_args,
    new_endpoint, new_endpoints, EndpointType, SetupType,
};

pub const SOURCE_FILE: &str = "cmd/endpoint_test.go";

#[test]
fn test_new_endpoint_line_30() {
    let tmp = tempdir().expect("tempdir");
    let path = tmp.path().join("foo");
    std::fs::create_dir_all(&path).expect("mkdir");
    let path_string = path.to_string_lossy().to_string();

    let cases = [
        (path_string.as_str(), true, Some(EndpointType::Path)),
        ("https://example.org/path", true, Some(EndpointType::Url)),
        ("http://192.168.253.200/path", true, Some(EndpointType::Url)),
        ("", false, None),
        ("/", false, None),
        ("\\", false, None),
        ("ftp://foo", false, None),
        ("http://server/path?location", false, None),
        ("http://:8080/path", false, None),
        ("http://server:8080/", false, None),
        ("192.168.1.210:9000", false, None),
    ];

    for (arg, success, expected_type) in cases {
        let result = new_endpoint(arg);
        assert_eq!(result.is_ok(), success, "{arg}: {result:?}");
        if let (Ok(endpoint), Some(expected_type)) = (result, expected_type) {
            assert_eq!(endpoint.endpoint_type(), expected_type);
        }
    }
}

#[test]
fn subtest_test_new_endpoint_fmt_sprint_case_line_59() {
    let endpoint = new_endpoint("https://example.org/path").expect("endpoint");
    assert_eq!(endpoint.to_string(), "https://example.org/path");
}

#[test]
fn test_new_endpoints_line_96() {
    let cases = [
        (vec!["/d1", "/d2", "/d3", "/d4"], true),
        (
            vec![
                "http://localhost/d1",
                "http://localhost/d2",
                "http://localhost/d3",
                "http://localhost/d4",
            ],
            true,
        ),
        (vec!["d1", "d2", "d3", "d1"], false),
        (
            vec![
                "http://localhost/d1",
                "http://localhost/d2",
                "http://localhost/d1",
                "http://localhost/d4",
            ],
            false,
        ),
        (
            vec![
                "ftp://server/d1",
                "http://server/d2",
                "http://server/d3",
                "http://server/d4",
            ],
            false,
        ),
        (vec!["d1", "http://localhost/d2", "d3", "d4"], false),
        (
            vec![
                "http://example.org/d1",
                "https://example.com/d1",
                "http://example.net/d1",
                "https://example.edut/d1",
            ],
            false,
        ),
    ];
    for (args, success) in cases {
        let result = new_endpoints(&args);
        assert_eq!(result.is_ok(), success, "{args:?}: {result:?}");
    }
}

#[test]
fn test_create_endpoints_line_132() {
    let cases = [
        (
            "localhost:9000",
            vec!["/d1"],
            true,
            Some(SetupType::ErasureSD),
        ),
        (
            ":1234",
            vec!["/d1", "/d2", "/d3", "/d4"],
            true,
            Some(SetupType::Erasure),
        ),
        (
            ":9000",
            vec![
                "http://localhost/d1",
                "http://localhost/d2",
                "http://localhost/d3",
                "http://localhost/d4",
            ],
            true,
            Some(SetupType::Erasure),
        ),
        (
            "127.0.0.1:10000",
            vec![
                "http://127.0.0.1:10000/d1",
                "http://example.org:10000/d3",
                "http://example.com:10000/d4",
            ],
            true,
            Some(SetupType::DistErasure),
        ),
        ("localhost", vec![] as Vec<&str>, false, None),
    ];
    for (server_addr, args, success, expected_setup) in cases {
        let args = args.into_iter().map(str::to_string).collect::<Vec<_>>();
        let result = merge_disks_layout_from_args(&args)
            .and_then(|layout| create_server_endpoints(server_addr, &layout.pools, layout.legacy));
        assert_eq!(
            result.is_ok(),
            success,
            "{server_addr} {args:?}: {result:?}"
        );
        if let (Ok((_, setup_type)), Some(expected_setup)) = (result, expected_setup) {
            assert_eq!(setup_type, expected_setup);
        }
    }
}

#[test]
fn subtest_test_create_endpoints_line_316() {
    let args = vec![
        "http://localhost:9000/d1".to_string(),
        "http://localhost:9000/d2".to_string(),
    ];
    let layout = merge_disks_layout_from_args(&args).expect("layout");
    let (pools, _) =
        create_server_endpoints(":9000", &layout.pools, layout.legacy).expect("endpoints");
    assert_eq!(pools[0].resolved_endpoints.len(), 2);
}

#[test]
fn test_get_local_peer_line_355() {
    let cases = [
        (vec!["/d1", "/d2", "d3", "d4"], "127.0.0.1:9000"),
        (
            vec![
                "http://localhost:9000/d1",
                "http://localhost:9000/d2",
                "http://example.org:9000/d3",
                "http://example.com:9000/d4",
            ],
            "localhost:9000",
        ),
        (
            vec![
                "http://localhost:9000/d1",
                "http://localhost:9001/d2",
                "http://localhost:9002/d3",
                "http://localhost:9003/d4",
            ],
            "localhost:9000",
        ),
    ];
    for (args, expected) in cases {
        let args = args.into_iter().map(str::to_string).collect::<Vec<_>>();
        let layout = merge_disks_layout_from_args(&args).expect("layout");
        let (pools, _) =
            create_server_endpoints(":9000", &layout.pools, layout.legacy).expect("endpoints");
        assert_eq!(get_local_peer(&pools, "", "9000"), expected);
    }
}

#[test]
fn test_get_remote_peers_line_395() {
    let cases = [
        (vec!["/d1", "/d2", "d3", "d4"], Vec::<&str>::new(), ""),
        (
            vec![
                "http://localhost:9000/d1",
                "http://localhost:9000/d2",
                "http://example.org:9000/d3",
                "http://example.com:9000/d4",
            ],
            vec!["example.com:9000", "example.org:9000", "localhost:9000"],
            "localhost:9000",
        ),
        (
            vec![
                "http://localhost:9000/d1",
                "http://localhost:9001/d2",
                "http://localhost:9002/d3",
                "http://localhost:9003/d4",
            ],
            vec![
                "localhost:9000",
                "localhost:9001",
                "localhost:9002",
                "localhost:9003",
            ],
            "localhost:9000",
        ),
    ];
    for (args, expected_peers, expected_local) in cases {
        let args = args.into_iter().map(str::to_string).collect::<Vec<_>>();
        let layout = merge_disks_layout_from_args(&args).expect("layout");
        let (pools, _) =
            create_server_endpoints(":9000", &layout.pools, layout.legacy).expect("endpoints");
        let (peers, local) = get_remote_peers(&pools, "9000");
        assert_eq!(
            peers,
            expected_peers
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        assert_eq!(local, expected_local);
    }
}
