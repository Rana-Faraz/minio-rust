// Rust test snapshot derived from cmd/endpoint-ellipses_test.go.

use minio_rust::cmd::{
    create_server_endpoints, find_ellipses_patterns, get_divisible_size, get_set_indexes,
    merge_disks_layout_from_args, parse_endpoint_set, EndpointSet, Pattern,
};

pub const SOURCE_FILE: &str = "cmd/endpoint-ellipses_test.go";

fn seq(start: usize, end: usize, width: usize) -> Vec<String> {
    (start..=end)
        .map(|value| {
            if width == 0 {
                value.to_string()
            } else {
                format!("{value:0width$}")
            }
        })
        .collect()
}

fn hex_seq(start: usize, end: usize, width: usize) -> Vec<String> {
    (start..=end)
        .map(|value| {
            if width == 0 {
                format!("{value:x}")
            } else {
                format!("{value:0width$x}")
            }
        })
        .collect()
}

#[test]
fn test_create_server_endpoints_line_29() {
    let cases = [
        ("", vec![] as Vec<&str>, false),
        (":9000", vec!["/export1{-1...1}"], false),
        (":9000", vec!["/export1{64...1}"], false),
        (":9000", vec!["/export1{a...z}"], false),
        (":9000", vec!["/export1{1...32}", "/export1{1...32}"], false),
        (
            ":9001",
            vec!["http://localhost:900{1...2}/export{1...64}"],
            false,
        ),
        (":9000", vec!["/export1"], true),
        (
            ":9000",
            vec!["/export1", "/export2", "/export3", "/export4"],
            true,
        ),
        (":9000", vec!["/export1{1...64}"], true),
        (":9000", vec!["/export1{01...64}"], true),
        (":9000", vec!["/export1{1...32}", "/export1{33...64}"], true),
        (":9001", vec!["http://localhost:9001/export{1...64}"], true),
        (":9001", vec!["http://localhost:9001/export{01...64}"], true),
    ];

    for (server_addr, args, success) in cases {
        let args = args.into_iter().map(str::to_string).collect::<Vec<_>>();
        let result = merge_disks_layout_from_args(&args)
            .and_then(|layout| create_server_endpoints(server_addr, &layout.pools, layout.legacy));
        assert_eq!(
            result.is_ok(),
            success,
            "{server_addr} {args:?}: {result:?}"
        );
    }
}

#[test]
fn subtest_test_create_server_endpoints_line_58() {
    let args = vec!["http://localhost:9001/export{1...64}".to_string()];
    let layout = merge_disks_layout_from_args(&args).expect("layout");
    assert!(
        create_server_endpoints(":9001", &layout.pools, layout.legacy).is_ok(),
        "single-port localhost expansion should be valid"
    );
}

#[test]
fn test_get_divisible_size_line_75() {
    let cases = [
        (vec![24, 32, 16], 8),
        (vec![32, 8, 4], 4),
        (vec![8, 8, 8], 8),
        (vec![24], 24),
    ];

    for (input, expected) in cases {
        assert_eq!(get_divisible_size(&input), expected, "{input:?}");
    }
}

#[test]
fn subtest_test_get_divisible_size_line_87() {
    assert_eq!(get_divisible_size(&[64, 16]), 16);
}

#[test]
fn test_get_set_indexes_env_override_line_97() {
    let cases = [
        (
            vec!["data{1...64}"],
            vec![64],
            Some(vec![vec![8; 8]]),
            8,
            true,
        ),
        (
            vec!["http://host{1...2}/data{1...180}"],
            vec![360],
            Some(vec![vec![15; 24]]),
            15,
            true,
        ),
        (
            vec!["http://host{1...12}/data{1...12}"],
            vec![144],
            Some(vec![vec![12; 12]]),
            12,
            true,
        ),
        (
            vec!["http://host{0...5}/data{1...28}"],
            vec![168],
            Some(vec![vec![12; 14]]),
            12,
            true,
        ),
        (
            vec!["http://host{0...5}/data{1...28}"],
            vec![168],
            None,
            10,
            false,
        ),
        (
            vec!["http://host{1...11}/data{1...11}"],
            vec![121],
            Some(vec![vec![11; 11]]),
            11,
            true,
        ),
        (vec!["data{1...60}"], vec![], None, 8, false),
        (vec!["data{1...64}"], vec![], None, 64, false),
        (vec!["data{1...64}"], vec![], None, 2, false),
    ];

    for (args, total_sizes, expected, override_count, success) in cases {
        let args = args.into_iter().map(str::to_string).collect::<Vec<_>>();
        let arg_patterns = args
            .iter()
            .map(|arg| find_ellipses_patterns(arg).expect("arg pattern"))
            .collect::<Vec<_>>();
        let result = get_set_indexes(&args, &total_sizes, override_count, &arg_patterns);
        assert_eq!(result.is_ok(), success, "{args:?}: {result:?}");
        if let Some(expected) = expected {
            assert_eq!(result.expect("set indexes"), expected, "{args:?}");
        }
    }
}

#[test]
fn subtest_test_get_set_indexes_env_override_line_173() {
    let args = vec!["data{1...64}".to_string()];
    let arg_patterns = vec![find_ellipses_patterns(&args[0]).expect("pattern")];
    assert_eq!(
        get_set_indexes(&args, &[64], 8, &arg_patterns).expect("set indexes"),
        vec![vec![8; 8]]
    );
}

#[test]
fn test_get_set_indexes_line_198() {
    let cases = vec![
        (
            vec!["data{1...17}/export{1...52}"],
            vec![14144],
            None,
            false,
        ),
        (vec!["data{1...3}"], vec![3], Some(vec![vec![3]]), true),
        (
            vec![
                "data/controller1/export{1...2}",
                "data/controller2/export{1...4}",
                "data/controller3/export{1...8}",
            ],
            vec![2, 4, 8],
            Some(vec![vec![2], vec![2, 2], vec![2, 2, 2, 2]]),
            true,
        ),
        (
            vec!["data{1...27}"],
            vec![27],
            Some(vec![vec![9, 9, 9]]),
            true,
        ),
        (
            vec!["http://host{1...3}/data{1...180}"],
            vec![540],
            Some(vec![vec![15; 36]]),
            true,
        ),
        (
            vec!["http://host{1...2}.rack{1...4}/data{1...180}"],
            vec![1440],
            Some(vec![vec![16; 90]]),
            true,
        ),
        (
            vec!["http://host{1...2}/data{1...180}"],
            vec![360],
            Some(vec![vec![12; 30]]),
            true,
        ),
        (
            vec![
                "data/controller1/export{1...4}",
                "data/controller2/export{1...8}",
                "data/controller3/export{1...12}",
            ],
            vec![4, 8, 12],
            Some(vec![vec![4], vec![4, 4], vec![4, 4, 4]]),
            true,
        ),
        (
            vec!["data{1...64}"],
            vec![64],
            Some(vec![vec![16; 4]]),
            true,
        ),
        (
            vec!["data{1...24}"],
            vec![24],
            Some(vec![vec![12, 12]]),
            true,
        ),
        (
            vec!["data/controller{1...11}/export{1...8}"],
            vec![88],
            Some(vec![vec![11; 8]]),
            true,
        ),
        (vec!["data{1...4}"], vec![4], Some(vec![vec![4]]), true),
        (
            vec![
                "data/controller1/export{1...10}",
                "data/controller2/export{1...10}",
                "data/controller3/export{1...10}",
            ],
            vec![10, 10, 10],
            Some(vec![vec![10], vec![10], vec![10]]),
            true,
        ),
        (
            vec!["data{1...16}/export{1...52}"],
            vec![832],
            Some(vec![vec![16; 52]]),
            true,
        ),
    ];

    for (args, total_sizes, expected, success) in cases {
        let args = args.into_iter().map(str::to_string).collect::<Vec<_>>();
        let arg_patterns = args
            .iter()
            .map(|arg| find_ellipses_patterns(arg).expect("arg pattern"))
            .collect::<Vec<_>>();
        let result = get_set_indexes(&args, &total_sizes, 0, &arg_patterns);
        assert_eq!(result.is_ok(), success, "{args:?}: {result:?}");
        if let Some(expected) = expected {
            assert_eq!(result.expect("set indexes"), expected, "{args:?}");
        }
    }
}

#[test]
fn subtest_test_get_set_indexes_line_294() {
    let args = vec!["data{1...27}".to_string()];
    let arg_patterns = vec![find_ellipses_patterns(&args[0]).expect("pattern")];
    assert_eq!(
        get_set_indexes(&args, &[27], 0, &arg_patterns).expect("set indexes"),
        vec![vec![9, 9, 9]]
    );
}

#[test]
fn test_parse_endpoint_set_line_340() {
    let cases = vec![
        ("...", None, false),
        ("{...}", None, false),
        ("http://minio{2...3}/export/set{1...0}", None, false),
        ("/export{1..2}", None, false),
        ("/export/test{1...2O}", None, false),
        (
            "{1...27}",
            Some(EndpointSet {
                arg_patterns: vec![vec![Pattern {
                    prefix: String::new(),
                    suffix: String::new(),
                    seq: seq(1, 27, 0),
                }]],
                endpoints: vec![],
                set_indexes: vec![vec![9, 9, 9]],
            }),
            true,
        ),
        (
            "/export/set{1...64}",
            Some(EndpointSet {
                arg_patterns: vec![vec![Pattern {
                    prefix: "/export/set".to_string(),
                    suffix: String::new(),
                    seq: seq(1, 64, 0),
                }]],
                endpoints: vec![],
                set_indexes: vec![vec![16, 16, 16, 16]],
            }),
            true,
        ),
        (
            "http://minio{2...3}/export/set{1...64}",
            Some(EndpointSet {
                arg_patterns: vec![vec![
                    Pattern {
                        prefix: String::new(),
                        suffix: String::new(),
                        seq: seq(1, 64, 0),
                    },
                    Pattern {
                        prefix: "http://minio".to_string(),
                        suffix: "/export/set".to_string(),
                        seq: seq(2, 3, 0),
                    },
                ]],
                endpoints: vec![],
                set_indexes: vec![vec![16, 16, 16, 16, 16, 16, 16, 16]],
            }),
            true,
        ),
        (
            "http://minio{1...64}.mydomain.net/data",
            Some(EndpointSet {
                arg_patterns: vec![vec![Pattern {
                    prefix: "http://minio".to_string(),
                    suffix: ".mydomain.net/data".to_string(),
                    seq: seq(1, 64, 0),
                }]],
                endpoints: vec![],
                set_indexes: vec![vec![16, 16, 16, 16]],
            }),
            true,
        ),
        (
            "http://rack{1...4}.mydomain.minio{1...16}/data",
            Some(EndpointSet {
                arg_patterns: vec![vec![
                    Pattern {
                        prefix: String::new(),
                        suffix: "/data".to_string(),
                        seq: seq(1, 16, 0),
                    },
                    Pattern {
                        prefix: "http://rack".to_string(),
                        suffix: ".mydomain.minio".to_string(),
                        seq: seq(1, 4, 0),
                    },
                ]],
                endpoints: vec![],
                set_indexes: vec![vec![16, 16, 16, 16]],
            }),
            true,
        ),
        (
            "http://minio{0...15}.mydomain.net/data{0...1}",
            Some(EndpointSet {
                arg_patterns: vec![vec![
                    Pattern {
                        prefix: String::new(),
                        suffix: String::new(),
                        seq: seq(0, 1, 0),
                    },
                    Pattern {
                        prefix: "http://minio".to_string(),
                        suffix: ".mydomain.net/data".to_string(),
                        seq: seq(0, 15, 0),
                    },
                ]],
                endpoints: vec![],
                set_indexes: vec![vec![16, 16]],
            }),
            true,
        ),
        (
            "http://server1/data{1...32}",
            Some(EndpointSet {
                arg_patterns: vec![vec![Pattern {
                    prefix: "http://server1/data".to_string(),
                    suffix: String::new(),
                    seq: seq(1, 32, 0),
                }]],
                endpoints: vec![],
                set_indexes: vec![vec![16, 16]],
            }),
            true,
        ),
        (
            "http://server1/data{01...32}",
            Some(EndpointSet {
                arg_patterns: vec![vec![Pattern {
                    prefix: "http://server1/data".to_string(),
                    suffix: String::new(),
                    seq: seq(1, 32, 2),
                }]],
                endpoints: vec![],
                set_indexes: vec![vec![16, 16]],
            }),
            true,
        ),
        (
            "http://minio{2...3}/export/set{1...64}/test{1...2}",
            Some(EndpointSet {
                arg_patterns: vec![vec![
                    Pattern {
                        prefix: String::new(),
                        suffix: String::new(),
                        seq: seq(1, 2, 0),
                    },
                    Pattern {
                        prefix: String::new(),
                        suffix: "/test".to_string(),
                        seq: seq(1, 64, 0),
                    },
                    Pattern {
                        prefix: "http://minio".to_string(),
                        suffix: "/export/set".to_string(),
                        seq: seq(2, 3, 0),
                    },
                ]],
                endpoints: vec![],
                set_indexes: vec![vec![16; 16]],
            }),
            true,
        ),
        (
            "/export{1...10}/disk{1...10}",
            Some(EndpointSet {
                arg_patterns: vec![vec![
                    Pattern {
                        prefix: String::new(),
                        suffix: String::new(),
                        seq: seq(1, 10, 0),
                    },
                    Pattern {
                        prefix: "/export".to_string(),
                        suffix: "/disk".to_string(),
                        seq: seq(1, 10, 0),
                    },
                ]],
                endpoints: vec![],
                set_indexes: vec![vec![10; 10]],
            }),
            true,
        ),
        (
            "http://[2001:3984:3989::{1...a}]/disk{1...10}",
            Some(EndpointSet {
                arg_patterns: vec![vec![
                    Pattern {
                        prefix: String::new(),
                        suffix: String::new(),
                        seq: seq(1, 10, 0),
                    },
                    Pattern {
                        prefix: "http://[2001:3984:3989::".to_string(),
                        suffix: "]/disk".to_string(),
                        seq: hex_seq(1, 10, 0),
                    },
                ]],
                endpoints: vec![],
                set_indexes: vec![vec![10; 10]],
            }),
            true,
        ),
        (
            "http://[2001:3984:3989::{001...00a}]/disk{1...10}",
            Some(EndpointSet {
                arg_patterns: vec![vec![
                    Pattern {
                        prefix: String::new(),
                        suffix: String::new(),
                        seq: seq(1, 10, 0),
                    },
                    Pattern {
                        prefix: "http://[2001:3984:3989::".to_string(),
                        suffix: "]/disk".to_string(),
                        seq: hex_seq(1, 10, 3),
                    },
                ]],
                endpoints: vec![],
                set_indexes: vec![vec![10; 10]],
            }),
            true,
        ),
    ];

    for (arg, expected, success) in cases {
        let result = parse_endpoint_set(0, arg);
        assert_eq!(result.is_ok(), success, "{arg}: {result:?}");
        if let Some(expected) = expected {
            assert_eq!(result.expect("endpoint set"), expected, "{arg}");
        }
    }
}

#[test]
fn subtest_test_parse_endpoint_set_line_636() {
    let got = parse_endpoint_set(0, "/export/set{1...64}").expect("endpoint set");
    assert_eq!(
        got.arg_patterns[0][0].seq.first().map(String::as_str),
        Some("1")
    );
    assert_eq!(
        got.arg_patterns[0][0].seq.last().map(String::as_str),
        Some("64")
    );
}
