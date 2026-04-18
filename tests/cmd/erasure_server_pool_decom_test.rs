// Rust test snapshot derived from cmd/erasure-server-pool-decom_test.go.

use minio_rust::cmd::{
    create_server_endpoints, merge_disks_layout_from_args, PoolMeta, PoolStatus,
};

pub const SOURCE_FILE: &str = "cmd/erasure-server-pool-decom_test.go";

fn server_pools(args: &[&str]) -> Vec<minio_rust::cmd::PoolEndpoints> {
    let args = args
        .iter()
        .map(|arg| (*arg).to_string())
        .collect::<Vec<_>>();
    let layout = merge_disks_layout_from_args(&args).expect("layout");
    let (pools, _) =
        create_server_endpoints(":9000", &layout.pools, layout.legacy).expect("server pools");
    pools
}

#[test]
fn test_pool_meta_validate_line_43() {
    let pools = server_pools(&[
        "http://localhost:9000/pool-a-disk{1...4}",
        "http://localhost:9000/pool-b-disk{1...4}",
    ]);

    let cases = [
        (
            "missing pools",
            PoolMeta {
                version: 1,
                pools: None,
            },
            None,
            Some("pool meta is missing pools"),
        ),
        (
            "mismatched pool count",
            PoolMeta {
                version: 1,
                pools: Some(vec![PoolStatus {
                    id: 0,
                    cmd_line: pools[0].cmd_line.clone(),
                    ..PoolStatus::default()
                }]),
            },
            None,
            Some("pool meta pool count mismatch"),
        ),
        (
            "matching metadata",
            PoolMeta {
                version: 1,
                pools: Some(vec![
                    PoolStatus {
                        id: 0,
                        cmd_line: pools[0].cmd_line.clone(),
                        ..PoolStatus::default()
                    },
                    PoolStatus {
                        id: 1,
                        cmd_line: pools[1].cmd_line.clone(),
                        ..PoolStatus::default()
                    },
                ]),
            },
            Some(false),
            None,
        ),
        (
            "cmd line changed",
            PoolMeta {
                version: 1,
                pools: Some(vec![
                    PoolStatus {
                        id: 0,
                        cmd_line: pools[0].cmd_line.clone(),
                        ..PoolStatus::default()
                    },
                    PoolStatus {
                        id: 1,
                        cmd_line: "http://localhost:9000/old-pool-b-disk{1...4}".to_string(),
                        ..PoolStatus::default()
                    },
                ]),
            },
            Some(true),
            None,
        ),
        (
            "pool id changed",
            PoolMeta {
                version: 1,
                pools: Some(vec![
                    PoolStatus {
                        id: 0,
                        cmd_line: pools[0].cmd_line.clone(),
                        ..PoolStatus::default()
                    },
                    PoolStatus {
                        id: 7,
                        cmd_line: pools[1].cmd_line.clone(),
                        ..PoolStatus::default()
                    },
                ]),
            },
            Some(true),
            None,
        ),
    ];

    for (name, meta, expected_updated, expected_err) in cases {
        let result = meta.validate(&pools);
        match (expected_updated, expected_err) {
            (_, Some(expected_err)) => {
                let err = result.expect_err(name);
                assert_eq!(err, expected_err, "{name}");
            }
            (Some(expected_updated), None) => {
                let updated = result.expect(name);
                assert_eq!(updated, expected_updated, "{name}");
            }
            _ => unreachable!("invalid test case"),
        }
    }
}

#[test]
fn subtest_test_pool_meta_validate_test_case_name_line_179() {
    let pools = server_pools(&[
        "http://localhost:9000/pool-a-disk{1...4}",
        "http://localhost:9000/pool-b-disk{1...4}",
    ]);

    let meta = PoolMeta {
        version: 1,
        pools: Some(vec![
            PoolStatus {
                id: 0,
                cmd_line: pools[0].cmd_line.clone(),
                ..PoolStatus::default()
            },
            PoolStatus {
                id: 1,
                cmd_line: "http://localhost:9000/old-pool-b-disk{1...4}".to_string(),
                ..PoolStatus::default()
            },
        ]),
    };

    assert!(meta.validate(&pools).expect("validate"));
}
