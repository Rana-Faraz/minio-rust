use minio_rust::cmd::{
    build_data_usage_cache, build_data_usage_cache_with_prefix, deserialize_data_usage_cache,
    serialize_data_usage_cache, UsageUpdate,
};

pub const SOURCE_FILE: &str = "cmd/data-usage_test.go";

#[test]
fn test_data_usage_update_line_39() {
    run_data_usage_update_cases();
}

#[test]
fn subtest_test_data_usage_update_p_line_117() {
    run_data_usage_update_cases();
}

#[test]
fn subtest_test_data_usage_update_p_line_225() {
    run_data_usage_update_cases();
}

#[test]
fn test_data_usage_update_prefix_line_255() {
    run_data_usage_update_prefix_cases();
}

#[test]
fn subtest_test_data_usage_update_prefix_w_path_line_363() {
    run_data_usage_update_prefix_cases();
}

#[test]
fn subtest_test_data_usage_update_prefix_w_path_line_481() {
    run_data_usage_update_prefix_cases();
}

#[test]
fn test_data_usage_cache_serialize_line_545() {
    let cache = build_data_usage_cache(
        "usage-cache",
        &[
            UsageUpdate::new("photos/2024/a.jpg", 512),
            UsageUpdate::new("photos/2024/b.jpg", 2048),
        ],
    );
    let encoded = serialize_data_usage_cache(&cache).expect("serialize cache");
    let decoded = deserialize_data_usage_cache(&encoded).expect("deserialize cache");
    assert_eq!(decoded, cache);
}

fn run_data_usage_update_cases() {
    let tests = [
        (
            vec![UsageUpdate::new("a.txt", 10)],
            vec![("", 10, 1_u64, vec![("a.txt", false)])],
        ),
        (
            vec![
                UsageUpdate::new("photos/2024/a.jpg", 100),
                UsageUpdate::new("photos/2024/b.jpg", 200),
                UsageUpdate::new("photos/2023/c.jpg", 300),
            ],
            vec![
                ("", 600, 3_u64, vec![("photos", true)]),
                ("photos", 600, 3_u64, vec![("2023", true), ("2024", true)]),
                (
                    "photos/2024",
                    300,
                    2_u64,
                    vec![("a.jpg", false), ("b.jpg", false)],
                ),
                ("photos/2023", 300, 1_u64, vec![("c.jpg", false)]),
            ],
        ),
    ];

    for (index, (updates, expectations)) in tests.into_iter().enumerate() {
        let cache = build_data_usage_cache("usage-cache", &updates);
        let entries = cache.cache.expect("cache map");
        for (key, size, objects, children) in expectations {
            let entry = entries
                .get(key)
                .unwrap_or_else(|| panic!("missing key {key}"));
            assert_eq!(entry.size, size, "case {} key {} size", index + 1, key);
            assert_eq!(
                entry.objects,
                objects,
                "case {} key {} objects",
                index + 1,
                key
            );
            for (child, is_dir) in children {
                assert_eq!(
                    entry
                        .children
                        .as_ref()
                        .and_then(|map| map.get(child))
                        .copied(),
                    Some(is_dir),
                    "case {} key {} child {}",
                    index + 1,
                    key,
                    child
                );
            }
        }
    }
}

fn run_data_usage_update_prefix_cases() {
    let updates = vec![
        UsageUpdate::new("photos/2024/a.jpg", 100),
        UsageUpdate::new("photos/2024/b.jpg", 200),
        UsageUpdate::new("photos/2023/c.jpg", 300),
        UsageUpdate::new("docs/readme.txt", 50),
    ];

    let tests = [
        (
            "photos/2024",
            300_i64,
            2_u64,
            vec![("a.jpg", false), ("b.jpg", false)],
        ),
        ("docs", 50_i64, 1_u64, vec![("readme.txt", false)]),
    ];

    for (index, (prefix, size, objects, children)) in tests.into_iter().enumerate() {
        let cache = build_data_usage_cache_with_prefix("usage-cache", &updates, prefix);
        let entries = cache.cache.expect("cache map");
        let root = entries.get("").expect("root entry");
        assert_eq!(root.size, size, "case {} size", index + 1);
        assert_eq!(root.objects, objects, "case {} objects", index + 1);
        for (child, is_dir) in children {
            assert_eq!(
                root.children
                    .as_ref()
                    .and_then(|map| map.get(child))
                    .copied(),
                Some(is_dir),
                "case {} child {}",
                index + 1,
                child
            );
        }
    }
}
