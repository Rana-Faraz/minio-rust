use minio_rust::cmd::{new_bucket_metacache, ListPathOptions, SLASH_SEPARATOR};

pub const SOURCE_FILE: &str = "cmd/metacache-bucket_test.go";

#[test]
fn benchmark_bucket_metacache_find_cache_line_25() {
    let mut cache = new_bucket_metacache("", false);
    const ELEMENTS: usize = 5_000;
    const PATHS: usize = 100;
    let path_names = (0..PATHS)
        .map(|i| format!("prefix/{i}"))
        .collect::<Vec<_>>();

    for i in 0..ELEMENTS {
        let result = cache.find_cache(ListPathOptions {
            id: format!("id-{i}"),
            bucket: String::new(),
            base_dir: path_names[i % PATHS].clone(),
            prefix: String::new(),
            filter_prefix: String::new(),
            marker: String::new(),
            limit: 0,
            ask_disks: "strict".to_string(),
            recursive: false,
            separator: SLASH_SEPARATOR.to_string(),
            create: true,
            ..Default::default()
        });
        assert_eq!(result.root, path_names[i % PATHS]);
    }

    for i in 0..256 {
        let result = cache.find_cache(ListPathOptions {
            id: format!("bench-{i}"),
            bucket: String::new(),
            base_dir: path_names[i % PATHS].clone(),
            prefix: String::new(),
            filter_prefix: String::new(),
            marker: String::new(),
            limit: 0,
            ask_disks: "strict".to_string(),
            recursive: false,
            separator: SLASH_SEPARATOR.to_string(),
            create: true,
            ..Default::default()
        });
        assert_eq!(result.root, path_names[i % PATHS]);
    }

    assert_eq!(cache.caches.len(), ELEMENTS + 256);
    assert!(cache.updated);
    assert_eq!(cache.caches_root.len(), PATHS);
}
