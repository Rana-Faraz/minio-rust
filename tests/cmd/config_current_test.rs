use tempfile::tempdir;

use minio_rust::cmd::{
    current_server_config, current_site, load_config, lookup_site, new_test_config,
    save_server_config, set_region, LocalObjectLayer, DEFAULT, GLOBAL_MINIO_DEFAULT_REGION,
    REGION_SUBSYS, SITE_SUBSYS,
};

pub const SOURCE_FILE: &str = "cmd/config-current_test.go";

#[test]
fn test_server_config_line_28() {
    let tmp = tempdir().expect("tempdir");
    let layer = LocalObjectLayer::new(vec![tmp.path().to_path_buf()]);

    new_test_config(GLOBAL_MINIO_DEFAULT_REGION, &layer).expect("new test config");

    assert_eq!(current_site().region(), GLOBAL_MINIO_DEFAULT_REGION);

    let mut config = current_server_config();
    set_region(&mut config, "us-west-1");

    let site = lookup_site(
        config.get(SITE_SUBSYS).and_then(|m| m.get(DEFAULT)),
        config.get(REGION_SUBSYS).and_then(|m| m.get(DEFAULT)),
    )
    .expect("lookup site");
    assert_eq!(site.region(), "us-west-1");

    save_server_config(&layer, &config).expect("save server config");
    load_config(&layer).expect("load config");

    assert_eq!(current_site().region(), "us-west-1");
}
