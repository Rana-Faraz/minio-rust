use minio_rust::cmd::{get_missing_site_names, SiteResyncStatus};

pub const SOURCE_FILE: &str = "cmd/site-replication_test.go";

#[test]
fn test_get_missing_site_names_line_28() {
    let configured = vec!["site-a", "site-b", "site-c", "site-d"];
    let statuses = vec![
        SiteResyncStatus {
            depl_id: "site-a".to_string(),
            ..Default::default()
        },
        SiteResyncStatus {
            depl_id: "site-c".to_string(),
            ..Default::default()
        },
        SiteResyncStatus {
            depl_id: String::new(),
            ..Default::default()
        },
    ];

    let missing = get_missing_site_names(&configured, &statuses);
    assert_eq!(missing, vec!["site-b", "site-d"]);
}
