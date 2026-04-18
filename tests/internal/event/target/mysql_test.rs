use minio_rust::internal::event::target::is_driver_registered;

pub const SOURCE_FILE: &str = "internal/event/target/mysql_test.go";

#[test]
fn mysql_registration_matches_reference_expectation() {
    assert!(is_driver_registered("mysql"));
}
