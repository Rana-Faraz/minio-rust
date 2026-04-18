use minio_rust::internal::event::target::{
    is_driver_registered, validate_psql_table_name, Error as TargetError,
};

pub const SOURCE_FILE: &str = "internal/event/target/postgresql_test.go";

#[test]
fn postgresql_registration_matches_reference_expectation() {
    assert!(is_driver_registered("postgres"));
}

#[test]
fn psql_table_name_validation_matches_reference_cases() {
    let valid_tables = [
        "táblë",
        "table",
        "TableName",
        "\"Table name\"",
        "\"✅✅\"",
        "table$one",
        "\"táblë\"",
    ];
    let invalid_tables = ["table name", "table \"name\"", "✅✅", "$table$"];

    for name in valid_tables {
        assert!(
            validate_psql_table_name(name).is_ok(),
            "expected valid table name: {name}"
        );
    }

    for name in invalid_tables {
        assert_eq!(
            validate_psql_table_name(name),
            Err(TargetError::InvalidPostgresqlTable),
            "expected invalid table name: {name}"
        );
    }
}
