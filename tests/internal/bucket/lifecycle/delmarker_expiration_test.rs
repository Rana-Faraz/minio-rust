use minio_rust::internal::bucket::lifecycle::{parse_lifecycle_config, Error};

#[test]
fn test_del_marker_exp_parse_and_validate_line_26() {
    for (xml, expected) in [
        (
            "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><DelMarkerExpiration><Days>1</Days></DelMarkerExpiration></Rule></LifecycleConfiguration>",
            None,
        ),
        (
            "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><DelMarkerExpiration><Days>-1</Days></DelMarkerExpiration></Rule></LifecycleConfiguration>",
            Some(Error::InvalidDaysDelMarkerExpiration),
        ),
    ] {
        let parsed = parse_lifecycle_config(xml);
        match expected {
            None => assert!(parsed.is_ok(), "expected parse success for {xml}"),
            Some(error) => assert_eq!(parsed.unwrap_err(), error),
        }
    }
}

#[test]
fn subtest_test_del_marker_exp_parse_and_validate_fmt_sprintf_test_del_marker_d_line_42() {
    test_del_marker_exp_parse_and_validate_line_26();
}
