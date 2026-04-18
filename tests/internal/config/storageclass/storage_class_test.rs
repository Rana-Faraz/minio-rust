use minio_rust::internal::config::storageclass::{
    is_valid, parse_storage_class, validate_parity, Config, StorageClass, RRS, STANDARD,
};

#[test]
fn parse_storage_class_matches_reference_cases() {
    let cases = [
        ("EC:3", Some(StorageClass { parity: 3 }), None),
        ("EC:4", Some(StorageClass { parity: 4 }), None),
        (
            "AB:4",
            None,
            Some("Unsupported scheme AB. Supported scheme is EC"),
        ),
        ("EC:4:5", None, Some("Too many sections in EC:4:5")),
        ("EC:A", None, Some("invalid digit found in string")),
        ("AB", None, Some("Too few sections in AB")),
    ];

    for (input, expected, expected_error_fragment) in cases {
        let result = parse_storage_class(input);
        match (result, expected, expected_error_fragment) {
            (Ok(actual), Some(expected), None) => assert_eq!(actual, expected),
            (Err(error), None, Some(fragment)) => assert!(
                error.to_string().contains(fragment),
                "expected error containing {fragment:?}, got {error}"
            ),
            (other, expected, error) => panic!(
                "unexpected result for {input}: result={other:?} expected={expected:?} error={error:?}"
            ),
        }
    }
}

#[test]
fn validate_parity_matches_reference_cases() {
    let cases = [
        (2, 4, 16, true),
        (3, 3, 16, true),
        (0, 0, 16, true),
        (1, 4, 16, true),
        (0, 4, 16, true),
        (7, 6, 16, false),
        (9, 0, 16, false),
        (9, 9, 16, false),
        (2, 9, 16, false),
        (9, 2, 16, false),
    ];

    for (rrs_parity, ss_parity, set_drive_count, success) in cases {
        let result = validate_parity(ss_parity, rrs_parity, set_drive_count);
        assert_eq!(
            result.is_ok(),
            success,
            "rrs={rrs_parity} ss={ss_parity} drives={set_drive_count}"
        );
    }
}

#[test]
fn parity_count_matches_reference_cases() {
    let cases = [
        (RRS, 16, 14, 2, 8, 2),
        (STANDARD, 16, 8, 8, 8, 2),
        ("", 16, 8, 8, 8, 2),
        (RRS, 16, 9, 7, 8, 7),
        (STANDARD, 16, 10, 6, 6, 2),
        ("", 16, 9, 7, 7, 2),
    ];

    for (storage_class, drive_count, expected_data, expected_parity, standard, rrs) in cases {
        let cfg = Config {
            standard: StorageClass { parity: standard },
            rrs: StorageClass { parity: rrs },
            initialized: true,
        };
        let parity = cfg.get_parity_for_sc(storage_class);
        assert_eq!(
            drive_count - parity,
            expected_data,
            "storage class {storage_class}"
        );
        assert_eq!(parity, expected_parity, "storage class {storage_class}");
    }
}

#[test]
fn is_valid_storage_class_kind_matches_reference_cases() {
    let cases = [
        ("STANDARD", true),
        ("REDUCED_REDUNDANCY", true),
        ("", false),
        ("INVALID", false),
        ("123", false),
        ("MINIO_STORAGE_CLASS_RRS", false),
        ("MINIO_STORAGE_CLASS_STANDARD", false),
    ];

    for (input, expected) in cases {
        assert_eq!(is_valid(input), expected, "input {input}");
    }
}
