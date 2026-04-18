use minio_rust::internal::config::{parse_bool_flag, BoolFlag};

#[test]
fn bool_flag_string_matches_reference_cases() {
    let cases = [
        (BoolFlag(false), "off"),
        (BoolFlag(true), "on"),
        (BoolFlag(false), "off"),
    ];

    for (flag, expected) in cases {
        assert_eq!(flag.to_string(), expected);
    }
}

#[test]
fn bool_flag_marshal_json_matches_reference_cases() {
    let cases = [
        (BoolFlag(false), "\"off\""),
        (BoolFlag(true), "\"on\""),
        (BoolFlag(false), "\"off\""),
    ];

    for (flag, expected) in cases {
        let data = flag.marshal_json().expect("json marshal should succeed");
        assert_eq!(String::from_utf8(data).expect("json is utf-8"), expected);
    }
}

#[test]
fn bool_flag_unmarshal_json_matches_reference_cases() {
    let cases = [
        (br#"{}"#.as_slice(), BoolFlag(false), true),
        (br#"["on"]"#.as_slice(), BoolFlag(false), true),
        (br#""junk""#.as_slice(), BoolFlag(false), true),
        (br#""""#.as_slice(), BoolFlag(true), false),
        (br#""on""#.as_slice(), BoolFlag(true), false),
        (br#""off""#.as_slice(), BoolFlag(false), false),
        (br#""true""#.as_slice(), BoolFlag(true), false),
        (br#""false""#.as_slice(), BoolFlag(false), false),
        (br#""ON""#.as_slice(), BoolFlag(true), false),
        (br#""OFF""#.as_slice(), BoolFlag(false), false),
    ];

    for (data, expected, should_err) in cases {
        let result = BoolFlag::unmarshal_json(data);
        assert_eq!(result.is_err(), should_err);
        if let Ok(flag) = result {
            assert_eq!(flag, expected);
        }
    }
}

#[test]
fn parse_bool_flag_matches_reference_cases() {
    let cases = [
        ("", BoolFlag(false), true),
        ("junk", BoolFlag(false), true),
        ("true", BoolFlag(true), false),
        ("false", BoolFlag(false), false),
        ("ON", BoolFlag(true), false),
        ("OFF", BoolFlag(false), false),
        ("on", BoolFlag(true), false),
        ("off", BoolFlag(false), false),
    ];

    for (input, expected, should_err) in cases {
        let result = parse_bool_flag(input);
        assert_eq!(result.is_err(), should_err);
        if let Ok(flag) = result {
            assert_eq!(flag, expected);
        }
    }
}
