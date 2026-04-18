use chrono::{FixedOffset, TimeZone};

use minio_rust::internal::s3select::sql::Value;

pub const SOURCE_FILE: &str = "internal/s3select/sql/value_test.go";

const FLOAT_CMP_TOLERANCE: f64 = 0.000001;

fn utc_timestamp(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> chrono::DateTime<FixedOffset> {
    FixedOffset::east_opt(0)
        .unwrap()
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .unwrap()
}

fn value_builders() -> Vec<Value> {
    vec![
        Value::from_null(),
        Value::from_bool(true),
        Value::from_bytes(b"byte contents".to_vec()),
        Value::from_float(std::f64::consts::PI),
        Value::from_int(0x1337),
        Value::from_timestamp(utc_timestamp(2006, 1, 2, 15, 4, 5)),
        Value::from_string("string contents"),
    ]
}

fn alt_value_builders() -> Vec<Value> {
    vec![
        Value::from_null(),
        Value::from_bool(false),
        Value::from_bytes(Vec::<u8>::new()),
        Value::from_float(0.0),
        Value::from_int(0),
        Value::from_timestamp(utc_timestamp(1, 1, 1, 0, 0, 0)),
        Value::from_string(""),
    ]
}

#[test]
fn value_same_type_as_matches_reference_cases() {
    let builders = value_builders();
    for (left_index, left) in builders.iter().enumerate() {
        for (right_index, right) in builders.iter().enumerate() {
            assert_eq!(
                left.same_type_as(right),
                left_index == right_index,
                "same_type_as {} == {}",
                left.get_type_string(),
                right.get_type_string()
            );
        }
    }
}

#[test]
fn value_equals_matches_reference_cases() {
    let builders = value_builders();
    let alt_builders = alt_value_builders();

    for (left_index, left) in builders.iter().enumerate() {
        for (right_index, right) in builders.iter().enumerate() {
            assert_eq!(
                left.equals(right),
                left_index == right_index,
                "equals {} == {}",
                left.get_type_string(),
                right.get_type_string()
            );
        }
    }

    for (left_index, left) in builders.iter().enumerate() {
        for (right_index, right) in alt_builders.iter().enumerate() {
            let expected = left.is_null() && right.is_null() && left_index == 0 && right_index == 0;
            assert_eq!(
                left.equals(right),
                expected,
                "equals alt {} != {}",
                left.get_type_string(),
                right.get_type_string()
            );
        }
    }
}

#[test]
fn value_csv_string_matches_reference_cases() {
    let builders = value_builders();
    let alt_builders = alt_value_builders();
    let expected = [
        ("", ""),
        ("true", "false"),
        ("byte contents", ""),
        ("3.141592653589793", "0"),
        ("4919", "0"),
        ("2006-01-02T15:04:05Z", "0001T"),
        ("string contents", ""),
    ];

    for (index, (want, want_alt)) in expected.into_iter().enumerate() {
        assert_eq!(
            builders[index].csv_string(),
            want,
            "csv string case {index}"
        );
        assert_eq!(
            alt_builders[index].csv_string(),
            want_alt,
            "csv alt string case {index}"
        );
    }
}

#[test]
fn value_bytes_to_int_matches_reference_cases() {
    let cases = vec![
        ("0".to_owned(), 0, true),
        ("-0".to_owned(), 0, true),
        ("1".to_owned(), 1, true),
        ("-1".to_owned(), -1, true),
        ("+1".to_owned(), 1, true),
        (i64::MAX.to_string(), i64::MAX, true),
        (i64::MIN.to_string(), i64::MIN, true),
        ("9223372036854775808".to_owned(), i64::MAX, false),
        ("-9223372036854775809".to_owned(), i64::MIN, false),
        (" 0".to_owned(), 0, true),
        ("1 ".to_owned(), 1, true),
        (" -1 ".to_owned(), -1, true),
        ("\t+1\t".to_owned(), 1, true),
        ("3e5".to_owned(), 0, false),
        ("0xff".to_owned(), 0, false),
    ];

    for (index, (input, want, want_ok)) in cases.into_iter().enumerate() {
        let value = Value::from_bytes(input.into_bytes());
        let (got, got_ok) = value.bytes_to_int();
        assert_eq!(got, want, "bytes_to_int case {index}");
        assert_eq!(got_ok, want_ok, "bytes_to_int ok case {index}");
    }
}

#[test]
fn value_bytes_to_float_matches_reference_cases() {
    let smallest_nonzero = f64::from_bits(1);
    let cases = vec![
        ("0".to_owned(), 0.0, true),
        ("-0".to_owned(), 0.0, true),
        ("1".to_owned(), 1.0, true),
        ("-1".to_owned(), -1.0, true),
        ("+1".to_owned(), 1.0, true),
        (i64::MAX.to_string(), i64::MAX as f64, true),
        (i64::MIN.to_string(), i64::MIN as f64, true),
        (
            "9223372036854775808".to_owned(),
            9_223_372_036_854_776_000.0,
            true,
        ),
        (
            "-9223372036854775809".to_owned(),
            -9_223_372_036_854_776_000.0,
            true,
        ),
        (f64::MAX.to_string(), f64::MAX, true),
        ((-f64::MAX).to_string(), -f64::MAX, true),
        (
            "1.797693134862315708145274237317043567981e+309".to_owned(),
            f64::INFINITY,
            false,
        ),
        (
            "-1.797693134862315708145274237317043567981e+309".to_owned(),
            f64::NEG_INFINITY,
            false,
        ),
        (smallest_nonzero.to_string(), smallest_nonzero, true),
        ((-smallest_nonzero).to_string(), -smallest_nonzero, true),
        (" 0".to_owned(), 0.0, true),
        ("1 ".to_owned(), 1.0, true),
        (" -1 ".to_owned(), -1.0, true),
        ("\t+1\t".to_owned(), 1.0, true),
        ("3e5".to_owned(), 300000.0, true),
        ("0xff".to_owned(), 0.0, false),
    ];

    for (index, (input, want, want_ok)) in cases.into_iter().enumerate() {
        let value = Value::from_bytes(input.into_bytes());
        let (got, got_ok) = value.bytes_to_float();
        if want.is_infinite() {
            assert_eq!(got, want, "bytes_to_float case {index}");
        } else {
            assert!(
                (got - want).abs() <= FLOAT_CMP_TOLERANCE,
                "bytes_to_float case {index}: got {got}, want {want}"
            );
        }
        assert_eq!(got_ok, want_ok, "bytes_to_float ok case {index}");
    }
}

#[test]
fn value_bytes_to_bool_matches_reference_cases() {
    let cases = [
        ("true", true, true),
        ("false", false, true),
        ("t", true, true),
        ("f", false, true),
        ("1", true, true),
        ("0", false, true),
        (" true ", true, true),
        ("\ttrue\t", true, true),
        ("TRUE", true, true),
        ("FALSE", false, true),
        ("no", false, false),
    ];

    for (index, (input, want, want_ok)) in cases.into_iter().enumerate() {
        let value = Value::from_bytes(input.as_bytes().to_vec());
        let (got, got_ok) = value.bytes_to_bool();
        assert_eq!(got, want, "bytes_to_bool case {index}");
        assert_eq!(got_ok, want_ok, "bytes_to_bool ok case {index}");
    }
}
