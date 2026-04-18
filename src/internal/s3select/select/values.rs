use chrono::{Days, FixedOffset, NaiveDate, TimeZone};
use serde_json::{Number, Value};

use crate::internal::s3select::sql::{format_sql_timestamp, parse_sql_timestamp};

use super::ast::{PathExpr, PathSegment, Predicate, Row, SelectExpr};

impl SelectExpr {
    pub(crate) fn evaluate(&self, row: &Row) -> (String, Value) {
        match self {
            Self::Column { path, output_name } => (output_name.clone(), path_value(row, path)),
            Self::DateAddDays {
                path,
                days,
                output_name,
            } => (
                output_name.clone(),
                path_scalar(row, path)
                    .as_ref()
                    .map(|value| add_days_to_value(value, *days))
                    .unwrap_or(Value::Null),
            ),
            Self::NotCastBool { path, output_name } => {
                let value = path_scalar(row, path)
                    .as_ref()
                    .and_then(value_as_bool)
                    .map(|value| Value::Bool(!value))
                    .unwrap_or(Value::Null);
                (output_name.clone(), value)
            }
        }
    }
}

impl Predicate {
    pub(crate) fn matches(&self, row: &Row) -> bool {
        match self {
            Self::Eq(path, literal) => path_scalar(row, path)
                .as_ref()
                .map(|value| compare_value(value, literal, CompareOp::Eq))
                .unwrap_or(false),
            Self::Ne(path, literal) => path_scalar(row, path)
                .as_ref()
                .map(|value| compare_value(value, literal, CompareOp::Ne))
                .unwrap_or(true),
            Self::Gt(path, literal) => path_scalar(row, path)
                .as_ref()
                .map(|value| compare_value(value, literal, CompareOp::Gt))
                .unwrap_or(false),
            Self::LtEq(path, literal) => path_scalar(row, path)
                .as_ref()
                .map(|value| compare_value(value, literal, CompareOp::LtEq))
                .unwrap_or(false),
            Self::GtEq(path, literal) => path_scalar(row, path)
                .as_ref()
                .map(|value| compare_value(value, literal, CompareOp::GtEq))
                .unwrap_or(false),
            Self::InPath(path, options) => path_scalar(row, path)
                .as_ref()
                .map(|value| {
                    options
                        .iter()
                        .any(|literal| compare_value(value, literal, CompareOp::Eq))
                })
                .unwrap_or(false),
            Self::Contains(needle, path) => membership_items(row, path)
                .into_iter()
                .any(|item| json_loose_eq(&item, needle)),
            Self::IsNull(path) => path_scalar(row, path).is_none_or(|value| value.is_null()),
            Self::IsNotNull(path) => path_scalar(row, path).is_some_and(|value| !value.is_null()),
            Self::IsNotEmpty(path) => path_scalar(row, path)
                .as_ref()
                .map(value_as_string)
                .map(|value| !value.is_empty())
                .unwrap_or(false),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    LtEq,
    GtEq,
}

fn compare_value(value: &Value, literal: &Value, op: CompareOp) -> bool {
    if let (Some(actual), Some(expected)) = (value_as_f64(value), value_as_f64(literal)) {
        return match op {
            CompareOp::Eq => (actual - expected).abs() < f64::EPSILON,
            CompareOp::Ne => (actual - expected).abs() >= f64::EPSILON,
            CompareOp::Gt => actual > expected,
            CompareOp::LtEq => actual <= expected,
            CompareOp::GtEq => actual >= expected,
        };
    }
    if let (Value::Bool(actual), Value::Bool(expected)) = (value, literal) {
        return match op {
            CompareOp::Eq => actual == expected,
            CompareOp::Ne => actual != expected,
            CompareOp::Gt => (*actual as u8) > (*expected as u8),
            CompareOp::LtEq => (*actual as u8) <= (*expected as u8),
            CompareOp::GtEq => (*actual as u8) >= (*expected as u8),
        };
    }
    if matches!(value, Value::Bool(_)) || matches!(literal, Value::Bool(_)) {
        return matches!(op, CompareOp::Ne);
    }
    let actual = value_as_string(value);
    let expected = value_as_string(literal);
    match op {
        CompareOp::Eq => actual == expected,
        CompareOp::Ne => actual != expected,
        CompareOp::Gt => actual > expected,
        CompareOp::LtEq => actual <= expected,
        CompareOp::GtEq => actual >= expected,
    }
}

pub(crate) fn value_as_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                value.to_string()
            } else if let Some(value) = number.as_u64() {
                value.to_string()
            } else if let Some(value) = number.as_f64() {
                if value.fract().abs() < f64::EPSILON {
                    format!("{value:.0}")
                } else {
                    number.to_string()
                }
            } else {
                number.to_string()
            }
        }
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).expect("json serialization should succeed")
        }
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(boolean) => Some(*boolean),
        Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Some(true),
            "false" | "f" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn add_days_to_value(value: &Value, days: u64) -> Value {
    let Value::String(text) = value else {
        return Value::Null;
    };
    let Ok(timestamp) = parse_sql_timestamp(text) else {
        return Value::Null;
    };
    let Some(date) = timestamp.date_naive().checked_add_days(Days::new(days)) else {
        return Value::Null;
    };
    let shifted = timestamp
        .timezone()
        .from_local_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid"))
        .single()
        .expect("fixed-offset datetime is unambiguous");
    Value::String(format_sql_timestamp(shifted))
}

pub(crate) fn number_to_value(number: f64) -> Value {
    Number::from_f64(number)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn path_scalar(row: &Row, path: &PathExpr) -> Option<Value> {
    if path.segments.len() == 1 {
        if let Some(PathSegment::Key(key)) = path.segments.first() {
            return row.get(key).cloned();
        }
    }
    row.resolve_path(path).into_iter().next()
}

fn path_value(row: &Row, path: &PathExpr) -> Value {
    let values = row.resolve_path(path);
    match values.as_slice() {
        [] => Value::Null,
        [single] => single.clone(),
        many => Value::Array(many.to_vec()),
    }
}

fn membership_items(row: &Row, path: &PathExpr) -> Vec<Value> {
    let values = row.resolve_path(path);
    if values.len() == 1 {
        if let Some(Value::Array(items)) = values.first() {
            return items.clone();
        }
    }
    values
}

fn json_loose_eq(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(_), Value::Number(_)) => value_as_f64(left)
            .zip(value_as_f64(right))
            .is_some_and(|(l, r)| (l - r).abs() < f64::EPSILON),
        (Value::Array(left_items), Value::Array(right_items)) => {
            left_items.len() == right_items.len()
                && left_items
                    .iter()
                    .zip(right_items.iter())
                    .all(|(left, right)| json_loose_eq(left, right))
        }
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::String(left), Value::String(right)) => left == right,
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

pub(crate) fn date_days_to_value(days_since_epoch: i32) -> Value {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("unix epoch date is valid");
    let Some(date) = epoch.checked_add_days(Days::new(days_since_epoch as u64)) else {
        return Value::Null;
    };
    let utc = FixedOffset::east_opt(0).expect("zero UTC offset is valid");
    let timestamp = utc
        .from_local_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid"))
        .single()
        .expect("UTC datetime is unambiguous");
    Value::String(format_sql_timestamp(timestamp))
}

pub(crate) fn timestamp_millis_to_value(value: i64) -> Value {
    let utc = FixedOffset::east_opt(0).expect("zero UTC offset is valid");
    let Some(timestamp) = utc.timestamp_millis_opt(value).single() else {
        return Value::Null;
    };
    Value::String(format_sql_timestamp(timestamp))
}

pub(crate) fn timestamp_micros_to_value(value: i64) -> Value {
    let utc = FixedOffset::east_opt(0).expect("zero UTC offset is valid");
    let seconds = value.div_euclid(1_000_000);
    let nanos = (value.rem_euclid(1_000_000) as u32) * 1_000;
    let Some(timestamp) = utc.timestamp_opt(seconds, nanos).single() else {
        return Value::Null;
    };
    Value::String(format_sql_timestamp(timestamp))
}
