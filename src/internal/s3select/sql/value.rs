use chrono::{DateTime, FixedOffset};

use super::timestamp::format_sql_timestamp;

#[derive(Debug, Clone, PartialEq)]
pub enum ValueRepr {
    Null,
    Bool(bool),
    String(String),
    Int(i64),
    Float(f64),
    Timestamp(DateTime<FixedOffset>),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Missing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Value {
    value: ValueRepr,
}

impl Value {
    pub fn from_null() -> Self {
        Self {
            value: ValueRepr::Null,
        }
    }

    pub fn from_bool(value: bool) -> Self {
        Self {
            value: ValueRepr::Bool(value),
        }
    }

    pub fn from_bytes(value: impl Into<Vec<u8>>) -> Self {
        Self {
            value: ValueRepr::Bytes(value.into()),
        }
    }

    pub fn from_float(value: f64) -> Self {
        Self {
            value: ValueRepr::Float(value),
        }
    }

    pub fn from_int(value: i64) -> Self {
        Self {
            value: ValueRepr::Int(value),
        }
    }

    pub fn from_timestamp(value: DateTime<FixedOffset>) -> Self {
        Self {
            value: ValueRepr::Timestamp(value),
        }
    }

    pub fn from_string(value: impl Into<String>) -> Self {
        Self {
            value: ValueRepr::String(value.into()),
        }
    }

    pub fn from_array(value: Vec<Value>) -> Self {
        Self {
            value: ValueRepr::Array(value),
        }
    }

    pub fn from_missing() -> Self {
        Self {
            value: ValueRepr::Missing,
        }
    }

    pub fn get_type_string(&self) -> &'static str {
        match self.value {
            ValueRepr::Null => "NULL",
            ValueRepr::Bool(_) => "BOOL",
            ValueRepr::String(_) => "STRING",
            ValueRepr::Int(_) => "INT",
            ValueRepr::Float(_) => "FLOAT",
            ValueRepr::Timestamp(_) => "TIMESTAMP",
            ValueRepr::Bytes(_) => "BYTES",
            ValueRepr::Array(_) => "ARRAY",
            ValueRepr::Missing => "MISSING",
        }
    }

    pub fn same_type_as(&self, other: &Self) -> bool {
        std::mem::discriminant(&self.value) == std::mem::discriminant(&other.value)
    }

    pub fn equals(&self, other: &Self) -> bool {
        self.same_type_as(other) && self.value == other.value
    }

    pub fn is_null(&self) -> bool {
        matches!(self.value, ValueRepr::Null)
    }

    pub fn csv_string(&self) -> String {
        match &self.value {
            ValueRepr::Null | ValueRepr::Missing => String::new(),
            ValueRepr::Bool(value) => value.to_string(),
            ValueRepr::String(value) => value.clone(),
            ValueRepr::Int(value) => value.to_string(),
            ValueRepr::Float(value) => value.to_string(),
            ValueRepr::Timestamp(value) => format_sql_timestamp(*value),
            ValueRepr::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
            ValueRepr::Array(_) => "CSV serialization not implemented for this type".to_owned(),
        }
    }

    pub fn to_bytes(&self) -> Option<&[u8]> {
        match &self.value {
            ValueRepr::Bytes(bytes) => Some(bytes.as_slice()),
            _ => None,
        }
    }

    pub fn bytes_to_int(&self) -> (i64, bool) {
        let Some(bytes) = self.to_bytes() else {
            return (0, false);
        };
        let trimmed = String::from_utf8_lossy(bytes).trim().to_owned();
        if let Ok(value) = trimmed.parse::<i64>() {
            return (value, true);
        }
        if let Ok(value) = trimmed.parse::<i128>() {
            if value > i64::MAX as i128 {
                return (i64::MAX, false);
            }
            if value < i64::MIN as i128 {
                return (i64::MIN, false);
            }
        }
        (0, false)
    }

    pub fn bytes_to_float(&self) -> (f64, bool) {
        let Some(bytes) = self.to_bytes() else {
            return (0.0, false);
        };
        let trimmed = String::from_utf8_lossy(bytes).trim().to_owned();
        match trimmed.parse::<f64>() {
            Ok(value) => (value, !value.is_infinite()),
            Err(_) => (0.0, false),
        }
    }

    pub fn bytes_to_bool(&self) -> (bool, bool) {
        let Some(bytes) = self.to_bytes() else {
            return (false, false);
        };
        match String::from_utf8_lossy(bytes)
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "t" | "true" | "1" => (true, true),
            "f" | "false" | "0" => (false, true),
            _ => (false, false),
        }
    }
}
