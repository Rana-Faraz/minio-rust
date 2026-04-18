use std::fmt;
use std::io::Read;

use serde_json::{Deserializer, Map, Number, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Unknown,
    Null,
    String,
    Number,
    Boolean,
    Array,
    Object,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetaValue {
    pub offset: usize,
    pub length: usize,
    pub depth: usize,
    pub value: EmittedValue,
    pub value_type: ValueType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EmittedValue {
    Json(Value),
    KV(KV),
    Kvs(KVS),
}

#[derive(Debug, Clone, PartialEq)]
pub struct KV {
    pub key: String,
    pub value: Value,
}

pub type KVS = Vec<KV>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    MaxDepth,
    Reader(String),
    Json(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxDepth => f.write_str("maximum recursion depth exceeded"),
            Self::Reader(message) | Self::Json(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecoderError {
    pub error: Error,
    pub reader_error: Option<String>,
}

impl DecoderError {
    pub fn reader_err(&self) -> Option<&str> {
        self.reader_error.as_deref()
    }
}

impl fmt::Display for DecoderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for DecoderError {}

pub struct Decoder<R: Read> {
    input: Option<R>,
    emit_depth: usize,
    emit_recursive: bool,
    emit_kv: bool,
    object_as_kvs: bool,
    max_depth: usize,
    err: Option<DecoderError>,
}

impl<R: Read> Decoder<R> {
    pub fn new(input: R, emit_depth: isize) -> Self {
        let (emit_depth, emit_recursive) = if emit_depth < 0 {
            (0, true)
        } else {
            (emit_depth as usize, false)
        };
        Self {
            input: Some(input),
            emit_depth,
            emit_recursive,
            emit_kv: false,
            object_as_kvs: false,
            max_depth: 0,
            err: None,
        }
    }

    pub fn emit_kv(mut self) -> Self {
        self.emit_kv = true;
        self
    }

    pub fn recursive(mut self) -> Self {
        self.emit_recursive = true;
        self
    }

    pub fn object_as_kvs(mut self) -> Self {
        self.object_as_kvs = true;
        self
    }

    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }

    pub fn stream(&mut self) -> std::vec::IntoIter<MetaValue> {
        let Some(mut input) = self.input.take() else {
            return Vec::new().into_iter();
        };

        let mut text = String::new();
        if let Err(err) = input.read_to_string(&mut text) {
            self.err = Some(DecoderError {
                error: Error::Reader(err.to_string()),
                reader_error: Some(err.to_string()),
            });
            return Vec::new().into_iter();
        }

        let mut out = Vec::new();
        let iter = Deserializer::from_str(&text).into_iter::<Value>();
        for value in iter {
            match value {
                Ok(value) => {
                    if self.max_depth > 0 && compute_depth(&value) > self.max_depth {
                        self.err = Some(DecoderError {
                            error: Error::MaxDepth,
                            reader_error: None,
                        });
                        return out.into_iter();
                    }
                    self.emit_value(&value, 0, &mut out);
                }
                Err(err) => {
                    self.err = Some(DecoderError {
                        error: Error::Json(err.to_string()),
                        reader_error: None,
                    });
                    return out.into_iter();
                }
            }
        }

        out.into_iter()
    }

    pub fn err(&self) -> Option<DecoderError> {
        self.err.clone()
    }

    fn emit_value(&self, value: &Value, depth: usize, out: &mut Vec<MetaValue>) {
        match value {
            Value::Object(map) => {
                if self.should_emit(depth) {
                    if self.object_as_kvs {
                        out.push(MetaValue {
                            offset: 0,
                            length: 0,
                            depth,
                            value: EmittedValue::Kvs(map_to_kvs(map)),
                            value_type: ValueType::Object,
                        });
                    } else {
                        out.push(MetaValue {
                            offset: 0,
                            length: 0,
                            depth,
                            value: EmittedValue::Json(value.clone()),
                            value_type: ValueType::Object,
                        });
                    }
                }

                if self.emit_recursive || depth < self.emit_depth {
                    for (key, child) in map {
                        if depth + 1 == self.emit_depth && self.emit_kv {
                            out.push(MetaValue {
                                offset: 0,
                                length: 0,
                                depth: depth + 1,
                                value: EmittedValue::KV(KV {
                                    key: key.clone(),
                                    value: child.clone(),
                                }),
                                value_type: value_type_of(child),
                            });
                        } else {
                            self.emit_value(child, depth + 1, out);
                        }
                    }
                }
            }
            Value::Array(values) => {
                if self.should_emit(depth) {
                    out.push(MetaValue {
                        offset: 0,
                        length: 0,
                        depth,
                        value: EmittedValue::Json(value.clone()),
                        value_type: ValueType::Array,
                    });
                }
                if self.emit_recursive || depth < self.emit_depth {
                    for child in values {
                        self.emit_value(child, depth + 1, out);
                    }
                }
            }
            _ => {
                if self.should_emit(depth) {
                    out.push(MetaValue {
                        offset: 0,
                        length: 0,
                        depth,
                        value: EmittedValue::Json(value.clone()),
                        value_type: value_type_of(value),
                    });
                }
            }
        }
    }

    fn should_emit(&self, depth: usize) -> bool {
        if self.emit_recursive {
            depth >= self.emit_depth
        } else {
            depth == self.emit_depth
        }
    }
}

fn map_to_kvs(map: &Map<String, Value>) -> KVS {
    map.iter()
        .map(|(key, value)| KV {
            key: key.clone(),
            value: value.clone(),
        })
        .collect()
}

fn value_type_of(value: &Value) -> ValueType {
    match value {
        Value::Null => ValueType::Null,
        Value::String(_) => ValueType::String,
        Value::Number(_) => ValueType::Number,
        Value::Bool(_) => ValueType::Boolean,
        Value::Array(_) => ValueType::Array,
        Value::Object(_) => ValueType::Object,
    }
}

fn compute_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(compute_depth).max().unwrap_or(0),
        Value::Object(map) => 1 + map.values().map(compute_depth).max().unwrap_or(0),
        _ => 0,
    }
}

pub struct Scanner<R: Read> {
    reader: R,
    pub pos: i64,
    done: bool,
    pub reader_err: Option<String>,
}

impl<R: Read> Scanner<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            pos: 0,
            done: false,
            reader_err: None,
        }
    }

    pub fn remaining(&self) -> i64 {
        if self.done {
            0
        } else {
            i64::MAX
        }
    }

    pub fn next(&mut self) -> u8 {
        if self.done {
            return 0;
        }
        let mut byte = [0u8; 1];
        match self.reader.read(&mut byte) {
            Ok(0) => {
                self.done = true;
                0
            }
            Ok(_) => {
                self.pos += 1;
                byte[0]
            }
            Err(err) => {
                self.reader_err = Some(err.to_string());
                self.done = true;
                0
            }
        }
    }
}

pub fn json_number(value: f64) -> Value {
    Value::Number(Number::from_f64(value).expect("finite number"))
}
