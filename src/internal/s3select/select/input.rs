use std::io::{self, Cursor};

use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field as ParquetField;
use serde_json::{Map, Number, Value};

use crate::internal::s3select::{
    csv::{Reader as CsvReader, ReaderArgs as CsvReaderArgs},
    json::{Reader as JsonReader, ReaderArgs as JsonReaderArgs},
};

use super::ast::Row;
use super::values::{
    date_days_to_value, number_to_value, timestamp_micros_to_value, timestamp_millis_to_value,
    value_as_string,
};

pub(crate) fn load_csv_rows(data: &[u8], args: &CsvReaderArgs) -> io::Result<Vec<Row>> {
    let mut reader = CsvReader::new(Cursor::new(data), args)?;
    let mut rows = Vec::new();
    loop {
        match reader.read(None) {
            Ok(record) => {
                let fields = record
                    .column_names
                    .iter()
                    .cloned()
                    .zip(record.values.into_iter().map(Value::String))
                    .collect();
                rows.push(Row { fields });
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        }
    }
    Ok(rows)
}

pub(crate) fn load_json_rows(data: &[u8]) -> io::Result<Vec<Row>> {
    let mut reader = JsonReader::new(Cursor::new(data), &JsonReaderArgs)?;
    let mut rows = Vec::new();
    loop {
        match reader.read() {
            Ok(record) => {
                let fields = match record.value {
                    Value::Object(map) => map.into_iter().collect(),
                    value => vec![("_1".to_owned(), value)],
                };
                rows.push(Row { fields });
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        }
    }
    Ok(rows)
}

pub(crate) fn load_parquet_rows(data: &[u8]) -> io::Result<Vec<Row>> {
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(data))
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let iter = reader
        .get_row_iter(None)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    iter.map(|row| {
        row.map(parquet_row_to_row)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    })
    .collect()
}

fn parquet_row_to_row(row: parquet::record::Row) -> Row {
    let fields = row
        .into_columns()
        .into_iter()
        .filter(|(name, _)| name != "__index_level_0__")
        .map(|(name, field)| (name, parquet_field_to_value(field)))
        .collect();
    Row { fields }
}

fn parquet_field_to_value(field: ParquetField) -> Value {
    match field {
        ParquetField::Null => Value::Null,
        ParquetField::Bool(value) => Value::Bool(value),
        ParquetField::Byte(value) => Value::Number(Number::from(value)),
        ParquetField::Short(value) => Value::Number(Number::from(value)),
        ParquetField::Int(value) => Value::Number(Number::from(value)),
        ParquetField::Long(value) => Value::Number(Number::from(value)),
        ParquetField::UByte(value) => Value::Number(Number::from(value)),
        ParquetField::UShort(value) => Value::Number(Number::from(value)),
        ParquetField::UInt(value) => Value::Number(Number::from(value)),
        ParquetField::ULong(value) => Value::Number(Number::from(value)),
        ParquetField::Float16(value) => number_to_value(f32::from(value) as f64),
        ParquetField::Float(value) => number_to_value(value as f64),
        ParquetField::Double(value) => number_to_value(value),
        ParquetField::Decimal(value) => Value::String(format!("{value:?}")),
        ParquetField::Str(value) => Value::String(value),
        ParquetField::Bytes(value) => {
            Value::String(String::from_utf8_lossy(value.data()).into_owned())
        }
        ParquetField::Date(days) => date_days_to_value(days),
        ParquetField::TimestampMillis(value) => timestamp_millis_to_value(value),
        ParquetField::TimestampMicros(value) => timestamp_micros_to_value(value),
        ParquetField::Group(row) => {
            let mut object = Map::new();
            for (name, field) in parquet_row_to_row(row).fields {
                object.insert(name, field);
            }
            Value::Object(object)
        }
        ParquetField::ListInternal(list) => Value::Array(
            list.elements()
                .iter()
                .cloned()
                .map(parquet_field_to_value)
                .collect(),
        ),
        ParquetField::MapInternal(map) => {
            let mut object = Map::new();
            for (key, value) in map.entries() {
                object.insert(
                    parquet_field_key(key),
                    parquet_field_to_value(value.clone()),
                );
            }
            Value::Object(object)
        }
    }
}

fn parquet_field_key(field: &ParquetField) -> String {
    match field {
        ParquetField::Str(value) => value.clone(),
        other => value_as_string(&parquet_field_to_value(other.clone())),
    }
}

pub fn generate_sample_csv_data(count: usize) -> Vec<u8> {
    let mut out = String::from("id,name,age,city\n");
    for idx in 0..count {
        out.push_str(&format!(
            "{idx},name{idx:04},age{:02},city{idx:04}\n",
            idx % 100
        ));
    }
    out.into_bytes()
}
