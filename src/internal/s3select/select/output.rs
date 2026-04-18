use std::io::{self, Write};

use serde_json::{Map, Value};

use super::ast::Row;
use super::values::value_as_string;

pub(crate) fn write_csv_row<W: Write>(writer: &mut W, row: &Row) -> io::Result<()> {
    let line = row
        .fields
        .iter()
        .map(|(_, value)| encode_csv_field(&value_as_string(value), ','))
        .collect::<Vec<_>>()
        .join(",");
    writer.write_all(line.as_bytes())?;
    writer.write_all(b"\n")
}

pub(crate) fn write_json_row<W: Write>(writer: &mut W, row: &Row) -> io::Result<()> {
    let mut object = Map::new();
    for (column, value) in &row.fields {
        object.insert(column.clone(), value.clone());
    }
    serde_json::to_writer(&mut *writer, &Value::Object(object))
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    writer.write_all(b"\n")
}

fn encode_csv_field(field: &str, delimiter: char) -> String {
    if field.contains(delimiter) || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_owned()
    }
}
