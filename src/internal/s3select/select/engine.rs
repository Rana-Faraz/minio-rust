use std::io::{self, Read, Write};

use quick_xml::de::from_reader;
use serde_json::{Number, Value};

use super::ast::{Query, QueryKind, Row};
use super::input::{load_csv_rows, load_json_rows, load_parquet_rows};
use super::output::{write_csv_row, write_json_row};
use super::parser::parse_query;
use super::request::{
    apply_scan_range, parse_input_format, parse_output_format, validate_scan_range, InputFormat,
    OutputFormat, ScanRange, SelectObjectContentRequest,
};

#[derive(Debug)]
pub struct S3Select {
    query: Query,
    input_format: InputFormat,
    output_format: OutputFormat,
    scan_range: Option<ScanRange>,
    input_data: Option<Vec<u8>>,
}

impl S3Select {
    pub fn from_xml<R: Read>(reader: R) -> io::Result<Self> {
        let request: SelectObjectContentRequest = from_reader(std::io::BufReader::new(reader))
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        if !request.expression_type.eq_ignore_ascii_case("sql") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only SQL expression type is supported",
            ));
        }

        let query = parse_query(&request.expression)?;
        let input_format = parse_input_format(request.input_serialization)?;
        let output_format = parse_output_format(request.output_serialization)?;

        Ok(Self {
            query,
            input_format,
            output_format,
            scan_range: validate_scan_range(request.scan_range)?,
            input_data: None,
        })
    }

    pub fn open<R: Read>(&mut self, mut reader: R) -> io::Result<()> {
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        if let Some(scan_range) = &self.scan_range {
            data = apply_scan_range(data, scan_range)?;
        }
        self.input_data = Some(data);
        Ok(())
    }

    pub fn evaluate<W: Write>(&self, mut writer: W) -> io::Result<()> {
        let data = self
            .input_data
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "input not opened"))?;
        let rows = match &self.input_format {
            InputFormat::Csv(csv_args) => load_csv_rows(data, csv_args)?,
            InputFormat::Parquet => load_parquet_rows(data)?,
            InputFormat::Json => load_json_rows(data)?,
        };
        let projected_rows = self.project_rows(rows)?;

        match &self.output_format {
            OutputFormat::Csv => {
                for row in projected_rows {
                    write_csv_row(&mut writer, &row)?;
                }
            }
            OutputFormat::Json => {
                for row in projected_rows {
                    write_json_row(&mut writer, &row)?;
                }
            }
        }

        Ok(())
    }

    fn project_rows(&self, rows: Vec<Row>) -> io::Result<Vec<Row>> {
        let filtered = rows
            .into_iter()
            .filter(|row| {
                self.query
                    .filter
                    .iter()
                    .all(|predicate| predicate.matches(row))
            })
            .collect::<Vec<_>>();

        match &self.query.kind {
            QueryKind::Count { column, alias } => {
                let count = match column {
                    Some(column) => filtered
                        .iter()
                        .filter(|row| !matches!(row.get(column), None | Some(Value::Null)))
                        .count(),
                    None => filtered.len(),
                };
                let value = Value::Number(Number::from(count as u64));
                match (&self.output_format, alias.as_deref()) {
                    (OutputFormat::Json, Some(alias)) => Ok(vec![Row {
                        fields: vec![(alias.to_owned(), value)],
                    }]),
                    _ => Ok(vec![Row {
                        fields: vec![("_1".to_owned(), value)],
                    }]),
                }
            }
            QueryKind::SelectAll => {
                let mut projected = filtered;
                if let Some(limit) = self.query.limit {
                    projected.truncate(limit);
                }
                Ok(projected)
            }
            QueryKind::SelectExpressions(expressions) => {
                let mut projected = filtered;
                if let Some(limit) = self.query.limit {
                    projected.truncate(limit);
                }
                Ok(projected
                    .into_iter()
                    .map(|row| Row {
                        fields: expressions
                            .iter()
                            .map(|expression| expression.evaluate(&row))
                            .collect(),
                    })
                    .collect())
            }
        }
    }
}
