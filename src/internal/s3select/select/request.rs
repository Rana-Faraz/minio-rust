use std::io;

use serde::Deserialize;

use crate::internal::s3select::csv::{FileHeaderInfo, ReaderArgs as CsvReaderArgs};

#[derive(Debug, Deserialize)]
pub(crate) struct SelectObjectContentRequest {
    #[serde(rename = "Expression")]
    pub(crate) expression: String,
    #[serde(rename = "ExpressionType")]
    pub(crate) expression_type: String,
    #[serde(rename = "InputSerialization")]
    pub(crate) input_serialization: InputSerialization,
    #[serde(rename = "OutputSerialization")]
    pub(crate) output_serialization: OutputSerialization,
    #[serde(rename = "ScanRange")]
    pub(crate) scan_range: Option<ScanRange>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InputSerialization {
    #[serde(rename = "CSV")]
    csv: Option<InputCsvSerialization>,
    #[serde(rename = "Parquet")]
    parquet: Option<InputParquetSerialization>,
    #[serde(rename = "JSON")]
    json: Option<InputJsonSerialization>,
}

#[derive(Debug, Deserialize)]
struct InputCsvSerialization {
    #[serde(rename = "FileHeaderInfo")]
    file_header_info: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InputJsonSerialization {
    #[serde(rename = "Type")]
    _json_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InputParquetSerialization;

#[derive(Debug, Deserialize)]
pub(crate) struct OutputSerialization {
    #[serde(rename = "CSV")]
    csv: Option<OutputCsvSerialization>,
    #[serde(rename = "JSON")]
    json: Option<OutputJsonSerialization>,
}

#[derive(Debug, Deserialize)]
struct OutputCsvSerialization;

#[derive(Debug, Deserialize)]
struct OutputJsonSerialization;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct ScanRange {
    #[serde(rename = "Start")]
    pub(crate) start: Option<usize>,
    #[serde(rename = "End")]
    pub(crate) end: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InputFormat {
    Csv(CsvReaderArgs),
    Parquet,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutputFormat {
    Csv,
    Json,
}

pub(crate) fn parse_input_format(
    input_serialization: InputSerialization,
) -> io::Result<InputFormat> {
    if let Some(csv) = input_serialization.csv {
        let file_header_info = csv
            .file_header_info
            .map(|value| {
                if value.eq_ignore_ascii_case("use") {
                    FileHeaderInfo::Use
                } else {
                    FileHeaderInfo::None
                }
            })
            .unwrap_or(FileHeaderInfo::None);
        Ok(InputFormat::Csv(CsvReaderArgs {
            file_header_info,
            record_delimiter: "\n".to_owned(),
            field_delimiter: ',',
        }))
    } else if input_serialization.parquet.is_some() {
        Ok(InputFormat::Parquet)
    } else if input_serialization.json.is_some() {
        Ok(InputFormat::Json)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "input serialization must define CSV or JSON",
        ))
    }
}

pub(crate) fn parse_output_format(
    output_serialization: OutputSerialization,
) -> io::Result<OutputFormat> {
    if output_serialization.csv.is_some() {
        Ok(OutputFormat::Csv)
    } else if output_serialization.json.is_some() {
        Ok(OutputFormat::Json)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "output serialization must define CSV or JSON",
        ))
    }
}

pub(crate) fn validate_scan_range(scan_range: Option<ScanRange>) -> io::Result<Option<ScanRange>> {
    let Some(scan_range) = scan_range else {
        return Ok(None);
    };
    if scan_range.start.is_none() && scan_range.end.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "scan range must include Start or End",
        ));
    }
    if let (Some(start), Some(end)) = (scan_range.start, scan_range.end) {
        if end < start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scan range end must be greater than or equal to start",
            ));
        }
    }
    Ok(Some(scan_range))
}

pub(crate) fn apply_scan_range(data: Vec<u8>, scan_range: &ScanRange) -> io::Result<Vec<u8>> {
    let len = data.len();
    match (scan_range.start, scan_range.end) {
        (Some(start), Some(end)) => {
            if start >= len || end >= len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "scan range extends past end of input",
                ));
            }
            Ok(data[start..=end].to_vec())
        }
        (Some(start), None) => {
            if start >= len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "scan range start extends past end of input",
                ));
            }
            Ok(data[start..].to_vec())
        }
        (None, Some(end)) => {
            if end == 0 || end > len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "scan range suffix extends past end of input",
                ));
            }
            Ok(data[len - end..].to_vec())
        }
        (None, None) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "scan range must include Start or End",
        )),
    }
}
