use std::collections::HashMap;
use std::io::{self, Read, Write};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileHeaderInfo {
    None,
    Use,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderArgs {
    pub file_header_info: FileHeaderInfo,
    pub record_delimiter: String,
    pub field_delimiter: char,
}

impl Default for ReaderArgs {
    fn default() -> Self {
        Self {
            file_header_info: FileHeaderInfo::None,
            record_delimiter: "\n".to_owned(),
            field_delimiter: ',',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    pub column_names: Vec<String>,
    pub values: Vec<String>,
    pub name_index_map: HashMap<String, usize>,
}

impl Record {
    pub fn write_csv<W: Write>(&self, mut writer: W, delimiter: char) -> io::Result<()> {
        let line = self
            .values
            .iter()
            .map(|field| encode_csv_field(field, delimiter))
            .collect::<Vec<_>>()
            .join(&delimiter.to_string());
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")
    }
}

#[derive(Debug)]
pub struct Reader {
    pub column_names: Vec<String>,
    pub name_index_map: HashMap<String, usize>,
    rows: Vec<Vec<String>>,
    index: usize,
    pending_error: Option<io::Error>,
    closed: bool,
}

impl Reader {
    pub fn new<R: Read>(mut read_closer: R, args: &ReaderArgs) -> io::Result<Self> {
        let mut bytes = Vec::new();
        let mut pending_error = None;
        let mut chunk = [0u8; 8192];
        loop {
            match read_closer.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => bytes.extend_from_slice(&chunk[..n]),
                Err(err) => {
                    pending_error = Some(err);
                    break;
                }
            }
        }

        let text = String::from_utf8(bytes)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let mut raw_rows = split_records(&text, &args.record_delimiter);
        if matches!(raw_rows.last(), Some(last) if last.is_empty()) {
            raw_rows.pop();
        }

        let mut parsed_rows = Vec::new();
        for raw in raw_rows {
            if raw.is_empty() {
                continue;
            }
            match parse_csv_row(&raw, args.field_delimiter) {
                Ok(row) => parsed_rows.push(row),
                Err(err) => {
                    pending_error = Some(err);
                    break;
                }
            }
        }

        let mut column_names = Vec::new();
        let mut rows = parsed_rows;
        if let Some(first_row) = rows.first().cloned() {
            match args.file_header_info {
                FileHeaderInfo::Use => {
                    column_names = first_row;
                    rows.remove(0);
                }
                FileHeaderInfo::None => {
                    column_names = (1..=first_row.len()).map(|idx| format!("_{idx}")).collect();
                }
            }
        }

        let name_index_map = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.clone(), idx))
            .collect();

        Ok(Self {
            column_names,
            name_index_map,
            rows,
            index: 0,
            pending_error,
            closed: false,
        })
    }

    pub fn read(&mut self, dst: Option<Record>) -> io::Result<Record> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }
        if let Some(values) = self.rows.get(self.index).cloned() {
            self.index += 1;
            let mut record = dst.unwrap_or_default();
            record.column_names = self.column_names.clone();
            record.name_index_map = self.name_index_map.clone();
            record.values = values;
            return Ok(record);
        }
        if let Some(err) = self.pending_error.take() {
            return Err(err);
        }
        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        Ok(())
    }
}

fn split_records(input: &str, delimiter: &str) -> Vec<String> {
    if delimiter == "\n" {
        input.split('\n').map(str::to_owned).collect()
    } else {
        input.split(delimiter).map(str::to_owned).collect()
    }
}

fn parse_csv_row(line: &str, delimiter: char) -> io::Result<Vec<String>> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && matches!(chars.peek(), Some('"')) {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ch if ch == delimiter && !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            '\r' if !in_quotes => {}
            _ => current.push(ch),
        }
    }

    if in_quotes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unterminated quoted field",
        ));
    }
    fields.push(current);
    Ok(fields)
}

fn encode_csv_field(field: &str, delimiter: char) -> String {
    if field.contains(delimiter) || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_owned()
    }
}
