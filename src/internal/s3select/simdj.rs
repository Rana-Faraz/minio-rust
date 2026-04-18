use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::mpsc::Receiver;

use serde_json::{Map, Value};

#[derive(Debug, Clone, Default)]
pub struct ReaderArgs;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Record {
    object: Map<String, Value>,
}

impl Record {
    pub fn write_csv<W: Write>(&self, mut writer: W) -> io::Result<()> {
        let fields = self
            .object
            .values()
            .map(csv_field_for_json)
            .collect::<Vec<_>>()
            .join(",");
        writer.write_all(fields.as_bytes())?;
        writer.write_all(b"\n")
    }

    pub fn write_json<W: Write>(&self, mut writer: W) -> io::Result<()> {
        let value = Value::Object(self.object.clone());
        let bytes = serde_json::to_vec(&value)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        writer.write_all(&bytes)
    }
}

#[derive(Debug)]
pub struct Reader {
    records: VecDeque<Record>,
    receiver: Option<Receiver<Map<String, Value>>>,
    closed: bool,
}

impl Reader {
    pub fn new<R: Read>(read_closer: R, _args: &ReaderArgs) -> io::Result<Self> {
        let mut records = VecDeque::new();
        let reader = BufReader::new(read_closer);
        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(trimmed)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            let Value::Object(object) = value else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected top-level JSON object",
                ));
            };
            records.push_back(Record { object });
        }
        Ok(Self {
            records,
            receiver: None,
            closed: false,
        })
    }

    pub fn new_element_reader(receiver: Receiver<Map<String, Value>>, _args: &ReaderArgs) -> Self {
        Self {
            records: VecDeque::new(),
            receiver: Some(receiver),
            closed: false,
        }
    }

    pub fn read(&mut self, dst: Option<Record>) -> io::Result<Record> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }
        if let Some(record) = self.records.pop_front() {
            return Ok(reuse_record(dst, record.object));
        }
        if let Some(receiver) = &self.receiver {
            return match receiver.recv() {
                Ok(object) => Ok(reuse_record(dst, object)),
                Err(_) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")),
            };
        }
        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        self.records.clear();
        self.receiver = None;
        Ok(())
    }
}

fn reuse_record(dst: Option<Record>, object: Map<String, Value>) -> Record {
    let mut record = dst.unwrap_or_default();
    record.object = object;
    record
}

fn csv_field_for_json(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}
