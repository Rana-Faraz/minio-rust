use std::io::{self, Read};

use serde_json::{Deserializer, Value};

#[derive(Debug, Clone, Default)]
pub struct ReaderArgs;

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub value: Value,
}

#[derive(Debug)]
pub struct Reader<R: Read> {
    records: Vec<Value>,
    index: usize,
    closed: bool,
    read_closer: Option<R>,
}

impl<R: Read> Reader<R> {
    pub fn new(mut read_closer: R, _args: &ReaderArgs) -> io::Result<Self> {
        let mut input = String::new();
        read_closer.read_to_string(&mut input)?;
        let stream = Deserializer::from_str(&input).into_iter::<Value>();
        let mut records = Vec::new();
        for value in stream {
            records.push(value.map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?);
        }
        Ok(Self {
            records,
            index: 0,
            closed: false,
            read_closer: Some(read_closer),
        })
    }

    pub fn read(&mut self) -> io::Result<Record> {
        if self.closed || self.index >= self.records.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }
        let value = self.records[self.index].clone();
        self.index += 1;
        Ok(Record { value })
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        self.read_closer = None;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PReader<R: Read> {
    inner: Reader<R>,
}

impl<R: Read> PReader<R> {
    pub fn new(read_closer: R, args: &ReaderArgs) -> io::Result<Self> {
        Ok(Self {
            inner: Reader::new(read_closer, args)?,
        })
    }

    pub fn read(&mut self) -> io::Result<Record> {
        self.inner.read()
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.inner.close()
    }
}
