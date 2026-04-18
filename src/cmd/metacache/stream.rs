use std::io::{self, Read, Write};

use crate::cmd::{MetaCacheEntriesSorted, MetaCacheEntryExt};

fn read_u32(reader: &mut impl Read) -> io::Result<Option<u32>> {
    let mut buf = [0_u8; 4];
    match reader.read_exact(&mut buf) {
        Ok(()) => Ok(Some(u32::from_be_bytes(buf))),
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err),
    }
}

fn read_entry(reader: &mut impl Read) -> io::Result<Option<MetaCacheEntryExt>> {
    let Some(name_len) = read_u32(reader)? else {
        return Ok(None);
    };
    let mut name = vec![0_u8; name_len as usize];
    reader.read_exact(&mut name)?;

    let mut meta_len_buf = [0_u8; 4];
    reader.read_exact(&mut meta_len_buf)?;
    let meta_len = u32::from_be_bytes(meta_len_buf) as usize;
    let mut metadata = vec![0_u8; meta_len];
    reader.read_exact(&mut metadata)?;

    Ok(Some(MetaCacheEntryExt {
        name: String::from_utf8(name)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?,
        metadata,
        cached: None,
        reusable: false,
    }))
}

#[derive(Debug, Clone)]
pub struct MetacacheReader {
    entries: Vec<MetaCacheEntryExt>,
    pos: usize,
}

impl MetacacheReader {
    pub fn new(mut reader: impl Read) -> io::Result<Self> {
        let mut entries = Vec::new();
        while let Some(entry) = read_entry(&mut reader)? {
            entries.push(entry);
        }
        Ok(Self { entries, pos: 0 })
    }

    fn remaining(&self) -> &[MetaCacheEntryExt] {
        &self.entries[self.pos..]
    }

    pub fn read_names(&mut self, n: isize) -> Vec<String> {
        self.read_n(n, true, "").names()
    }

    pub fn read_n(&mut self, n: isize, include_dirs: bool, prefix: &str) -> MetaCacheEntriesSorted {
        if n == 0 {
            return MetaCacheEntriesSorted::new(Vec::new());
        }
        let mut selected = Vec::new();
        let mut consumed = 0usize;
        for entry in self.remaining() {
            if !prefix.is_empty() && !entry.name.starts_with(prefix) {
                if !selected.is_empty() {
                    break;
                }
                if entry.name.as_str() > prefix {
                    break;
                }
                continue;
            }
            if include_dirs || !entry.is_dir() {
                selected.push(entry.clone());
            }
            consumed += 1;
            if n >= 0 && selected.len() >= n as usize {
                break;
            }
        }

        if prefix.is_empty() {
            if n >= 0 {
                self.pos = (self.pos + consumed).min(self.entries.len());
            } else {
                self.pos = self.entries.len();
            }
        } else {
            self.pos = (self.pos + consumed).min(self.entries.len());
        }

        MetaCacheEntriesSorted::new(selected)
    }

    pub fn read_fn<F>(&mut self, mut f: F)
    where
        F: FnMut(MetaCacheEntryExt) -> bool,
    {
        while let Some(entry) = self.next() {
            if !f(entry) {
                break;
            }
        }
    }

    pub fn read_all(&mut self) -> Vec<MetaCacheEntryExt> {
        let out = self.remaining().to_vec();
        self.pos = self.entries.len();
        out
    }

    pub fn forward_to(&mut self, name: &str) {
        let offset = self
            .remaining()
            .partition_point(|entry| entry.name.as_str() < name);
        self.pos = (self.pos + offset).min(self.entries.len());
    }

    pub fn next(&mut self) -> Option<MetaCacheEntryExt> {
        let entry = self.peek()?;
        self.pos += 1;
        Some(entry)
    }

    pub fn peek(&self) -> Option<MetaCacheEntryExt> {
        self.entries.get(self.pos).cloned()
    }

    pub fn skip(&mut self, n: usize) -> bool {
        self.pos = (self.pos + n).min(self.entries.len());
        self.pos < self.entries.len()
    }
}

pub struct MetacacheWriter<W> {
    writer: W,
}

impl<W: Write> MetacacheWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn write(&mut self, entry: &MetaCacheEntryExt) -> io::Result<()> {
        self.writer
            .write_all(&(entry.name.len() as u32).to_be_bytes())?;
        self.writer.write_all(entry.name.as_bytes())?;
        self.writer
            .write_all(&(entry.metadata.len() as u32).to_be_bytes())?;
        self.writer.write_all(&entry.metadata)?;
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<W> {
        self.writer.flush()?;
        Ok(self.writer)
    }
}

pub fn new_metacache_reader(reader: impl Read) -> io::Result<MetacacheReader> {
    MetacacheReader::new(reader)
}

pub fn new_metacache_writer<W: Write>(writer: W) -> MetacacheWriter<W> {
    MetacacheWriter::new(writer)
}
