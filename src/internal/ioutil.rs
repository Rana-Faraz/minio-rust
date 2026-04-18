use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

pub const SMALL_BLOCK: usize = 32 * 1024;
pub const DIRECTIO_ALIGN_SIZE: usize = 4096;
pub const ERR_OVERREAD: &str = "input provided more bytes than specified";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeadlineError {
    DeadlineExceeded,
    Closed,
}

pub struct AlignedBytePool {
    size: usize,
    buffers: Mutex<Vec<Vec<u8>>>,
}

impl AlignedBytePool {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            buffers: Mutex::new(Vec::new()),
        }
    }

    pub fn get(&self) -> Vec<u8> {
        self.buffers
            .lock()
            .expect("aligned pool should lock")
            .pop()
            .unwrap_or_else(|| vec![0_u8; self.size])
    }

    pub fn put(&self, mut buffer: Vec<u8>) {
        if buffer.len() == self.size {
            buffer.fill(0);
            self.buffers
                .lock()
                .expect("aligned pool should lock")
                .push(buffer);
        }
    }
}

pub fn odirect_pool_small() -> &'static AlignedBytePool {
    static POOL: OnceLock<AlignedBytePool> = OnceLock::new();
    POOL.get_or_init(|| AlignedBytePool::new(SMALL_BLOCK))
}

pub struct WriteOnCloser<W> {
    writer: W,
    has_written: bool,
}

impl<W> WriteOnCloser<W> {
    pub fn has_written(&self) -> bool {
        self.has_written
    }
}

impl<W: Write> Write for WriteOnCloser<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.has_written = true;
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write> WriteOnCloser<W> {
    pub fn close(&mut self) -> io::Result<()> {
        if !self.has_written {
            let _ = self.write(&[])?;
        }
        Ok(())
    }
}

pub fn write_on_close<W: Write>(writer: W) -> WriteOnCloser<W> {
    WriteOnCloser {
        writer,
        has_written: false,
    }
}

pub struct DeadlineWorker {
    timeout: Duration,
}

impl DeadlineWorker {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    pub fn run<F>(&self, work: F) -> Result<(), DeadlineError>
    where
        F: FnOnce() -> io::Result<()>,
    {
        let start = Instant::now();
        let result = work();
        if start.elapsed() > self.timeout {
            return Err(DeadlineError::DeadlineExceeded);
        }
        result.map_err(|_| DeadlineError::Closed)
    }
}

pub trait WriteClose: Write {
    fn close(&mut self) -> io::Result<()>;
}

pub struct DeadlineWriter<W> {
    writer: W,
    timeout: Duration,
    closed: bool,
    timed_out: bool,
}

impl<W: WriteClose> DeadlineWriter<W> {
    pub fn new(writer: W, timeout: Duration) -> Self {
        Self {
            writer,
            timeout,
            closed: false,
            timed_out: false,
        }
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        self.writer.close()
    }
}

impl<W: WriteClose> Write for DeadlineWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.closed {
            return Err(io::Error::other("we are closed"));
        }
        if self.timed_out {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline exceeded"));
        }

        let start = Instant::now();
        let result = self.writer.write(buf);
        if start.elapsed() > self.timeout {
            self.timed_out = true;
            return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline exceeded"));
        }
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

pub fn append_file(dst: impl AsRef<Path>, src: impl AsRef<Path>, osync: bool) -> io::Result<()> {
    let mut append_file = OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(dst)?;
    if osync {
        append_file.sync_all()?;
    }

    let mut src_file = File::open(src)?;
    io::copy(&mut src_file, &mut append_file)?;
    Ok(())
}

pub struct SkipReader<R> {
    reader: R,
    skip_count: i64,
}

impl<R: Read> Read for SkipReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.skip_count > 0 {
            let mut scratch = vec![0_u8; SMALL_BLOCK.max(buf.len())];
            while self.skip_count > 0 {
                let len = usize::min(self.skip_count as usize, scratch.len());
                let n = self.reader.read(&mut scratch[..len])?;
                if n == 0 {
                    return Ok(0);
                }
                self.skip_count -= n as i64;
            }
        }
        self.reader.read(buf)
    }
}

pub fn new_skip_reader<R: Read>(reader: R, skip_len: i64) -> SkipReader<R> {
    SkipReader {
        reader,
        skip_count: skip_len,
    }
}

#[cfg(unix)]
pub fn same_file(fi1: &Metadata, fi2: &Metadata) -> bool {
    fi1.dev() == fi2.dev()
        && fi1.ino() == fi2.ino()
        && fi1.mtime() == fi2.mtime()
        && fi1.mtime_nsec() == fi2.mtime_nsec()
        && fi1.mode() == fi2.mode()
        && fi1.size() == fi2.size()
}

#[cfg(not(unix))]
pub fn same_file(fi1: &Metadata, fi2: &Metadata) -> bool {
    fi1.len() == fi2.len()
        && fi1.permissions().readonly() == fi2.permissions().readonly()
        && fi1.modified().ok() == fi2.modified().ok()
}

pub fn copy_aligned(
    mut writer: &File,
    mut reader: impl Read,
    aligned_buf: &mut [u8],
    total_size: i64,
    _file: &File,
) -> io::Result<i64> {
    if total_size == 0 {
        return Ok(0);
    }

    let mut written = 0_i64;
    loop {
        let remaining = total_size - written;
        let len = usize::min(aligned_buf.len(), remaining.max(0) as usize);
        let buf = &mut aligned_buf[..len];
        let nr = match reader.read(buf) {
            Ok(0) => {
                if written != total_size {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "unexpected eof",
                    ));
                }
                return Ok(written);
            }
            Ok(n) => n,
            Err(error) => return Err(error),
        };

        writer.write_all(&buf[..nr])?;
        written += nr as i64;

        if written == total_size {
            return Ok(written);
        }
    }
}

pub struct HardLimitedReader<R> {
    reader: R,
    remaining: i64,
}

impl<R> HardLimitedReader<R> {
    pub fn new(reader: R, remaining: i64) -> Self {
        Self { reader, remaining }
    }
}

impl<R: Read> Read for HardLimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining < 0 {
            return Err(io::Error::other(ERR_OVERREAD));
        }

        let n = self.reader.read(buf)?;
        self.remaining -= n as i64;
        if self.remaining < 0 {
            return Err(io::Error::other(ERR_OVERREAD));
        }
        Ok(n)
    }
}

pub fn hard_limit_reader<R>(reader: R, remaining: i64) -> HardLimitedReader<R> {
    HardLimitedReader::new(reader, remaining)
}
