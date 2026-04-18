use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use minio_rust::internal::ioutil::{
    append_file, copy_aligned, new_skip_reader, odirect_pool_small, same_file, write_on_close,
    DeadlineError, DeadlineWorker, DeadlineWriter, WriteClose,
};

pub const SOURCE_FILE: &str = "internal/ioutil/ioutil_test.go";

fn temp_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("minio_rust_{name}_{}", rand::random::<u64>()));
    path
}

struct SleepWriter {
    timeout: Duration,
}

impl Write for SleepWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        thread::sleep(self.timeout);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl WriteClose for SleepWriter {
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn deadline_worker_matches_reference_cases() {
    let worker = DeadlineWorker::new(Duration::from_millis(500));

    let err = worker.run(|| {
        thread::sleep(Duration::from_millis(600));
        Ok(())
    });
    assert_eq!(err, Err(DeadlineError::DeadlineExceeded));

    let ok = worker.run(|| {
        thread::sleep(Duration::from_millis(450));
        Ok(())
    });
    assert_eq!(ok, Ok(()));
}

#[test]
fn deadline_writer_matches_reference_cases() {
    let mut writer = DeadlineWriter::new(
        SleepWriter {
            timeout: Duration::from_millis(500),
        },
        Duration::from_millis(450),
    );

    let err = writer.write(b"1").expect_err("slow write should timeout");
    assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    let err = writer
        .write(b"1")
        .expect_err("subsequent write should keep timing out");
    assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    writer.close().expect("close should succeed");

    let mut writer = DeadlineWriter::new(
        SleepWriter {
            timeout: Duration::from_millis(100),
        },
        Duration::from_millis(600),
    );
    let n = writer.write(b"abcd").expect("fast write should succeed");
    writer.close().expect("close should succeed");
    assert_eq!(n, 4);
}

#[test]
fn close_on_writer_matches_reference_cases() {
    let mut writer = write_on_close(io::sink());
    assert!(!writer.has_written());
    writer.write(&[]).expect("write should succeed");
    assert!(writer.has_written());

    let mut writer = write_on_close(io::sink());
    writer.close().expect("close should succeed");
    assert!(writer.has_written());
}

#[test]
fn append_file_matches_reference_case() {
    let name1 = temp_path("append_a");
    let name2 = temp_path("append_b");
    fs::write(&name1, "aaaaaaaaaa").expect("first file should write");
    fs::write(&name2, "bbbbbbbbbb").expect("second file should write");

    append_file(&name1, &name2, false).expect("append should succeed");

    let bytes = fs::read_to_string(&name1).expect("combined file should read");
    assert_eq!(bytes, "aaaaaaaaaabbbbbbbbbb");
    let _ = fs::remove_file(&name1);
    let _ = fs::remove_file(&name2);
}

#[test]
fn skip_reader_matches_reference_cases() {
    let cases = [
        ("", 0_i64, ""),
        ("", 1, ""),
        ("abc", 0, "abc"),
        ("abc", 1, "bc"),
        ("abc", 2, "c"),
        ("abc", 3, ""),
        ("abc", 4, ""),
    ];

    for (content, skip_len, expected) in cases {
        let mut reader = new_skip_reader(io::Cursor::new(content.as_bytes()), skip_len);
        let mut out = String::new();
        reader
            .read_to_string(&mut out)
            .expect("skip reader should read");
        assert_eq!(out, expected);
    }
}

#[test]
fn same_file_matches_reference_cases() {
    let path = temp_path("same_file");
    fs::write(&path, "").expect("file should be created");

    let fi1 = fs::metadata(&path).expect("metadata should load");
    let fi2 = fs::metadata(&path).expect("metadata should load");
    assert!(same_file(&fi1, &fi2));

    fs::write(&path, "aaa").expect("file should be updated");
    let fi2 = fs::metadata(&path).expect("metadata should load");
    assert!(!same_file(&fi1, &fi2));
    let _ = fs::remove_file(&path);
}

#[test]
fn copy_aligned_matches_reference_cases() {
    let path = temp_path("copy_aligned");
    let file = File::create(&path).expect("output file should be created");

    let mut reader = io::Cursor::new("hello world");
    let pool = odirect_pool_small();
    let mut buf = pool.get();

    let err = copy_aligned(&file, reader.by_ref().take(5), &mut buf, 11, &file)
        .expect_err("short copy should error");
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    pool.put(buf);

    let file = File::options()
        .write(true)
        .truncate(true)
        .open(&path)
        .expect("output file should reopen");
    let mut reader = io::Cursor::new("hello world");
    let pool = odirect_pool_small();
    let mut buf = pool.get();
    let written =
        copy_aligned(&file, &mut reader, &mut buf, 11, &file).expect("full copy should succeed");
    pool.put(buf);
    assert_eq!(written, 11);
    let _ = fs::remove_file(&path);
}
