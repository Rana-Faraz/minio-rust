use std::io::{self, Cursor, Read};

use minio_rust::internal::s3select::jstream::Scanner;

pub const SOURCE_FILE: &str = "internal/s3select/jstream/scanner_test.go";

struct MockReader {
    pos: usize,
    fail_after: usize,
    fill: u8,
}

impl Read for MockReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.fail_after {
            return Err(io::Error::other("intentionally unexpected reader error"));
        }
        self.pos += 1;
        if !buf.is_empty() {
            buf[0] = self.fill;
            Ok(1)
        } else {
            Ok(0)
        }
    }
}

#[test]
fn scanner_matches_reference_case() {
    let data = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut scanner = Scanner::new(Cursor::new(data));
    let mut out = Vec::new();
    while scanner.remaining() > 0 {
        let byte = scanner.next();
        if byte == 0 {
            break;
        }
        out.push(byte);
    }
    assert_eq!(out, data);
    assert!(scanner.reader_err.is_none());
}

#[test]
fn scanner_failure_matches_reference_case() {
    let mut scanner = Scanner::new(MockReader {
        pos: 0,
        fail_after: 900,
        fill: b' ',
    });
    let mut read = 0usize;
    while read < 1000 {
        let byte = scanner.next();
        if byte == 0 {
            break;
        }
        assert_eq!(byte, b' ');
        read += 1;
    }
    let byte = scanner.next();
    assert_eq!(byte, 0);
    assert!(scanner.reader_err.is_some());
}

#[test]
fn scanner_benchmark_shapes_match_reference_cases() {
    for size in [12 * 1024, 12 * 1024 * 1024, 128 * 1024 * 1024] {
        let data = vec![0u8; size];
        let mut scanner = Scanner::new(Cursor::new(data));
        let mut count = 0usize;
        while scanner.remaining() > 0 {
            let byte = scanner.next();
            if byte == 0 {
                break;
            }
            count += 1;
        }
        assert!(count <= size);
        assert!(scanner.reader_err.is_none());
    }
}
