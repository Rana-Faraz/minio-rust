use std::io::{self, Cursor, Read};

use minio_rust::internal::s3select::csv::{FileHeaderInfo, Reader, ReaderArgs};

pub const SOURCE_FILE: &str = "internal/s3select/csv/reader_contrib_test.go";

#[derive(Debug)]
struct ErrReader<R> {
    inner: R,
    err: Option<io::Error>,
}

impl<R: Read> Read for ErrReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.inner.read(buf) {
            Ok(0) => Err(self
                .err
                .take()
                .unwrap_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))),
            other => other,
        }
    }
}

fn read_all(mut reader: Reader) -> (Vec<String>, Vec<String>, io::ErrorKind) {
    let mut lines = Vec::new();
    let mut record = None;
    loop {
        match reader.read(record.take()) {
            Ok(rec) => {
                let mut output = Vec::new();
                rec.write_csv(&mut output, ',')
                    .expect("csv write should succeed");
                lines.push(String::from_utf8(output).expect("csv should be utf8"));
                record = Some(rec);
            }
            Err(err) => {
                let kind = err.kind();
                reader.close().expect("close should succeed");
                return (reader.column_names.clone(), lines, kind);
            }
        }
    }
}

#[test]
fn test_read_line_33() {
    let cases = [
        ("1,2,3\na,b,c\n", "\n", ','),
        ("1,2,3\ta,b,c\t", "\t", ','),
        ("1,2,3\r\na,b,c\r\n", "\r\n", ','),
    ];

    for (index, (content, record_delimiter, field_delimiter)) in cases.into_iter().enumerate() {
        let reader = Reader::new(
            Cursor::new(content.as_bytes()),
            &ReaderArgs {
                file_header_info: FileHeaderInfo::None,
                record_delimiter: record_delimiter.to_owned(),
                field_delimiter,
            },
        )
        .unwrap_or_else(|err| panic!("case {index} init failed: {err}"));
        let (_columns, lines, err_kind) = read_all(reader);
        assert_eq!(err_kind, io::ErrorKind::UnexpectedEof, "case {index}");
        assert_eq!(
            lines.concat(),
            content.replace(record_delimiter, "\n"),
            "case {index}"
        );
    }
}

#[test]
fn test_read_extended_line_117() {
    let cases = [
        (
            "id,name,age\n1,Ada,42\n2,Bob,35\n",
            "\n",
            ',',
            true,
            vec!["id", "name", "age"],
            "1,Ada,42\n2,Bob,35\n",
        ),
        (
            "id\tname\tage^1\tAda\t42^2\tBob\t35^",
            "^",
            '\t',
            true,
            vec!["id", "name", "age"],
            "1,Ada,42\n2,Bob,35\n",
        ),
        (
            "1|Ada|42%!2|Bob|35%!",
            "%!",
            '|',
            false,
            vec!["_1", "_2", "_3"],
            "1,Ada,42\n2,Bob,35\n",
        ),
    ];

    for (index, (content, record_delimiter, field_delimiter, header, want_columns, want_lines)) in
        cases.into_iter().enumerate()
    {
        let reader = Reader::new(
            Cursor::new(content.as_bytes()),
            &ReaderArgs {
                file_header_info: if header {
                    FileHeaderInfo::Use
                } else {
                    FileHeaderInfo::None
                },
                record_delimiter: record_delimiter.to_owned(),
                field_delimiter,
            },
        )
        .unwrap_or_else(|err| panic!("case {index} init failed: {err}"));
        let (columns, lines, err_kind) = read_all(reader);
        assert_eq!(err_kind, io::ErrorKind::UnexpectedEof, "case {index}");
        assert_eq!(columns, want_columns, "case {index}");
        assert_eq!(lines.concat(), want_lines, "case {index}");
    }
}

#[test]
fn test_read_failures_line_288() {
    let custom_err = io::Error::new(io::ErrorKind::Other, "unable to read file :(");
    let reader = Reader::new(
        ErrReader {
            inner: Cursor::new(b"id,name,age\n1,Ada,42\n2,Bob,35\n".as_slice()),
            err: Some(custom_err),
        },
        &ReaderArgs {
            file_header_info: FileHeaderInfo::Use,
            record_delimiter: "\n".to_owned(),
            field_delimiter: ',',
        },
    )
    .expect("reader init should succeed");

    let (columns, lines, err_kind) = read_all(reader);
    assert_eq!(columns, vec!["id", "name", "age"]);
    assert_eq!(lines.concat(), "1,Ada,42\n2,Bob,35\n");
    assert_eq!(err_kind, io::ErrorKind::Other);

    let invalid = Reader::new(
        Cursor::new(b"header1,header2\n\"unterminated,value\n".as_slice()),
        &ReaderArgs {
            file_header_info: FileHeaderInfo::Use,
            record_delimiter: "\n".to_owned(),
            field_delimiter: ',',
        },
    )
    .expect("reader init should succeed");
    let (columns, lines, err_kind) = read_all(invalid);
    assert_eq!(columns, vec!["header1", "header2"]);
    assert!(lines.is_empty());
    assert_eq!(err_kind, io::ErrorKind::InvalidData);
}

#[test]
fn benchmark_reader_basic_line_493() {
    let content = "id,name,age\n1,Ada,42\n2,Bob,35\n3,Cam,27\n".repeat(50);
    for _ in 0..10 {
        let reader = Reader::new(
            Cursor::new(content.as_bytes()),
            &ReaderArgs {
                file_header_info: FileHeaderInfo::Use,
                record_delimiter: "\n".to_owned(),
                field_delimiter: ',',
            },
        )
        .expect("reader init should succeed");
        let (_columns, lines, err_kind) = read_all(reader);
        assert_eq!(err_kind, io::ErrorKind::UnexpectedEof);
        assert_eq!(lines.len(), 199);
    }
}

#[test]
fn benchmark_reader_huge_line_529() {
    let mut content = "id,name,age\n1,Ada,42\n2,Bob,35\n".to_owned();
    for _ in 0..5 {
        content.push_str(&content.clone());
    }
    for _ in 0..3 {
        let reader = Reader::new(
            Cursor::new(content.as_bytes()),
            &ReaderArgs {
                file_header_info: FileHeaderInfo::Use,
                record_delimiter: "\n".to_owned(),
                field_delimiter: ',',
            },
        )
        .expect("reader init should succeed");
        let (_columns, lines, err_kind) = read_all(reader);
        assert_eq!(err_kind, io::ErrorKind::UnexpectedEof);
        assert!(!lines.is_empty());
    }
}

#[test]
fn benchmark_reader_replace_line_575() {
    let content = "id|name|age^1|Ada|42^2|Bob|35^".repeat(20);
    for _ in 0..5 {
        let reader = Reader::new(
            Cursor::new(content.as_bytes()),
            &ReaderArgs {
                file_header_info: FileHeaderInfo::Use,
                record_delimiter: "^".to_owned(),
                field_delimiter: '|',
            },
        )
        .expect("reader init should succeed");
        let (_columns, lines, err_kind) = read_all(reader);
        assert_eq!(err_kind, io::ErrorKind::UnexpectedEof);
        assert!(lines.len() > 10);
    }
}

#[test]
fn benchmark_reader_replace_two_line_612() {
    let content = "id|name|age%!1|Ada|42%!2|Bob|35%!".repeat(20);
    for _ in 0..5 {
        let reader = Reader::new(
            Cursor::new(content.as_bytes()),
            &ReaderArgs {
                file_header_info: FileHeaderInfo::Use,
                record_delimiter: "%!".to_owned(),
                field_delimiter: '|',
            },
        )
        .expect("reader init should succeed");
        let (_columns, lines, err_kind) = read_all(reader);
        assert_eq!(err_kind, io::ErrorKind::UnexpectedEof);
        assert!(lines.len() > 10);
    }
}
