use std::io::Cursor;

use minio_rust::cmd::{
    new_metacache_reader, new_metacache_writer, MetaCacheEntryExt, SLASH_SEPARATOR,
};

pub const SOURCE_FILE: &str = "cmd/metacache-stream_test.go";

fn sample_names() -> Vec<&'static str> {
    vec![
        "src/compress/bzip2/",
        "src/compress/bzip2/bit_reader.go",
        "src/compress/bzip2/bzip2.go",
        "src/compress/bzip2/testdata/",
        "src/compress/bzip2/testdata/pass-random1.bin",
        "src/compress/flate/",
        "src/compress/flate/deflate.go",
        "src/compress/flate/testdata/",
        "src/compress/flate/testdata/huffman-zero.in",
        "src/compress/zlib/",
        "src/compress/zlib/example_test.go",
        "src/compress/zlib/reader.go",
        "src/compress/zlib/reader_test.go",
        "src/compress/zlib/writer.go",
        "src/compress/zlib/writer_test.go",
    ]
}

fn make_entry(name: &str) -> MetaCacheEntryExt {
    MetaCacheEntryExt {
        name: name.to_string(),
        metadata: if name.ends_with(SLASH_SEPARATOR) {
            Vec::new()
        } else {
            format!("meta:{name}").into_bytes()
        },
        cached: None,
        reusable: false,
    }
}

fn sample_entries() -> Vec<MetaCacheEntryExt> {
    sample_names().into_iter().map(make_entry).collect()
}

fn encoded_sample() -> Vec<u8> {
    let mut writer = new_metacache_writer(Vec::new());
    for entry in sample_entries() {
        writer.write(&entry).expect("write entry");
    }
    writer.finish().expect("finish writer")
}

fn sample_reader() -> minio_rust::cmd::MetacacheReader {
    new_metacache_reader(Cursor::new(encoded_sample())).expect("reader")
}

#[test]
fn test_metacache_reader_read_names_line_50() {
    let mut reader = sample_reader();
    let names = reader.read_names(-1);
    assert_eq!(
        names,
        sample_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_metacache_reader_read_n_line_63() {
    let mut reader = sample_reader();
    let entries = reader.read_n(-1, true, "");
    assert_eq!(
        entries.names(),
        sample_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );

    let mut reader = sample_reader();
    let entries = reader.read_n(0, true, "");
    assert!(entries.names().is_empty());

    let mut reader = sample_reader();
    let entries = reader.read_n(5, true, "");
    assert_eq!(
        entries.names(),
        sample_names()[..5]
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_metacache_reader_read_ndirs_line_116() {
    let want = sample_names()
        .into_iter()
        .filter(|name| !name.ends_with(SLASH_SEPARATOR))
        .map(str::to_string)
        .collect::<Vec<_>>();

    let mut reader = sample_reader();
    let entries = reader.read_n(-1, false, "");
    assert_eq!(entries.names(), want);

    let mut reader = sample_reader();
    let entries = reader.read_n(5, false, "");
    assert_eq!(entries.names(), want[..5].to_vec());
}

#[test]
fn test_metacache_reader_read_nprefix_line_189() {
    let mut reader = sample_reader();
    let entries = reader.read_n(-1, true, "src/compress/bzip2/");
    assert_eq!(
        entries.names(),
        sample_names()[..5]
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );

    let mut reader = sample_reader();
    assert!(reader.read_n(-1, true, "src/nonexist").names().is_empty());

    let mut reader = sample_reader();
    assert!(reader.read_n(-1, true, "src/a").names().is_empty());

    let mut reader = sample_reader();
    assert_eq!(
        reader.read_n(-1, true, "src/compress/zlib/e").names(),
        vec!["src/compress/zlib/example_test.go".to_string()]
    );
}

#[test]
fn test_metacache_reader_read_fn_line_255() {
    let mut reader = sample_reader();
    let mut names = Vec::new();
    reader.read_fn(|entry| {
        names.push(entry.name);
        true
    });
    assert_eq!(
        names,
        sample_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_metacache_reader_read_all_line_272() {
    let mut reader = sample_reader();
    let names = reader
        .read_all()
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        sample_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_metacache_reader_forward_to_line_297() {
    let want = vec![
        "src/compress/zlib/reader_test.go".to_string(),
        "src/compress/zlib/writer.go".to_string(),
        "src/compress/zlib/writer_test.go".to_string(),
    ];

    let mut reader = sample_reader();
    reader.forward_to("src/compress/zlib/reader_test.go");
    assert_eq!(reader.read_names(-1), want);

    let mut reader = sample_reader();
    reader.forward_to("src/compress/zlib/reader_t");
    assert_eq!(reader.read_names(-1), want);
}

#[test]
fn test_metacache_reader_next_line_328() {
    let mut reader = sample_reader();
    for want in sample_names() {
        let got = reader.next().expect("next entry");
        assert_eq!(got.name, want);
    }
    assert!(reader.next().is_none());
}

#[test]
fn test_metacache_reader_peek_line_342() {
    let mut reader = sample_reader();
    for want in sample_names() {
        let peeked = reader.peek().expect("peek entry");
        assert_eq!(peeked.name, want);
        let got = reader.next().expect("next entry");
        assert_eq!(got.name, want);
    }
    assert!(reader.peek().is_none());
}

#[test]
fn test_new_metacache_stream_line_366() {
    let mut input = sample_reader();
    let mut writer = new_metacache_writer(Vec::new());
    input.read_fn(|entry| {
        writer.write(&entry).expect("write entry");
        true
    });

    let bytes = writer.finish().expect("finish writer");
    let mut roundtrip = new_metacache_reader(Cursor::new(bytes)).expect("roundtrip reader");
    assert_eq!(
        roundtrip.read_names(-1),
        sample_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_metacache_reader_skip_line_399() {
    let mut reader = sample_reader();
    assert_eq!(
        reader.read_names(5),
        sample_names()[..5]
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert!(reader.skip(5));
    assert_eq!(
        reader.read_names(5),
        sample_names()[10..15]
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert!(!reader.skip(sample_names().len()));
}
