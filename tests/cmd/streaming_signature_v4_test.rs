use std::io::{BufReader, Cursor};

use minio_rust::cmd::{
    parse_hex_uint, parse_s3_chunk_extension, read_chunk_line, read_crlf, ERR_LINE_TOO_LONG,
    ERR_MALFORMED_ENCODING,
};

pub const SOURCE_FILE: &str = "cmd/streaming-signature-v4_test.go";

#[test]
fn test_read_chunk_line_line_30() {
    let mut reader = BufReader::new(Cursor::new(
        b"1000;chunk-signature=111123333333333333334444211\r\n".to_vec(),
    ));
    let (size, signature) = read_chunk_line(&mut reader).expect("chunk line");
    assert_eq!(size, b"1000");
    assert_eq!(signature, b"111123333333333333334444211");

    let mut reader = BufReader::new(Cursor::new(b"1000;".to_vec()));
    assert_eq!(
        read_chunk_line(&mut reader).unwrap_err(),
        "unexpected eof".to_string()
    );

    let long = "1".repeat(4097) + "\r\n";
    let mut reader = BufReader::new(Cursor::new(long.into_bytes()));
    assert_eq!(read_chunk_line(&mut reader).unwrap_err(), ERR_LINE_TOO_LONG);
}

#[test]
fn test_parse_s3_chunk_extension_line_94() {
    let (size, sign) = parse_s3_chunk_extension(
        b"10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
    );
    assert_eq!(size, b"10000");
    assert_eq!(
        sign,
        b"ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
    );

    let (size, sign) = parse_s3_chunk_extension(b"10000;");
    assert_eq!(size, b"10000;");
    assert!(sign.is_empty());

    let (size, sign) = parse_s3_chunk_extension(b";chunk-signature=");
    assert!(size.is_empty());
    assert!(sign.is_empty());
}

#[test]
fn test_read_crlf_line_141() {
    let mut ok = Cursor::new(b"\r\n".to_vec());
    assert_eq!(read_crlf(&mut ok), Ok(()));

    let mut malformed = Cursor::new(b"he".to_vec());
    assert_eq!(
        read_crlf(&mut malformed).unwrap_err(),
        ERR_MALFORMED_ENCODING
    );

    let mut short = Cursor::new(b"h".to_vec());
    assert_eq!(read_crlf(&mut short).unwrap_err(), "unexpected eof");
}

#[test]
fn test_parse_hex_uint_line_165() {
    assert_eq!(
        parse_hex_uint(b"x").unwrap_err(),
        "invalid byte in chunk length"
    );
    assert_eq!(parse_hex_uint(b"0000000000000000").unwrap(), 0);
    assert_eq!(parse_hex_uint(b"0000000000000001").unwrap(), 1);
    assert_eq!(parse_hex_uint(b"ffffffffffffffff").unwrap(), u64::MAX);
    assert_eq!(parse_hex_uint(b"FFFFFFFFFFFFFFFF").unwrap(), u64::MAX);
    assert_eq!(
        parse_hex_uint(b"000000000000bogus").unwrap_err(),
        "invalid byte in chunk length"
    );
    assert_eq!(
        parse_hex_uint(b"00000000000000000").unwrap_err(),
        "http chunk length too large"
    );
    for value in 0_u64..=1234 {
        assert_eq!(
            parse_hex_uint(format!("{value:x}").as_bytes()).unwrap(),
            value
        );
    }
}
