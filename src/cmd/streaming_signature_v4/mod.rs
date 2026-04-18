use std::io::{BufRead, Read};

pub const ERR_LINE_TOO_LONG: &str = "line too long";
pub const ERR_MALFORMED_ENCODING: &str = "malformed encoding";

pub fn parse_s3_chunk_extension(buf: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let trimmed = String::from_utf8_lossy(buf).trim_end().as_bytes().to_vec();
    let marker = b";chunk-signature=";
    if let Some(index) = trimmed
        .windows(marker.len())
        .position(|window| window == marker)
    {
        let chunk_size = trimmed[..index].to_vec();
        if chunk_size.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let signature = trimmed[index + marker.len()..].to_vec();
        return (chunk_size, signature);
    }
    (trimmed, Vec::new())
}

pub fn read_chunk_line(reader: &mut impl BufRead) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut line = Vec::new();
    let read = reader
        .read_until(b'\n', &mut line)
        .map_err(|err| err.to_string())?;
    if read == 0 || !line.ends_with(b"\r\n") {
        return Err("unexpected eof".to_string());
    }
    if line.len() > 4096 {
        return Err(ERR_LINE_TOO_LONG.to_string());
    }
    line.truncate(line.len() - 2);
    let (chunk_size, signature) = parse_s3_chunk_extension(&line);
    Ok((chunk_size, signature))
}

pub fn read_crlf(reader: &mut impl Read) -> Result<(), String> {
    let mut buf = [0_u8; 2];
    reader.read_exact(&mut buf).map_err(|err| {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            "unexpected eof".to_string()
        } else {
            err.to_string()
        }
    })?;
    if &buf == b"\r\n" {
        Ok(())
    } else {
        Err(ERR_MALFORMED_ENCODING.to_string())
    }
}

pub fn parse_hex_uint(bytes: &[u8]) -> Result<u64, String> {
    let mut value = 0_u64;
    for (index, byte) in bytes.iter().enumerate() {
        if index >= 16 {
            return Err("http chunk length too large".to_string());
        }
        let digit = match byte {
            b'0'..=b'9' => (byte - b'0') as u64,
            b'a'..=b'f' => (byte - b'a' + 10) as u64,
            b'A'..=b'F' => (byte - b'A' + 10) as u64,
            _ => return Err("invalid byte in chunk length".to_string()),
        };
        value = value
            .checked_mul(16)
            .and_then(|current| current.checked_add(digit))
            .ok_or_else(|| "http chunk length too large".to_string())?;
    }
    Ok(value)
}
