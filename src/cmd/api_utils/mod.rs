fn should_escape(byte: u8) -> bool {
    if byte.is_ascii_alphanumeric() {
        return false;
    }

    !matches!(byte, b'-' | b'_' | b'.' | b'/' | b'*')
}

pub fn s3_url_encode(value: &str) -> String {
    let mut space_count = 0usize;
    let mut hex_count = 0usize;

    for byte in value.as_bytes() {
        if should_escape(*byte) {
            if *byte == b' ' {
                space_count += 1;
            } else {
                hex_count += 1;
            }
        }
    }

    if space_count == 0 && hex_count == 0 {
        return value.to_string();
    }

    let mut encoded = String::with_capacity(value.len() + 2 * hex_count);
    for byte in value.as_bytes() {
        match *byte {
            b' ' => encoded.push('+'),
            byte if should_escape(byte) => {
                encoded.push('%');
                encoded.push_str(&format!("{byte:02X}"));
            }
            byte => encoded.push(byte as char),
        }
    }
    encoded
}

pub fn s3_encode_name(name: &str, encoding_type: &str) -> String {
    if encoding_type.eq_ignore_ascii_case("url") {
        s3_url_encode(name)
    } else {
        name.to_string()
    }
}
