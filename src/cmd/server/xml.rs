use super::*;

#[derive(Debug, Deserialize)]
struct CompleteMultipartUploadXml {
    #[serde(rename = "Part", default)]
    parts: Vec<CompletePartXml>,
}

#[derive(Debug, Deserialize)]
struct CompletePartXml {
    #[serde(rename = "ETag")]
    etag: String,
    #[serde(rename = "PartNumber")]
    part_number: i32,
}

#[derive(Debug, Deserialize)]
struct DeleteObjectsXml {
    #[serde(rename = "Object", default)]
    objects: Vec<DeleteObjectXml>,
    #[serde(rename = "Quiet", default)]
    quiet: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DeleteObjectXml {
    #[serde(rename = "Key")]
    key: String,
}

pub(super) fn parse_multipart_form(
    body: &[u8],
    content_type: &str,
) -> Result<(BTreeMap<String, String>, Vec<u8>), String> {
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("boundary="))
        .ok_or_else(|| "missing multipart boundary".to_string())?;
    let boundary = format!("--{boundary}").into_bytes();

    let mut fields = BTreeMap::new();
    let mut file_bytes = Vec::new();

    for part in split_bytes(body, &boundary) {
        let mut part = trim_ascii_whitespace(part);
        if part.is_empty() || part == b"--" {
            continue;
        }
        if let Some(stripped) = part.strip_prefix(b"\r\n") {
            part = stripped;
        }
        if let Some(stripped) = part.strip_suffix(b"\r\n") {
            part = stripped;
        }
        if let Some(stripped) = part.strip_suffix(b"--") {
            part = stripped;
        }
        let Some(header_end) = find_bytes(part, b"\r\n\r\n") else {
            continue;
        };
        let header_block = &part[..header_end];
        let mut value_block = &part[header_end + 4..];
        if let Some(stripped) = value_block.strip_suffix(b"\r\n") {
            value_block = stripped;
        }

        let header_text = String::from_utf8_lossy(header_block);
        let disposition = header_text
            .lines()
            .find(|line| {
                line.to_ascii_lowercase()
                    .starts_with("content-disposition:")
            })
            .ok_or_else(|| "missing content-disposition".to_string())?;
        let name = disposition
            .split(';')
            .map(str::trim)
            .find_map(|part| {
                part.strip_prefix("name=\"")
                    .and_then(|value| value.strip_suffix('"'))
            })
            .ok_or_else(|| "missing multipart field name".to_string())?;
        let filename = disposition.split(';').map(str::trim).find_map(|part| {
            part.strip_prefix("filename=\"")
                .and_then(|value| value.strip_suffix('"'))
        });
        if filename.is_some() {
            file_bytes = value_block.to_vec();
        } else {
            fields.insert(
                name.to_string(),
                String::from_utf8_lossy(value_block).to_string(),
            );
        }
    }

    Ok((fields, file_bytes))
}

fn split_bytes<'a>(haystack: &'a [u8], needle: &[u8]) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut start = 0usize;
    while let Some(pos) = find_bytes(&haystack[start..], needle) {
        out.push(&haystack[start..start + pos]);
        start += pos + needle.len();
    }
    out.push(&haystack[start..]);
    out
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let mut start = 0usize;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

pub(super) fn parse_complete_multipart_upload(
    body: &[u8],
) -> Result<Vec<CompletePart>, quick_xml::DeError> {
    from_str::<CompleteMultipartUploadXml>(&String::from_utf8_lossy(body)).map(|complete| {
        complete
            .parts
            .into_iter()
            .map(|part| CompletePart {
                etag: part.etag.trim_matches('"').to_string(),
                part_number: part.part_number,
            })
            .collect()
    })
}

pub(super) fn parse_delete_objects(body: &[u8]) -> Result<(Vec<String>, bool), quick_xml::DeError> {
    from_str::<DeleteObjectsXml>(&String::from_utf8_lossy(body)).map(|delete| {
        (
            delete
                .objects
                .into_iter()
                .map(|object| object.key)
                .collect::<Vec<_>>(),
            delete.quiet.unwrap_or(false),
        )
    })
}
