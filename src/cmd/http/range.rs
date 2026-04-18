#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpRangeSpec {
    FromTo { start: i64, end: Option<i64> },
    Suffix { length: i64 },
}

fn invalid_range() -> String {
    "invalid range".to_string()
}

pub fn is_err_invalid_range(err: &str) -> bool {
    err == "invalid range"
}

pub fn parse_request_range_spec(spec: &str) -> Result<HttpRangeSpec, String> {
    let Some(body) = spec.strip_prefix("bytes=") else {
        return Err("parse error".to_string());
    };
    if body.is_empty() || body.contains(',') || body.matches('-').count() != 1 {
        return Err("parse error".to_string());
    }
    let Some((start, end)) = body.split_once('-') else {
        return Err("parse error".to_string());
    };

    if start.is_empty() {
        let length = end.parse::<i64>().map_err(|_| "parse error".to_string())?;
        if length <= 0 {
            return Err(invalid_range());
        }
        return Ok(HttpRangeSpec::Suffix { length });
    }

    if start.starts_with('+') || end.starts_with('+') {
        return Err("parse error".to_string());
    }

    let start = start
        .parse::<i64>()
        .map_err(|_| "parse error".to_string())?;
    if start < 0 {
        return Err("parse error".to_string());
    }
    if end.is_empty() {
        return Ok(HttpRangeSpec::FromTo { start, end: None });
    }

    let end = end.parse::<i64>().map_err(|_| "parse error".to_string())?;
    if end < start {
        return Err(invalid_range());
    }
    Ok(HttpRangeSpec::FromTo {
        start,
        end: Some(end),
    })
}

impl HttpRangeSpec {
    pub fn get_offset_length(&self, resource_size: i64) -> Result<(i64, i64), String> {
        if resource_size <= 0 {
            return Err(invalid_range());
        }
        match self {
            Self::Suffix { length } => {
                let length = (*length).min(resource_size);
                Ok((resource_size - length, length))
            }
            Self::FromTo { start, end } => {
                if *start >= resource_size {
                    return Err(invalid_range());
                }
                let end = end.unwrap_or(resource_size).min(resource_size - 1);
                if end < *start {
                    return Err(invalid_range());
                }
                Ok((*start, end - *start + 1))
            }
        }
    }

    pub fn to_header(&self) -> Result<String, String> {
        match self {
            Self::FromTo { start, end } => match end {
                Some(end) => Ok(format!("bytes={start}-{end}")),
                None => Ok(format!("bytes={start}-")),
            },
            Self::Suffix { length } => {
                if *length <= 0 {
                    return Err(invalid_range());
                }
                Ok(format!("bytes=-{length}"))
            }
        }
    }
}
