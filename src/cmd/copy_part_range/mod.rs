#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyPartRangeSpec {
    pub start: i64,
    pub end: i64,
}

impl CopyPartRangeSpec {
    pub fn get_offset_length(&self, object_size: i64) -> Result<(i64, i64), String> {
        check_copy_part_range_with_size(self, object_size)?;
        Ok((self.start, self.end - self.start + 1))
    }
}

pub const ERR_INVALID_RANGE_SOURCE: &str = "Range specified is not valid for source object";

pub fn parse_copy_part_range_spec(range: &str) -> Result<CopyPartRangeSpec, String> {
    if !range.starts_with("bytes=") {
        return Err("invalid copy part range".to_string());
    }
    if range.contains(' ') {
        return Err("invalid copy part range".to_string());
    }
    let value = &range["bytes=".len()..];
    if value.is_empty() || value.contains(',') {
        return Err("invalid copy part range".to_string());
    }
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| "invalid copy part range".to_string())?;
    if start.is_empty() || end.is_empty() {
        return Err("invalid copy part range".to_string());
    }
    if start.starts_with('+')
        || end.starts_with('+')
        || start.starts_with('-')
        || end.starts_with('-')
        || start.contains('-')
        || end.contains('-')
    {
        return Err("invalid copy part range".to_string());
    }
    let start = start
        .parse::<i64>()
        .map_err(|_| "invalid copy part range".to_string())?;
    let end = end
        .parse::<i64>()
        .map_err(|_| "invalid copy part range".to_string())?;
    if start > end {
        return Err("invalid copy part range".to_string());
    }
    Ok(CopyPartRangeSpec { start, end })
}

pub fn check_copy_part_range_with_size(
    range: &CopyPartRangeSpec,
    object_size: i64,
) -> Result<(), String> {
    if range.start >= object_size || range.end >= object_size {
        return Err(ERR_INVALID_RANGE_SOURCE.to_string());
    }
    Ok(())
}
