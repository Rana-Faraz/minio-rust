use super::*;

pub fn check_path_length(path: &str) -> Result<(), String> {
    if matches!(path, "." | ".." | SLASH_SEPARATOR) {
        return Err(cmd_err(ERR_FILE_ACCESS_DENIED));
    }

    let too_long = Path::new(path)
        .components()
        .any(|component| component.as_os_str().to_string_lossy().chars().count() > 255);
    if too_long {
        return Err(cmd_err(ERR_FILE_NAME_TOO_LONG));
    }

    Ok(())
}

pub fn is_valid_volname(name: &str) -> bool {
    !name.is_empty() && name != SLASH_SEPARATOR && name.chars().count() >= 3
}

pub fn concat(parts: &[&str]) -> String {
    let total: usize = parts.iter().map(|part| part.len()).sum();
    let mut out = String::with_capacity(total);
    for part in parts {
        out.push_str(part);
    }
    out
}

pub fn path_needs_clean(path: &[u8]) -> bool {
    if path.is_empty() {
        return true;
    }

    let rooted = path[0] == b'/';
    let n = path.len();
    let mut r = 0usize;
    let mut w = 0usize;
    if rooted {
        r = 1;
        w = 1;
    }

    while r < n {
        match path[r] {
            byte if byte > 127 => return true,
            b'/' => return true,
            b'.' if r + 1 == n || path[r + 1] == b'/' => return true,
            b'.' if r + 1 < n && path[r + 1] == b'.' && (r + 2 == n || path[r + 2] == b'/') => {
                return true
            }
            _ => {
                if (rooted && w != 1) || (!rooted && w != 0) {
                    w += 1;
                }
                while r < n && path[r] != b'/' {
                    w += 1;
                    r += 1;
                }
                if r < n - 1 && path[r] == b'/' {
                    r += 1;
                }
            }
        }
    }

    w == 0
}

fn clean_path(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }

    let rooted = path.starts_with('/');
    let mut stack: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => match stack.last().copied() {
                Some("..") | None if !rooted => stack.push(".."),
                Some(_) => {
                    stack.pop();
                }
                None => {}
            },
            other => stack.push(other),
        }
    }

    let mut cleaned = if rooted {
        format!("/{}", stack.join("/"))
    } else {
        stack.join("/")
    };
    if cleaned.is_empty() {
        cleaned = if rooted {
            "/".to_string()
        } else {
            ".".to_string()
        };
    }
    cleaned
}

pub fn path_join(parts: &[&str]) -> String {
    let trailing_slash = parts.last().is_some_and(|last| last.ends_with('/'));
    let mut joined = String::new();
    let mut added = 0usize;
    for part in parts {
        if added > 0 || !part.is_empty() {
            if added > 0 {
                joined.push('/');
            }
            joined.push_str(part);
            added += part.len();
        }
    }
    let mut out = if path_needs_clean(joined.as_bytes()) {
        clean_path(&joined)
    } else {
        joined
    };
    if trailing_slash && !out.ends_with('/') {
        out.push('/');
    }
    out
}

fn has_bad_path_component(path: &str) -> bool {
    if path.len() > (32 << 10) {
        return true;
    }
    let bytes = path.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() && (bytes[i] == b'/' || bytes[i] == b'\\') {
        i += 1;
    }
    while i < bytes.len() {
        let start = i;
        while i < bytes.len() && bytes[i] != b'/' && bytes[i] != b'\\' {
            i += 1;
        }
        let mut segment = &path[start..i];
        segment = segment.trim();
        if segment == "." || segment == ".." {
            return true;
        }
        i += 1;
    }
    false
}

pub fn is_minio_meta_bucket_name(bucket: &str) -> bool {
    bucket.starts_with(MINIO_META_BUCKET)
}

pub fn is_valid_bucket_name(bucket: &str) -> bool {
    if is_minio_meta_bucket_name(bucket) {
        return true;
    }
    if bucket.len() < 3 || bucket.len() > 63 {
        return false;
    }

    let pieces: Vec<&str> = bucket.split('.').collect();
    let mut all_numbers = true;
    for piece in &pieces {
        if piece.is_empty() || piece.starts_with('-') || piece.ends_with('-') {
            return false;
        }
        let mut is_not_number = false;
        for byte in piece.bytes() {
            match byte {
                b'a'..=b'z' | b'-' => is_not_number = true,
                b'0'..=b'9' => {}
                _ => return false,
            }
        }
        all_numbers &= !is_not_number;
    }
    pieces.len() != 4 || !all_numbers
}

pub fn is_valid_object_name(object: &str) -> bool {
    if object.is_empty() || object.ends_with('/') {
        return false;
    }
    is_valid_object_prefix(object)
}

pub fn is_valid_object_prefix(object: &str) -> bool {
    if has_bad_path_component(object) {
        return false;
    }
    if object.contains('\u{FFFD}') {
        return false;
    }
    if object.contains("//") {
        return false;
    }
    if object.contains('\0') {
        return false;
    }
    std::str::from_utf8(object.as_bytes()).is_ok()
}

pub fn path2_bucket_object(path: &str) -> (String, String) {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return (String::new(), String::new());
    }
    let mut parts = path.splitn(2, '/');
    let bucket = parts.next().unwrap_or_default().to_string();
    let object = parts.next().unwrap_or_default().to_string();
    (bucket, object)
}
