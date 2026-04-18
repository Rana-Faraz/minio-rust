use super::errors::Error;

pub const RUNE_ZERO: char = '\0';

pub fn drop_rune(text: &str) -> (String, bool) {
    let mut chars = text.chars();
    if chars.next().is_none() {
        return (String::new(), false);
    }
    (chars.collect(), true)
}

pub fn matcher(text: &str, pat: &str, leading_percent: bool) -> (String, bool) {
    if !leading_percent {
        if let Some(rest) = text.strip_prefix(pat) {
            (rest.to_owned(), true)
        } else {
            (String::new(), false)
        }
    } else if let Some((_, rest)) = text.split_once(pat) {
        (rest.to_owned(), true)
    } else {
        (String::new(), false)
    }
}

pub fn eval_sql_like(text: &str, pattern: &str, escape: char) -> Result<bool, Error> {
    let mut text = text.to_owned();
    let mut segment = Vec::new();
    let mut prev = RUNE_ZERO;
    let mut has_leading_percent = false;
    let pattern_chars: Vec<char> = pattern.chars().collect();

    for (index, rune) in pattern_chars.iter().copied().enumerate() {
        if index > 0 && prev == escape {
            match rune {
                '%' | '_' => {
                    segment.push(rune);
                    prev = rune;
                }
                r if r == escape => {
                    segment.push(rune);
                    prev = RUNE_ZERO;
                }
                _ => return Err(Error::MalformedEscapeSequence),
            }
            continue;
        }

        prev = rune;

        match rune {
            '%' => {
                if segment.is_empty() {
                    has_leading_percent = true;
                    continue;
                }
                let (rest, ok) = matcher(
                    &text,
                    &segment.iter().collect::<String>(),
                    has_leading_percent,
                );
                if !ok {
                    return Ok(false);
                }
                text = rest;
                has_leading_percent = true;
                segment.clear();
                if index == pattern_chars.len() - 1 {
                    return Ok(true);
                }
            }
            '_' => {
                if segment.is_empty() {
                    let (rest, ok) = drop_rune(&text);
                    if !ok {
                        return Ok(false);
                    }
                    text = rest;
                    continue;
                }
                let (rest, ok) = matcher(
                    &text,
                    &segment.iter().collect::<String>(),
                    has_leading_percent,
                );
                if !ok {
                    return Ok(false);
                }
                has_leading_percent = false;
                let (rest, ok) = drop_rune(&rest);
                if !ok {
                    return Ok(false);
                }
                text = rest;
                segment.clear();
            }
            r if r == escape => {
                if index == pattern_chars.len() - 1 {
                    return Err(Error::MalformedEscapeSequence);
                }
            }
            _ => segment.push(rune),
        }
    }

    let suffix = segment.iter().collect::<String>();
    if has_leading_percent {
        Ok(text.ends_with(&suffix))
    } else {
        Ok(suffix == text)
    }
}

pub fn eval_sql_substring(s: &str, mut start_idx: i32, length: i32) -> Result<String, Error> {
    let chars: Vec<char> = s.chars().collect();
    if start_idx < 1 {
        start_idx = 1;
    }
    let len = chars.len() as i32;
    if start_idx > len {
        start_idx = len + 1;
    }
    let start = (start_idx - 1) as usize;
    let mut end = chars.len();
    if length != -1 {
        if length < 0 {
            return Err(Error::InvalidSubstringIndexLen);
        }
        let max_len = end.saturating_sub(start);
        let take = (length as usize).min(max_len);
        end = start + take;
    }
    Ok(chars[start..end].iter().collect())
}
