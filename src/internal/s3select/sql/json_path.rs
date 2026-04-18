use serde_json::Value as JsonValue;

use super::{
    ast::{JsonPathElement, JsonPathValue, ParsedJsonPath},
    errors::{JsonPathError, ParserError},
};

pub fn eval_json_path(
    path: &[JsonPathElement],
    value: &JsonValue,
) -> Result<(JsonPathValue, bool), JsonPathError> {
    if path.is_empty() {
        return Ok((JsonPathValue::Json(value.clone()), false));
    }

    match &path[0] {
        JsonPathElement::Key(key) => match value {
            JsonValue::Object(map) => match map.get(key) {
                Some(next) => eval_json_path(&path[1..], next),
                None => Ok((JsonPathValue::Missing, false)),
            },
            _ => Err(JsonPathError::KeyLookup),
        },
        JsonPathElement::Index(index) => match value {
            JsonValue::Array(items) => match items.get(*index) {
                Some(next) => eval_json_path(&path[1..], next),
                None => Ok((JsonPathValue::Json(JsonValue::Null), false)),
            },
            _ => Err(JsonPathError::IndexLookup),
        },
        JsonPathElement::ObjectWildcard => match value {
            JsonValue::Object(_) => {
                if path.len() > 1 {
                    Err(JsonPathError::WildcardObjectUsageInvalid)
                } else {
                    Ok((JsonPathValue::Json(value.clone()), false))
                }
            }
            _ => Err(JsonPathError::WildcardObjectLookup),
        },
        JsonPathElement::ArrayWildcard => match value {
            JsonValue::Array(items) => {
                let mut result = Vec::with_capacity(items.len());
                for item in items {
                    let (next, flatten, err_path) = match eval_json_path(&path[1..], item) {
                        Ok((next, flatten)) => (next, flatten, None),
                        Err(err) => (JsonPathValue::Missing, false, Some(err)),
                    };
                    if let Some(err) = err_path {
                        return Err(err);
                    }
                    if flatten {
                        match next {
                            JsonPathValue::Sequence(values) => {
                                result.extend(values);
                            }
                            JsonPathValue::Json(JsonValue::Array(values)) => {
                                result.extend(values.into_iter().map(JsonPathValue::Json));
                            }
                            other => result.push(other),
                        }
                    } else {
                        result.push(next);
                    }
                }
                Ok((JsonPathValue::Sequence(result), true))
            }
            _ => Err(JsonPathError::WildcardArrayLookup),
        },
    }
}

pub fn parse_identifier(input: &str) -> Result<String, ParserError> {
    if input.len() >= 2 && input.starts_with('"') && input.ends_with('"') {
        let mut result = String::new();
        let inner = &input[1..input.len() - 1];
        let chars: Vec<char> = inner.chars().collect();
        let mut index = 0usize;
        while index < chars.len() {
            if chars[index] == '"' {
                if index + 1 < chars.len() && chars[index + 1] == '"' {
                    result.push('"');
                    index += 2;
                    continue;
                }
                return Err(ParserError::InvalidIdentifier);
            }
            result.push(chars[index]);
            index += 1;
        }
        return Ok(result);
    }

    let mut chars = input.chars();
    let Some(first) = chars.next() else {
        return Err(ParserError::InvalidIdentifier);
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(ParserError::InvalidIdentifier);
    }
    if chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        Ok(input.to_owned())
    } else {
        Err(ParserError::InvalidIdentifier)
    }
}

pub fn parse_object_key(input: &str) -> Result<String, ParserError> {
    if !(input.starts_with("['") && input.ends_with("']")) {
        return Err(ParserError::InvalidLiteralString);
    }
    let inner = &input[2..input.len() - 2];
    let chars: Vec<char> = inner.chars().collect();
    let mut result = String::new();
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] == '\'' {
            if index + 1 < chars.len() && chars[index + 1] == '\'' {
                result.push('\'');
                index += 2;
                continue;
            }
            return Err(ParserError::InvalidLiteralString);
        }
        result.push(chars[index]);
        index += 1;
    }
    Ok(result)
}

pub fn parse_json_path_element(input: &str) -> Result<JsonPathElement, ParserError> {
    if input == ".*" {
        return Ok(JsonPathElement::ObjectWildcard);
    }
    if input == "[*]" {
        return Ok(JsonPathElement::ArrayWildcard);
    }
    if input.starts_with("['") && input.ends_with("']") {
        return Ok(JsonPathElement::Key(parse_object_key(input)?));
    }
    if input.starts_with('[') && input.ends_with(']') {
        let inner = &input[1..input.len() - 1];
        if let Ok(index) = inner.parse::<usize>() {
            return Ok(JsonPathElement::Index(index));
        }
        return Err(ParserError::InvalidJsonPathElement);
    }
    if let Some(rest) = input.strip_prefix('.') {
        return Ok(JsonPathElement::Key(parse_identifier(rest)?));
    }
    Err(ParserError::InvalidJsonPathElement)
}

pub fn parse_json_path(input: &str) -> Result<ParsedJsonPath, ParserError> {
    let mut chars = input.char_indices().peekable();
    let mut base_end = None;
    while let Some((idx, ch)) = chars.peek().copied() {
        if ch == '.' || ch == '[' {
            base_end = Some(idx);
            break;
        }
        chars.next();
    }
    let base_end = base_end.unwrap_or(input.len());
    let base_key = parse_identifier(&input[..base_end])?;
    let mut path_expr = Vec::new();
    let mut index = base_end;
    while index < input.len() {
        let tail = &input[index..];
        if tail.starts_with(".*") {
            path_expr.push(JsonPathElement::ObjectWildcard);
            index += 2;
            continue;
        }
        if tail.starts_with("[*]") {
            path_expr.push(JsonPathElement::ArrayWildcard);
            index += 3;
            continue;
        }
        if tail.starts_with('.') {
            if tail.len() > 1 && tail.as_bytes()[1] == b'"' {
                let mut end = 2usize;
                let bytes = tail.as_bytes();
                while end < bytes.len() {
                    if bytes[end] == b'"' {
                        if end + 1 < bytes.len() && bytes[end + 1] == b'"' {
                            end += 2;
                            continue;
                        }
                        break;
                    }
                    end += 1;
                }
                if end >= bytes.len() || bytes[end] != b'"' {
                    return Err(ParserError::InvalidJsonPath);
                }
                path_expr.push(parse_json_path_element(&tail[..=end])?);
                index += end + 1;
                continue;
            }
            let end = tail[1..]
                .char_indices()
                .find_map(|(offset, ch)| (ch == '.' || ch == '[').then_some(offset + 1))
                .unwrap_or(tail.len());
            path_expr.push(parse_json_path_element(&tail[..end])?);
            index += end;
            continue;
        }
        if tail.starts_with("['") {
            let mut end = 2usize;
            let bytes = tail.as_bytes();
            while end < bytes.len() {
                if bytes[end] == b'\'' {
                    if end + 1 < bytes.len() && bytes[end + 1] == b'\'' {
                        end += 2;
                        continue;
                    }
                    if end + 1 < bytes.len() && bytes[end + 1] == b']' {
                        break;
                    }
                }
                end += 1;
            }
            if end + 1 >= bytes.len() {
                return Err(ParserError::InvalidJsonPath);
            }
            path_expr.push(parse_json_path_element(&tail[..=end + 1])?);
            index += end + 2;
            continue;
        }
        if tail.starts_with('[') {
            let Some(end) = tail.find(']') else {
                return Err(ParserError::InvalidJsonPath);
            };
            path_expr.push(parse_json_path_element(&tail[..=end])?);
            index += end + 1;
            continue;
        }
        return Err(ParserError::InvalidJsonPath);
    }
    Ok(ParsedJsonPath {
        base_key,
        path_expr,
    })
}
