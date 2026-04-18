use std::io;

use serde_json::Value;

use super::ast::{
    normalize_output_name, unquote_identifier, PathExpr, PathSegment, Predicate, Query, QueryKind,
    SelectExpr,
};
use super::values::number_to_value;

pub(crate) fn parse_query(query: &str) -> io::Result<Query> {
    let query = query.trim().trim_end_matches(';');
    let normalized = query.to_ascii_lowercase();
    if !normalized.starts_with("select ") {
        return Err(unsupported_query(query));
    }

    let Some(from_index) = find_keyword(&normalized, " from s3object") else {
        return Err(unsupported_query(query));
    };

    let select_part = query["select ".len()..from_index].trim();
    let remainder = query[from_index + " from s3object".len()..].trim();
    let (where_clause, limit) = parse_tail(remainder)?;

    let filter = match where_clause {
        Some(where_clause) => parse_where_clause(where_clause)?,
        None => Vec::new(),
    };

    let select_lower = select_part.to_ascii_lowercase();
    let kind = if select_part == "*" {
        QueryKind::SelectAll
    } else if select_lower.starts_with("count(") {
        parse_count_query(select_part)?
    } else {
        let expressions = split_top_level(select_part, ',')
            .into_iter()
            .map(|expr| parse_select_expr(expr.trim()))
            .collect::<io::Result<Vec<_>>>()?;
        QueryKind::SelectExpressions(expressions)
    };

    Ok(Query {
        kind,
        filter,
        limit,
    })
}

fn parse_count_query(select_part: &str) -> io::Result<QueryKind> {
    let Some(close_paren) = select_part.find(')') else {
        return Err(unsupported_query(select_part));
    };
    let inner = select_part["count(".len()..close_paren].trim();
    let alias = parse_alias(select_part[close_paren + 1..].trim())?;
    let column = if inner == "*" {
        None
    } else {
        Some(normalize_output_name(&parse_path_expr(inner)?))
    };
    Ok(QueryKind::Count { column, alias })
}

fn parse_select_expr(expression: &str) -> io::Result<SelectExpr> {
    let (expression, alias) = split_alias(expression)?;
    let lower = expression.to_ascii_lowercase();
    if lower.starts_with("date_add(") && lower.ends_with(')') {
        let inside = expression["DATE_ADD(".len()..expression.len() - 1].trim();
        let args = split_top_level(inside, ',');
        if args.len() != 3 || !args[0].trim().eq_ignore_ascii_case("day") {
            return Err(unsupported_query(expression));
        }
        let days = args[1]
            .trim()
            .parse::<u64>()
            .map_err(|_| unsupported_query(expression))?;
        let path = parse_path_expr(args[2].trim())?;
        let output_name = alias.unwrap_or_else(|| normalize_output_name(&path));
        return Ok(SelectExpr::DateAddDays {
            path,
            days,
            output_name,
        });
    }
    if lower.starts_with("not cast(") && lower.ends_with(')') {
        let inside = expression["NOT CAST(".len()..expression.len() - 1].trim();
        let lower_inside = inside.to_ascii_lowercase();
        let Some(as_index) = lower_inside.rfind(" as bool") else {
            return Err(unsupported_query(expression));
        };
        let path = parse_path_expr(inside[..as_index].trim())?;
        let output_name = alias.unwrap_or_else(|| normalize_output_name(&path));
        return Ok(SelectExpr::NotCastBool { path, output_name });
    }
    if lower.starts_with("cast(") && lower.ends_with(')') {
        let inside = expression["CAST(".len()..expression.len() - 1].trim();
        let lower_inside = inside.to_ascii_lowercase();
        let Some(as_index) = lower_inside.rfind(" as string") else {
            return Err(unsupported_query(expression));
        };
        let path = parse_path_expr(inside[..as_index].trim())?;
        let output_name = alias.unwrap_or_else(|| normalize_output_name(&path));
        return Ok(SelectExpr::Column { path, output_name });
    }

    let path = parse_path_expr(expression)?;
    let output_name = alias.unwrap_or_else(|| normalize_output_name(&path));
    Ok(SelectExpr::Column { path, output_name })
}

fn parse_tail(remainder: &str) -> io::Result<(Option<&str>, Option<usize>)> {
    let trimmed = remainder.trim();
    if trimmed.is_empty() {
        return Ok((None, None));
    }

    let lower = trimmed.to_ascii_lowercase();
    let where_pos = find_clause_start(&lower, "where");
    let limit_pos = find_clause_start(&lower, "limit");

    let mut where_clause = None;
    let mut limit = None;

    match (where_pos, limit_pos) {
        (Some(where_pos), Some(limit_pos)) if where_pos < limit_pos => {
            where_clause = Some(trimmed[where_pos + "where ".len()..limit_pos].trim());
            limit = Some(parse_limit_value(&trimmed[limit_pos + "limit ".len()..])?);
        }
        (Some(where_pos), None) => {
            where_clause = Some(trimmed[where_pos + "where ".len()..].trim());
        }
        (None, Some(limit_pos)) => {
            limit = Some(parse_limit_value(&trimmed[limit_pos + "limit ".len()..])?);
        }
        _ => {}
    }

    Ok((where_clause, limit))
}

fn parse_limit_value(value: &str) -> io::Result<usize> {
    value
        .trim()
        .parse::<usize>()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid LIMIT: {err}")))
}

fn parse_where_clause(where_clause: &str) -> io::Result<Vec<Predicate>> {
    split_keyword_top_level(where_clause, "AND")
        .into_iter()
        .map(|clause| parse_predicate(clause.trim()))
        .collect()
}

fn parse_predicate(clause: &str) -> io::Result<Predicate> {
    let clause = strip_wrapping_parens(clause.trim());
    let lower = clause.to_ascii_lowercase();
    if let Some(index) = find_keyword(&lower, " is not null") {
        return Ok(Predicate::IsNotNull(parse_path_expr(&clause[..index])?));
    }
    if let Some(index) = find_keyword(&lower, " is null") {
        return Ok(Predicate::IsNull(parse_path_expr(&clause[..index])?));
    }
    if let Some(index) = find_keyword(&lower, " is not ''") {
        return Ok(Predicate::IsNotEmpty(parse_path_expr(&clause[..index])?));
    }
    if let Some(index) = find_top_level_keyword(clause, " in ") {
        let left = clause[..index].trim();
        let right = clause[index + " in ".len()..].trim();
        if right.starts_with('[') || right.starts_with('(') {
            return Ok(Predicate::InPath(
                parse_path_expr(left)?,
                parse_list_literal(right)?,
            ));
        }
        return Ok(Predicate::Contains(
            parse_literal_or_list(left)?,
            parse_path_expr(right)?,
        ));
    }
    if let Some(index) = clause.find("<=") {
        return Ok(Predicate::LtEq(
            parse_path_expr(&clause[..index])?,
            parse_literal_or_list(clause[index + 2..].trim())?,
        ));
    }
    if let Some(index) = clause.find(">=") {
        return Ok(Predicate::GtEq(
            parse_path_expr(&clause[..index])?,
            parse_literal_or_list(clause[index + 2..].trim())?,
        ));
    }
    if let Some(index) = clause.find("!=") {
        return Ok(Predicate::Ne(
            parse_path_expr(&clause[..index])?,
            parse_literal_or_list(clause[index + 2..].trim())?,
        ));
    }
    if let Some(index) = clause.find('>') {
        return Ok(Predicate::Gt(
            parse_path_expr(&clause[..index])?,
            parse_literal_or_list(clause[index + 1..].trim())?,
        ));
    }
    if let Some(index) = clause.find('=') {
        return Ok(Predicate::Eq(
            parse_path_expr(&clause[..index])?,
            parse_literal_or_list(clause[index + 1..].trim())?,
        ));
    }
    Err(unsupported_query(clause))
}

fn parse_list_literal(list: &str) -> io::Result<Vec<Value>> {
    let list = list.trim();
    let (open, close) = match (list.chars().next(), list.chars().last()) {
        (Some('['), Some(']')) => ('[', ']'),
        (Some('('), Some(')')) => ('(', ')'),
        _ => {
            return Err(unsupported_query(list));
        }
    };
    if !list.starts_with(open) || !list.ends_with(close) {
        return Err(unsupported_query(list));
    }
    let inner = &list[1..list.len() - 1];
    split_top_level(inner, ',')
        .into_iter()
        .map(|item| parse_literal_or_list(item.trim()))
        .collect()
}

fn parse_literal_or_list(literal: &str) -> io::Result<Value> {
    let literal = literal.trim();
    if literal.starts_with('[') || literal.starts_with('(') {
        return Ok(Value::Array(parse_list_literal(literal)?));
    }
    if let Some(stripped) = literal
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
    {
        return Ok(Value::String(stripped.to_owned()));
    }
    if literal.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if literal.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }
    if let Ok(number) = literal.parse::<f64>() {
        return Ok(number_to_value(number));
    }
    Err(unsupported_query(literal))
}

fn parse_alias(value: &str) -> io::Result<Option<String>> {
    if value.is_empty() {
        return Ok(None);
    }
    let lower = value.to_ascii_lowercase();
    let Some(alias) = lower.strip_prefix("as ") else {
        return Err(unsupported_query(value));
    };
    Ok(Some(unquote_identifier(alias.trim()).to_owned()))
}

fn split_alias(expression: &str) -> io::Result<(&str, Option<String>)> {
    if let Some(index) = find_top_level_keyword(expression, " as ") {
        let alias = unquote_identifier(expression[index + " as ".len()..].trim()).to_owned();
        Ok((expression[..index].trim(), Some(alias)))
    } else {
        Ok((expression.trim(), None))
    }
}

fn parse_path_expr(value: &str) -> io::Result<PathExpr> {
    let value = strip_table_prefix(value.trim());
    let mut chars = value.chars().peekable();
    let mut segments = Vec::new();

    while chars.peek().is_some() {
        match chars.peek().copied() {
            Some('.') => {
                chars.next();
            }
            Some('[') => {
                chars.next();
                match chars.peek().copied() {
                    Some('*') => {
                        chars.next();
                        expect_char(&mut chars, ']')?;
                        segments.push(PathSegment::Wildcard);
                    }
                    Some('"') => {
                        chars.next();
                        let key = collect_until_quote(&mut chars);
                        expect_char(&mut chars, ']')?;
                        segments.push(PathSegment::Key(key));
                    }
                    Some(ch) if ch.is_ascii_digit() => {
                        let mut digits = String::new();
                        while let Some(ch) = chars.peek().copied() {
                            if ch.is_ascii_digit() {
                                digits.push(ch);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        expect_char(&mut chars, ']')?;
                        segments.push(PathSegment::Index(
                            digits
                                .parse::<usize>()
                                .map_err(|_| unsupported_query(value))?,
                        ));
                    }
                    _ => return Err(unsupported_query(value)),
                }
            }
            Some('"') => {
                chars.next();
                segments.push(PathSegment::Key(collect_until_quote(&mut chars)));
            }
            Some(_) => {
                let mut ident = String::new();
                while let Some(ch) = chars.peek().copied() {
                    if ch == '.' || ch == '[' {
                        break;
                    }
                    ident.push(ch);
                    chars.next();
                }
                let ident = ident.trim();
                if !ident.is_empty() {
                    segments.push(PathSegment::Key(ident.to_owned()));
                }
            }
            None => break,
        }
    }

    if segments.is_empty() {
        return Err(unsupported_query(value));
    }
    Ok(PathExpr { segments })
}

fn find_keyword(haystack: &str, needle: &str) -> Option<usize> {
    haystack.find(needle)
}

fn strip_table_prefix(value: &str) -> &str {
    let lower = value.to_ascii_lowercase();
    for prefix in ["s3object.", "s."] {
        if lower.starts_with(prefix) {
            return &value[prefix.len()..];
        }
    }
    value
}

fn find_clause_start(haystack: &str, keyword: &str) -> Option<usize> {
    let prefix = format!("{keyword} ");
    if haystack.starts_with(&prefix) {
        Some(0)
    } else {
        haystack.find(&format!(" {prefix}")).map(|index| index + 1)
    }
}

fn split_keyword_top_level<'a>(input: &'a str, keyword: &str) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut bracket_depth = 0usize;
    let mut paren_depth = 0usize;
    let keyword_lower = keyword.to_ascii_lowercase();
    let lower = input.to_ascii_lowercase();
    let bytes = input.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        let ch = bytes[index] as char;
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '[' if !in_single && !in_double => bracket_depth += 1,
            ']' if !in_single && !in_double && bracket_depth > 0 => bracket_depth -= 1,
            '(' if !in_single && !in_double => paren_depth += 1,
            ')' if !in_single && !in_double && paren_depth > 0 => paren_depth -= 1,
            _ => {}
        }

        if !in_single
            && !in_double
            && bracket_depth == 0
            && paren_depth == 0
            && lower[index..].starts_with(&keyword_lower)
        {
            parts.push(input[start..index].trim());
            index += keyword.len();
            start = index;
            continue;
        }
        index += ch.len_utf8();
    }

    parts.push(input[start..].trim());
    parts
}

fn strip_wrapping_parens(input: &str) -> &str {
    let mut value = input.trim();
    loop {
        if !value.starts_with('(') || !value.ends_with(')') {
            return value;
        }

        let mut in_single = false;
        let mut in_double = false;
        let mut bracket_depth = 0usize;
        let mut paren_depth = 0usize;
        let mut wraps = false;

        for (index, ch) in value.char_indices() {
            match ch {
                '\'' if !in_double => in_single = !in_single,
                '"' if !in_single => in_double = !in_double,
                '[' if !in_single && !in_double => bracket_depth += 1,
                ']' if !in_single && !in_double && bracket_depth > 0 => bracket_depth -= 1,
                '(' if !in_single && !in_double => paren_depth += 1,
                ')' if !in_single && !in_double && paren_depth > 0 => {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        wraps = index == value.len() - 1;
                        break;
                    }
                }
                _ => {}
            }
        }

        if !wraps {
            return value;
        }

        value = value[1..value.len() - 1].trim();
    }
}

fn find_top_level_keyword(input: &str, keyword: &str) -> Option<usize> {
    let mut in_single = false;
    let mut in_double = false;
    let mut bracket_depth = 0usize;
    let mut paren_depth = 0usize;
    let keyword_lower = keyword.to_ascii_lowercase();
    let lower = input.to_ascii_lowercase();
    let bytes = input.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        let ch = bytes[index] as char;
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '[' if !in_single && !in_double => bracket_depth += 1,
            ']' if !in_single && !in_double && bracket_depth > 0 => bracket_depth -= 1,
            '(' if !in_single && !in_double => paren_depth += 1,
            ')' if !in_single && !in_double && paren_depth > 0 => paren_depth -= 1,
            _ => {}
        }

        if !in_single
            && !in_double
            && bracket_depth == 0
            && paren_depth == 0
            && lower[index..].starts_with(&keyword_lower)
        {
            return Some(index);
        }
        index += ch.len_utf8();
    }

    None
}

fn expect_char(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    expected: char,
) -> io::Result<()> {
    match chars.next() {
        Some(ch) if ch == expected => Ok(()),
        _ => Err(unsupported_query("malformed path")),
    }
}

fn collect_until_quote(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut out = String::new();
    for ch in chars.by_ref() {
        if ch == '"' {
            break;
        }
        out.push(ch);
    }
    out
}

fn split_top_level<'a>(input: &'a str, delimiter: char) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut bracket_depth = 0usize;
    let mut paren_depth = 0usize;

    for (index, ch) in input.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '[' if !in_single && !in_double => bracket_depth += 1,
            ']' if !in_single && !in_double && bracket_depth > 0 => bracket_depth -= 1,
            '(' if !in_single && !in_double => paren_depth += 1,
            ')' if !in_single && !in_double && paren_depth > 0 => paren_depth -= 1,
            _ => {}
        }

        if ch == delimiter && !in_single && !in_double && bracket_depth == 0 && paren_depth == 0 {
            parts.push(input[start..index].trim());
            start = index + ch.len_utf8();
        }
    }

    parts.push(input[start..].trim());
    parts
}

fn unsupported_query(query: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("unsupported query: {query}"),
    )
}
