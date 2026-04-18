use super::{
    ast::{
        AliasedExpression, ParsedFunction, SelectAst, SelectExpressionAst, SelectStatement,
        TableExpressionAst,
    },
    errors::ParserError,
    json_path::{parse_identifier, parse_json_path},
};

pub fn lex_sql(input: &str) -> Result<Vec<String>, ParserError> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let ch = bytes[index] as char;
        if ch.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if index + 1 < bytes.len() {
            let pair = &input[index..index + 2];
            if matches!(pair, "<>" | "!=" | "<=" | ">=" | ".*") {
                tokens.push(pair.to_owned());
                index += 2;
                continue;
            }
        }
        if index + 2 < bytes.len() && &input[index..index + 3] == "[*]" {
            tokens.push("[*]".to_owned());
            index += 3;
            continue;
        }
        if ch == '\'' || ch == '"' {
            let quote = ch;
            let start = index;
            index += 1;
            while index < bytes.len() {
                let current = bytes[index] as char;
                if current == quote {
                    if index + 1 < bytes.len() && bytes[index + 1] as char == quote {
                        index += 2;
                        continue;
                    }
                    index += 1;
                    break;
                }
                index += 1;
            }
            tokens.push(input[start..index].to_owned());
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = index;
            index += 1;
            while index < bytes.len() {
                let current = bytes[index] as char;
                if current.is_ascii_alphanumeric() || current == '_' {
                    index += 1;
                } else {
                    break;
                }
            }
            tokens.push(input[start..index].to_owned());
            continue;
        }
        if ch.is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len() {
                let current = bytes[index] as char;
                if current.is_ascii_digit()
                    || current == '.'
                    || current == 'e'
                    || current == 'E'
                    || current == '-'
                    || current == '+'
                {
                    index += 1;
                } else {
                    break;
                }
            }
            tokens.push(input[start..index].to_owned());
            continue;
        }
        if "[](),.=<>*/%+-".contains(ch) {
            tokens.push(ch.to_string());
            index += 1;
            continue;
        }
        return Err(ParserError::InvalidSelect);
    }
    tokens.push("EOF".to_owned());
    Ok(tokens)
}

pub fn parse_function_expr(input: &str) -> Result<ParsedFunction, ParserError> {
    let trimmed = input.trim();
    let Some(open) = trimmed.find('(') else {
        return Err(ParserError::InvalidFunction);
    };
    if !trimmed.ends_with(')') || open == 0 {
        return Err(ParserError::InvalidFunction);
    }
    let name = trimmed[..open].trim().to_ascii_lowercase();
    let supported = [
        "avg",
        "max",
        "min",
        "sum",
        "coalesce",
        "nullif",
        "to_string",
        "to_timestamp",
        "utcnow",
        "char_length",
        "character_length",
        "lower",
        "upper",
        "count",
        "cast",
        "substring",
        "extract",
        "trim",
        "date_add",
        "date_diff",
    ];
    if !supported.contains(&name.as_str()) {
        return Err(ParserError::UnsupportedFunction);
    }
    let args = &trimmed[open + 1..trimmed.len() - 1];
    let mut paren_depth = 0i32;
    let mut single_quote = false;
    let mut double_quote = false;
    for ch in args.chars() {
        match ch {
            '\'' if !double_quote => single_quote = !single_quote,
            '"' if !single_quote => double_quote = !double_quote,
            '(' if !single_quote && !double_quote => paren_depth += 1,
            ')' if !single_quote && !double_quote => paren_depth -= 1,
            _ => {}
        }
        if paren_depth < 0 {
            return Err(ParserError::InvalidFunction);
        }
    }
    if single_quote || double_quote || paren_depth != 0 {
        return Err(ParserError::InvalidFunction);
    }
    Ok(ParsedFunction {
        name,
        args: args.trim().to_owned(),
    })
}

fn split_top_level(input: &str, delimiter: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0i32;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut previous = '\0';
    for (index, ch) in input.char_indices() {
        match ch {
            '\'' if !double_quote && previous != '\\' => single_quote = !single_quote,
            '"' if !single_quote && previous != '\\' => double_quote = !double_quote,
            '(' if !single_quote && !double_quote => paren_depth += 1,
            ')' if !single_quote && !double_quote => paren_depth -= 1,
            _ => {}
        }
        if ch == delimiter && !single_quote && !double_quote && paren_depth == 0 {
            parts.push(input[start..index].trim().to_owned());
            start = index + ch.len_utf8();
        }
        previous = ch;
    }
    parts.push(input[start..].trim().to_owned());
    parts
}

fn find_keyword_top_level(input: &str, keyword: &str) -> Option<usize> {
    let keyword_len = keyword.len();
    let bytes = input.as_bytes();
    let mut paren_depth = 0i32;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut index = 0usize;
    while index < bytes.len() {
        let ch = bytes[index] as char;
        match ch {
            '\'' if !double_quote => single_quote = !single_quote,
            '"' if !single_quote => double_quote = !double_quote,
            '(' if !single_quote && !double_quote => paren_depth += 1,
            ')' if !single_quote && !double_quote => paren_depth -= 1,
            _ => {}
        }
        if !single_quote
            && !double_quote
            && paren_depth == 0
            && index + keyword_len <= bytes.len()
            && input[index..index + keyword_len].eq_ignore_ascii_case(keyword)
        {
            let before = if index == 0 {
                None
            } else {
                Some(bytes[index - 1] as char)
            };
            let after = if index + keyword_len >= bytes.len() {
                None
            } else {
                Some(bytes[index + keyword_len] as char)
            };
            let is_boundary = |ch: Option<char>| match ch {
                None => true,
                Some(ch) => !(ch.is_ascii_alphanumeric() || ch == '_'),
            };
            if is_boundary(before) && is_boundary(after) {
                return Some(index);
            }
        }
        index += ch.len_utf8();
    }
    None
}

fn parse_alias(raw: &str) -> Result<String, ParserError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        let inner = &trimmed[1..trimmed.len() - 1];
        return Ok(inner.replace("''", "'"));
    }
    parse_identifier(trimmed)
}

pub fn parse_select(input: &str) -> Result<SelectAst, ParserError> {
    let trimmed = input.trim();
    if !trimmed[..trimmed.len().min(6)].eq_ignore_ascii_case("select") {
        return Err(ParserError::InvalidSelect);
    }
    let select_rest = trimmed[6..].trim_start();
    let Some(from_index) = find_keyword_top_level(select_rest, "from") else {
        return Err(ParserError::InvalidSelect);
    };
    let projection_sql = select_rest[..from_index].trim();
    let from_rest = select_rest[from_index + 4..].trim_start();
    if projection_sql.is_empty() || from_rest.is_empty() {
        return Err(ParserError::InvalidSelect);
    }

    let where_index = find_keyword_top_level(from_rest, "where");
    let limit_index = find_keyword_top_level(from_rest, "limit");
    let boundary = match (where_index, limit_index) {
        (Some(w), Some(l)) => Some(w.min(l)),
        (Some(w), None) => Some(w),
        (None, Some(l)) => Some(l),
        (None, None) => None,
    };
    let table_sql = boundary
        .map(|idx| from_rest[..idx].trim())
        .unwrap_or(from_rest)
        .trim();
    let mut where_clause = None;
    let mut limit = None;

    if let Some(idx) = where_index {
        let where_rest = from_rest[idx + 5..].trim_start();
        let where_limit = find_keyword_top_level(where_rest, "limit");
        let where_sql = where_limit
            .map(|limit_idx| where_rest[..limit_idx].trim())
            .unwrap_or(where_rest)
            .trim();
        if where_sql.is_empty() {
            return Err(ParserError::InvalidSelect);
        }
        where_clause = Some(where_sql.to_owned());
    }
    if let Some(idx) = limit_index {
        let limit_rest = from_rest[idx + 5..].trim_start();
        let parsed_limit = limit_rest
            .split_whitespace()
            .next()
            .ok_or(ParserError::InvalidLimit)?
            .parse::<i64>()
            .map_err(|_| ParserError::InvalidLimit)?;
        if parsed_limit < 0 {
            return Err(ParserError::InvalidLimit);
        }
        limit = Some(parsed_limit);
    }

    let table_parts: Vec<&str> = table_sql.split_whitespace().collect();
    if table_parts.is_empty() {
        return Err(ParserError::InvalidSelect);
    }
    let table = parse_json_path(table_parts[0])?;
    let as_alias = match table_parts.as_slice() {
        [_] => String::new(),
        [_, alias] => parse_identifier(alias)?,
        [_, as_kw, alias] if as_kw.eq_ignore_ascii_case("as") => parse_identifier(alias)?,
        _ => return Err(ParserError::InvalidSelect),
    };

    let expression = if projection_sql == "*" {
        SelectExpressionAst {
            all: true,
            expressions: Vec::new(),
        }
    } else {
        let mut expressions = Vec::new();
        for expr in split_top_level(projection_sql, ',') {
            if expr.is_empty() {
                return Err(ParserError::InvalidSelect);
            }
            if let Some(as_index) = find_keyword_top_level(&expr, "as") {
                let expression_sql = expr[..as_index].trim().to_owned();
                let alias_sql = expr[as_index + 2..].trim();
                expressions.push(AliasedExpression {
                    expression_sql,
                    as_alias: parse_alias(alias_sql)?,
                });
            } else {
                expressions.push(AliasedExpression {
                    expression_sql: expr,
                    as_alias: String::new(),
                });
            }
        }
        SelectExpressionAst {
            all: false,
            expressions,
        }
    };

    Ok(SelectAst {
        expression,
        from: TableExpressionAst { table, as_alias },
        where_clause,
        limit,
    })
}

pub fn parse_select_statement(input: &str) -> Result<SelectStatement, ParserError> {
    Ok(SelectStatement {
        select_ast: parse_select(input)?,
    })
}
