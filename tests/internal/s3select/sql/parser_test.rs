use minio_rust::internal::s3select::sql::{
    lex_sql, parse_function_expr, parse_identifier, parse_json_path, parse_json_path_element,
    parse_object_key, parse_select, parse_select_statement, JsonPathElement,
};

pub const SOURCE_FILE: &str = "internal/s3select/sql/parser_test.go";

#[test]
fn test_jsonpath_element_line_28() {
    let cases = [
        ("['name']", JsonPathElement::key("name")),
        (".name", JsonPathElement::key("name")),
        (r#"."name""#, JsonPathElement::key("name")),
        ("[2]", JsonPathElement::index(2)),
        ("[0]", JsonPathElement::index(0)),
        ("[100]", JsonPathElement::index(100)),
        (".*", JsonPathElement::object_wildcard()),
        ("[*]", JsonPathElement::array_wildcard()),
    ];

    for (index, (input, expected)) in cases.into_iter().enumerate() {
        let parsed = parse_json_path_element(input)
            .unwrap_or_else(|err| panic!("{index}: failed to parse {input}: {err}"));
        assert_eq!(parsed, expected, "case {index}");
    }
}

#[test]
fn test_jsonpath_line_59() {
    let cases = [
        "S3Object",
        "S3Object.id",
        "S3Object.book.title",
        "S3Object.id[1]",
        "S3Object.id['abc']",
        "S3Object.id['ab']",
        "S3Object.words.*.id",
        "S3Object.words.name[*].val",
        "S3Object.words.name[*].val[*]",
        "S3Object.words.name[*].val.*",
    ];

    for (index, input) in cases.into_iter().enumerate() {
        parse_json_path(input)
            .unwrap_or_else(|err| panic!("{index}: failed to parse {input}: {err}"));
    }
}

#[test]
fn test_identifier_parsing_line_89() {
    let valid_cases = ["a", "_a", "abc_a", "a2", r#""abc""#, r#""abc\a""ac""#];
    for (index, input) in valid_cases.into_iter().enumerate() {
        parse_identifier(input)
            .unwrap_or_else(|err| panic!("{index}: expected valid identifier {input}: {err}"));
    }

    let invalid_cases = [
        "+a", "-a", "1a", r#""ab"#, r#"abc""#, r#"aa""a"#, r#""a"a""#,
    ];
    for (index, input) in invalid_cases.into_iter().enumerate() {
        assert!(
            parse_identifier(input).is_err(),
            "{index}: expected invalid identifier {input}"
        );
    }
}

#[test]
fn test_literal_string_parsing_line_131() {
    let valid_cases = [
        "['abc']",
        "['ab''c']",
        "['a''b''c']",
        r#"['abc-x_1##@(*&(#*))/\']"#,
    ];
    for (index, input) in valid_cases.into_iter().enumerate() {
        let parsed = parse_object_key(input)
            .unwrap_or_else(|err| panic!("{index}: expected valid key {input}: {err}"));
        assert!(!parsed.is_empty(), "case {index}");
    }

    let invalid_cases = ["['abc'']", "['-abc'sc']", "[abc']", "['ac]"];
    for (index, input) in invalid_cases.into_iter().enumerate() {
        assert!(
            parse_object_key(input).is_err(),
            "{index}: expected invalid key {input}"
        );
    }
}

#[test]
fn test_function_parsing_line_171() {
    let cases = [
        "count(*)",
        "sum(2 + s.id)",
        "sum(t)",
        "avg(s.id[1])",
        "coalesce(s.id[1], 2, 2 + 3)",
        "cast(s as string)",
        "cast(s AS INT)",
        "cast(s as DECIMAL)",
        "extract(YEAR from '2018-01-09')",
        "extract(month from '2018-01-09')",
        "extract(hour from '2018-01-09')",
        "extract(day from '2018-01-09')",
        "substring('abcd' from 2 for 2)",
        "substring('abcd' from 2)",
        "substring('abcd' , 2 , 2)",
        "substring('abcd' , 22 )",
        "trim('  aab  ')",
        "trim(leading from '  aab  ')",
        "trim(trailing from '  aab  ')",
        "trim(both from '  aab  ')",
        "trim(both '12' from '  aab  ')",
        "trim(leading '12' from '  aab  ')",
        "trim(trailing '12' from '  aab  ')",
        "count(23)",
    ];
    for (index, input) in cases.into_iter().enumerate() {
        parse_function_expr(input)
            .unwrap_or_else(|err| panic!("{index}: failed to parse function {input}: {err}"));
    }
}

#[test]
fn test_sql_lexer_line_219() {
    let tokens = lex_sql("S3Object.words.*.id").expect("lexer should succeed");
    assert_eq!(tokens.len(), 7);
}

#[test]
fn test_select_where_line_239() {
    let cases = [
        "select * from s3object",
        "select a, b from s3object s",
        "select a, b from s3object as s",
        "select a, b from s3object as s where a = 1",
        "select a, b from s3object s where a = 1",
        "select a, b from s3object where a = 1",
    ];
    for (index, input) in cases.into_iter().enumerate() {
        parse_select(input)
            .unwrap_or_else(|err| panic!("{index}: failed to parse select {input}: {err}"));
    }
}

#[test]
fn test_like_clause_line_265() {
    let cases = [
        r#"select * from s3object where Name like 'abcd'"#,
        r#"select Name like 'abc' from s3object"#,
        r#"select * from s3object where Name not like 'abc'"#,
        r#"select * from s3object where Name like 'abc' escape 't'"#,
        r#"select * from s3object where Name like 'a\%' escape '?'"#,
        r#"select * from s3object where Name not like 'abc\' escape '?'"#,
        r#"select * from s3object where Name like 'a\%' escape LOWER('?')"#,
        r#"select * from s3object where Name not like LOWER('Bc\') escape '?'"#,
    ];
    for (index, input) in cases.into_iter().enumerate() {
        parse_select(input)
            .unwrap_or_else(|err| panic!("{index}: failed to parse like select {input}: {err}"));
    }
}

#[test]
fn test_between_clause_line_291() {
    let cases = [
        "select * from s3object where Id between 1 and 2",
        "select * from s3object where Id between 1 and 2 and name = 'Ab'",
        "select * from s3object where Id not between 1 and 2",
        "select * from s3object where Id not between 1 and 2 and name = 'Bc'",
    ];
    for (index, input) in cases.into_iter().enumerate() {
        parse_select(input)
            .unwrap_or_else(|err| panic!("{index}: failed to parse between select {input}: {err}"));
    }
}

#[test]
fn test_from_clause_jsonpath_line_313() {
    let cases = [
        "select * from s3object",
        "select * from s3object[*].name",
        "select * from s3object[*].books[*]",
        "select * from s3object[*].books[*].name",
        "select * from s3object where name > 2",
        "select * from s3object[*].name where name > 2",
        "select * from s3object[*].books[*] where name > 2",
        "select * from s3object[*].books[*].name where name > 2",
        "select * from s3object[*].books[*] s",
        "select * from s3object[*].books[*].name as s",
        "select * from s3object s where name > 2",
        "select * from s3object[*].name as s where name > 2",
        "select * from s3object[*].books[*] limit 1",
    ];
    for (index, input) in cases.into_iter().enumerate() {
        parse_select(input).unwrap_or_else(|err| {
            panic!("{index}: failed to parse from/jsonpath select {input}: {err}")
        });
    }
}

#[test]
fn test_select_parsing_line_346() {
    let cases = [
        "select * from s3object where name > 2 or value > 1 or word > 2",
        "select s.word.id + 2 from s3object s",
        "select 1-2-3 from s3object s limit 1",
    ];
    for (index, input) in cases.into_iter().enumerate() {
        parse_select(input)
            .unwrap_or_else(|err| panic!("{index}: failed to parse select {input}: {err}"));
    }
}

#[test]
fn test_sql_lexer_arith_ops_line_369() {
    let tokens = lex_sql("year from select month hour distinct").expect("lexer should succeed");
    assert_eq!(tokens.len(), 7);
}

#[test]
fn test_parse_select_statement_line_387() {
    let parsed = parse_select_statement("select _3,_1,_2 as 'mytest'  from S3object")
        .expect("parse alias sql should succeed");
    assert_eq!(
        parsed.select_ast.expression.expressions[2].as_alias,
        "mytest"
    );
}
