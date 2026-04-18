use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedJsonPath {
    pub base_key: String,
    pub path_expr: Vec<JsonPathElement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedFunction {
    pub name: String,
    pub args: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasedExpression {
    pub expression_sql: String,
    pub as_alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectExpressionAst {
    pub all: bool,
    pub expressions: Vec<AliasedExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableExpressionAst {
    pub table: ParsedJsonPath,
    pub as_alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectAst {
    pub expression: SelectExpressionAst,
    pub from: TableExpressionAst,
    pub where_clause: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub select_ast: SelectAst,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonPathElement {
    Key(String),
    Index(usize),
    ObjectWildcard,
    ArrayWildcard,
}

impl JsonPathElement {
    pub fn key(value: impl Into<String>) -> Self {
        Self::Key(value.into())
    }

    pub fn index(value: usize) -> Self {
        Self::Index(value)
    }

    pub fn object_wildcard() -> Self {
        Self::ObjectWildcard
    }

    pub fn array_wildcard() -> Self {
        Self::ArrayWildcard
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonPathValue {
    Json(JsonValue),
    Missing,
    Sequence(Vec<JsonPathValue>),
}

impl JsonPathValue {
    pub fn json(value: JsonValue) -> Self {
        Self::Json(value)
    }
}
