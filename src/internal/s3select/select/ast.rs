use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QueryKind {
    SelectAll,
    SelectExpressions(Vec<SelectExpr>),
    Count {
        column: Option<String>,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Query {
    pub(crate) kind: QueryKind,
    pub(crate) filter: Vec<Predicate>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SelectExpr {
    Column {
        path: PathExpr,
        output_name: String,
    },
    DateAddDays {
        path: PathExpr,
        days: u64,
        output_name: String,
    },
    NotCastBool {
        path: PathExpr,
        output_name: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Predicate {
    Eq(PathExpr, Value),
    Ne(PathExpr, Value),
    Gt(PathExpr, Value),
    LtEq(PathExpr, Value),
    GtEq(PathExpr, Value),
    InPath(PathExpr, Vec<Value>),
    Contains(Value, PathExpr),
    IsNull(PathExpr),
    IsNotNull(PathExpr),
    IsNotEmpty(PathExpr),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathExpr {
    pub(crate) segments: Vec<PathSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathSegment {
    Key(String),
    Index(usize),
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Row {
    pub(crate) fields: Vec<(String, Value)>,
}

impl Row {
    pub(crate) fn get(&self, name: &str) -> Option<&Value> {
        if let Some(index) = parse_positional_index(name) {
            return self.fields.get(index).map(|(_, value)| value);
        }

        let target = normalize_identifier(name);
        self.fields
            .iter()
            .find(|(column, _)| normalize_identifier(column) == target)
            .map(|(_, value)| value)
    }

    pub(crate) fn resolve_path(&self, path: &PathExpr) -> Vec<Value> {
        let Some((first, rest)) = path.segments.split_first() else {
            return Vec::new();
        };
        let PathSegment::Key(first_key) = first else {
            return Vec::new();
        };
        let Some(root) = self.get(first_key).cloned() else {
            return Vec::new();
        };
        let mut current = vec![root];
        for segment in rest {
            let mut next = Vec::new();
            for value in current {
                match (segment, value) {
                    (PathSegment::Key(key), Value::Object(map)) => {
                        if let Some(value) = map.get(key) {
                            next.push(value.clone());
                        }
                    }
                    (PathSegment::Index(index), Value::Array(items)) => {
                        if let Some(value) = items.get(*index) {
                            next.push(value.clone());
                        }
                    }
                    (PathSegment::Wildcard, Value::Array(items)) => {
                        next.extend(items);
                    }
                    (PathSegment::Wildcard, Value::Object(map)) => {
                        next.extend(map.into_values());
                    }
                    _ => {}
                }
            }
            current = next;
        }
        current
    }
}

pub(crate) fn normalize_output_name(path: &PathExpr) -> String {
    path.segments
        .iter()
        .rev()
        .find_map(|segment| match segment {
            PathSegment::Key(key) => Some(key.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "_1".to_owned())
}

pub(crate) fn unquote_identifier(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
        .unwrap_or(value)
}

fn normalize_identifier(value: &str) -> String {
    unquote_identifier(value.trim()).to_ascii_lowercase()
}

fn parse_positional_index(value: &str) -> Option<usize> {
    let normalized = normalize_identifier(value);
    normalized
        .strip_prefix('_')
        .and_then(|digits| digits.parse::<usize>().ok())
        .and_then(|index| index.checked_sub(1))
}
