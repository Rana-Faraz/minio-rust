use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    MalformedEscapeSequence,
    InvalidSubstringIndexLen,
    TimestampParse(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedEscapeSequence => {
                f.write_str("Malformed escape sequence in LIKE clause")
            }
            Self::InvalidSubstringIndexLen => {
                f.write_str("Substring start index or length falls outside the string")
            }
            Self::TimestampParse(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonPathError {
    KeyLookup,
    IndexLookup,
    WildcardObjectLookup,
    WildcardArrayLookup,
    WildcardObjectUsageInvalid,
}

impl fmt::Display for JsonPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyLookup => f.write_str("Cannot look up key in non-object value"),
            Self::IndexLookup => f.write_str("Cannot look up array index in non-array value"),
            Self::WildcardObjectLookup => f.write_str("Object wildcard used on non-object value"),
            Self::WildcardArrayLookup => f.write_str("Array wildcard used on non-array value"),
            Self::WildcardObjectUsageInvalid => f.write_str("Invalid usage of object wildcard"),
        }
    }
}

impl std::error::Error for JsonPathError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserError {
    InvalidIdentifier,
    InvalidLiteralString,
    InvalidJsonPathElement,
    InvalidJsonPath,
    UnsupportedFunction,
    InvalidFunction,
    InvalidSelect,
    InvalidLimit,
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifier => f.write_str("invalid identifier"),
            Self::InvalidLiteralString => f.write_str("invalid literal string"),
            Self::InvalidJsonPathElement => f.write_str("invalid JSON path element"),
            Self::InvalidJsonPath => f.write_str("invalid JSON path"),
            Self::UnsupportedFunction => f.write_str("unsupported function"),
            Self::InvalidFunction => f.write_str("invalid function expression"),
            Self::InvalidSelect => f.write_str("invalid select statement"),
            Self::InvalidLimit => f.write_str("invalid limit"),
        }
    }
}

impl std::error::Error for ParserError {}
