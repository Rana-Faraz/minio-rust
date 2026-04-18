use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCompressIncludesError {
    message: &'static str,
}

impl ParseCompressIncludesError {
    fn new(message: &'static str) -> Self {
        Self { message }
    }
}

impl fmt::Display for ParseCompressIncludesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for ParseCompressIncludesError {}

pub fn parse_compress_includes(include: &str) -> Result<Vec<String>, ParseCompressIncludesError> {
    let includes = include.split(',').map(str::to_owned).collect::<Vec<_>>();

    for entry in &includes {
        if entry.is_empty() {
            return Err(ParseCompressIncludesError::new(
                "extension/mime-type cannot be empty",
            ));
        }
        if entry == "/" {
            return Err(ParseCompressIncludesError::new(
                "extension/mime-type cannot be '/'",
            ));
        }
    }

    Ok(includes)
}
