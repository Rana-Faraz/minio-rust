use std::fs;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvKv {
    pub key: String,
    pub value: String,
}

pub fn read_from_secret(path: &str) -> Result<String, String> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
        Err(err) => return Err(err.to_string()),
    };
    Ok(content.trim().to_string())
}

pub fn minio_environ_from_file(path: &str) -> Result<Vec<EnvKv>, String> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.to_string()),
    };
    let mut values = Vec::new();

    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let line = line.strip_prefix("export ").unwrap_or(line).trim();
        let Some((key, value)) = line.split_once('=') else {
            return Err(format!("invalid env assignment: {line}"));
        };

        let value = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        values.push(EnvKv {
            key: key.trim().to_string(),
            value,
        });
    }

    Ok(values)
}
