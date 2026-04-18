use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct NetworkError {
    pub err: Box<dyn Error + Send + Sync>,
}

impl NetworkError {
    pub fn new(err: impl Error + Send + Sync + 'static) -> Self {
        Self { err: Box::new(err) }
    }
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.err.fmt(f)
    }
}

impl Error for NetworkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.err.as_ref())
    }
}

#[derive(Debug, Clone)]
pub struct TimeoutRestError(pub String);

impl fmt::Display for TimeoutRestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for TimeoutRestError {}

impl TimeoutRestError {
    pub fn is_timeout(&self) -> bool {
        true
    }
}
