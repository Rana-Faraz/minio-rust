use std::error::Error;
use std::fmt;
use std::io;

use minio_rust::internal::rest::{NetworkError, TimeoutRestError};

pub const SOURCE_FILE: &str = "internal/rest/client_test.go";

#[derive(Debug)]
struct UrlLikeError {
    inner: TimeoutRestError,
}

impl fmt::Display for UrlLikeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PUT http://localhost/1234: {}", self.inner)
    }
}

impl Error for UrlLikeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.inner)
    }
}

#[test]
fn network_error_unwrap_matches_reference_behavior() {
    let wrapped = NetworkError::new(UrlLikeError {
        inner: TimeoutRestError("remote server offline".to_owned()),
    });

    let source = wrapped.source().expect("wrapped source");
    assert!(source.is::<UrlLikeError>());
    let nested = source.source().expect("nested source");
    assert!(nested.is::<TimeoutRestError>());

    let non_network = NetworkError::new(io::Error::other("something"));
    let source = non_network.source().expect("wrapped source");
    assert!(source.is::<io::Error>());
    assert!(!source.is::<UrlLikeError>());
}
