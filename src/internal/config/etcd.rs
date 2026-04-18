use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseEndpointsError(String);

impl ParseEndpointsError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ParseEndpointsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseEndpointsError {}

pub fn parse_endpoints(endpoints: &str) -> Result<(Vec<String>, bool), ParseEndpointsError> {
    let endpoints = endpoints.split(',').collect::<Vec<_>>();

    let mut etcd_secure = false;
    for endpoint in &endpoints {
        let scheme_end = endpoint
            .find("://")
            .ok_or_else(|| ParseEndpointsError::new(format!("invalid endpoint: {endpoint}")))?;
        let scheme = &endpoint[..scheme_end];
        let host_port = &endpoint[scheme_end + 3..];

        if scheme != "http" && scheme != "https" {
            return Err(ParseEndpointsError::new(format!(
                "invalid endpoint: {endpoint}"
            )));
        }
        if host_port.is_empty() || host_port.contains('/') {
            return Err(ParseEndpointsError::new(format!(
                "invalid endpoint: {endpoint}"
            )));
        }

        let Some((host, port)) = split_host_port(host_port) else {
            return Err(ParseEndpointsError::new(format!(
                "invalid endpoint: {endpoint}"
            )));
        };
        if host.is_empty() || port.parse::<u16>().is_err() {
            return Err(ParseEndpointsError::new(format!(
                "invalid endpoint: {endpoint}"
            )));
        }

        if etcd_secure && scheme == "http" {
            return Err(ParseEndpointsError::new(format!(
                "all endpoints should be https or http: {endpoint}"
            )));
        }
        etcd_secure |= scheme == "https";
    }

    Ok((
        endpoints.into_iter().map(str::to_owned).collect(),
        etcd_secure,
    ))
}

fn split_host_port(host_port: &str) -> Option<(&str, &str)> {
    if host_port.starts_with('[') {
        let end = host_port.find(']')?;
        let host = &host_port[..=end];
        let remainder = &host_port[end + 1..];
        let port = remainder.strip_prefix(':')?;
        return Some((host, port));
    }

    let colon = host_port.rfind(':')?;
    let host = &host_port[..colon];
    let port = &host_port[colon + 1..];
    Some((host, port))
}
