use std::collections::BTreeMap;

pub const HTTP_SCHEME: &str = "http";
pub const HTTPS_SCHEME: &str = "https";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LocationRequest {
    pub host: String,
    pub headers: BTreeMap<String, String>,
    pub path: String,
}

pub fn get_url_scheme(tls: bool) -> &'static str {
    if tls {
        HTTPS_SCHEME
    } else {
        HTTP_SCHEME
    }
}

fn clean_join(bucket: &str, object: &str) -> String {
    let mut out = String::from("/");
    out.push_str(bucket.trim_matches('/'));
    if !object.is_empty() {
        out.push('/');
        out.push_str(object.trim_matches('/'));
    }
    out
}

pub fn get_object_location(
    request: &LocationRequest,
    domains: &[String],
    bucket: &str,
    object: &str,
) -> String {
    if request.host.is_empty() {
        return clean_join(bucket, object);
    }

    let proto = request
        .headers
        .get("X-Forwarded-Scheme")
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(HTTP_SCHEME);

    let mut path = clean_join(bucket, object);
    for domain in domains {
        if request.host.starts_with(&format!("{bucket}.{domain}")) {
            path = format!("/{}", object.trim_matches('/'));
            break;
        }
    }

    format!("{proto}://{}{}", request.host, path)
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseRecorder {
    pub code: i32,
    pub body: Vec<u8>,
    pub headers: BTreeMap<String, String>,
}

impl ResponseRecorder {
    pub fn new() -> Self {
        Self {
            code: 200,
            body: Vec::new(),
            headers: BTreeMap::new(),
        }
    }
}

pub trait HeaderWriter {
    fn write_header(&mut self, status: u16);
    fn write_body(&mut self, body: &[u8]) -> Result<usize, String>;
    fn header_written(&self) -> bool;
    fn unwrap_ref(&self) -> Option<&dyn HeaderWriter> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TrackingResponseWriter {
    pub response: ResponseRecorder,
    pub header_written: bool,
}

impl TrackingResponseWriter {
    pub fn unwrap(&self) -> &ResponseRecorder {
        &self.response
    }
}

impl HeaderWriter for TrackingResponseWriter {
    fn write_header(&mut self, status: u16) {
        self.header_written = true;
        self.response.code = status as i32;
    }

    fn write_body(&mut self, body: &[u8]) -> Result<usize, String> {
        self.response.body.extend_from_slice(body);
        Ok(body.len())
    }

    fn header_written(&self) -> bool {
        self.header_written
    }
}

pub struct WrappedWriter<T: HeaderWriter> {
    pub inner: T,
}

impl<T: HeaderWriter> HeaderWriter for WrappedWriter<T> {
    fn write_header(&mut self, status: u16) {
        self.inner.write_header(status);
    }

    fn write_body(&mut self, body: &[u8]) -> Result<usize, String> {
        self.inner.write_body(body)
    }

    fn header_written(&self) -> bool {
        self.inner.header_written()
    }

    fn unwrap_ref(&self) -> Option<&dyn HeaderWriter> {
        Some(&self.inner)
    }
}

pub fn headers_already_written(writer: &dyn HeaderWriter) -> bool {
    if writer.header_written() {
        return true;
    }
    let mut current = writer;
    while let Some(inner) = current.unwrap_ref() {
        if inner.header_written() {
            return true;
        }
        current = inner;
    }
    false
}

pub fn write_response(writer: &mut dyn HeaderWriter, status: u16, body: &[u8], content_type: &str) {
    if headers_already_written(writer) {
        return;
    }
    writer.write_header(status);
    let _ = writer.write_body(body);
    let _ = content_type;
}
