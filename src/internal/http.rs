use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use rustls::ServerConfig;

pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_HEADER_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Default)]
pub struct TCPOptions {
    pub user_timeout: i32,
    pub send_buf_size: i32,
    pub recv_buf_size: i32,
    pub no_delay: bool,
    pub interface: String,
    pub idle_timeout: Duration,
}

pub fn check_port_availability(host: &str, port: &str, _opts: TCPOptions) -> io::Result<()> {
    let bind_addr = if host.is_empty() {
        format!(":{port}")
    } else {
        format!("{host}:{port}")
    };

    let listener = TcpListener::bind(bind_addr)?;
    drop(listener);
    Ok(())
}

pub struct HttpListener {
    listeners: Vec<TcpListener>,
    pub opts: TCPOptions,
}

impl fmt::Debug for HttpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpListener")
            .field("listeners", &self.listeners.len())
            .field("opts", &self.opts)
            .finish()
    }
}

impl HttpListener {
    pub fn addr(&self) -> SocketAddr {
        let first = self.listeners[0]
            .local_addr()
            .expect("listener must have a local addr");
        if self.listeners.len() == 1 {
            return first;
        }

        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), first.port())
    }

    pub fn addrs(&self) -> Vec<SocketAddr> {
        self.listeners
            .iter()
            .map(|listener| {
                listener
                    .local_addr()
                    .expect("listener must have local addr")
            })
            .collect()
    }

    pub fn close(self) -> io::Result<()> {
        drop(self);
        Ok(())
    }
}

pub fn new_http_listener(
    server_addrs: &[impl AsRef<str>],
    opts: TCPOptions,
) -> (Option<HttpListener>, Vec<Option<io::Error>>) {
    let mut listeners = Vec::with_capacity(server_addrs.len());
    let mut errors = Vec::with_capacity(server_addrs.len());

    for server_addr in server_addrs {
        match bind_listener(server_addr.as_ref()) {
            Ok(listener) => {
                listeners.push(listener);
                errors.push(None);
            }
            Err(err) => errors.push(Some(err)),
        }
    }

    if listeners.is_empty() {
        return (None, errors);
    }

    (Some(HttpListener { listeners, opts }), errors)
}

#[derive(Clone, Default)]
pub struct Server {
    pub addrs: Vec<String>,
    pub tcp_options: TCPOptions,
    pub tls_config: Option<Arc<ServerConfig>>,
    pub max_header_bytes: usize,
    pub idle_timeout: Duration,
    pub read_timeout: Duration,
    pub read_header_timeout: Duration,
    pub write_timeout: Duration,
    handler_set: bool,
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server")
            .field("addrs", &self.addrs)
            .field("tcp_options", &self.tcp_options)
            .field(
                "tls_config",
                &self.tls_config.as_ref().map(|_| "configured"),
            )
            .field("max_header_bytes", &self.max_header_bytes)
            .finish()
    }
}

impl Server {
    pub fn new(addrs: Vec<String>) -> Self {
        Self {
            addrs,
            tcp_options: TCPOptions::default(),
            tls_config: None,
            max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            read_timeout: Duration::default(),
            read_header_timeout: DEFAULT_READ_HEADER_TIMEOUT,
            write_timeout: Duration::default(),
            handler_set: false,
        }
    }

    pub fn use_handler(mut self) -> Self {
        self.handler_set = true;
        self
    }

    pub fn use_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn use_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    pub fn use_read_header_timeout(mut self, timeout: Duration) -> Self {
        self.read_header_timeout = timeout;
        self
    }

    pub fn use_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    pub fn use_tcp_options(mut self, opts: TCPOptions) -> Self {
        self.tcp_options = opts;
        self
    }

    pub fn use_tls_config(mut self, cfg: ServerConfig) -> Self {
        self.tls_config = Some(Arc::new(cfg));
        self
    }

    pub fn has_handler(&self) -> bool {
        self.handler_set
    }
}

fn bind_listener(server_addr: &str) -> io::Result<TcpListener> {
    let (host, port) = split_host_port(server_addr)?;
    let bind_host = if host.eq_ignore_ascii_case("localhost") {
        "127.0.0.1".to_owned()
    } else {
        host
    };

    let socket_addrs = (bind_host.as_str(), port).to_socket_addrs()?;
    let socket_addr = socket_addrs.into_iter().next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("no socket addresses resolved for {server_addr}"),
        )
    })?;

    TcpListener::bind(socket_addr)
}

fn split_host_port(server_addr: &str) -> io::Result<(String, u16)> {
    if server_addr.starts_with('[') {
        let addr: SocketAddr = server_addr.parse().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid socket address {server_addr}: {err}"),
            )
        })?;
        return Ok((addr.ip().to_string(), addr.port()));
    }

    let (host, port) = server_addr.rsplit_once(':').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing port in address {server_addr}"),
        )
    })?;

    let port = port.parse::<u16>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid port in {server_addr}: {err}"),
        )
    })?;

    Ok((host.to_owned(), port))
}
