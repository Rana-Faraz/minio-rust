use regex::Regex;
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader};
use std::net::TcpStream;
use std::sync::{Arc, OnceLock};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    ClientConfig, ClientConnection, DigitallySignedStruct, RootCertStore, SignatureScheme,
    StreamOwned,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Message(String),
    InvalidPostgresqlTable,
}

impl Error {
    fn message(value: impl Into<String>) -> Self {
        Self::Message(value.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(value) => f.write_str(value),
            Self::InvalidPostgresqlTable => f.write_str("invalid PostgreSQL table"),
        }
    }
}

impl std::error::Error for Error {}

pub fn registered_drivers() -> &'static [&'static str] {
    &["mysql", "postgres"]
}

pub fn is_driver_registered(driver: &str) -> bool {
    registered_drivers().contains(&driver)
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Host {
    pub name: String,
    pub port: u16,
    pub is_port_set: bool,
}

impl Host {
    pub fn new(name: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            port,
            is_port_set: true,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.name.is_empty() || !self.is_port_set
    }

    fn address(&self) -> String {
        format!("{}:{}", self.name, self.port)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NsqTlsArgs {
    pub enable: bool,
    pub skip_verify: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NsqArgs {
    pub enable: bool,
    pub nsqd_address: Host,
    pub topic: String,
    pub tls: NsqTlsArgs,
    pub queue_dir: String,
}

pub enum NsqConnection {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl fmt::Debug for NsqConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("NsqConnection::Plain(..)"),
            Self::Tls(_) => f.write_str("NsqConnection::Tls(..)"),
        }
    }
}

impl NsqConnection {
    pub fn close(self) {}
}

impl NsqArgs {
    pub fn validate(&self) -> Result<(), Error> {
        if !self.enable {
            return Ok(());
        }
        if self.nsqd_address.is_empty() {
            return Err(Error::message("empty nsqdAddress"));
        }
        if self.topic.is_empty() {
            return Err(Error::message("empty topic"));
        }
        if !self.queue_dir.is_empty() && !self.queue_dir.starts_with('/') {
            return Err(Error::message("queueDir path should be absolute"));
        }
        Ok(())
    }

    pub fn connect_nsq(&self) -> Result<NsqConnection, Error> {
        self.validate()?;
        let stream = TcpStream::connect(self.nsqd_address.address()).map_err(io_error)?;
        if !self.tls.enable {
            return Ok(NsqConnection::Plain(stream));
        }

        let client_config = self.client_config()?;
        let server_name = ServerName::try_from(self.nsqd_address.name.clone())
            .map_err(|error| Error::message(error.to_string()))?;
        let connection = ClientConnection::new(client_config, server_name)
            .map_err(|error| Error::message(error.to_string()))?;
        let mut tls = StreamOwned::new(connection, stream);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).map_err(io_error)?;
        }
        Ok(NsqConnection::Tls(Box::new(tls)))
    }

    fn client_config(&self) -> Result<Arc<ClientConfig>, Error> {
        ensure_rustls_provider();
        let builder = if self.tls.skip_verify {
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoCertificateVerification::new()))
        } else {
            let roots = RootCertStore::empty();
            ClientConfig::builder().with_root_certificates(roots)
        };
        Ok(Arc::new(builder.with_no_client_auth()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NatsArgs {
    pub enable: bool,
    pub address: Host,
    pub subject: String,
    pub username: String,
    pub password: String,
    pub token: String,
    pub nkey_seed: String,
    pub secure: bool,
    pub tls_skip_verify: bool,
    pub cert_authority: String,
    pub client_cert: String,
    pub client_key: String,
    pub tls_handshake_first: bool,
}

pub enum NatsConnection {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl fmt::Debug for NatsConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("NatsConnection::Plain(..)"),
            Self::Tls(_) => f.write_str("NatsConnection::Tls(..)"),
        }
    }
}

impl NatsConnection {
    pub fn close(self) {}
}

impl NatsArgs {
    pub fn connect_nats(&self) -> Result<NatsConnection, Error> {
        if self.address.is_empty() {
            return Err(Error::message("empty address"));
        }
        if self.subject.is_empty() {
            return Err(Error::message("empty subject"));
        }
        if !self.nkey_seed.is_empty() {
            std::fs::read_to_string(&self.nkey_seed).map_err(io_error)?;
        }

        let stream = TcpStream::connect(self.address.address()).map_err(io_error)?;
        if !self.secure {
            return Ok(NatsConnection::Plain(stream));
        }

        let client_config = self.client_config()?;
        let server_name = ServerName::try_from(self.address.name.clone())
            .map_err(|error| Error::message(error.to_string()))?;
        let connection = ClientConnection::new(client_config, server_name)
            .map_err(|error| Error::message(error.to_string()))?;
        let mut tls = StreamOwned::new(connection, stream);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).map_err(io_error)?;
        }
        Ok(NatsConnection::Tls(Box::new(tls)))
    }

    fn client_config(&self) -> Result<Arc<ClientConfig>, Error> {
        ensure_rustls_provider();
        let config = if self.tls_skip_verify {
            let builder = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoCertificateVerification::new()));
            if !self.client_cert.is_empty() || !self.client_key.is_empty() {
                if self.client_cert.is_empty() || self.client_key.is_empty() {
                    return Err(Error::message("cert and key must be specified as a pair"));
                }
                let certificates = load_certificates(&self.client_cert)?;
                let key = load_private_key(&self.client_key)?;
                builder
                    .with_client_auth_cert(certificates, key)
                    .map_err(|error| Error::message(error.to_string()))?
            } else {
                builder.with_no_client_auth()
            }
        } else {
            let mut roots = RootCertStore::empty();
            if !self.cert_authority.is_empty() {
                for certificate in load_certificates(&self.cert_authority)? {
                    roots
                        .add(certificate)
                        .map_err(|error| Error::message(error.to_string()))?;
                }
            }

            let builder = ClientConfig::builder().with_root_certificates(roots);
            if self.client_cert.is_empty() || self.client_key.is_empty() {
                if self.client_cert.is_empty() && self.client_key.is_empty() {
                    builder.with_no_client_auth()
                } else {
                    return Err(Error::message("cert and key must be specified as a pair"));
                }
            } else {
                let certificates = load_certificates(&self.client_cert)?;
                let key = load_private_key(&self.client_key)?;
                builder
                    .with_client_auth_cert(certificates, key)
                    .map_err(|error| Error::message(error.to_string()))?
            }
        };

        Ok(Arc::new(config))
    }
}

pub fn validate_psql_table_name(name: &str) -> Result<(), Error> {
    let quoted = Regex::new(r#"^"[^"]+"$"#).expect("quoted regex is valid");
    if quoted.is_match(name) {
        return Ok(());
    }

    let mut valid = true;
    let cleaned: String = name
        .chars()
        .filter_map(|ch| {
            if ch.is_alphabetic() {
                Some('a')
            } else if ch.is_numeric() {
                Some('0')
            } else if ch == '_' || ch == '$' {
                Some(ch)
            } else {
                valid = false;
                None
            }
        })
        .collect();

    if valid {
        let simple = Regex::new(r"^[a_][a0_$]*$").expect("simple table regex is valid");
        if simple.is_match(&cleaned) {
            return Ok(());
        }
    }

    Err(Error::InvalidPostgresqlTable)
}

fn load_certificates(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, Error> {
    let file = File::open(path).map_err(io_error)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Error::message(error.to_string()))
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, Error> {
    let file = File::open(path).map_err(io_error)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|error| Error::message(error.to_string()))?
        .ok_or_else(|| Error::message("missing private key"))
}

fn io_error(error: io::Error) -> Error {
    Error::message(error.to_string())
}

fn ensure_rustls_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[derive(Debug)]
struct NoCertificateVerification {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl NoCertificateVerification {
    fn new() -> Self {
        Self {
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        }
    }
}

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
