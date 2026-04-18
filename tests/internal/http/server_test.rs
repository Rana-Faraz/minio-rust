use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, UdpSocket};
use std::sync::OnceLock;

use minio_rust::internal::http::{self, Server};
use rustls::ServerConfig;

pub const SOURCE_FILE: &str = "internal/http/server_test.go";

fn ensure_rustls_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[test]
fn test_new_server() {
    let Some(non_loopback_ip) = get_non_loopback_ip() else {
        return;
    };

    let test_cases = [
        (vec!["127.0.0.1:9000".to_owned()], false),
        (vec![format!("{non_loopback_ip}:9000")], false),
        (
            vec![
                "127.0.0.1:9000".to_owned(),
                format!("{non_loopback_ip}:9000"),
            ],
            false,
        ),
        (vec!["127.0.0.1:9000".to_owned()], true),
        (vec![format!("{non_loopback_ip}:9000")], true),
        (
            vec![
                "127.0.0.1:9000".to_owned(),
                format!("{non_loopback_ip}:9000"),
            ],
            true,
        ),
    ];

    for (idx, (addrs, use_tls)) in test_cases.into_iter().enumerate() {
        let mut server = Server::new(addrs.clone()).use_handler();
        if use_tls {
            server = server.use_tls_config(test_server_config());
        }

        assert_eq!(server.addrs, addrs, "case {}", idx + 1);
        assert!(server.has_handler(), "case {}", idx + 1);
        assert_eq!(
            server.tls_config.is_some(),
            use_tls,
            "case {} tls mismatch",
            idx + 1
        );
        assert_eq!(
            server.max_header_bytes,
            http::DEFAULT_MAX_HEADER_BYTES,
            "case {}",
            idx + 1
        );
    }
}

fn get_non_loopback_ip() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect((Ipv4Addr::new(8, 8, 8, 8), 80)).ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(ip) if !ip.is_loopback() => Some(ip),
        _ => None,
    }
}

fn test_server_config() -> ServerConfig {
    ensure_rustls_provider();
    let certs = rustls_pemfile::certs(&mut Cursor::new(TEST_CERT_PEM))
        .collect::<Result<Vec<_>, _>>()
        .expect("certificate PEM should parse");
    let mut keys = rustls_pemfile::rsa_private_keys(&mut Cursor::new(TEST_KEY_PEM))
        .collect::<Result<Vec<_>, _>>()
        .expect("private key PEM should parse");
    let private_key = keys
        .drain(..)
        .next()
        .expect("one private key is expected")
        .into();

    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .expect("static keypair should build")
}

const TEST_CERT_PEM: &str = concat!(
    "-----BEGIN CERTIFICATE-----\n",
    "MIIDXTCCAkWgAwIBAgIJAKlqK5HKlo9MMA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNV\n",
    "BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX\n",
    "aWRnaXRzIFB0eSBMdGQwHhcNMTcwNjE5MTA0MzEyWhcNMjcwNjE3MTA0MzEyWjBF\n",
    "MQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50\n",
    "ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB\n",
    "CgKCAQEApEkbPrT6wzcWK1W5atQiGptvuBsRdf8MCg4u6SN10QbslA5k6BYRdZfF\n",
    "eRpwAwYyzkumug6+eBJatDZEd7+0FF86yxB7eMTSiHKRZ5Mi5ZyCFsezdndknGBe\n",
    "K6I80s1jd5ZsLLuMKErvbNwSbfX+X6d2mBeYW8Scv9N+qYnNrHHHohvXoxy1gZ18\n",
    "EhhogQhrD22zaqg/jtmOT8ImUiXzB1mKInt2LlSkoRYuBzepkDJrsE1L/cyYZbtc\n",
    "O/ASDj+/qQAuQ66v9pNyJkIQ7bDOUyxaT5Hx9XvbqI1OqUVAdGLLi+eZIFguFyYd\n",
    "0lemwdN/IDvxftzegTO3cO0D28d1UQIDAQABo1AwTjAdBgNVHQ4EFgQUqMVdMIA1\n",
    "68Dv+iwGugAaEGUSd0IwHwYDVR0jBBgwFoAUqMVdMIA168Dv+iwGugAaEGUSd0Iw\n",
    "DAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAjQVoqRv2HlE5PJIX/qk5\n",
    "oMOKZlHTyJP+s2HzOOVt+eCE/jNdfC7+8R/HcPldQs7p9GqH2F6hQ9aOtDhJVEaU\n",
    "pjxCi4qKeZ1kWwqv8UMBXW92eHGysBvE2Gmm/B1JFl8S2GR5fBmheZVnYW893MoI\n",
    "gp+bOoCcIuMJRqCra4vJgrOsQjgRElQvd2OlP8qQzInf/fRqO/AnZPwMkGr3+KZ0\n",
    "BKEOXtmSZaPs3xEsnvJd8wrTgA0NQK7v48E+gHSXzQtaHmOLqisRXlUOu2r1gNCJ\n",
    "rr3DRiUP6V/10CZ/ImeSJ72k69VuTw9vq2HzB4x6pqxF2X7JQSLUCS2wfNN13N0d\n",
    "9A==\n",
    "-----END CERTIFICATE-----\n"
);

const TEST_KEY_PEM: &str = concat!(
    "-----BEGIN RSA PRIVATE KEY-----\n",
    "MIIEpAIBAAKCAQEApEkbPrT6wzcWK1W5atQiGptvuBsRdf8MCg4u6SN10QbslA5k\n",
    "6BYRdZfFeRpwAwYyzkumug6+eBJatDZEd7+0FF86yxB7eMTSiHKRZ5Mi5ZyCFsez\n",
    "dndknGBeK6I80s1jd5ZsLLuMKErvbNwSbfX+X6d2mBeYW8Scv9N+qYnNrHHHohvX\n",
    "oxy1gZ18EhhogQhrD22zaqg/jtmOT8ImUiXzB1mKInt2LlSkoRYuBzepkDJrsE1L\n",
    "/cyYZbtcO/ASDj+/qQAuQ66v9pNyJkIQ7bDOUyxaT5Hx9XvbqI1OqUVAdGLLi+eZ\n",
    "IFguFyYd0lemwdN/IDvxftzegTO3cO0D28d1UQIDAQABAoIBAB42x8j3lerTNcOQ\n",
    "h4JLM157WcedSs/NsVQkGaKM//0KbfYo04wPivR6jjngj9suh6eDKE2tqoAAuCfO\n",
    "lzcCzca1YOW5yUuDv0iS8YT//XoHF7HC1pGiEaHk40zZEKCgX3u98XUkpPlAFtqJ\n",
    "euY4SKkk7l24cS/ncACjj/b0PhxJoT/CncuaaJKqmCc+vdL4wj1UcrSNPZqRjDR/\n",
    "sh5DO0LblB0XrqVjpNxqxM60/IkbftB8YTnyGgtO2tbTPr8KdQ8DhHQniOp+WEPV\n",
    "u/iXt0LLM7u62LzadkGab2NDWS3agnmdvw2ADtv5Tt8fZ7WnPqiOpNyD5Bv1a3/h\n",
    "YBw5HsUCgYEA0Sfv6BiSAFEby2KusRoq5UeUjp/SfL7vwpO1KvXeiYkPBh2XYVq2\n",
    "azMnOw7Rz5ixFhtUtto2XhYdyvvr3dZu1fNHtxWo9ITBivqTGGRNwfiaQa58Bugo\n",
    "gy7vCdIE/f6xE5LYIovBnES2vs/ZayMyhTX84SCWd0pTY0kdDA8ePGsCgYEAyRSA\n",
    "OTzX43KUR1G/trpuM6VBc0W6YUNYzGRa1TcUxBP4K7DfKMpPGg6ulqypfoHmu8QD\n",
    "L+z+iQmG9ySSuvScIW6u8LgkrTwZga8y2eb/A2FAVYY/bnelef1aMkis+bBX2OQ4\n",
    "QAg2uq+pkhpW1k5NSS9lVCPkj4e5Ur9RCm9fRDMCgYAf3CSIR03eLHy+Y37WzXSh\n",
    "TmELxL6sb+1Xx2Y+cAuBCda3CMTpeIb3F2ivb1d4dvrqsikaXW0Qse/B3tQUC7kA\n",
    "cDmJYwxEiwBsajUD7yuFE5hzzt9nse+R5BFXfp1yD1zr7V9tC7rnUfRAZqrozgjB\n",
    "D/NAW9VvwGupYRbCon7plwKBgQCRPfeoYGRoa9ji8w+Rg3QaZeGyy8jmfGjlqg9a\n",
    "NyEOyIXXuThYFFmyrqw5NZpwQJBTTDApK/xnK7SLS6WY2Rr1oydFxRzo7KJX5B7M\n",
    "+md1H4gCvqeOuWmThgbij1AyQsgRaDehOM2fZ0cKu2/B+Gkm1c9RSWPMsPKR7JMz\n",
    "AGNFtQKBgQCRCFIdGJHnvz35vJfLoihifCejBWtZbAnZoBHpF3xMCtV755J96tUf\n",
    "k1Tv9hz6WfSkOSlwLq6eGZY2dCENJRW1ft1UelpFvCjbfrfLvoFFLs3gu0lfqXHi\n",
    "CS6fjhn9Ahvz10yD6fd4ixRUjoJvULzI0Sxc1O95SYVF1lIAuVr9Hw==\n",
    "-----END RSA PRIVATE KEY-----\n"
);
