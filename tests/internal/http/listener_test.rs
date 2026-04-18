use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, UdpSocket};

use minio_rust::internal::http::{self, TCPOptions};

pub const SOURCE_FILE: &str = "internal/http/listener_test.go";

#[test]
fn test_new_http_listener() {
    let free_port = reserve_free_port();
    let mixed_port = reserve_free_port();
    let ipv6_port = reserve_free_port();

    let test_cases = [
        (vec![format!("93.184.216.34:{free_port}")], vec![true]),
        (vec![format!("example.org:{free_port}")], vec![true]),
        (vec!["unknown-host".to_owned()], vec![true]),
        (vec![format!("unknown-host:{free_port}")], vec![true]),
        (vec![format!("localhost:{free_port}")], vec![false]),
        (
            vec![
                format!("localhost:{mixed_port}"),
                format!("93.184.216.34:{mixed_port}"),
            ],
            vec![false, true],
        ),
        (
            vec![
                format!("localhost:{mixed_port}"),
                format!("unknown-host:{mixed_port}"),
            ],
            vec![false, true],
        ),
        (
            vec!["[::1".to_owned(), "unknown-host:-1".to_owned()],
            vec![true, true],
        ),
        (vec!["localhost:0".to_owned()], vec![false]),
        (vec!["localhost:0".to_owned()], vec![false]),
        (
            vec![format!("[::1]:{ipv6_port}"), "127.0.0.1:90900".to_owned()],
            vec![false, true],
        ),
        (
            vec![format!("[::1]:{ipv6_port}"), "localhost:0".to_owned()],
            vec![false, false],
        ),
    ];

    for (idx, (server_addrs, expected_errs)) in test_cases.into_iter().enumerate() {
        let (listener, listen_errs) = http::new_http_listener(&server_addrs, TCPOptions::default());
        assert_eq!(listen_errs.len(), expected_errs.len(), "case {}", idx + 1);
        for (i, expected_err) in expected_errs.into_iter().enumerate() {
            if expected_err {
                assert!(
                    listen_errs[i].is_some(),
                    "case {} listener {} should fail",
                    idx + 1,
                    i
                );
            } else {
                assert!(
                    listen_errs[i].is_none(),
                    "case {} listener {} should succeed: {:?}",
                    idx + 1,
                    i,
                    listen_errs[i]
                );
            }
        }
        if let Some(listener) = listener {
            listener.close().expect("listener close must succeed");
        }
    }
}

#[test]
fn test_http_listener_start_close() {
    let Some(non_loopback_ip) = get_non_loopback_ip() else {
        return;
    };

    let test_cases = [
        vec!["localhost:0".to_owned()],
        vec![format!("{non_loopback_ip}:0")],
        vec!["127.0.0.1:0".to_owned(), format!("{non_loopback_ip}:0")],
    ];

    for (idx, server_addrs) in test_cases.into_iter().enumerate() {
        let (listener, errs) = http::new_http_listener(&server_addrs, TCPOptions::default());
        for err in errs.into_iter().flatten() {
            if is_unbindable(&err) || is_address_in_use(&err) {
                continue;
            }
            panic!("case {} unexpected listener error: {err}", idx + 1);
        }

        let listener = match listener {
            Some(listener) => listener,
            None => continue,
        };

        for server_addr in listener.addrs() {
            let conn = std::net::TcpStream::connect(server_addr)
                .unwrap_or_else(|err| panic!("case {} dial failed: {err}", idx + 1));
            drop(conn);
        }

        listener.close().expect("listener close must succeed");
    }
}

#[test]
fn test_http_listener_addr() {
    let Some(non_loopback_ip) = get_non_loopback_ip() else {
        return;
    };

    let case_ports = vec![
        reserve_free_port(),
        reserve_free_port(),
        reserve_free_port(),
        reserve_free_port(),
        reserve_free_port(),
        reserve_free_port(),
    ];

    let test_cases = [
        (
            vec![format!("localhost:{}", case_ports[0])],
            format!("127.0.0.1:{}", case_ports[0]),
        ),
        (
            vec![format!("{non_loopback_ip}:{}", case_ports[1])],
            format!("{non_loopback_ip}:{}", case_ports[1]),
        ),
        (
            vec![
                format!("127.0.0.1:{}", case_ports[2]),
                format!("{non_loopback_ip}:{}", case_ports[2]),
            ],
            format!("0.0.0.0:{}", case_ports[2]),
        ),
    ];

    for (idx, (server_addrs, expected_addr)) in test_cases.into_iter().enumerate() {
        let (listener, errs) = http::new_http_listener(&server_addrs, TCPOptions::default());
        for err in errs.into_iter().flatten() {
            if is_unbindable(&err) || is_address_in_use(&err) {
                continue;
            }
            panic!("case {} unexpected listener error: {err}", idx + 1);
        }
        let listener = match listener {
            Some(listener) => listener,
            None => continue,
        };
        assert_eq!(
            listener.addr().to_string(),
            expected_addr,
            "case {}",
            idx + 1
        );
        listener.close().expect("listener close must succeed");
    }
}

#[test]
fn test_http_listener_addrs() {
    let Some(non_loopback_ip) = get_non_loopback_ip() else {
        return;
    };

    let case_ports = vec![
        reserve_free_port(),
        reserve_free_port(),
        reserve_free_port(),
        reserve_free_port(),
        reserve_free_port(),
        reserve_free_port(),
    ];

    let test_cases = [
        (
            vec![format!("localhost:{}", case_ports[0])],
            HashSet::from([format!("127.0.0.1:{}", case_ports[0])]),
        ),
        (
            vec![format!("{non_loopback_ip}:{}", case_ports[1])],
            HashSet::from([format!("{non_loopback_ip}:{}", case_ports[1])]),
        ),
        (
            vec![
                format!("127.0.0.1:{}", case_ports[2]),
                format!("{non_loopback_ip}:{}", case_ports[2]),
            ],
            HashSet::from([
                format!("127.0.0.1:{}", case_ports[2]),
                format!("{non_loopback_ip}:{}", case_ports[2]),
            ]),
        ),
    ];

    for (idx, (server_addrs, expected_addrs)) in test_cases.into_iter().enumerate() {
        let (listener, errs) = http::new_http_listener(&server_addrs, TCPOptions::default());
        for err in errs.into_iter().flatten() {
            if is_unbindable(&err) || is_address_in_use(&err) {
                continue;
            }
            panic!("case {} unexpected listener error: {err}", idx + 1);
        }
        let listener = match listener {
            Some(listener) => listener,
            None => continue,
        };

        let addrs = listener
            .addrs()
            .into_iter()
            .map(|addr| addr.to_string())
            .collect::<HashSet<_>>();
        assert_eq!(addrs, expected_addrs, "case {}", idx + 1);
        listener.close().expect("listener close must succeed");
    }
}

fn reserve_free_port() -> u16 {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .expect("must bind localhost");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

fn get_non_loopback_ip() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect((Ipv4Addr::new(8, 8, 8, 8), 80)).ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(ip) if !ip.is_loopback() => Some(ip),
        _ => None,
    }
}

fn is_unbindable(err: &std::io::Error) -> bool {
    err.to_string().contains("requested address")
}

fn is_address_in_use(err: &std::io::Error) -> bool {
    err.to_string().contains("address already in use")
}
