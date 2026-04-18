use std::net::TcpListener;

use minio_rust::internal::http::{check_port_availability, TCPOptions};

pub const SOURCE_FILE: &str = "internal/http/check_port_test.go";

#[test]
fn check_port_availability_matches_reference_behavior() {
    if !cfg!(target_os = "linux") {
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let port = listener.local_addr().expect("addr").port();

    let empty_host = check_port_availability("", &port.to_string(), TCPOptions::default())
        .expect_err("bound port should fail");
    assert!(empty_host.to_string().contains("address already in use"));

    let localhost = check_port_availability("127.0.0.1", &port.to_string(), TCPOptions::default())
        .expect_err("bound host port should fail");
    assert!(localhost.to_string().contains("address already in use"));
}
