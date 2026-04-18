use minio_rust::cmd::{
    print_cli_access_msg, print_server_common_message, print_startup_message, strip_standard_ports,
};

pub const SOURCE_FILE: &str = "cmd/server-startup-msg_test.go";

#[test]
fn test_strip_standard_ports_line_28() {
    assert_eq!(
        strip_standard_ports("http://127.0.0.1:80"),
        "http://127.0.0.1"
    );
    assert_eq!(
        strip_standard_ports("https://play.min.io:443"),
        "https://play.min.io"
    );
    assert_eq!(
        strip_standard_ports("http://127.0.0.1:9000"),
        "http://127.0.0.1:9000"
    );
}

#[test]
fn test_print_server_common_message_line_51() {
    let msg = print_server_common_message(
        &[String::from("http://127.0.0.1:9000")],
        &[String::from("http://127.0.0.1:9001")],
    );
    assert!(msg.contains("API: http://127.0.0.1:9000"));
    assert!(msg.contains("Console: http://127.0.0.1:9001"));
}

#[test]
fn test_print_cliaccess_msg_line_69() {
    let msg = print_cli_access_msg("local", "http://127.0.0.1:9000", "minio", "minio123");
    assert_eq!(
        msg,
        "mc alias set local http://127.0.0.1:9000 minio minio123"
    );
}

#[test]
fn test_print_startup_message_line_87() {
    let msg = print_startup_message(
        &[String::from("http://127.0.0.1:9000")],
        &[String::from("http://127.0.0.1:9001")],
        "local",
        "minio",
        "minio123",
    );
    assert!(msg.contains("API: http://127.0.0.1:9000"));
    assert!(msg.contains("Console: http://127.0.0.1:9001"));
    assert!(msg.contains("mc alias set local http://127.0.0.1:9000 minio minio123"));
}
