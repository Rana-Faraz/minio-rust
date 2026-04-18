use std::thread;
use std::time::Duration;

use minio_rust::internal::grid::Connection;

pub const SOURCE_FILE: &str = "internal/grid/connection_test.go";

#[test]
fn test_disconnect_line_31() {
    let conn = Connection::new("local", "remote");
    assert!(conn.wait_for_connect(Duration::from_millis(20)));

    let request = conn.request();
    let request_wait = thread::spawn(move || request.wait());
    thread::sleep(Duration::from_millis(10));
    conn.disconnect();
    let request_err = request_wait.join().expect("request join");
    assert_eq!(request_err, Err("remote disconnected".to_owned()));
    assert!(!conn.is_connected());

    conn.reconnect();
    assert!(conn.wait_for_connect(Duration::from_millis(20)));

    let stream = conn.new_stream();
    let stream_wait = thread::spawn(move || stream.wait());
    thread::sleep(Duration::from_millis(10));
    conn.disconnect();
    let stream_err = stream_wait.join().expect("stream join");
    assert_eq!(stream_err, Err("remote disconnected".to_owned()));

    conn.reconnect();
    assert!(conn.wait_for_connect(Duration::from_millis(20)));
}

#[test]
fn test_should_connect_line_172() {
    let hosts = [
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
        "s", "t", "u", "v", "x", "y", "z", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    ];

    for (x, local) in hosts.iter().enumerate() {
        let mut should = 0;
        for (y, remote) in hosts.iter().enumerate() {
            if x == y {
                continue;
            }
            let forward = Connection::new(*local, *remote);
            let reverse = Connection::new(*remote, *local);
            assert_ne!(
                forward.should_connect(),
                reverse.should_connect(),
                "should_connect({local:?}, {remote:?}) should invert for the reverse pair"
            );
            if forward.should_connect() {
                should += 1;
            }
        }
        assert!(
            should >= 10,
            "host {local:?} should proactively connect to a meaningful subset, got {should}"
        );
    }
}
