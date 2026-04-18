use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime};

use minio_rust::internal::deadlineconn::{DeadlineConn, UPDATE_INTERVAL};

pub const SOURCE_FILE: &str = "internal/deadlineconn/deadlineconn_test.go";

#[test]
fn buff_conn_read_timeout_matches_reference_behavior() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let addr = listener.local_addr().expect("local addr");
    let (tx, rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept");
        let mut conn = DeadlineConn::new(stream)
            .with_read_deadline(Duration::from_secs(1))
            .with_write_deadline(Duration::from_secs(1));

        let mut buffer = [0_u8; 12];
        conn.read_exact(&mut buffer).expect("first read");
        assert_eq!(&buffer, b"message one\n");

        thread::sleep(Duration::from_secs(3));

        conn.read_exact(&mut buffer).expect("second read");
        assert_eq!(&buffer, b"message two\n");

        conn.write_all(b"messages received\n").expect("response");
        tx.send(()).expect("signal");
    });

    let mut client = TcpStream::connect(addr).expect("connect");
    client.write_all(b"message one\n").expect("write one");
    client.write_all(b"message two\n").expect("write two");

    let mut line = String::new();
    BufReader::new(client)
        .read_line(&mut line)
        .expect("read response");
    assert_eq!(line, "messages received\n");

    rx.recv_timeout(Duration::from_secs(1))
        .expect("server done");
    server.join().expect("server join");
}

#[test]
fn buff_conn_read_check_timeout_matches_reference_behavior() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let addr = listener.local_addr().expect("local addr");
    let (tx, rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept");
        let mut conn = DeadlineConn::new(stream)
            .with_read_deadline(Duration::from_secs(1))
            .with_write_deadline(Duration::from_secs(1));

        let mut buffer = [0_u8; 12];
        conn.read_exact(&mut buffer).expect("first read");
        assert_eq!(&buffer, b"message one\n");

        conn.set_read_deadline(Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)))
            .expect("set past deadline");
        thread::sleep(UPDATE_INTERVAL * 2);

        let err = conn.read(&mut buffer).expect_err("read should fail");
        tx.send(err.kind()).expect("send error kind");
    });

    let mut client = TcpStream::connect(addr).expect("connect");
    client.write_all(b"message one\n").expect("write one");
    client.write_all(b"message two\n").expect("write two");

    let error_kind = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("server error");
    assert_eq!(error_kind, std::io::ErrorKind::TimedOut);
    server.join().expect("server join");
}
