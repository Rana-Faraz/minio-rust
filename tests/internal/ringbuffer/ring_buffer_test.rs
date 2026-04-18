use std::io::{Read, Write};
use std::sync::Arc;
use std::thread;

use minio_rust::internal::ringbuffer::{Error, RingBuffer};

pub const SOURCE_FILE: &str = "internal/ringbuffer/ring_buffer_test.go";

fn assert_reader<T: Read>() {}
fn assert_writer<T: Write>() {}

#[test]
fn ring_buffer_interface_matches_reference_case() {
    assert_reader::<RingBuffer>();
    assert_writer::<RingBuffer>();
}

#[test]
fn ring_buffer_write_matches_reference_case() {
    let rb = RingBuffer::new(64);
    assert!(rb.is_empty());
    assert!(!rb.is_full());
    assert_eq!(rb.length(), 0);
    assert_eq!(rb.free(), 64);

    assert_eq!(rb.write(&vec![b'a'; 16]).expect("write should succeed"), 16);
    assert_eq!(rb.length(), 16);
    assert_eq!(rb.free(), 48);
    assert_eq!(RingBuffer::bytes(&rb), vec![b'a'; 16]);

    assert_eq!(rb.write(&vec![b'b'; 48]).expect("write should succeed"), 48);
    assert!(rb.is_full());
    assert_eq!(rb.length(), 64);
    assert_eq!(rb.free(), 0);

    assert_eq!(rb.write(b"zz"), Err(Error::IsFull));

    rb.reset();
    assert!(matches!(
        rb.write(&vec![b'c'; 80]),
        Err(Error::TooMuchDataToWrite)
    ));
    assert!(rb.is_full());
    assert_eq!(rb.length(), 64);
}

#[test]
fn ring_buffer_write_blocking_matches_reference_case() {
    let rb = Arc::new(RingBuffer::new(8).set_blocking(true));
    rb.write(b"abcdefgh").expect("initial write should succeed");

    let writer = {
        let rb = Arc::clone(&rb);
        thread::spawn(move || {
            rb.write(b"ijkl")
                .expect("blocking write should eventually succeed")
        })
    };

    let mut first = [0u8; 4];
    assert_eq!(rb.read(&mut first).expect("read should succeed"), 4);
    assert_eq!(&first, b"abcd");
    assert_eq!(writer.join().expect("writer thread should join"), 4);

    let mut rest = vec![0u8; 8];
    assert_eq!(rb.read(&mut rest).expect("read should succeed"), 8);
    assert_eq!(&rest, b"efghijkl");
}

#[test]
fn ring_buffer_read_matches_reference_case() {
    let rb = RingBuffer::new(64);
    let mut buf = vec![0u8; 128];
    assert_eq!(rb.read(&mut buf), Err(Error::IsEmpty));

    rb.write(b"abcdabcdabcdabcd").expect("write should succeed");
    let n = rb.read(&mut buf).expect("read should succeed");
    assert_eq!(n, 16);
    assert_eq!(&buf[..n], b"abcdabcdabcdabcd");
    assert!(rb.is_empty());
}

#[test]
fn ring_buffer_blocking_matches_reference_case() {
    let rb = Arc::new(RingBuffer::new(256).set_blocking(true));
    let payload: Vec<u8> = (0..5000).map(|i| (i % 251) as u8).collect();

    let writer = {
        let rb = Arc::clone(&rb);
        let payload = payload.clone();
        thread::spawn(move || {
            for chunk in payload.chunks(37) {
                rb.write(chunk).expect("blocking write should succeed");
            }
            rb.close_writer();
        })
    };

    let reader = {
        let rb = Arc::clone(&rb);
        thread::spawn(move || {
            let mut out = Vec::new();
            let mut buf = [0u8; 41];
            loop {
                match rb.read(&mut buf) {
                    Ok(n) => out.extend_from_slice(&buf[..n]),
                    Err(Error::Eof) => break out,
                    Err(err) => panic!("unexpected read error: {err}"),
                }
            }
        })
    };

    writer.join().expect("writer should join");
    let received = reader.join().expect("reader should join");
    assert_eq!(received, payload);
}

#[test]
fn ring_buffer_blocking_big_matches_reference_case() {
    let rb = Arc::new(RingBuffer::new(1024).set_blocking(true));
    let payload: Vec<u8> = (0..100_000).map(|i| (i % 199) as u8).collect();

    let writer = {
        let rb = Arc::clone(&rb);
        let payload = payload.clone();
        thread::spawn(move || {
            for chunk in payload.chunks(4096) {
                rb.write(chunk).expect("blocking write should succeed");
            }
            rb.close_writer();
        })
    };

    let reader = {
        let rb = Arc::clone(&rb);
        thread::spawn(move || {
            let mut out = Vec::new();
            let mut buf = vec![0u8; 2048];
            loop {
                match rb.read(&mut buf) {
                    Ok(n) => out.extend_from_slice(&buf[..n]),
                    Err(Error::Eof) => break out,
                    Err(err) => panic!("unexpected read error: {err}"),
                }
            }
        })
    };

    writer.join().expect("writer should join");
    let received = reader.join().expect("reader should join");
    assert_eq!(received, payload);
}

#[test]
fn ring_buffer_byte_interface_matches_reference_case() {
    let rb = RingBuffer::new(2);
    rb.write_byte(b'a').expect("write byte should succeed");
    rb.write_byte(b'b').expect("write byte should succeed");
    assert_eq!(rb.write_byte(b'c'), Err(Error::IsFull));
    assert_eq!(RingBuffer::bytes(&rb), b"ab");
    assert_eq!(rb.read_byte().expect("read byte should succeed"), b'a');
    assert_eq!(rb.read_byte().expect("read byte should succeed"), b'b');
    assert_eq!(rb.read_byte(), Err(Error::IsEmpty));
}

#[test]
fn ring_buffer_close_error_matches_reference_case() {
    let rb = RingBuffer::new(100);
    rb.close_with_error(Some(Error::Closed("test error".to_owned())));

    assert_eq!(rb.write(&[1]), Err(Error::Closed("test error".to_owned())));
    assert_eq!(
        rb.write_byte(0),
        Err(Error::Closed("test error".to_owned()))
    );
    assert_eq!(
        rb.try_write(&[1]),
        Err(Error::Closed("test error".to_owned()))
    );
    assert_eq!(
        rb.try_write_byte(0),
        Err(Error::Closed("test error".to_owned()))
    );
    assert_eq!(rb.flush(), Err(Error::Closed("test error".to_owned())));

    rb.reset();
    rb.close_with_error(Some(Error::Closed("read error".to_owned())));
    let mut buf = [0u8; 1];
    assert_eq!(
        rb.read(&mut buf),
        Err(Error::Closed("read error".to_owned()))
    );
    assert_eq!(rb.read_byte(), Err(Error::Closed("read error".to_owned())));
    assert_eq!(
        rb.try_read(&mut buf),
        Err(Error::Closed("read error".to_owned()))
    );
}

#[test]
fn ring_buffer_close_error_unblocks_matches_reference_case() {
    let rb = Arc::new(RingBuffer::new(8).set_blocking(true));

    let waiting_reader = {
        let rb = Arc::clone(&rb);
        thread::spawn(move || {
            let mut buf = [0u8; 4];
            rb.read(&mut buf)
        })
    };

    thread::sleep(std::time::Duration::from_millis(20));
    rb.close_with_error(Some(Error::Closed("stop".to_owned())));
    assert_eq!(
        waiting_reader.join().expect("reader should join"),
        Err(Error::Closed("stop".to_owned()))
    );
}

#[test]
fn write_after_writer_close_matches_reference_case() {
    let rb = RingBuffer::new(100).set_blocking(true);
    assert_eq!(rb.write(b"hello").expect("write should succeed"), 5);
    rb.close_writer();
    assert_eq!(rb.write(b"world"), Err(Error::WriteOnClosed));
    assert_eq!(rb.write_byte(0), Err(Error::WriteOnClosed));
    assert_eq!(rb.try_write(b"world"), Err(Error::WriteOnClosed));
    assert_eq!(rb.try_write_byte(0), Err(Error::WriteOnClosed));

    let mut buf = [0u8; 8];
    let n = rb
        .read(&mut buf)
        .expect("remaining data should be readable");
    assert_eq!(&buf[..n], b"hello");
    assert_eq!(rb.read(&mut buf), Err(Error::Eof));
}
