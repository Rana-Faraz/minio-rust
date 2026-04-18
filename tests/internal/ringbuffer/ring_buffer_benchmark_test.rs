use std::sync::Arc;
use std::thread;

use minio_rust::internal::ringbuffer::{Error, RingBuffer};

pub const SOURCE_FILE: &str = "internal/ringbuffer/ring_buffer_benchmark_test.go";

#[test]
fn benchmark_ring_buffer_sync_matches_reference_shape() {
    let rb = RingBuffer::new(1024);
    let data = vec![b'a'; 512];
    let mut buf = vec![0u8; 512];

    for _ in 0..32 {
        rb.write(&data).expect("sync write should succeed");
        let n = rb.read(&mut buf).expect("sync read should succeed");
        assert_eq!(n, 512);
    }
}

#[test]
fn benchmark_ring_buffer_async_read_matches_reference_shape() {
    let rb = Arc::new(RingBuffer::new(1024).set_blocking(true));
    let data = vec![b'a'; 512];

    let reader = {
        let rb = Arc::clone(&rb);
        thread::spawn(move || {
            let mut buf = vec![0u8; 512];
            for _ in 0..16 {
                rb.read(&mut buf).expect("async read should succeed");
            }
        })
    };

    for _ in 0..16 {
        rb.write(&data).expect("write should succeed");
    }
    reader.join().expect("reader should join");
}

#[test]
fn benchmark_ring_buffer_async_read_blocking_matches_reference_shape() {
    let rb = Arc::new(RingBuffer::new(5120).set_blocking(true));
    let data = vec![b'a'; 512];

    let reader = {
        let rb = Arc::clone(&rb);
        thread::spawn(move || {
            let mut buf = vec![0u8; 512];
            for _ in 0..16 {
                rb.read(&mut buf).expect("blocking read should succeed");
            }
        })
    };

    for _ in 0..16 {
        rb.write(&data).expect("blocking write should succeed");
    }
    reader.join().expect("reader should join");
}

#[test]
fn benchmark_ring_buffer_async_write_matches_reference_shape() {
    let rb = Arc::new(RingBuffer::new(1024).set_blocking(true));
    let data = vec![b'a'; 512];

    let writer = {
        let rb = Arc::clone(&rb);
        let data = data.clone();
        thread::spawn(move || {
            for _ in 0..16 {
                rb.write(&data).expect("async write should succeed");
            }
        })
    };

    let mut buf = vec![0u8; 512];
    for _ in 0..16 {
        rb.read(&mut buf).expect("read should succeed");
    }
    writer.join().expect("writer should join");
}

#[test]
fn benchmark_ring_buffer_async_write_blocking_matches_reference_shape() {
    let rb = Arc::new(RingBuffer::new(5120).set_blocking(true));
    let data = vec![b'a'; 512];

    let writer = {
        let rb = Arc::clone(&rb);
        let data = data.clone();
        thread::spawn(move || {
            for _ in 0..16 {
                rb.write(&data).expect("blocking write should succeed");
            }
        })
    };

    let mut buf = vec![0u8; 512];
    for _ in 0..16 {
        rb.read(&mut buf).expect("read should succeed");
    }
    writer.join().expect("writer should join");
}

#[test]
fn benchmark_io_pipe_reader_matches_reference_shape() {
    let rb = RingBuffer::new(1024).set_blocking(true);
    rb.write(&vec![b'a'; 512]).expect("write should succeed");
    let mut buf = vec![0u8; 512];
    assert_eq!(rb.read(&mut buf).expect("read should succeed"), 512);
    assert!(buf.iter().all(|byte| *byte == b'a'));
    rb.close_writer();
    assert_eq!(rb.read(&mut buf), Err(Error::Eof));
}
