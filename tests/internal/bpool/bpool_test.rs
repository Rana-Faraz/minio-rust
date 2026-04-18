use minio_rust::internal::bpool::BytePoolCap;

#[test]
fn byte_pool_matches_reference_cases() {
    let size = 4_u64;
    let width = 1024_usize;
    let cap_width = 2048_usize;

    let pool = BytePoolCap::new(size, width, cap_width);

    assert_eq!(pool.width(), width);
    assert_eq!(pool.width_cap(), cap_width);

    let mut buffer = pool.get();
    assert_eq!(buffer.len(), width);
    assert_eq!(buffer.capacity(), cap_width);

    buffer[0] = 7;
    pool.put(buffer);

    for _ in 0..size * 2 {
        let mut extra = Vec::with_capacity(cap_width);
        extra.resize(width, 0);
        pool.put(extra);
    }

    let buffer = pool.get();
    assert_eq!(buffer.len(), width);
    assert_eq!(buffer.capacity(), cap_width);
    pool.put(buffer);

    assert_eq!(pool.buffered_count(), size as usize);

    for _ in 0..size {
        let _ = pool.get();
    }

    let mut too_small = Vec::with_capacity(cap_width - 1);
    too_small.resize(width, 0);
    pool.put(too_small);

    let mut too_large = Vec::with_capacity(cap_width + 1);
    too_large.resize(width, 0);
    pool.put(too_large);

    let wrong_default = vec![0_u8; width];
    pool.put(wrong_default);
    assert_eq!(pool.buffered_count(), 0);

    let mut short = Vec::with_capacity(cap_width);
    short.resize(width, 0);
    short.truncate(2);
    pool.put(short);
    assert_eq!(pool.buffered_count(), 1);

    let buffer = pool.get();
    assert_eq!(buffer.len(), width);
    assert_eq!(pool.current_size(), 0);
}
