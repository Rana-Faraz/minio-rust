use std::io::Cursor;

use minio_rust::cmd::{ErasureCoder, BLOCK_SIZE_V2};
use rand::{rngs::StdRng, Rng, SeedableRng};

pub const SOURCE_FILE: &str = "cmd/erasure-decode_test.go";

fn patterned_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|idx| (idx % 251) as u8).collect()
}

fn clear_data_shards(shards: &mut [Vec<u8>], count: usize) {
    for shard in shards.iter_mut().take(count) {
        shard.clear();
    }
}

fn clear_parity_shards(shards: &mut [Vec<u8>], data_blocks: usize, count: usize) {
    for shard in shards.iter_mut().skip(data_blocks).take(count) {
        shard.clear();
    }
}

#[test]
fn test_erasure_decode_line_86() {
    struct Case {
        data_blocks: usize,
        on_disks: usize,
        off_disks: usize,
        block_size: i64,
        data: i64,
        offset: i64,
        length: i64,
        should_fail: bool,
        should_fail_quorum: bool,
    }

    let one_mib = 1024 * 1024;
    let cases = [
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 3,
            on_disks: 6,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 4,
            on_disks: 8,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 5,
            on_disks: 10,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 1,
            length: one_mib as i64 - 1,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 6,
            on_disks: 12,
            off_disks: 0,
            block_size: one_mib as i64,
            data: one_mib as i64,
            offset: one_mib as i64,
            length: 0,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 7,
            on_disks: 14,
            off_disks: 0,
            block_size: one_mib as i64,
            data: one_mib as i64,
            offset: 3,
            length: 1024,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 8,
            on_disks: 16,
            off_disks: 0,
            block_size: one_mib as i64,
            data: one_mib as i64,
            offset: 4,
            length: 8 * 1024,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 7,
            on_disks: 14,
            off_disks: 7,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: one_mib as i64,
            length: 1,
            should_fail: true,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 6,
            on_disks: 12,
            off_disks: 6,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 5,
            on_disks: 10,
            off_disks: 5,
            block_size: one_mib as i64,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 4,
            on_disks: 8,
            off_disks: 4,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 3,
            on_disks: 6,
            off_disks: 3,
            block_size: one_mib as i64,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 2,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 1,
            block_size: one_mib as i64,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 3,
            on_disks: 6,
            off_disks: 2,
            block_size: one_mib as i64,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 4,
            on_disks: 8,
            off_disks: 3,
            block_size: (2 * one_mib) as i64,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 5,
            on_disks: 10,
            off_disks: 6,
            block_size: one_mib as i64,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: true,
        },
        Case {
            data_blocks: 5,
            on_disks: 10,
            off_disks: 2,
            block_size: BLOCK_SIZE_V2,
            data: (2 * one_mib) as i64,
            offset: one_mib as i64,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 5,
            on_disks: 10,
            off_disks: 1,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 6,
            on_disks: 12,
            off_disks: 3,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 6,
            on_disks: 12,
            off_disks: 7,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: true,
        },
        Case {
            data_blocks: 8,
            on_disks: 16,
            off_disks: 8,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 8,
            on_disks: 16,
            off_disks: 9,
            block_size: one_mib as i64,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: true,
        },
        Case {
            data_blocks: 8,
            on_disks: 16,
            off_disks: 7,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 1,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 0,
            length: one_mib as i64,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: BLOCK_SIZE_V2 + 1,
            offset: 0,
            length: BLOCK_SIZE_V2 + 1,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: 2 * BLOCK_SIZE_V2,
            offset: 12,
            length: BLOCK_SIZE_V2 + 17,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 3,
            on_disks: 6,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: 2 * BLOCK_SIZE_V2,
            offset: 1023,
            length: BLOCK_SIZE_V2 + 1024,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 4,
            on_disks: 8,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: 2 * BLOCK_SIZE_V2,
            offset: 11,
            length: BLOCK_SIZE_V2 + 2 * 1024,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 6,
            on_disks: 12,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: 2 * BLOCK_SIZE_V2,
            offset: 512,
            length: BLOCK_SIZE_V2 + 8 * 1024,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 8,
            on_disks: 16,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: 2 * BLOCK_SIZE_V2,
            offset: BLOCK_SIZE_V2,
            length: BLOCK_SIZE_V2 - 1,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: -1,
            length: 3,
            should_fail: true,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: one_mib as i64,
            offset: 1024,
            length: -1,
            should_fail: true,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 4,
            on_disks: 6,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data: BLOCK_SIZE_V2,
            offset: 0,
            length: BLOCK_SIZE_V2,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 4,
            on_disks: 6,
            off_disks: 1,
            block_size: BLOCK_SIZE_V2,
            data: 2 * BLOCK_SIZE_V2,
            offset: 12,
            length: BLOCK_SIZE_V2 + 17,
            should_fail: false,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 4,
            on_disks: 6,
            off_disks: 3,
            block_size: BLOCK_SIZE_V2,
            data: 2 * BLOCK_SIZE_V2,
            offset: 1023,
            length: BLOCK_SIZE_V2 + 1024,
            should_fail: false,
            should_fail_quorum: true,
        },
        Case {
            data_blocks: 8,
            on_disks: 12,
            off_disks: 4,
            block_size: BLOCK_SIZE_V2,
            data: 2 * BLOCK_SIZE_V2,
            offset: 11,
            length: BLOCK_SIZE_V2 + 2 * 1024,
            should_fail: false,
            should_fail_quorum: false,
        },
    ];

    for (index, case) in cases.iter().enumerate() {
        let parity_blocks = case.on_disks - case.data_blocks;
        let coder = ErasureCoder::new(case.data_blocks, parity_blocks, case.block_size)
            .expect("create erasure coder");
        let data = patterned_bytes(case.data as usize);
        let encoded = coder.encode_data(&data).expect("encode data");

        let mut initial_decode = encoded.clone();
        let initial_result = {
            let mut out = Cursor::new(Vec::new());
            coder
                .decode(
                    &mut out,
                    &mut initial_decode,
                    case.offset,
                    case.length,
                    case.data,
                )
                .map(|_| out.into_inner())
        };

        if case.should_fail {
            assert!(initial_result.is_err(), "case {index} should fail");
        } else {
            let got = initial_result
                .unwrap_or_else(|err| panic!("case {index} should pass initial decode: {err}"));
            assert_eq!(
                got,
                data[case.offset as usize..(case.offset + case.length) as usize],
                "case {index} returned wrong content"
            );
        }

        if !case.should_fail {
            let mut quorum_decode = encoded.clone();
            let missing_total = case.off_disks.min(quorum_decode.len());
            let data_missing = missing_total.min(case.data_blocks);
            let parity_missing = missing_total.saturating_sub(data_missing);
            clear_data_shards(&mut quorum_decode, data_missing);
            clear_parity_shards(&mut quorum_decode, case.data_blocks, parity_missing);

            let quorum_result = {
                let mut out = Cursor::new(Vec::new());
                coder
                    .decode(
                        &mut out,
                        &mut quorum_decode,
                        case.offset,
                        case.length,
                        case.data,
                    )
                    .map(|_| out.into_inner())
            };

            if case.should_fail_quorum {
                assert!(
                    quorum_result.is_err(),
                    "case {index} should fail quorum decode"
                );
            } else {
                let got = quorum_result
                    .unwrap_or_else(|err| panic!("case {index} should pass quorum decode: {err}"));
                assert_eq!(
                    got,
                    data[case.offset as usize..(case.offset + case.length) as usize],
                    "case {index} quorum decode returned wrong content"
                );
            }
        }
    }
}

#[test]
fn test_erasure_decode_random_offset_length_line_200() {
    let data_blocks = 7;
    let parity_blocks = 7;
    let coder = ErasureCoder::new(data_blocks, parity_blocks, 1024 * 1024).expect("create coder");
    let data = patterned_bytes(5 * 1024 * 1024);
    let encoded = coder.encode_data(&data).expect("encode data");
    let length = data.len();
    let mut rng = StdRng::seed_from_u64(42);

    for _ in 0..200 {
        let offset = rng.gen_range(0..length) as i64;
        let read_len = rng.gen_range(0..=(length - offset as usize)) as i64;
        let mut shards = encoded.clone();
        let mut out = Cursor::new(Vec::new());
        let written = coder
            .decode(&mut out, &mut shards, offset, read_len, length as i64)
            .expect("random decode");
        assert_eq!(written, read_len);
        assert_eq!(
            out.into_inner(),
            data[offset as usize..(offset + read_len) as usize]
        );
    }
}

#[test]
fn benchmark_erasure_decode_quick_line_336() {
    benchmark_decode_smoke(2, 2, &[(0, 0), (0, 1), (1, 0), (1, 1)], 12 * 1024 * 1024);
}

#[test]
fn benchmark_erasure_decode_4_64_kb_line_344() {
    benchmark_decode_smoke(
        2,
        2,
        &[(0, 0), (0, 1), (1, 0), (1, 1), (0, 2), (2, 0)],
        64 * 1024,
    );
}

#[test]
fn benchmark_erasure_decode_8_20_mb_line_354() {
    benchmark_decode_smoke(
        4,
        4,
        &[(0, 0), (0, 1), (1, 0), (1, 1), (0, 4), (2, 2), (4, 0)],
        20 * 1024 * 1024,
    );
}

#[test]
fn benchmark_erasure_decode_12_30_mb_line_365() {
    benchmark_decode_smoke(
        6,
        6,
        &[(0, 0), (0, 1), (1, 0), (1, 1), (0, 6), (3, 3), (6, 0)],
        30 * 1024 * 1024,
    );
}

#[test]
fn benchmark_erasure_decode_16_40_mb_line_376() {
    benchmark_decode_smoke(
        8,
        8,
        &[(0, 0), (0, 1), (1, 0), (1, 1), (0, 8), (4, 4), (8, 0)],
        40 * 1024 * 1024,
    );
}

fn benchmark_decode_smoke(
    data_blocks: usize,
    parity_blocks: usize,
    scenarios: &[(usize, usize)],
    size: usize,
) {
    let coder = ErasureCoder::new(data_blocks, parity_blocks, BLOCK_SIZE_V2).expect("create coder");
    let data = vec![0x5a; size];
    let encoded = coder.encode_data(&data).expect("encode data");

    for &(missing_data, missing_parity) in scenarios {
        let mut shards = encoded.clone();
        clear_data_shards(&mut shards, missing_data.min(data_blocks));
        clear_parity_shards(&mut shards, data_blocks, missing_parity.min(parity_blocks));
        let mut out = Cursor::new(Vec::new());
        let result = coder.decode(&mut out, &mut shards, 0, size as i64, size as i64);
        let can_reconstruct = missing_data + missing_parity <= parity_blocks;
        if can_reconstruct {
            let written = result.expect("benchmark smoke decode");
            assert_eq!(written, size as i64);
        } else {
            assert!(result.is_err(), "decode should fail when quorum is lost");
        }
    }
}
