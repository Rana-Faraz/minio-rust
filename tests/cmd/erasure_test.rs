use std::io::Cursor;

pub const SOURCE_FILE: &str = "cmd/erasure_test.go";

use minio_rust::cmd::{write_data_blocks, ErasureCoder, BLOCK_SIZE_V2};

fn deterministic_data(len: usize) -> Vec<u8> {
    (0..len).map(|idx| (idx % 251) as u8).collect()
}

#[test]
fn test_erasure_encode_decode_line_45() {
    struct Case {
        data_blocks: usize,
        parity_blocks: usize,
        missing_data: usize,
        missing_parity: usize,
        reconstruct_parity: bool,
        should_fail: bool,
    }

    let cases = [
        Case {
            data_blocks: 2,
            parity_blocks: 2,
            missing_data: 0,
            missing_parity: 0,
            reconstruct_parity: true,
            should_fail: false,
        },
        Case {
            data_blocks: 3,
            parity_blocks: 3,
            missing_data: 1,
            missing_parity: 0,
            reconstruct_parity: true,
            should_fail: false,
        },
        Case {
            data_blocks: 4,
            parity_blocks: 4,
            missing_data: 2,
            missing_parity: 0,
            reconstruct_parity: false,
            should_fail: false,
        },
        Case {
            data_blocks: 5,
            parity_blocks: 5,
            missing_data: 0,
            missing_parity: 1,
            reconstruct_parity: true,
            should_fail: false,
        },
        Case {
            data_blocks: 6,
            parity_blocks: 6,
            missing_data: 0,
            missing_parity: 2,
            reconstruct_parity: true,
            should_fail: false,
        },
        Case {
            data_blocks: 7,
            parity_blocks: 7,
            missing_data: 1,
            missing_parity: 1,
            reconstruct_parity: false,
            should_fail: false,
        },
        Case {
            data_blocks: 8,
            parity_blocks: 8,
            missing_data: 3,
            missing_parity: 2,
            reconstruct_parity: false,
            should_fail: false,
        },
        Case {
            data_blocks: 2,
            parity_blocks: 2,
            missing_data: 2,
            missing_parity: 1,
            reconstruct_parity: true,
            should_fail: true,
        },
        Case {
            data_blocks: 4,
            parity_blocks: 2,
            missing_data: 2,
            missing_parity: 2,
            reconstruct_parity: false,
            should_fail: true,
        },
        Case {
            data_blocks: 8,
            parity_blocks: 4,
            missing_data: 2,
            missing_parity: 2,
            reconstruct_parity: false,
            should_fail: false,
        },
    ];

    let data = deterministic_data(256);
    for (index, case) in cases.iter().enumerate() {
        let coder = ErasureCoder::new(case.data_blocks, case.parity_blocks, BLOCK_SIZE_V2)
            .expect("create erasure coder");
        let mut encoded = coder.encode_data(&data).expect("encode data");

        for shard in encoded.iter_mut().take(case.missing_data) {
            shard.clear();
        }
        for shard in encoded
            .iter_mut()
            .skip(case.data_blocks)
            .take(case.missing_parity)
        {
            shard.clear();
        }

        let result = if case.reconstruct_parity {
            coder.decode_data_and_parity_blocks(&mut encoded)
        } else {
            coder.decode_data_blocks(&mut encoded)
        };

        if case.should_fail {
            assert!(result.is_err(), "case {index} should fail");
            continue;
        }

        result.unwrap_or_else(|err| panic!("case {index} should pass: {err}"));

        if case.reconstruct_parity {
            for (shard_idx, shard) in encoded.iter().enumerate() {
                assert!(
                    !shard.is_empty(),
                    "case {index} should reconstruct shard {shard_idx}"
                );
            }
        } else {
            for (shard_idx, shard) in encoded.iter().take(case.data_blocks).enumerate() {
                assert!(
                    !shard.is_empty(),
                    "case {index} should reconstruct data shard {shard_idx}"
                );
            }
        }

        let mut decoded = Cursor::new(Vec::new());
        write_data_blocks(
            &mut decoded,
            &encoded,
            case.data_blocks,
            0,
            data.len() as i64,
        )
        .unwrap_or_else(|err| panic!("case {index} failed to write data blocks: {err}"));
        assert_eq!(
            decoded.into_inner(),
            data,
            "case {index} decoded data mismatch"
        );
    }
}
