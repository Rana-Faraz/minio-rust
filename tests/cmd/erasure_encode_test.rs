use std::io::{Cursor, Write};

pub const SOURCE_FILE: &str = "cmd/erasure-encode_test.go";

use minio_rust::cmd::{ErasureCoder, BLOCK_SIZE_V2};

struct MockWriter {
    bytes: Vec<u8>,
    fail: bool,
}

impl Write for MockWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.fail {
            return Err(std::io::Error::other("faulty disk"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn test_erasure_encode_line_84() {
    struct Case {
        data_blocks: usize,
        on_disks: usize,
        off_disks: usize,
        block_size: i64,
        data_len: usize,
        offset: usize,
        should_fail_quorum: bool,
    }

    let one_mib = 1024 * 1024;
    let cases = [
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 0,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 3,
            on_disks: 6,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 1,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 4,
            on_disks: 8,
            off_disks: 2,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 2,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 5,
            on_disks: 10,
            off_disks: 3,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: one_mib,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 6,
            on_disks: 12,
            off_disks: 4,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: one_mib,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 7,
            on_disks: 14,
            off_disks: 5,
            block_size: BLOCK_SIZE_V2,
            data_len: 0,
            offset: 0,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 8,
            on_disks: 16,
            off_disks: 7,
            block_size: BLOCK_SIZE_V2,
            data_len: 0,
            offset: 0,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 2,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 0,
            should_fail_quorum: true,
        },
        Case {
            data_blocks: 4,
            on_disks: 8,
            off_disks: 4,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 0,
            should_fail_quorum: true,
        },
        Case {
            data_blocks: 7,
            on_disks: 14,
            off_disks: 7,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 0,
            should_fail_quorum: true,
        },
        Case {
            data_blocks: 8,
            on_disks: 16,
            off_disks: 8,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 0,
            should_fail_quorum: true,
        },
        Case {
            data_blocks: 5,
            on_disks: 10,
            off_disks: 3,
            block_size: one_mib as i64,
            data_len: one_mib,
            offset: 0,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 3,
            on_disks: 6,
            off_disks: 1,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: one_mib / 2,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 4,
            off_disks: 0,
            block_size: (one_mib / 2) as i64,
            data_len: one_mib,
            offset: one_mib / 2 + 1,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 4,
            on_disks: 8,
            off_disks: 0,
            block_size: (one_mib - 1) as i64,
            data_len: one_mib,
            offset: one_mib - 1,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 8,
            on_disks: 12,
            off_disks: 2,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 2,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 8,
            on_disks: 10,
            off_disks: 1,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 0,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 10,
            on_disks: 14,
            off_disks: 0,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 17,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 2,
            on_disks: 6,
            off_disks: 2,
            block_size: one_mib as i64,
            data_len: one_mib,
            offset: one_mib / 2,
            should_fail_quorum: false,
        },
        Case {
            data_blocks: 10,
            on_disks: 16,
            off_disks: 8,
            block_size: BLOCK_SIZE_V2,
            data_len: one_mib,
            offset: 0,
            should_fail_quorum: true,
        },
    ];

    for (index, case) in cases.iter().enumerate() {
        let parity_blocks = case.on_disks - case.data_blocks;
        let coder = ErasureCoder::new(case.data_blocks, parity_blocks, case.block_size)
            .expect("create erasure coder");
        let data: Vec<u8> = (0..case.data_len).map(|v| (v % 251) as u8).collect();
        let mut first_pass_buf = vec![0_u8; case.block_size as usize];
        let mut first_pass_writers: Vec<MockWriter> = (0..case.on_disks)
            .map(|_| MockWriter {
                bytes: Vec::new(),
                fail: false,
            })
            .collect();
        let mut first_refs: Vec<Option<&mut dyn Write>> = first_pass_writers
            .iter_mut()
            .map(|writer| Some(writer as &mut dyn Write))
            .collect();
        let first_written = coder
            .encode(
                &mut Cursor::new(&data[case.offset..]),
                &mut first_refs,
                &mut first_pass_buf,
                coder.data_blocks() + 1,
            )
            .unwrap_or_else(|err| panic!("case {index} initial encode failed: {err}"));
        assert_eq!(
            first_written,
            (data.len() - case.offset) as i64,
            "case {index} byte count mismatch"
        );

        let mut quorum_writers: Vec<MockWriter> = (0..case.on_disks)
            .map(|slot| MockWriter {
                bytes: Vec::new(),
                fail: slot > 0 && slot < case.off_disks,
            })
            .collect();
        let mut quorum_refs: Vec<Option<&mut dyn Write>> = quorum_writers
            .iter_mut()
            .map(|writer| Some(writer as &mut dyn Write))
            .collect();
        if case.off_disks > 0 {
            quorum_refs[0] = None;
        }
        let mut quorum_buf = vec![0_u8; case.block_size as usize];
        let result = coder.encode(
            &mut Cursor::new(&data[case.offset..]),
            &mut quorum_refs,
            &mut quorum_buf,
            coder.data_blocks() + 1,
        );

        if case.should_fail_quorum {
            assert!(result.is_err(), "case {index} should fail quorum");
        } else {
            let written =
                result.unwrap_or_else(|err| panic!("case {index} should pass quorum: {err}"));
            assert_eq!(
                written,
                (data.len() - case.offset) as i64,
                "case {index} quorum byte count mismatch"
            );
        }
    }
}

#[test]
fn benchmark_erasure_encode_quick_line_204() {
    benchmark_encode_smoke(2, 2, &[(0, 0), (0, 1), (1, 0)], 12 * 1024 * 1024);
}

#[test]
fn benchmark_erasure_encode_4_64_kb_line_211() {
    benchmark_encode_smoke(2, 2, &[(0, 0), (0, 1), (1, 0)], 64 * 1024);
}

#[test]
fn benchmark_erasure_encode_8_20_mb_line_218() {
    benchmark_encode_smoke(
        4,
        4,
        &[(0, 0), (0, 1), (1, 0), (0, 3), (3, 0)],
        20 * 1024 * 1024,
    );
}

#[test]
fn benchmark_erasure_encode_12_30_mb_line_227() {
    benchmark_encode_smoke(
        6,
        6,
        &[(0, 0), (0, 1), (1, 0), (0, 5), (5, 0)],
        30 * 1024 * 1024,
    );
}

#[test]
fn benchmark_erasure_encode_16_40_mb_line_236() {
    benchmark_encode_smoke(
        8,
        8,
        &[(0, 0), (0, 1), (1, 0), (0, 7), (7, 0)],
        40 * 1024 * 1024,
    );
}

fn benchmark_encode_smoke(data: usize, parity: usize, scenarios: &[(usize, usize)], size: usize) {
    let total = data + parity;
    let coder = ErasureCoder::new(data, parity, BLOCK_SIZE_V2).expect("create erasure coder");
    let payload = vec![0x5a; size];

    for &(data_down, parity_down) in scenarios {
        let mut writers: Vec<MockWriter> = (0..total)
            .map(|slot| MockWriter {
                bytes: Vec::new(),
                fail: (slot < data_down) || (slot >= data && slot < data + parity_down),
            })
            .collect();
        let mut refs: Vec<Option<&mut dyn Write>> = writers
            .iter_mut()
            .map(|writer| Some(writer as &mut dyn Write))
            .collect();
        let mut buf = vec![0_u8; BLOCK_SIZE_V2 as usize];
        let written = coder
            .encode(
                &mut Cursor::new(payload.as_slice()),
                &mut refs,
                &mut buf,
                coder.data_blocks() + 1,
            )
            .expect("benchmark smoke encode");
        assert_eq!(written, payload.len() as i64);
    }
}
