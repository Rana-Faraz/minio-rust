use std::io::{Cursor, Read, Write};

use minio_rust::cmd::{
    BitrotAlgorithm, ErasureCoder, BLOCK_SIZE_V2, ERR_ERASURE_READ_QUORUM, ERR_ERASURE_WRITE_QUORUM,
};

pub const SOURCE_FILE: &str = "cmd/erasure-heal_test.go";

#[derive(Debug, Clone)]
struct MockReader {
    inner: Cursor<Vec<u8>>,
    fail: bool,
}

impl Read for MockReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.fail {
            return Err(std::io::Error::other("faulty disk"));
        }
        self.inner.read(buf)
    }
}

#[derive(Debug, Default)]
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

struct Case {
    data_blocks: usize,
    disks: usize,
    off_disks: usize,
    bad_disks: usize,
    bad_stale_disks: usize,
    block_size: i64,
    size: usize,
    algorithm: BitrotAlgorithm,
    should_fail: bool,
}

fn deterministic_data(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|idx| seed.wrapping_add((idx % 251) as u8))
        .collect()
}

#[test]
fn test_erasure_heal_line_64() {
    let one_block = 64 * 1024;
    let cases = [
        Case {
            data_blocks: 2,
            disks: 4,
            off_disks: 1,
            bad_disks: 0,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::Sha256,
            should_fail: false,
        },
        Case {
            data_blocks: 3,
            disks: 6,
            off_disks: 2,
            bad_disks: 0,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::Blake2b512,
            should_fail: false,
        },
        Case {
            data_blocks: 4,
            disks: 8,
            off_disks: 2,
            bad_disks: 1,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::Blake2b512,
            should_fail: false,
        },
        Case {
            data_blocks: 5,
            disks: 10,
            off_disks: 3,
            bad_disks: 1,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: false,
        },
        Case {
            data_blocks: 6,
            disks: 12,
            off_disks: 2,
            bad_disks: 3,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::Sha256,
            should_fail: false,
        },
        Case {
            data_blocks: 7,
            disks: 14,
            off_disks: 4,
            bad_disks: 1,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: false,
        },
        Case {
            data_blocks: 8,
            disks: 16,
            off_disks: 6,
            bad_disks: 1,
            bad_stale_disks: 1,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: false,
        },
        Case {
            data_blocks: 7,
            disks: 14,
            off_disks: 2,
            bad_disks: 3,
            bad_stale_disks: 0,
            block_size: one_block as i64 / 2,
            size: one_block,
            algorithm: BitrotAlgorithm::Blake2b512,
            should_fail: false,
        },
        Case {
            data_blocks: 6,
            disks: 12,
            off_disks: 1,
            bad_disks: 0,
            bad_stale_disks: 1,
            block_size: one_block as i64 - 1,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: true,
        },
        Case {
            data_blocks: 5,
            disks: 10,
            off_disks: 3,
            bad_disks: 0,
            bad_stale_disks: 3,
            block_size: one_block as i64 / 2,
            size: one_block,
            algorithm: BitrotAlgorithm::Sha256,
            should_fail: true,
        },
        Case {
            data_blocks: 4,
            disks: 8,
            off_disks: 1,
            bad_disks: 1,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: false,
        },
        Case {
            data_blocks: 2,
            disks: 4,
            off_disks: 1,
            bad_disks: 0,
            bad_stale_disks: 1,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: true,
        },
        Case {
            data_blocks: 6,
            disks: 12,
            off_disks: 8,
            bad_disks: 3,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: true,
        },
        Case {
            data_blocks: 7,
            disks: 14,
            off_disks: 3,
            bad_disks: 4,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::Blake2b512,
            should_fail: false,
        },
        Case {
            data_blocks: 7,
            disks: 14,
            off_disks: 6,
            bad_disks: 1,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: false,
        },
        Case {
            data_blocks: 8,
            disks: 16,
            off_disks: 4,
            bad_disks: 5,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: true,
        },
        Case {
            data_blocks: 2,
            disks: 4,
            off_disks: 1,
            bad_disks: 0,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: false,
        },
        Case {
            data_blocks: 12,
            disks: 16,
            off_disks: 2,
            bad_disks: 1,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::HighwayHash256,
            should_fail: false,
        },
        Case {
            data_blocks: 6,
            disks: 8,
            off_disks: 1,
            bad_disks: 0,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block,
            algorithm: BitrotAlgorithm::Blake2b512,
            should_fail: false,
        },
        Case {
            data_blocks: 2,
            disks: 4,
            off_disks: 1,
            bad_disks: 0,
            bad_stale_disks: 0,
            block_size: BLOCK_SIZE_V2,
            size: one_block * 8,
            algorithm: BitrotAlgorithm::Sha256,
            should_fail: false,
        },
    ];

    for (index, case) in cases.iter().enumerate() {
        assert!(
            case.off_disks >= case.bad_stale_disks,
            "case {index}: stale disk count must cover bad stale disks"
        );
        let _algorithm = case.algorithm;
        let coder = ErasureCoder::new(
            case.data_blocks,
            case.disks - case.data_blocks,
            case.block_size,
        )
        .unwrap_or_else(|err| panic!("case {index}: coder creation failed: {err}"));
        let data = deterministic_data(case.size, index as u8);
        let encoded = coder
            .encode_data(&data)
            .unwrap_or_else(|err| panic!("case {index}: encode failed: {err}"));

        let mut reader_backing = encoded
            .iter()
            .cloned()
            .map(|bytes| MockReader {
                inner: Cursor::new(bytes),
                fail: false,
            })
            .collect::<Vec<_>>();
        let mut stale_writers = (0..case.disks)
            .map(|_| MockWriter::default())
            .collect::<Vec<_>>();

        for (slot, reader) in reader_backing.iter_mut().enumerate() {
            if slot < case.off_disks {
                reader.fail = true;
            } else if slot < case.off_disks + case.bad_disks {
                reader.fail = true;
            }
        }
        for writer in stale_writers.iter_mut().take(case.bad_stale_disks) {
            writer.fail = true;
        }

        let mut reader_refs = reader_backing
            .iter_mut()
            .enumerate()
            .map(|(slot, reader)| {
                if slot < case.off_disks {
                    None
                } else {
                    Some(reader as &mut dyn Read)
                }
            })
            .collect::<Vec<_>>();
        let mut writer_refs = stale_writers
            .iter_mut()
            .enumerate()
            .map(|(slot, writer)| {
                if slot < case.off_disks {
                    Some(writer as &mut dyn Write)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let result = coder.heal(&mut writer_refs, &mut reader_refs, data.len() as i64);
        if case.should_fail {
            let err = result.expect_err(&format!("case {index} should fail"));
            assert!(
                err == ERR_ERASURE_READ_QUORUM || err == ERR_ERASURE_WRITE_QUORUM,
                "case {index}: unexpected error {err}"
            );
            continue;
        }

        let healed =
            result.unwrap_or_else(|err| panic!("case {index}: heal should succeed: {err}"));
        for shard_index in 0..case.off_disks {
            assert_eq!(
                healed[shard_index], encoded[shard_index],
                "case {index}: returned shard {shard_index} mismatch"
            );
            if shard_index < case.bad_stale_disks {
                assert!(
                    stale_writers[shard_index].bytes.is_empty(),
                    "case {index}: bad stale writer {shard_index} should remain empty"
                );
            } else {
                assert_eq!(
                    stale_writers[shard_index].bytes, encoded[shard_index],
                    "case {index}: healed shard {shard_index} mismatch"
                );
            }
        }
    }
}
