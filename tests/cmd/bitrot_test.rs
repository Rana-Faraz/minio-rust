use tempfile::tempdir;

use minio_rust::cmd::{bitrot_checksum, BitrotAlgorithm, LocalXlStorage};

pub const SOURCE_FILE: &str = "cmd/bitrot_test.go";

fn test_bitrot_reader_writer_algo(algorithm: BitrotAlgorithm) {
    let tmp = tempdir().expect("tempdir");
    let storage = LocalXlStorage::new(tmp.path().to_str().expect("utf8")).expect("storage");
    let volume = "testvol";
    let path = "testfile";
    let data = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    storage.make_vol(volume).expect("make vol");
    storage
        .append_file(volume, path, data)
        .expect("append file");

    let checksum = bitrot_checksum(algorithm, data);
    storage
        .verify_file(volume, path, data.len(), algorithm, &checksum)
        .expect("verify file");

    let mut out = vec![0_u8; 10];
    let read = storage
        .read_file(volume, path, 0, &mut out)
        .expect("read 0");
    assert_eq!(read, 10);
    assert_eq!(&out, b"aaaaaaaaaa");

    let read = storage
        .read_file(volume, path, 10, &mut out)
        .expect("read 10");
    assert_eq!(read, 10);
    assert_eq!(&out, b"aaaaaaaaaa");

    let read = storage
        .read_file(volume, path, 20, &mut out)
        .expect("read 20");
    assert_eq!(read, 10);
    assert_eq!(&out, b"aaaaaaaaaa");

    let mut tail = vec![0_u8; 5];
    let read = storage
        .read_file(volume, path, 30, &mut tail)
        .expect("read tail");
    assert_eq!(read, 5);
    assert_eq!(&tail, b"aaaaa");
}

#[test]
fn test_all_bitrot_algorithms_line_79() {
    for algorithm in [
        BitrotAlgorithm::Sha256,
        BitrotAlgorithm::Blake2b512,
        BitrotAlgorithm::HighwayHash256,
        BitrotAlgorithm::HighwayHash256S,
    ] {
        test_bitrot_reader_writer_algo(algorithm);
    }
}
