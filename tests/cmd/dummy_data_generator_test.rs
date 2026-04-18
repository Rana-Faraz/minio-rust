use std::io::{Cursor, Read};

use minio_rust::cmd::{cmp_readers, DummyDataGenerator};

pub const SOURCE_FILE: &str = "cmd/dummy-data-generator_test.go";

#[test]
fn test_dummy_data_generator_line_114() {
    let mut generator = DummyDataGenerator::new(32, 7);
    let mut actual = Vec::new();
    generator.read_to_end(&mut actual).expect("read generator");

    let mut same_seed = DummyDataGenerator::new(32, 7);
    let mut expected = Vec::new();
    same_seed
        .read_to_end(&mut expected)
        .expect("read same seed");

    let mut different_seed = DummyDataGenerator::new(32, 8);
    let mut different = Vec::new();
    different_seed
        .read_to_end(&mut different)
        .expect("read different seed");

    assert_eq!(actual.len(), 32);
    assert_eq!(actual, expected);
    assert_ne!(actual, different);
}

#[test]
fn test_cmp_readers_line_163() {
    let mut left = Cursor::new(vec![1_u8, 2, 3, 4]);
    let mut right = Cursor::new(vec![1_u8, 2, 3, 4]);
    let mut wrong = Cursor::new(vec![1_u8, 2, 9, 4]);

    assert!(cmp_readers(&mut left, &mut right).expect("compare equal readers"));
    assert!(
        !cmp_readers(&mut Cursor::new(vec![1_u8, 2, 3, 4]), &mut wrong)
            .expect("compare different readers")
    );
}
