use minio_rust::cmd::{crc_hash_mod, sip_hash_mod, ErasureSets};

pub const SOURCE_FILE: &str = "cmd/erasure-sets_test.go";

#[test]
fn benchmark_crc_hash_line_31() {
    let mut seen = 0usize;
    for index in 0..10_000 {
        seen ^= crc_hash_mod(&format!("object-{index}"), 16);
    }
    assert!(seen < 16);
}

#[test]
fn subbenchmark_benchmark_crc_hash_line_44() {
    let mut seen = 0usize;
    for index in 0..1_000 {
        seen ^= crc_hash_mod(&format!("bench-{index}"), 8);
    }
    assert!(seen < 8);
}

#[test]
fn benchmark_sip_hash_line_55() {
    let mut seen = 0usize;
    for index in 0..10_000 {
        seen ^= sip_hash_mod(&format!("object-{index}"), 16);
    }
    assert!(seen < 16);
}

#[test]
fn subbenchmark_benchmark_sip_hash_line_68() {
    let mut seen = 0usize;
    for index in 0..1_000 {
        seen ^= sip_hash_mod(&format!("bench-{index}"), 8);
    }
    assert!(seen < 8);
}

#[test]
fn test_sip_hash_mod_line_80() {
    let tests = [
        ("", 4_usize),
        ("object-a", 4),
        ("object-b", 16),
        ("nested/path/object", 7),
    ];

    for (index, (key, cardinality)) in tests.into_iter().enumerate() {
        let hash = sip_hash_mod(key, cardinality);
        assert!(hash < cardinality, "case {} bounds", index + 1);
        assert_eq!(
            hash,
            sip_hash_mod(key, cardinality),
            "case {} stable",
            index + 1
        );
    }
}

#[test]
fn test_crc_hash_mod_line_119() {
    let tests = [
        ("", 4_usize),
        ("object-a", 4),
        ("object-b", 16),
        ("nested/path/object", 7),
    ];

    for (index, (key, cardinality)) in tests.into_iter().enumerate() {
        let hash = crc_hash_mod(key, cardinality);
        assert!(hash < cardinality, "case {} bounds", index + 1);
        assert_eq!(
            hash,
            crc_hash_mod(key, cardinality),
            "case {} stable",
            index + 1
        );
    }
}

#[test]
fn test_new_erasure_sets_line_159() {
    let args = vec![
        "/disk1".to_string(),
        "/disk2".to_string(),
        "/disk3".to_string(),
        "/disk4".to_string(),
    ];
    let sets = ErasureSets::new(2, &args).expect("create erasure sets");
    assert_eq!(sets.set_count(), 2);
    assert_eq!(sets.drives_per_set(), 2);
    assert_eq!(
        sets.sets,
        vec![
            vec!["/disk1".to_string(), "/disk2".to_string()],
            vec!["/disk3".to_string(), "/disk4".to_string()],
        ]
    );
}

#[test]
fn test_hashed_layer_line_204() {
    let args = vec![
        "/disk1".to_string(),
        "/disk2".to_string(),
        "/disk3".to_string(),
        "/disk4".to_string(),
    ];
    let sets = ErasureSets::new(2, &args).expect("create erasure sets");

    let key = "bucket/object.txt";
    let crc_index = sets.crc_hash_mod(key);
    let sip_index = sets.sip_hash_mod(key);

    assert_eq!(sets.hashed_layer_crc(key), &sets.sets[crc_index]);
    assert_eq!(sets.hashed_layer_sip(key), &sets.sets[sip_index]);
    assert_eq!(sets.hashed_layer_crc(key), sets.hashed_layer_crc(key));
    assert_eq!(sets.hashed_layer_sip(key), sets.hashed_layer_sip(key));
}
