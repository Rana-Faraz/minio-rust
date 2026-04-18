use minio_rust::cmd::{
    disk_count, eval_disks, hash_order, hash_order_bytes, reduce_errs, reduce_read_quorum_errs,
    reduce_write_quorum_errs, shuffle_disks, ERR_DISK_NOT_FOUND, ERR_ERASURE_READ_QUORUM,
    ERR_ERASURE_WRITE_QUORUM, ERR_FILE_NOT_FOUND, ERR_VOLUME_NOT_FOUND,
};

pub const SOURCE_FILE: &str = "cmd/erasure-metadata-utils_test.go";

#[test]
fn test_disk_count_line_30() {
    let cases = [
        (vec![Some(1), Some(2), Some(3), Some(4)], 4usize),
        (vec![None, Some(2), Some(3), Some(4)], 3usize),
    ];
    for (disks, expected) in cases {
        assert_eq!(disk_count(&disks), expected);
    }
}

#[test]
fn test_reduce_errs_line_56() {
    let canceled_errs = vec![
        Some("error 0: context canceled"),
        Some("error 1: context canceled"),
        Some("error 2: context canceled"),
        Some("error 3: context canceled"),
        Some("error 4: context canceled"),
    ];
    let cases = [
        (
            vec![
                Some(ERR_DISK_NOT_FOUND),
                Some(ERR_DISK_NOT_FOUND),
                Some("disk full"),
            ],
            vec![],
            Some(ERR_ERASURE_READ_QUORUM.to_string()),
        ),
        (
            vec![Some("disk full"), Some(ERR_DISK_NOT_FOUND), None, None],
            vec![],
            Some(ERR_ERASURE_READ_QUORUM.to_string()),
        ),
        (
            vec![
                Some(ERR_VOLUME_NOT_FOUND),
                Some(ERR_VOLUME_NOT_FOUND),
                Some(ERR_VOLUME_NOT_FOUND),
                Some(ERR_VOLUME_NOT_FOUND),
                Some(ERR_VOLUME_NOT_FOUND),
                Some(ERR_DISK_NOT_FOUND),
                Some(ERR_DISK_NOT_FOUND),
            ],
            vec![ERR_DISK_NOT_FOUND],
            Some(ERR_VOLUME_NOT_FOUND.to_string()),
        ),
        (vec![], vec![], Some(ERR_ERASURE_READ_QUORUM.to_string())),
        (
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                None,
                None,
                None,
                None,
                None,
            ],
            vec![],
            None,
        ),
        (
            canceled_errs.clone(),
            vec![],
            Some("context canceled".to_string()),
        ),
    ];

    for (errs, ignored, expected_read) in cases {
        assert_eq!(reduce_read_quorum_errs(&errs, &ignored, 5), expected_read);
        assert_eq!(
            reduce_write_quorum_errs(&errs, &ignored, 6),
            Some(ERR_ERASURE_WRITE_QUORUM.to_string())
        );
    }

    let (count, err) = reduce_errs(&[Some("alpha"), Some("alpha"), None, None], &[]);
    assert_eq!(count, 2);
    assert_eq!(err, None);
}

#[test]
fn test_hash_order_line_114() {
    let cases = [
        (
            "object",
            vec![14, 15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        ),
        (
            "The Shining Script <v1>.pdf",
            vec![16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        ),
        (
            "Cost Benefit Analysis (2009-2010).pptx",
            vec![15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
        ),
        (
            "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A",
            vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 2],
        ),
        (
            "SHØRT",
            vec![11, 12, 13, 14, 15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        ),
        (
            "There are far too many object names, and far too few bucket names!",
            vec![15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
        ),
        (
            "a/b/c/",
            vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 2],
        ),
        (
            "/a/b/c",
            vec![6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 2, 3, 4, 5],
        ),
    ];
    for (object_name, expected) in cases {
        assert_eq!(hash_order(object_name, 16), Some(expected));
    }
    assert_eq!(
        hash_order_bytes(&[0xff, 0xfe, 0xfd], 16),
        Some(vec![15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])
    );
    assert_eq!(hash_order("This will fail", -1), None);
    assert_eq!(hash_order("This will fail", 0), None);
}

#[test]
fn test_shuffle_disks_line_150() {
    let disks: Vec<i32> = (1..=16).collect();
    let distribution = vec![16, 14, 12, 10, 8, 6, 4, 2, 1, 3, 5, 7, 9, 11, 13, 15];
    let shuffled = shuffle_disks(&disks, &distribution);
    assert_eq!(
        shuffled,
        vec![9, 8, 10, 7, 11, 6, 12, 5, 13, 4, 14, 3, 15, 2, 16, 1]
    );
}

#[test]
fn test_eval_disks_line_198() {
    let disks = vec!["d1", "d2", "d3", "d4"];
    let errs = vec![None, Some("boom"), None, Some("boom")];
    let evaluated = eval_disks(&disks, &errs).expect("eval");
    assert_eq!(evaluated, vec![Some("d1"), None, Some("d3"), None]);
    assert!(eval_disks(&disks, &errs[..3]).is_none());
}

#[test]
fn test_hash_order_line_217() {
    for x in 1..17 {
        let first = hash_order("prefix/abc", x).expect("hash order");
        assert_eq!(first.len(), x as usize);
        for entry in first {
            assert!((1..=x).contains(&entry));
        }
    }
}

#[test]
fn subtest_test_hash_order_fmt_sprintf_d_line_219() {
    for x in 1..17 {
        let mut counts = vec![0usize; x as usize];
        for i in 0..10_000 {
            let value = hash_order(&format!("prefix/{i:08x}"), x).expect("hash order");
            counts[(value[0] - 1) as usize] += 1;
        }
        assert_eq!(counts.iter().sum::<usize>(), 10_000);
        assert!(counts.iter().all(|count| *count > 0));
    }
}
