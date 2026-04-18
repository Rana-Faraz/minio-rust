use std::collections::BTreeMap;

use minio_rust::cmd::{
    ErasureInfo, FileInfo, MetaCacheEntriesSorted, MetaCacheEntryExt, MetadataResolutionParams,
    ObjectPartInfo, XlMetaV2, SLASH_SEPARATOR,
};

pub const SOURCE_FILE: &str = "cmd/metacache-entries_test.go";

fn sample_names() -> Vec<&'static str> {
    vec![
        "src/compress/bzip2/",
        "src/compress/bzip2/bit_reader.go",
        "src/compress/bzip2/bzip2.go",
        "src/compress/bzip2/testdata/",
        "src/compress/bzip2/testdata/pass-random1.bin",
        "src/compress/flate/",
        "src/compress/flate/deflate.go",
        "src/compress/flate/testdata/",
        "src/compress/flate/testdata/huffman-zero.in",
        "src/compress/zlib/",
        "src/compress/zlib/example_test.go",
        "src/compress/zlib/reader.go",
        "src/compress/zlib/reader_test.go",
        "src/compress/zlib/writer.go",
        "src/compress/zlib/writer_test.go",
    ]
}

fn make_dir_entry(name: &str) -> MetaCacheEntryExt {
    MetaCacheEntryExt {
        name: name.to_string(),
        metadata: Vec::new(),
        cached: None,
        reusable: false,
    }
}

fn sample_file_info(name: &str, version_id: &str, mod_time: i64) -> FileInfo {
    FileInfo {
        volume: "bucket".to_string(),
        name: name.to_string(),
        version_id: version_id.to_string(),
        is_latest: true,
        deleted: false,
        transition_status: String::new(),
        transitioned_obj_name: String::new(),
        transition_tier: String::new(),
        transition_version_id: String::new(),
        expire_restored: false,
        data_dir: format!("data-{version_id}"),
        xlv1: false,
        mod_time,
        size: 128,
        mode: 0,
        written_by_version: 0,
        metadata: Some(BTreeMap::from([(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        )])),
        parts: Some(vec![ObjectPartInfo {
            number: 1,
            size: 128,
            actual_size: 128,
            etag: String::new(),
        }]),
        erasure: ErasureInfo {
            algorithm: "reedsolomon".to_string(),
            data_blocks: 2,
            parity_blocks: 2,
            block_size: 1024,
            index: 1,
            distribution: Some(vec![1, 2, 3, 4]),
        },
        mark_deleted: false,
        replication_state: Default::default(),
        data: None,
        num_versions: 0,
        successor_mod_time: 0,
        fresh: false,
        idx: 0,
        checksum: None,
        versioned: true,
    }
}

fn make_object_entry(name: &str, versions: &[(&str, i64)]) -> MetaCacheEntryExt {
    let mut xl = XlMetaV2::default();
    for (version_id, mod_time) in versions {
        xl.add_version(sample_file_info(name, version_id, *mod_time))
            .expect("add version");
    }
    xl.sort_by_mod_time();
    MetaCacheEntryExt {
        name: name.to_string(),
        metadata: xl.append_to(None).expect("encode xl"),
        cached: None,
        reusable: false,
    }
}

fn load_sample_entries() -> MetaCacheEntriesSorted {
    let mut entries = Vec::new();
    for name in sample_names() {
        if name.ends_with('/') {
            entries.push(make_dir_entry(name));
        } else {
            entries.push(make_object_entry(name, &[("v1", 1_700_000_000)]));
        }
    }
    MetaCacheEntriesSorted::new(entries)
}

#[test]
fn test_meta_cache_entries_sort_line_29() {
    let mut entries = load_sample_entries();
    assert!(entries.is_sorted());

    let mut reversed = entries.shallow_clone();
    reversed.sort();
    reversed.entries_mut().reverse();
    assert!(!reversed.is_sorted());

    reversed.sort();
    assert!(reversed.is_sorted());
    assert_eq!(
        reversed.names(),
        sample_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );

    entries.sort();
    assert_eq!(
        entries.names(),
        sample_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_meta_cache_entries_forward_to_line_58() {
    let mut entries = load_sample_entries();
    entries.forward_to("src/compress/zlib/reader_test.go");
    assert_eq!(
        entries.names(),
        vec![
            "src/compress/zlib/reader_test.go",
            "src/compress/zlib/writer.go",
            "src/compress/zlib/writer_test.go",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );

    let mut prefix_entries = load_sample_entries();
    prefix_entries.forward_to("src/compress/zlib/reader_t");
    assert_eq!(prefix_entries.names(), entries.names());
}

#[test]
fn test_meta_cache_entries_merge_line_77() {
    let original = load_sample_entries();
    let mut left = original.shallow_clone();
    let mut right = original.shallow_clone();
    for entry in right.entries_mut() {
        if entry.is_object() {
            entry.metadata = b"other-metadata".to_vec();
        }
    }

    left.merge(right, -1);
    let mut want = sample_names()
        .into_iter()
        .flat_map(|name| {
            if name.ends_with('/') {
                vec![name.to_string()]
            } else {
                vec![name.to_string(), name.to_string()]
            }
        })
        .collect::<Vec<_>>();
    want.sort();
    assert_eq!(left.names(), want);
}

#[test]
fn test_meta_cache_entries_filter_objects_line_102() {
    let mut data = load_sample_entries();
    data.filter_objects_only();
    assert_eq!(
        data.names(),
        vec![
            "src/compress/bzip2/bit_reader.go",
            "src/compress/bzip2/bzip2.go",
            "src/compress/bzip2/testdata/pass-random1.bin",
            "src/compress/flate/deflate.go",
            "src/compress/flate/testdata/huffman-zero.in",
            "src/compress/zlib/example_test.go",
            "src/compress/zlib/reader.go",
            "src/compress/zlib/reader_test.go",
            "src/compress/zlib/writer.go",
            "src/compress/zlib/writer_test.go",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );
}

#[test]
fn test_meta_cache_entries_filter_prefixes_line_112() {
    let mut data = load_sample_entries();
    data.filter_prefixes_only();
    assert_eq!(
        data.names(),
        vec![
            "src/compress/bzip2/",
            "src/compress/bzip2/testdata/",
            "src/compress/flate/",
            "src/compress/flate/testdata/",
            "src/compress/zlib/",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );
}

#[test]
fn test_meta_cache_entries_filter_recursive_line_122() {
    let mut data = load_sample_entries();
    data.filter_recursive_entries("src/compress/bzip2/", SLASH_SEPARATOR);
    assert_eq!(
        data.names(),
        vec![
            "src/compress/bzip2/",
            "src/compress/bzip2/bit_reader.go",
            "src/compress/bzip2/bzip2.go",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );
}

#[test]
fn test_meta_cache_entries_filter_recursive_root_line_132() {
    let mut data = load_sample_entries();
    data.filter_recursive_entries("", SLASH_SEPARATOR);
    assert!(data.names().is_empty());
}

#[test]
fn test_meta_cache_entries_filter_recursive_root_sep_line_142() {
    let mut data = load_sample_entries();
    data.filter_recursive_entries("", "bzip2/");
    assert_eq!(
        data.names(),
        vec![
            "src/compress/flate/",
            "src/compress/flate/deflate.go",
            "src/compress/flate/testdata/",
            "src/compress/flate/testdata/huffman-zero.in",
            "src/compress/zlib/",
            "src/compress/zlib/example_test.go",
            "src/compress/zlib/reader.go",
            "src/compress/zlib/reader_test.go",
            "src/compress/zlib/writer.go",
            "src/compress/zlib/writer_test.go",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );
}

#[test]
fn test_meta_cache_entries_filter_prefix_line_153() {
    let mut data = load_sample_entries();
    data.filter_prefix("src/compress/bzip2/");
    assert_eq!(
        data.names(),
        vec![
            "src/compress/bzip2/",
            "src/compress/bzip2/bit_reader.go",
            "src/compress/bzip2/bzip2.go",
            "src/compress/bzip2/testdata/",
            "src/compress/bzip2/testdata/pass-random1.bin",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );
}

#[test]
fn test_meta_cache_entry_is_in_dir_line_163() {
    let cases = [
        ("src/file", "src/", SLASH_SEPARATOR, true),
        ("src/dir/", "src/", SLASH_SEPARATOR, true),
        ("src/dir/somewhere.ext", "src/", SLASH_SEPARATOR, false),
        ("src/dir/somewhere/", "src/", SLASH_SEPARATOR, false),
        ("doc/", "", SLASH_SEPARATOR, true),
        ("word.doc", "", SLASH_SEPARATOR, true),
    ];
    for (entry, dir, sep, want) in cases {
        let entry = MetaCacheEntryExt {
            name: entry.to_string(),
            metadata: if entry.ends_with('/') {
                Vec::new()
            } else {
                b"meta".to_vec()
            },
            cached: None,
            reusable: false,
        };
        assert_eq!(entry.is_in_dir(dir, sep), want);
    }
}

#[test]
fn subtest_test_meta_cache_entry_is_in_dir_tt_test_name_line_215() {
    let tests = [
        ("basic-file", "src/file", "src/", true),
        ("root-dir", "doc/", "", true),
        ("deeper-file", "src/dir/file", "src/", false),
    ];
    for (name, entry_name, dir, want) in tests {
        let entry = MetaCacheEntryExt {
            name: entry_name.to_string(),
            metadata: if entry_name.ends_with('/') {
                Vec::new()
            } else {
                b"meta".to_vec()
            },
            cached: None,
            reusable: false,
        };
        assert_eq!(entry.is_in_dir(dir, SLASH_SEPARATOR), want, "{name}");
    }
}

#[test]
fn test_meta_cache_entries_resolve_line_226() {
    let exact = make_object_entry("testobject", &[("v1", 100)]);
    let newer = make_object_entry("testobject", &[("v1", 100), ("v2", 200)]);
    let older = make_object_entry("testobject", &[("v1", 90)]);
    let directory = make_dir_entry("testobject/");

    let selected = MetaCacheEntriesSorted::resolve(
        &[exact.clone(), exact.clone(), exact.clone(), older.clone()],
        &MetadataResolutionParams {
            dir_quorum: 3,
            obj_quorum: 3,
            strict: false,
        },
    )
    .expect("resolve exact quorum");
    assert_eq!(selected.metadata, exact.metadata);

    let merged = MetaCacheEntriesSorted::resolve(
        &[exact.clone(), newer.clone()],
        &MetadataResolutionParams {
            dir_quorum: 1,
            obj_quorum: 1,
            strict: false,
        },
    )
    .expect("resolve merged");
    let merged_meta = merged.xlmeta().expect("merged meta");
    assert_eq!(merged_meta.versions.len(), 2);

    let none = MetaCacheEntriesSorted::resolve(
        &[exact.clone(), older.clone()],
        &MetadataResolutionParams {
            dir_quorum: 3,
            obj_quorum: 3,
            strict: true,
        },
    );
    assert!(none.is_none());

    let dir = MetaCacheEntriesSorted::resolve(
        &[directory.clone(), directory.clone(), exact.clone()],
        &MetadataResolutionParams {
            dir_quorum: 2,
            obj_quorum: 2,
            strict: false,
        },
    )
    .expect("resolve directory");
    assert!(dir.is_dir());
}

#[test]
fn subtest_test_meta_cache_entries_resolve_fmt_sprintf_test_d_s_run_d_line_637() {
    let base = make_object_entry("testobject", &[("v1", 100)]);
    let variant = make_object_entry("testobject", &[("v1", 100), ("v2", 150)]);
    let inputs = [
        vec![base.clone(), base.clone(), variant.clone()],
        vec![variant.clone(), base.clone(), base.clone()],
        vec![base.clone(), variant.clone(), base.clone()],
    ];

    for (idx, entries) in inputs.into_iter().enumerate() {
        let selected = MetaCacheEntriesSorted::resolve(
            &entries,
            &MetadataResolutionParams {
                dir_quorum: 2,
                obj_quorum: 2,
                strict: false,
            },
        )
        .expect("resolved");
        assert_eq!(selected.metadata, base.metadata, "run {idx}");
    }
}
