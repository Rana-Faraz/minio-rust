// Rust test snapshot derived from cmd/object-api-utils_test.go.

use std::collections::BTreeMap;
use std::io::{Read, Write};

use snap::read::FrameDecoder;
use snap::write::FrameEncoder;

use minio_rust::cmd::{
    clean_metadata, clean_metadata_keys, concat, exclude_for_compression,
    get_complete_multipart_md5, get_compressed_offsets, is_minio_meta_bucket_name,
    is_valid_bucket_name, is_valid_object_name, path_join, path_needs_clean,
    remove_standard_storage_class, CompletePart, CompressConfig, ObjectInfo, ObjectPartInfo,
    COMPRESSION_ALGORITHM_V1, COMPRESSION_ALGORITHM_V2, COMPRESSION_KEY, MINIO_META_BUCKET,
    MINIO_META_MULTIPART_BUCKET, MINIO_META_TMP_BUCKET,
};

pub const SOURCE_FILE: &str = "cmd/object-api-utils_test.go";

#[test]
fn subbenchmark_file_scope_concat_naive_line_61() {
    let data = ["f00ba4", "deadbeef"];
    for _ in 0..100 {
        let mut naive = data[0].to_string();
        naive.push_str(data[1]);
        assert_eq!(naive, concat(&data));
    }
}

#[test]
fn subbenchmark_file_scope_concat_fast_line_68() {
    let data = ["abc", "def", "ghi"];
    for _ in 0..100 {
        assert_eq!(concat(&data), "abcdefghi");
    }
}

#[test]
fn benchmark_concat_implementation_line_77() {
    let data = ["0123456789abcdef", "fedcba9876543210"];
    for _ in 0..200 {
        assert_eq!(concat(&data), "0123456789abcdeffedcba9876543210");
    }
}

#[test]
fn benchmark_path_join_old_line_89() {
    for _ in 0..100 {
        assert_eq!(
            path_join(&["volume", "path/path/path"]),
            "volume/path/path/path"
        );
    }
}

#[test]
fn subbenchmark_benchmark_path_join_old_path_join_line_90() {
    for _ in 0..100 {
        assert_eq!(
            path_join(&["volume", "path/path/path/"]),
            "volume/path/path/path/"
        );
    }
}

#[test]
fn benchmark_path_join_line_100() {
    for _ in 0..100 {
        assert_eq!(
            path_join(&["volume", "./path/../path/path"]),
            "volume/path/path"
        );
    }
}

#[test]
fn subbenchmark_benchmark_path_join_path_join_line_101() {
    for _ in 0..100 {
        assert_eq!(
            path_join(&["/volume//", "path/path/path"]),
            "/volume/path/path/path"
        );
    }
}

#[test]
fn test_path_traversal_exploit_line_112() {
    if !cfg!(windows) {
        return;
    }

    let object_name = r"\../.minio.sys/config/hello.txt";
    assert!(!is_valid_object_name(object_name));
}

#[test]
fn test_is_valid_bucket_name_line_164() {
    let cases = [
        ("lol", true),
        ("1-this-is-valid", true),
        ("1-this-too-is-valid-1", true),
        ("this.works.too.1", true),
        ("1234567", true),
        ("123", true),
        ("s3-eu-west-1.amazonaws.com", true),
        ("ideas-are-more-powerful-than-guns", true),
        ("testbucket", true),
        ("1bucket", true),
        ("bucket1", true),
        ("a.b", true),
        ("ab.a.bc", true),
        ("------", false),
        ("my..bucket", false),
        ("192.168.1.1", false),
        ("$this-is-not-valid-too", false),
        ("contains-$-dollar", false),
        ("contains-^-caret", false),
        ("......", false),
        ("", false),
        ("a", false),
        ("ab", false),
        (".starts-with-a-dot", false),
        ("ends-with-a-dot.", false),
        ("ends-with-a-dash-", false),
        ("-starts-with-a-dash", false),
        ("THIS-BEGINS-WITH-UPPERCASe", false),
        ("tHIS-ENDS-WITH-UPPERCASE", false),
        ("ThisBeginsAndEndsWithUpperCasE", false),
        ("una ñina", false),
        ("dash-.may-not-appear-next-to-dot", false),
        ("dash.-may-not-appear-next-to-dot", false),
        ("dash-.-may-not-appear-next-to-dot", false),
        (
            "lalalallalallalalalallalallalala-thestring-size-is-greater-than-63",
            false,
        ),
    ];
    for (bucket, expected) in cases {
        assert_eq!(is_valid_bucket_name(bucket), expected, "{bucket}");
    }
}

#[test]
fn test_is_valid_object_name_line_224() {
    let valid = [
        "object",
        "The Shining Script <v1>.pdf",
        "Cost Benefit Analysis (2009-2010).pptx",
        "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A",
        "SHØRT",
        "f*le",
        "contains-^-caret",
        "contains-|-pipe",
        "contains-`-tick",
        "..test",
        ".. test",
        ". test",
        ".test",
        "There are far too many object names, and far too few bucket names!",
        "!\"#$%&'()*+,-.／:;<=>?@[\\]^_`{|}~/!\"#$%&'()*+,-.／:;<=>?@[\\]^_`{|}~)",
        "!\"#$%&'()*+,-.／:;<=>?@[\\]^_`{|}~",
        "trailing VT\u{240b}/trailing VT\u{240b}",
        "\u{240b}leading VT/\u{240b}leading VT",
        "~leading tilde",
        "\rleading CR",
        "\nleading LF",
        "\tleading HT",
        "trailing CR\r",
        "trailing LF\n",
        "trailing HT\t",
    ];
    for object in valid {
        assert!(is_valid_object_name(object), "{object}");
    }

    let invalid = [
        "",
        "a/b/c/",
        "../../etc",
        "../../",
        "/../../etc",
        " ../etc",
        "./././",
        "./etc",
        "contains//double/forwardslash",
        "//contains/double-forwardslash-prefix",
    ];
    for object in invalid {
        assert!(!is_valid_object_name(object), "{object}");
    }

    let invalid_utf8 = String::from_utf8_lossy(&[0xff, 0xfe, 0xfd]).into_owned();
    assert!(!is_valid_object_name(&invalid_utf8));
}

#[test]
fn test_get_complete_multipart_md5_line_284() {
    let cases = [
        (
            vec![CompletePart {
                etag: "wrong-md5-hash-string".to_string(),
                part_number: 1,
            }],
            "0deb8cb07527b4b2669c861cb9653607-1",
        ),
        (
            vec![CompletePart {
                etag: "cf1f738a5924e645913c984e0fe3d708".to_string(),
                part_number: 1,
            }],
            "10dc1617fbcf0bd0858048cb96e6bd77-1",
        ),
        (
            vec![
                CompletePart {
                    etag: "cf1f738a5924e645913c984e0fe3d708".to_string(),
                    part_number: 1,
                },
                CompletePart {
                    etag: "9ccbc9a80eee7fb6fdd22441db2aedbd".to_string(),
                    part_number: 2,
                },
            ],
            "0239a86b5266bb624f0ac60ba2aed6c8-2",
        ),
    ];

    for (parts, expected) in cases {
        assert_eq!(get_complete_multipart_md5(&parts), expected);
    }
}

#[test]
fn test_is_minio_meta_bucket_name_line_309() {
    let cases = [
        (MINIO_META_BUCKET, true),
        (MINIO_META_MULTIPART_BUCKET, true),
        (MINIO_META_TMP_BUCKET, true),
        ("mybucket", false),
    ];
    for (bucket, expected) in cases {
        assert_eq!(is_minio_meta_bucket_name(bucket), expected);
    }
}

#[test]
fn test_remove_standard_storage_class_line_347() {
    let input = BTreeMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        (
            "etag".to_string(),
            "de75a98baf2c6aef435b57dd0fc33c86".to_string(),
        ),
        ("x-amz-storage-class".to_string(), "STANDARD".to_string()),
    ]);
    let expected = BTreeMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        (
            "etag".to_string(),
            "de75a98baf2c6aef435b57dd0fc33c86".to_string(),
        ),
    ]);
    assert_eq!(remove_standard_storage_class(&input), expected);
}

#[test]
fn test_clean_metadata_line_378() {
    let input = BTreeMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        (
            "etag".to_string(),
            "de75a98baf2c6aef435b57dd0fc33c86".to_string(),
        ),
        ("x-amz-storage-class".to_string(), "STANDARD".to_string()),
        ("md5Sum".to_string(), "abcde".to_string()),
    ]);
    let expected = BTreeMap::from([(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    )]);
    assert_eq!(clean_metadata(&input), expected);
}

#[test]
fn test_clean_metadata_keys_line_409() {
    let input = BTreeMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        (
            "etag".to_string(),
            "de75a98baf2c6aef435b57dd0fc33c86".to_string(),
        ),
        ("x-amz-storage-class".to_string(), "STANDARD".to_string()),
        ("md5".to_string(), "abcde".to_string()),
    ]);
    let expected = BTreeMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("x-amz-storage-class".to_string(), "STANDARD".to_string()),
    ]);
    assert_eq!(clean_metadata_keys(&input, &["etag", "md5"]), expected);
}

#[test]
fn test_is_compressed_line_443() {
    let cases = [
        (
            ObjectInfo {
                user_defined: BTreeMap::from([
                    (
                        COMPRESSION_KEY.to_string(),
                        COMPRESSION_ALGORITHM_V1.to_string(),
                    ),
                    (
                        "content-type".to_string(),
                        "application/octet-stream".to_string(),
                    ),
                ]),
                ..ObjectInfo::default()
            },
            true,
            false,
        ),
        (
            ObjectInfo {
                user_defined: BTreeMap::from([
                    (
                        COMPRESSION_KEY.to_string(),
                        COMPRESSION_ALGORITHM_V2.to_string(),
                    ),
                    (
                        "content-type".to_string(),
                        "application/octet-stream".to_string(),
                    ),
                ]),
                ..ObjectInfo::default()
            },
            true,
            false,
        ),
        (
            ObjectInfo {
                user_defined: BTreeMap::from([
                    (
                        COMPRESSION_KEY.to_string(),
                        "unknown/compression/type".to_string(),
                    ),
                    (
                        "content-type".to_string(),
                        "application/octet-stream".to_string(),
                    ),
                ]),
                ..ObjectInfo::default()
            },
            true,
            true,
        ),
        (
            ObjectInfo {
                user_defined: BTreeMap::from([(
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                )]),
                ..ObjectInfo::default()
            },
            false,
            false,
        ),
    ];

    for (idx, (object_info, compressed, expect_err)) in cases.into_iter().enumerate() {
        assert_eq!(object_info.is_compressed(), compressed, "{idx}");
        assert_eq!(object_info.is_compressed_ok().is_ok(), !expect_err, "{idx}");
        assert_eq!(
            object_info.is_compressed_ok().unwrap_or(true),
            compressed,
            "{idx}"
        );
    }
}

#[test]
fn subtest_test_is_compressed_strconv_itoa_i_line_513() {
    let object_info = ObjectInfo {
        user_defined: BTreeMap::from([(
            COMPRESSION_KEY.to_string(),
            COMPRESSION_ALGORITHM_V2.to_string(),
        )]),
        ..ObjectInfo::default()
    };
    assert!(object_info
        .is_compressed_ok()
        .expect("compression should be valid"));
}

#[test]
fn test_exclude_for_compression_line_532() {
    let cases = [
        (
            "object.txt",
            BTreeMap::from([("Content-Type".to_string(), "application/zip".to_string())]),
            true,
        ),
        (
            "object.zip",
            BTreeMap::from([("Content-Type".to_string(), "application/XYZ".to_string())]),
            true,
        ),
        (
            "object.json",
            BTreeMap::from([("Content-Type".to_string(), "application/json".to_string())]),
            false,
        ),
        (
            "object.txt",
            BTreeMap::from([("Content-Type".to_string(), "text/plain".to_string())]),
            false,
        ),
        (
            "object",
            BTreeMap::from([("Content-Type".to_string(), "text/something".to_string())]),
            false,
        ),
    ];

    let config = CompressConfig {
        enabled: true,
        ..CompressConfig::default()
    };
    for (object, headers, expected) in cases {
        assert_eq!(
            exclude_for_compression(&headers, object, &config),
            expected,
            "{object}"
        );
    }
}

#[test]
fn benchmark_get_part_file_with_trie_line_585() {
    let parts: Vec<String> = (1..=10_000)
        .map(|i| format!("{i:05}.8a034f82cb9cb31140d87d3ce2a9ede3.67108864"))
        .collect();
    for i in 1..=100 {
        let prefix = format!("{i:05}.8a034f82cb9cb31140d87d3ce2a9ede3.");
        assert!(parts.iter().any(|entry| entry.starts_with(&prefix)));
    }
}

#[test]
fn test_get_actual_size_line_603() {
    let cases = [
        (
            ObjectInfo {
                user_defined: BTreeMap::from([(
                    COMPRESSION_KEY.to_string(),
                    COMPRESSION_ALGORITHM_V2.to_string(),
                )]),
                parts: vec![
                    ObjectPartInfo {
                        number: 1,
                        size: 39_235_668,
                        etag: String::new(),
                        actual_size: 67_108_864,
                    },
                    ObjectPartInfo {
                        number: 2,
                        size: 19_177_372,
                        etag: String::new(),
                        actual_size: 32_891_137,
                    },
                ],
                size: 100_000_001,
                ..ObjectInfo::default()
            },
            100_000_001,
        ),
        (
            ObjectInfo {
                user_defined: BTreeMap::from([
                    (
                        COMPRESSION_KEY.to_string(),
                        COMPRESSION_ALGORITHM_V2.to_string(),
                    ),
                    (
                        "X-Minio-Internal-actual-size".to_string(),
                        "841".to_string(),
                    ),
                ]),
                size: 841,
                ..ObjectInfo::default()
            },
            841,
        ),
        (
            ObjectInfo {
                user_defined: BTreeMap::from([(
                    COMPRESSION_KEY.to_string(),
                    COMPRESSION_ALGORITHM_V2.to_string(),
                )]),
                size: 100,
                ..ObjectInfo::default()
            },
            -1,
        ),
    ];
    for (object_info, expected) in cases {
        assert_eq!(object_info.get_actual_size().expect("size"), expected);
    }
}

#[test]
fn test_get_compressed_offsets_line_664() {
    let object_info = ObjectInfo {
        parts: vec![
            ObjectPartInfo {
                number: 1,
                size: 39_235_668,
                etag: String::new(),
                actual_size: 67_108_864,
            },
            ObjectPartInfo {
                number: 2,
                size: 19_177_372,
                etag: String::new(),
                actual_size: 32_891_137,
            },
        ],
        ..ObjectInfo::default()
    };

    let cases = [
        (79_109_865, 39_235_668, 12_001_001, 1usize),
        (19_109_865, 0, 19_109_865, 0usize),
        (0, 0, 0, 0usize),
    ];
    for (offset, expected_start, expected_part_skip, expected_first_part) in cases {
        let (start, skip, first_part) = get_compressed_offsets(&object_info, offset);
        assert_eq!(start, expected_start);
        assert_eq!(skip, expected_part_skip);
        assert_eq!(first_part, expected_first_part);
    }
}

#[test]
fn test_s2_compress_reader_line_742() {
    let data = b"hello, world!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
    let mut encoded = Vec::new();
    {
        let mut writer = FrameEncoder::new(&mut encoded);
        writer.write_all(data).expect("encode");
        writer.flush().expect("flush");
    }

    let mut decoded = Vec::new();
    FrameDecoder::new(encoded.as_slice())
        .read_to_end(&mut decoded)
        .expect("decode");
    assert_eq!(decoded, data);
}

#[test]
fn subtest_test_s2_compress_reader_tt_name_line_754() {
    let cases = [
        ("empty", Vec::new()),
        (
            "small",
            b"hello, world!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!".to_vec(),
        ),
        ("large", b"hello, world".repeat(10_000)),
    ];
    for (name, data) in cases {
        let mut encoded = Vec::new();
        {
            let mut writer = FrameEncoder::new(&mut encoded);
            writer.write_all(&data).expect("encode");
            writer.flush().expect("flush");
        }

        let mut decoded = Vec::new();
        FrameDecoder::new(encoded.as_slice())
            .read_to_end(&mut decoded)
            .expect("decode");
        assert_eq!(decoded, data, "{name}");
    }
}

#[test]
fn test_path_needs_clean_line_818() {
    let cases = [
        ("", true),
        ("abc", false),
        ("abc/def", false),
        ("a/b/c", false),
        (".", true),
        ("..", true),
        ("../..", true),
        ("../../abc", true),
        ("/abc", false),
        ("/abc/def", false),
        ("/", false),
        ("abc/", true),
        ("abc/def/", true),
        ("a/b/c/", true),
        ("./", true),
        ("../", true),
        ("../../", true),
        ("/abc/", true),
        ("abc//def//ghi", true),
        ("//abc", true),
        ("///abc", true),
        ("//abc//", true),
        ("abc//", true),
        ("abc/./def", true),
        ("/./abc/def", true),
        ("abc/.", true),
        ("abc/def/ghi/../jkl", true),
        ("abc/def/../ghi/../jkl", true),
        ("abc/def/..", true),
        ("abc/def/../..", true),
        ("/abc/def/../..", true),
        ("abc/def/../../..", true),
        ("/abc/def/../../..", true),
        ("abc/def/../../../ghi/jkl/../../../mno", true),
        ("abc/./../def", true),
        ("abc//./../def", true),
        ("abc/../../././../def", true),
    ];
    for (path, expected) in cases {
        assert_eq!(path_needs_clean(path.as_bytes()), expected, "{path}");
    }
}
