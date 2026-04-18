use std::collections::HashMap;
use std::io;

use minio_rust::internal::etag::{
    decrypt, equal, from_content_md5, multipart, parse, wrap, ETag, Reader, Tagger,
};

pub const SOURCE_FILE: &str = "internal/etag/etag_test.go";

fn must(value: &str) -> ETag {
    parse(value).expect("etag should parse")
}

#[test]
fn parse_matches_reference_cases() {
    let cases = [
        ("3b83ef96387f1465", Some(ETag::new(vec![59, 131, 239, 150, 56, 127, 20, 101])), false),
        (
            "3b83ef96387f14655fc854ddc3c6bd57",
            Some(ETag::new(vec![
                59, 131, 239, 150, 56, 127, 20, 101, 95, 200, 84, 221, 195, 198, 189, 87,
            ])),
            false,
        ),
        (
            "\"3b83ef96387f14655fc854ddc3c6bd57\"",
            Some(ETag::new(vec![
                59, 131, 239, 150, 56, 127, 20, 101, 95, 200, 84, 221, 195, 198, 189, 87,
            ])),
            false,
        ),
        (
            "ceb8853ddc5086cc4ab9e149f8f09c88-1",
            Some(ETag::new(vec![
                206, 184, 133, 61, 220, 80, 134, 204, 74, 185, 225, 73, 248, 240, 156, 136,
                45, 49,
            ])),
            false,
        ),
        (
            "\"ceb8853ddc5086cc4ab9e149f8f09c88-2\"",
            Some(ETag::new(vec![
                206, 184, 133, 61, 220, 80, 134, 204, 74, 185, 225, 73, 248, 240, 156, 136,
                45, 50,
            ])),
            false,
        ),
        (
            "90402c78d2dccddee1e9e86222ce2c6361675f3529d26000ae2e900ff216b3cb59e130e092d8a2981e776f4d0bd60941",
            Some(ETag::new(vec![
                144, 64, 44, 120, 210, 220, 205, 222, 225, 233, 232, 98, 34, 206, 44, 99, 97,
                103, 95, 53, 41, 210, 96, 0, 174, 46, 144, 15, 242, 22, 179, 203, 89, 225, 48,
                224, 146, 216, 162, 152, 30, 119, 111, 77, 11, 214, 9, 65,
            ])),
            false,
        ),
        ("\"3b83ef96387f14655fc854ddc3c6bd57", None, true),
        ("ceb8853ddc5086cc4ab9e149f8f09c88-", None, true),
        ("ceb8853ddc5086cc4ab9e149f8f09c88-2a", None, true),
        ("ceb8853ddc5086cc4ab9e149f8f09c88-2-1", None, true),
        ("90402c78d2dccddee1e9e86222ce2c-1", None, true),
        (
            "90402c78d2dccddee1e9e86222ce2c6361675f3529d26000ae2e900ff216b3cb59e130e092d8a2981e776f4d0bd60941-1",
            None,
            true,
        ),
    ];

    for (input, expected, should_fail) in cases {
        let result = parse(input);
        assert_eq!(result.is_err(), should_fail, "parse case {input}");
        if let Some(expected) = expected {
            assert!(equal(&result.expect("etag should parse"), &expected));
        }
    }
}

#[test]
fn string_matches_reference_cases() {
    let cases = [
        (
            ETag::new(vec![59, 131, 239, 150, 56, 127, 20, 101]),
            "3b83ef96387f1465",
        ),
        (
            ETag::new(vec![
                59, 131, 239, 150, 56, 127, 20, 101, 95, 200, 84, 221, 195, 198, 189, 87,
            ]),
            "3b83ef96387f14655fc854ddc3c6bd57",
        ),
        (
            ETag::new(vec![
                206, 184, 133, 61, 220, 80, 134, 204, 74, 185, 225, 73, 248, 240, 156, 136, 45, 49,
            ]),
            "ceb8853ddc5086cc4ab9e149f8f09c88-1",
        ),
        (
            ETag::new(vec![
                206, 184, 133, 61, 220, 80, 134, 204, 74, 185, 225, 73, 248, 240, 156, 136, 45, 50,
            ]),
            "ceb8853ddc5086cc4ab9e149f8f09c88-2",
        ),
    ];

    for (etag, expected) in cases {
        assert_eq!(etag.to_string(), expected);
    }
}

#[test]
fn equal_matches_reference_cases() {
    let cases = [
        (
            "3b83ef96387f14655fc854ddc3c6bd57",
            "3b83ef96387f14655fc854ddc3c6bd57",
            true,
        ),
        (
            "3b83ef96387f14655fc854ddc3c6bd57",
            "\"3b83ef96387f14655fc854ddc3c6bd57\"",
            true,
        ),
        (
            "3b83ef96387f14655fc854ddc3c6bd57",
            "3b83ef96387f14655fc854ddc3c6bd57-2",
            false,
        ),
        (
            "3b83ef96387f14655fc854ddc3c6bd57",
            "ceb8853ddc5086cc4ab9e149f8f09c88",
            false,
        ),
    ];

    for (a, b, expected) in cases {
        let a = parse(a).expect("etag A should parse");
        let b = parse(b).expect("etag B should parse");
        assert_eq!(equal(&a, &b), expected);
    }
}

#[test]
fn reader_matches_reference_cases() {
    let cases = [
        ("", must("d41d8cd98f00b204e9800998ecf8427e")),
        (" ", must("7215ee9c7d9dc229d2921a40e899ec5f")),
        ("Hello World", must("b10a8db164e0754105b7a99be72e3fe5")),
    ];

    for (content, expected) in cases {
        let mut reader = Reader::new(io::Cursor::new(content.as_bytes()), expected.clone(), None);
        io::copy(&mut reader, &mut io::sink()).expect("copy should succeed");
        assert!(equal(&reader.etag(), &expected));
    }
}

#[test]
fn multipart_matches_reference_cases() {
    let cases = [
        (vec![], ETag::default()),
        (
            vec![must("b10a8db164e0754105b7a99be72e3fe5")],
            must("7b976cc68452e003eec7cb0eb631a19a-1"),
        ),
        (
            vec![
                must("5f363e0e58a95f06cbe9bbc662c5dfb6"),
                must("5f363e0e58a95f06cbe9bbc662c5dfb6"),
            ],
            must("a7d414b9133d6483d9a1c4e04e856e3b-2"),
        ),
        (
            vec![
                must("5f363e0e58a95f06cbe9bbc662c5dfb6"),
                must("a096eb5968d607c2975fb2c4af9ab225"),
                must("b10a8db164e0754105b7a99be72e3fe5"),
            ],
            must("9a0d1febd9265f59f368ceb652770bc2-3"),
        ),
        (
            vec![
                must("5f363e0e58a95f06cbe9bbc662c5dfb6"),
                must("5f363e0e58a95f06cbe9bbc662c5dfb6"),
                must("ceb8853ddc5086cc4ab9e149f8f09c88-1"),
            ],
            must("a7d414b9133d6483d9a1c4e04e856e3b-2"),
        ),
        (
            vec![
                must("90402c78d2dccddee1e9e86222ce2c6361675f3529d26000ae2e900ff216b3cb59e130e092d8a2981e776f4d0bd60941"),
                must("5f363e0e58a95f06cbe9bbc662c5dfb6"),
                must("5f363e0e58a95f06cbe9bbc662c5dfb6"),
            ],
            must("a7d414b9133d6483d9a1c4e04e856e3b-2"),
        ),
    ];

    for (etags, expected) in cases {
        assert!(equal(&multipart(&etags), &expected));
    }
}

#[test]
fn is_encrypted_matches_reference_cases() {
    let cases = [
        (
            "20000f00db2d90a7b40782d4cff2b41a7799fc1e7ead25972db65150118dfbe2ba76a3c002da28f85c840cd2001a28a9",
            true,
        ),
        ("3b83ef96387f14655fc854ddc3c6bd57", false),
        ("7b976cc68452e003eec7cb0eb631a19a-1", false),
        ("a7d414b9133d6483d9a1c4e04e856e3b-2", false),
        ("7b976cc68452e003eec7cb0eb631a19a-10000", false),
    ];

    for (input, expected) in cases {
        let etag = parse(input).expect("etag should parse");
        assert_eq!(etag.is_encrypted(), expected);
    }
}

#[test]
fn format_matches_reference_cases() {
    let cases = [
        ("3b83ef96387f14655fc854ddc3c6bd57", "3b83ef96387f14655fc854ddc3c6bd57"),
        ("7b976cc68452e003eec7cb0eb631a19a-1", "7b976cc68452e003eec7cb0eb631a19a-1"),
        ("a7d414b9133d6483d9a1c4e04e856e3b-2", "a7d414b9133d6483d9a1c4e04e856e3b-2"),
        (
            "7b976cc68452e003eec7cb0eb631a19a-10000",
            "7b976cc68452e003eec7cb0eb631a19a-10000",
        ),
        (
            "20000f00db2d90a7b40782d4cff2b41a7799fc1e7ead25972db65150118dfbe2ba76a3c002da28f85c840cd2001a28a9",
            "ba76a3c002da28f85c840cd2001a28a9",
        ),
    ];

    for (input, expected) in cases {
        let etag = parse(input).expect("etag should parse");
        assert_eq!(etag.format().to_string(), expected);
    }
}

#[test]
fn from_content_md5_matches_reference_cases() {
    let headers0 = HashMap::new();

    let mut headers1 = HashMap::new();
    headers1.insert(
        "Content-Md5".to_owned(),
        vec!["1B2M2Y8AsgTpgAmY7PhCfg==".to_owned()],
    );

    let mut headers2 = HashMap::new();
    headers2.insert(
        "Content-Md5".to_owned(),
        vec!["sQqNsWTgdUEFt6mb5y4/5Q==".to_owned()],
    );

    let mut headers3 = HashMap::new();
    headers3.insert(
        "Content-MD5".to_owned(),
        vec!["1B2M2Y8AsgTpgAmY7PhCfg==".to_owned()],
    );

    let mut headers4 = HashMap::new();
    headers4.insert(
        "Content-Md5".to_owned(),
        vec![
            "sQqNsWTgdUEFt6mb5y4/5Q==".to_owned(),
            "1B2M2Y8AsgTpgAmY7PhCfg==".to_owned(),
        ],
    );

    let mut headers5 = HashMap::new();
    headers5.insert("Content-Md5".to_owned(), vec!["".to_owned()]);

    let mut headers6 = HashMap::new();
    headers6.insert(
        "Content-Md5".to_owned(),
        vec!["".to_owned(), "sQqNsWTgdUEFt6mb5y4/5Q==".to_owned()],
    );

    let mut headers7 = HashMap::new();
    headers7.insert(
        "Content-Md5".to_owned(),
        vec!["d41d8cd98f00b204e9800998ecf8427e".to_owned()],
    );

    let cases = [
        (headers0, Some(ETag::default()), false),
        (
            headers1,
            Some(must("d41d8cd98f00b204e9800998ecf8427e")),
            false,
        ),
        (
            headers2,
            Some(must("b10a8db164e0754105b7a99be72e3fe5")),
            false,
        ),
        (headers3, Some(ETag::default()), false),
        (
            headers4,
            Some(must("b10a8db164e0754105b7a99be72e3fe5")),
            false,
        ),
        (headers5, None, true),
        (headers6, None, true),
        (headers7, None, true),
    ];

    for (headers, expected, should_fail) in cases {
        let result = from_content_md5(&headers);
        assert_eq!(result.is_err(), should_fail);
        if let Some(expected) = expected {
            assert!(equal(
                &result.expect("content-md5 should decode"),
                &expected
            ));
        }
    }
}

#[test]
fn decrypt_matches_reference_cases() {
    let cases = [
        (
            vec![0_u8; 32],
            must("3b83ef96387f14655fc854ddc3c6bd57"),
            must("3b83ef96387f14655fc854ddc3c6bd57"),
        ),
        (
            vec![0_u8; 32],
            must("7b976cc68452e003eec7cb0eb631a19a-1"),
            must("7b976cc68452e003eec7cb0eb631a19a-1"),
        ),
        (
            vec![0_u8; 32],
            must("7b976cc68452e003eec7cb0eb631a19a-10000"),
            must("7b976cc68452e003eec7cb0eb631a19a-10000"),
        ),
        (
            vec![0_u8; 32],
            must("20000f00f2cc184414bc982927ec56abb7e18426faa205558982e9a8125c1370a9cf5754406e428b3343f21ee1125965"),
            must("6d6cdccb9a7498c871bde8eab2f49141"),
        ),
    ];

    for (key, etag, expected) in cases {
        let decrypted = decrypt(&key, &etag).expect("etag should decrypt");
        assert!(equal(&decrypted, &expected));
    }
}

#[test]
fn wrap_reader_implements_tagger() {
    let reader = wrap(io::Cursor::new(Vec::<u8>::new()), None);
    let tagger: &dyn Tagger = &reader;
    assert_eq!(tagger.etag(), ETag::default());
}
