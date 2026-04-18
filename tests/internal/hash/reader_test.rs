use std::io::{self, Cursor, Read};

use base64::Engine;
use minio_rust::internal::hash::Reader;
use minio_rust::internal::ioutil;

pub const SOURCE_FILE: &str = "internal/hash/reader_test.go";

fn read_all<R: Read>(mut reader: R) -> Result<(), String> {
    let mut sink = Vec::new();
    io::copy(&mut reader, &mut sink)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn must_reader(
    src: Cursor<Vec<u8>>,
    size: i64,
    md5_hex: &str,
    sha256_hex: &str,
    actual_size: i64,
) -> Reader<Cursor<Vec<u8>>> {
    Reader::new(src, size, md5_hex, sha256_hex, actual_size).expect("reader should build")
}

#[test]
fn hash_reader_helper_methods_match_reference_behavior() {
    let mut reader = Reader::new(
        Cursor::new(b"abcd".to_vec()),
        4,
        "e2fc714c4727ee9395f324cd2e7f331f",
        "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
        4,
    )
    .expect("reader should initialize");

    read_all(&mut reader).expect("reader should drain");
    assert_eq!(
        hex::encode(reader.md5_current()),
        "e2fc714c4727ee9395f324cd2e7f331f"
    );
    assert_eq!(
        reader.sha256_hex_string(),
        "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589"
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(reader.md5_current()),
        "4vxxTEcn7pOV8yTNLn8zHw=="
    );
    assert_eq!(reader.size(), 4);
    assert_eq!(reader.actual_size(), 4);
    assert_eq!(
        reader.sha256(),
        hex::decode("88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589")
            .expect("sha should decode")
    );
}

#[test]
fn hash_reader_verification_matches_reference_matrix() {
    let correct_md5 = "e2fc714c4727ee9395f324cd2e7f331f";
    let correct_sha256 = "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589";
    let wrong_md5 = "0773da587b322af3a8718cb418a715ce";
    let wrong_sha256 = "50d858e0985ecc7f60418aaf0cc5ab587f42c2570a884095a9e8ccacd0f6545c";
    let empty_md5 = "d41d8cd98f00b204e9800998ecf8427f";

    let cases = [
        ("no verification", read_all(Reader::new(Cursor::new(b"abcd".to_vec()), 4, "", "", 4).unwrap()), None),
        (
            "md5 mismatch",
            read_all(Reader::new(Cursor::new(b"abcd".to_vec()), 4, empty_md5, "", 4).unwrap()),
            Some(format!("Bad digest: Expected {empty_md5} does not match calculated {correct_md5}")),
        ),
        (
            "sha256 mismatch",
            read_all(Reader::new(Cursor::new(b"abcd".to_vec()), 4, "", "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580", 4).unwrap()),
            Some("Bad sha256: Expected 88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580 does not match calculated 88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589".to_owned()),
        ),
        (
            "nested ok",
            read_all(Reader::merge(must_reader(Cursor::new(b"abcd".to_vec()), 4, "", "", 4), 4, "", "", 4).unwrap()),
            None,
        ),
        (
            "nested sha mismatch",
            read_all(Reader::merge(must_reader(Cursor::new(b"abcd".to_vec()), 4, "", "", 4), 4, "", wrong_sha256, 4).unwrap()),
            Some(format!("Bad sha256: Expected {wrong_sha256} does not match calculated {correct_sha256}")),
        ),
        (
            "nested sha ok",
            read_all(Reader::merge(must_reader(Cursor::new(b"abcd".to_vec()), 4, "", "", 4), 4, "", correct_sha256, 4).unwrap()),
            None,
        ),
        (
            "nested truncated",
            read_all(Reader::merge(must_reader(Cursor::new(b"abcd-more-stuff-to-be ignored".to_vec()), 4, "", "", 4), 4, "", correct_sha256, -1).unwrap()),
            Some(ioutil::ERR_OVERREAD.to_owned()),
        ),
        (
            "nested truncated swapped",
            read_all(Reader::merge(must_reader(Cursor::new(b"abcd-more-stuff-to-be ignored".to_vec()), 4, "", "", -1), 4, "", correct_sha256, -1).unwrap()),
            Some(ioutil::ERR_OVERREAD.to_owned()),
        ),
        (
            "nested md5 mismatch",
            read_all(Reader::merge(must_reader(Cursor::new(b"abcd".to_vec()), 4, "", "", 4), 4, wrong_md5, "", 4).unwrap()),
            Some(format!("Bad digest: Expected {wrong_md5} does not match calculated {correct_md5}")),
        ),
        (
            "truncated sha256",
            read_all(Reader::new(Cursor::new(b"abcd-morethan-4-bytes".to_vec()), 4, "", correct_sha256, 4).unwrap()),
            Some(ioutil::ERR_OVERREAD.to_owned()),
        ),
        (
            "nested md5 ok",
            read_all(Reader::merge(must_reader(Cursor::new(b"abcd".to_vec()), 4, "", "", 4), 4, correct_md5, "", 4).unwrap()),
            None,
        ),
        (
            "truncated md5",
            read_all(Reader::new(Cursor::new(b"abcd-morethan-4-bytes".to_vec()), 4, correct_md5, "", 4).unwrap()),
            Some(ioutil::ERR_OVERREAD.to_owned()),
        ),
        (
            "nested truncated md5",
            read_all(Reader::merge(must_reader(Cursor::new(b"abcd-morestuff".to_vec()), -1, "", "", -1), 4, correct_md5, "", 4).unwrap()),
            Some(ioutil::ERR_OVERREAD.to_owned()),
        ),
    ];

    for (name, result, expected_error) in cases {
        match expected_error {
            None => assert!(result.is_ok(), "case {name}: {result:?}"),
            Some(expected) => assert_eq!(result.unwrap_err(), expected, "case {name}"),
        }
    }
}

#[test]
fn hash_reader_invalid_arguments_match_reference_matrix() {
    let cases = [
        (
            "invalid md5",
            Reader::new(Cursor::new(b"abcd".to_vec()), 4, "invalid-md5", "", 4).map(|_| ()),
            false,
        ),
        (
            "invalid sha256",
            Reader::new(Cursor::new(b"abcd".to_vec()), 4, "", "invalid-sha256", 4).map(|_| ()),
            false,
        ),
        (
            "nested merge ok",
            Reader::merge(
                must_reader(Cursor::new(b"abcd".to_vec()), 4, "", "", 4),
                4,
                "",
                "",
                4,
            )
            .map(|_| ()),
            true,
        ),
        (
            "mismatching sha256",
            Reader::merge(
                must_reader(
                    Cursor::new(b"abcd".to_vec()),
                    4,
                    "",
                    "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
                    4,
                ),
                4,
                "",
                "50d858e0985ecc7f60418aaf0cc5ab587f42c2570a884095a9e8ccacd0f6545c",
                4,
            )
            .map(|_| ()),
            false,
        ),
        (
            "matching sha256",
            Reader::merge(
                must_reader(
                    Cursor::new(b"abcd".to_vec()),
                    4,
                    "",
                    "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
                    4,
                ),
                4,
                "",
                "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
                4,
            )
            .map(|_| ()),
            true,
        ),
        (
            "mismatching md5",
            Reader::merge(
                must_reader(
                    Cursor::new(b"abcd".to_vec()),
                    4,
                    "e2fc714c4727ee9395f324cd2e7f331f",
                    "",
                    4,
                ),
                4,
                "0773da587b322af3a8718cb418a715ce",
                "",
                4,
            )
            .map(|_| ()),
            false,
        ),
        (
            "matching md5",
            Reader::merge(
                must_reader(
                    Cursor::new(b"abcd".to_vec()),
                    4,
                    "e2fc714c4727ee9395f324cd2e7f331f",
                    "",
                    4,
                ),
                4,
                "e2fc714c4727ee9395f324cd2e7f331f",
                "",
                4,
            )
            .map(|_| ()),
            true,
        ),
        (
            "plain ok",
            Reader::new(Cursor::new(b"abcd".to_vec()), 4, "", "", 4).map(|_| ()),
            true,
        ),
        (
            "nested size mismatch",
            Reader::merge(
                must_reader(Cursor::new(b"abcd-morestuff".to_vec()), 4, "", "", -1),
                2,
                "",
                "",
                -1,
            )
            .map(|_| ()),
            false,
        ),
    ];

    for (name, result, success) in cases {
        assert_eq!(
            result.is_ok(),
            success,
            "case {name}: {:?}",
            result.err().map(|error| error.to_string())
        );
    }
}
