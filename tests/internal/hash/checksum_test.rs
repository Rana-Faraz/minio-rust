use std::collections::HashMap;

use minio_rust::internal::hash::{
    add_checksum_header, checksum_from_bytes, get_content_checksum, new_checksum_from_data,
    new_checksum_type, read_part_checksums, ChecksumType, AMZ_CHECKSUM_ALGO, AMZ_CHECKSUM_TYPE,
    AMZ_CHECKSUM_TYPE_COMPOSITE, AMZ_CHECKSUM_TYPE_FULL_OBJECT,
};

pub const SOURCE_FILE: &str = "internal/hash/checksum_test.go";

#[test]
fn checksum_add_to_header_matches_reference_matrix() {
    let cases = [
        ("CRC32-composite", ChecksumType::CRC32, false, false),
        ("CRC32-full-object", ChecksumType::CRC32, true, false),
        ("CRC32C-composite", ChecksumType::CRC32C, false, false),
        ("CRC32C-full-object", ChecksumType::CRC32C, true, false),
        (
            "CRC64NVME-full-object",
            ChecksumType::CRC64NVME,
            false,
            false,
        ),
        ("ChecksumSHA1-composite", ChecksumType::SHA1, false, false),
        (
            "ChecksumSHA256-composite",
            ChecksumType::SHA256,
            false,
            false,
        ),
        ("ChecksumSHA1-full-object", ChecksumType::SHA1, true, true),
        (
            "ChecksumSHA256-full-object",
            ChecksumType::SHA256,
            true,
            true,
        ),
    ];

    for (name, checksum_type, full_object, want_err) in cases {
        if (checksum_type.is(ChecksumType::SHA1) || checksum_type.is(ChecksumType::SHA256))
            && full_object
        {
            let typ = new_checksum_type(checksum_type.string(), AMZ_CHECKSUM_TYPE_FULL_OBJECT);
            assert!(typ.is(ChecksumType::INVALID), "case {name}");
            continue;
        }

        let data = b"this-is-a-checksum-data-test";
        let mut checksum =
            new_checksum_from_data(checksum_type, data).expect("checksum should be created");
        if full_object {
            checksum.type_ |= ChecksumType::FULL_OBJECT;
        }
        if checksum.type_.base().is(ChecksumType::CRC64NVME) {
            checksum.type_ |= ChecksumType::FULL_OBJECT;
        }

        let mut serialized = checksum.as_map();
        serialized.insert(
            AMZ_CHECKSUM_ALGO.to_owned(),
            checksum.type_.string().to_owned(),
        );
        serialized.insert(
            AMZ_CHECKSUM_TYPE.to_owned(),
            if checksum.type_.full_object_requested() {
                AMZ_CHECKSUM_TYPE_FULL_OBJECT.to_owned()
            } else {
                AMZ_CHECKSUM_TYPE_COMPOSITE.to_owned()
            },
        );

        let mut headers = HashMap::new();
        add_checksum_header(&mut headers, &serialized);
        headers.insert(
            AMZ_CHECKSUM_ALGO.to_owned(),
            serialized[AMZ_CHECKSUM_ALGO].clone(),
        );
        headers.insert(
            AMZ_CHECKSUM_TYPE.to_owned(),
            serialized[AMZ_CHECKSUM_TYPE].clone(),
        );

        let got = get_content_checksum(&headers);
        if want_err {
            assert!(got.is_err(), "case {name} should fail");
            continue;
        }

        let got = got
            .expect("checksum extraction should succeed")
            .expect("checksum should exist");
        assert!(checksum.equal(&got), "case {name}");
        assert_eq!(got.type_, checksum.type_, "case {name}");
    }
}

#[test]
fn checksum_serialize_deserialize_round_trips() {
    let data = b"this-is-a-checksum-data-test";
    let checksum = new_checksum_from_data(ChecksumType::CRC32, data).expect("checksum");
    let bytes = checksum.append_to(Vec::new(), &[]);
    let out = checksum_from_bytes(&bytes).expect("checksum should decode");
    out.matches(data, 0).expect("checksum should match");
    assert!(out.equal(&checksum));
}

#[test]
fn checksum_serialize_deserialize_multipart_round_trips() {
    let payload = b"The quick brown fox jumps over the lazy dog. Pack my box with five dozen brown eggs. Have another go it will all make sense in the end!";
    let part_size = payload.len() / 3;
    let part1 = &payload[..part_size];
    let part2 = &payload[part_size..part_size * 2];
    let part3 = &payload[part_size * 2..];

    let part1_checksum = new_checksum_from_data(ChecksumType::CRC32C, part1).expect("part1");
    let part2_checksum = new_checksum_from_data(ChecksumType::CRC32C, part2).expect("part2");
    let part3_checksum = new_checksum_from_data(ChecksumType::CRC32C, part3).expect("part3");

    let mut combined = Vec::new();
    combined.extend_from_slice(&part1_checksum.raw);
    combined.extend_from_slice(&part2_checksum.raw);
    combined.extend_from_slice(&part3_checksum.raw);

    let mut final_checksum = new_checksum_from_data(
        ChecksumType::CRC32C | ChecksumType::MULTIPART | ChecksumType::INCLUDES_MULTIPART,
        &combined,
    )
    .expect("final checksum");
    final_checksum.want_parts = 3;

    let serialized = final_checksum.append_to(Vec::new(), &combined);
    let decoded = checksum_from_bytes(&serialized).expect("multipart checksum should decode");
    assert!(decoded.equal(&final_checksum));

    let parts = read_part_checksums(&serialized);
    let expected = [
        &part1_checksum.encoded,
        &part2_checksum.encoded,
        &part3_checksum.encoded,
    ];
    for (index, want) in expected.iter().enumerate() {
        assert_eq!(
            parts[index][ChecksumType::CRC32C.string()],
            **want,
            "part {} checksum should match",
            index + 1
        );
    }
}
