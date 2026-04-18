use minio_rust::internal::event::{parse_name, Name};

#[test]
fn name_expand_matches_reference_cases() {
    let cases = [
        (Name::BucketCreated, vec![Name::BucketCreated]),
        (Name::BucketRemoved, vec![Name::BucketRemoved]),
        (
            Name::ObjectAccessedAll,
            vec![
                Name::ObjectAccessedGet,
                Name::ObjectAccessedHead,
                Name::ObjectAccessedGetRetention,
                Name::ObjectAccessedGetLegalHold,
                Name::ObjectAccessedAttributes,
            ],
        ),
        (
            Name::ObjectCreatedAll,
            vec![
                Name::ObjectCreatedCompleteMultipartUpload,
                Name::ObjectCreatedCopy,
                Name::ObjectCreatedPost,
                Name::ObjectCreatedPut,
                Name::ObjectCreatedPutRetention,
                Name::ObjectCreatedPutLegalHold,
                Name::ObjectCreatedPutTagging,
                Name::ObjectCreatedDeleteTagging,
            ],
        ),
        (
            Name::ObjectRemovedAll,
            vec![
                Name::ObjectRemovedDelete,
                Name::ObjectRemovedDeleteMarkerCreated,
                Name::ObjectRemovedNoOP,
                Name::ObjectRemovedDeleteAllVersions,
            ],
        ),
        (Name::ObjectAccessedHead, vec![Name::ObjectAccessedHead]),
    ];

    for (name, expected) in cases {
        assert_eq!(name.expand(), expected);
    }
}

#[test]
fn name_string_matches_reference_cases() {
    let cases = [
        (Name::BucketCreated, "s3:BucketCreated:*"),
        (Name::BucketRemoved, "s3:BucketRemoved:*"),
        (Name::ObjectAccessedAll, "s3:ObjectAccessed:*"),
        (Name::ObjectAccessedGet, "s3:ObjectAccessed:Get"),
        (Name::ObjectAccessedHead, "s3:ObjectAccessed:Head"),
        (Name::ObjectCreatedAll, "s3:ObjectCreated:*"),
        (
            Name::ObjectCreatedCompleteMultipartUpload,
            "s3:ObjectCreated:CompleteMultipartUpload",
        ),
        (Name::ObjectCreatedCopy, "s3:ObjectCreated:Copy"),
        (Name::ObjectCreatedPost, "s3:ObjectCreated:Post"),
        (Name::ObjectCreatedPut, "s3:ObjectCreated:Put"),
        (Name::ObjectRemovedAll, "s3:ObjectRemoved:*"),
        (Name::ObjectRemovedDelete, "s3:ObjectRemoved:Delete"),
        (
            Name::ObjectRemovedDeleteAllVersions,
            "s3:ObjectRemoved:DeleteAllVersions",
        ),
        (
            Name::IlmDelMarkerExpirationDelete,
            "s3:LifecycleDelMarkerExpiration:Delete",
        ),
        (Name::ObjectRemovedNoOP, "s3:ObjectRemoved:NoOP"),
        (
            Name::ObjectCreatedPutRetention,
            "s3:ObjectCreated:PutRetention",
        ),
        (
            Name::ObjectCreatedPutLegalHold,
            "s3:ObjectCreated:PutLegalHold",
        ),
        (
            Name::ObjectAccessedGetRetention,
            "s3:ObjectAccessed:GetRetention",
        ),
        (
            Name::ObjectAccessedGetLegalHold,
            "s3:ObjectAccessed:GetLegalHold",
        ),
    ];

    for (name, expected) in cases {
        assert_eq!(name.to_string(), expected);
    }
}

#[test]
fn name_marshal_xml_matches_reference_cases() {
    let cases = [
        (Name::ObjectAccessedAll, "<Name>s3:ObjectAccessed:*</Name>"),
        (
            Name::ObjectRemovedDelete,
            "<Name>s3:ObjectRemoved:Delete</Name>",
        ),
        (
            Name::ObjectRemovedNoOP,
            "<Name>s3:ObjectRemoved:NoOP</Name>",
        ),
    ];

    for (name, expected) in cases {
        assert_eq!(name.marshal_xml(), expected);
    }
}

#[test]
fn name_unmarshal_xml_matches_reference_cases() {
    let cases = [
        (
            "<Name>s3:ObjectAccessed:*</Name>",
            Some(Name::ObjectAccessedAll),
            false,
        ),
        (
            "<Name>s3:ObjectRemoved:Delete</Name>",
            Some(Name::ObjectRemovedDelete),
            false,
        ),
        (
            "<Name>s3:ObjectRemoved:NoOP</Name>",
            Some(Name::ObjectRemovedNoOP),
            false,
        ),
        ("<Name></Name>", None, true),
    ];

    for (data, expected, should_err) in cases {
        let result = Name::unmarshal_xml(data.as_bytes());
        assert_eq!(result.is_err(), should_err);
        if let Ok(name) = result {
            assert_eq!(Some(name), expected);
        }
    }
}

#[test]
fn name_marshal_json_matches_reference_cases() {
    let cases = [
        (Name::ObjectAccessedAll, "\"s3:ObjectAccessed:*\""),
        (Name::ObjectRemovedDelete, "\"s3:ObjectRemoved:Delete\""),
        (Name::ObjectRemovedNoOP, "\"s3:ObjectRemoved:NoOP\""),
    ];

    for (name, expected) in cases {
        let data = name.marshal_json().expect("json marshal should succeed");
        assert_eq!(String::from_utf8(data).expect("json is utf-8"), expected);
    }
}

#[test]
fn name_unmarshal_json_matches_reference_cases() {
    let cases = [
        (
            br#""s3:ObjectAccessed:*""#.as_slice(),
            Some(Name::ObjectAccessedAll),
            false,
        ),
        (
            br#""s3:ObjectRemoved:Delete""#.as_slice(),
            Some(Name::ObjectRemovedDelete),
            false,
        ),
        (
            br#""s3:ObjectRemoved:NoOP""#.as_slice(),
            Some(Name::ObjectRemovedNoOP),
            false,
        ),
        (br#""""#.as_slice(), None, true),
    ];

    for (data, expected, should_err) in cases {
        let result = Name::unmarshal_json(data);
        assert_eq!(result.is_err(), should_err);
        if let Ok(name) = result {
            assert_eq!(Some(name), expected);
        }
    }
}

#[test]
fn parse_name_matches_reference_cases() {
    let cases = [
        ("s3:ObjectAccessed:*", Some(Name::ObjectAccessedAll), false),
        (
            "s3:ObjectRemoved:Delete",
            Some(Name::ObjectRemovedDelete),
            false,
        ),
        (
            "s3:ObjectRemoved:NoOP",
            Some(Name::ObjectRemovedNoOP),
            false,
        ),
        (
            "s3:LifecycleDelMarkerExpiration:Delete",
            Some(Name::IlmDelMarkerExpirationDelete),
            false,
        ),
        ("", None, true),
    ];

    for (value, expected, should_err) in cases {
        let result = parse_name(value);
        assert_eq!(result.is_err(), should_err);
        if let Ok(name) = result {
            assert_eq!(Some(name), expected);
        }
    }
}
