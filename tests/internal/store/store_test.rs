use minio_rust::internal::store::{parse_key, Key};

pub const SOURCE_FILE: &str = "internal/store/store_test.go";

#[test]
fn test_key_string() {
    let cases = [
        (
            Key {
                name: "01894394-d046-4783-ba0d-f1c6885790dc".to_owned(),
                extension: ".event".to_owned(),
                ..Key::default()
            },
            "01894394-d046-4783-ba0d-f1c6885790dc.event",
        ),
        (
            Key {
                name: "01894394-d046-4783-ba0d-f1c6885790dc".to_owned(),
                compress: true,
                extension: ".event".to_owned(),
                item_count: 100,
            },
            "100:01894394-d046-4783-ba0d-f1c6885790dc.event.snappy",
        ),
        (
            Key {
                name: "01894394-d046-4783-ba0d-f1c6885790dc".to_owned(),
                extension: ".event".to_owned(),
                item_count: 100,
                ..Key::default()
            },
            "100:01894394-d046-4783-ba0d-f1c6885790dc.event",
        ),
        (
            Key {
                name: "01894394-d046-4783-ba0d-f1c6885790dc".to_owned(),
                compress: true,
                extension: ".event".to_owned(),
                item_count: 1,
            },
            "01894394-d046-4783-ba0d-f1c6885790dc.event.snappy",
        ),
    ];

    for (idx, (key, expected)) in cases.into_iter().enumerate() {
        assert_eq!(key.string(), expected, "case {}", idx);
    }
}

#[test]
fn test_parse_key() {
    let cases = [
        (
            "01894394-d046-4783-ba0d-f1c6885790dc.event",
            Key {
                name: "01894394-d046-4783-ba0d-f1c6885790dc".to_owned(),
                extension: ".event".to_owned(),
                item_count: 1,
                ..Key::default()
            },
        ),
        (
            "100:01894394-d046-4783-ba0d-f1c6885790dc.event.snappy",
            Key {
                name: "01894394-d046-4783-ba0d-f1c6885790dc".to_owned(),
                compress: true,
                extension: ".event".to_owned(),
                item_count: 100,
            },
        ),
        (
            "100:01894394-d046-4783-ba0d-f1c6885790dc.event",
            Key {
                name: "01894394-d046-4783-ba0d-f1c6885790dc".to_owned(),
                extension: ".event".to_owned(),
                item_count: 100,
                ..Key::default()
            },
        ),
        (
            "01894394-d046-4783-ba0d-f1c6885790dc.event.snappy",
            Key {
                name: "01894394-d046-4783-ba0d-f1c6885790dc".to_owned(),
                compress: true,
                extension: ".event".to_owned(),
                item_count: 1,
            },
        ),
    ];

    for (idx, (value, expected)) in cases.into_iter().enumerate() {
        let parsed = parse_key(value);
        assert_eq!(parsed, expected, "case {}", idx);
        assert_eq!(parsed.string(), expected.string(), "case {}", idx);
    }
}
