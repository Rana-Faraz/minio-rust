use minio_rust::internal::config::compress::parse_compress_includes;

#[test]
fn parse_compress_includes_matches_reference_cases() {
    let cases = [
        (",,,", Vec::<&str>::new(), true),
        ("", Vec::<&str>::new(), true),
        (",", Vec::<&str>::new(), true),
        ("/", Vec::<&str>::new(), true),
        ("text/*,/", Vec::<&str>::new(), true),
        (".txt,.log", vec![".txt", ".log"], false),
        (
            "text/*,application/json",
            vec!["text/*", "application/json"],
            false,
        ),
    ];

    for (input, expected, should_err) in cases {
        let result = parse_compress_includes(input);
        assert_eq!(result.is_err(), should_err, "input {input}");
        if let Ok(patterns) = result {
            assert_eq!(
                patterns,
                expected.into_iter().map(str::to_owned).collect::<Vec<_>>()
            );
        }
    }
}
