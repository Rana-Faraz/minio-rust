use minio_rust::internal::s3select::sql::{drop_rune, eval_sql_like, matcher, RUNE_ZERO};

pub const SOURCE_FILE: &str = "internal/s3select/sql/stringfuncs_test.go";

#[test]
fn eval_sqllike_matches_reference_cases() {
    let drop_cases = [
        ("", "", false),
        ("a", "", true),
        ("ab", "b", true),
        ("தமிழ்", "மிழ்", true),
    ];

    for (index, (input, expected, matched)) in drop_cases.into_iter().enumerate() {
        let (result, ok) = drop_rune(input);
        assert_eq!(result, expected, "drop case {index}");
        assert_eq!(ok, matched, "drop case {index}");
    }

    let matcher_cases = [
        ("abcd", "bcd", false, "", false),
        ("abcd", "bcd", true, "", true),
        ("abcd", "abcd", false, "", true),
        ("abcd", "abcd", true, "", true),
        ("abcd", "ab", false, "cd", true),
        ("abcd", "ab", true, "cd", true),
        ("abcd", "bc", false, "", false),
        ("abcd", "bc", true, "d", true),
    ];

    for (index, (text, pat, leading_percent, expected, matched)) in
        matcher_cases.into_iter().enumerate()
    {
        let (result, ok) = matcher(text, pat, leading_percent);
        assert_eq!(result, expected, "matcher case {index}");
        assert_eq!(ok, matched, "matcher case {index}");
    }

    let eval_cases = [
        ("abcd", "abc", RUNE_ZERO, false, false),
        ("abcd", "abcd", RUNE_ZERO, true, false),
        ("abcd", "abc_", RUNE_ZERO, true, false),
        ("abcd", "_bdd", RUNE_ZERO, false, false),
        ("abcd", "_b_d", RUNE_ZERO, true, false),
        ("abcd", "____", RUNE_ZERO, true, false),
        ("abcd", "____%", RUNE_ZERO, true, false),
        ("abcd", "%____", RUNE_ZERO, true, false),
        ("abcd", "%__", RUNE_ZERO, true, false),
        ("", "_", RUNE_ZERO, false, false),
        ("", "%", RUNE_ZERO, true, false),
        ("abcd", "%%%%%", RUNE_ZERO, true, false),
        ("abcd", "_____", RUNE_ZERO, false, false),
        ("a%%d", r"a\%\%d", '\\', true, false),
        ("a%%d", r"a\%d", '\\', false, false),
        (r"a%%\d", r"a\%\%\\d", '\\', true, false),
        (r"a%%\", r"a\%\%\\", '\\', true, false),
        (r"a%__%\", r"a\%\_\_\%\\", '\\', true, false),
        (r"a%__%\", r"a\%\_\_\%_", '\\', true, false),
        (r"a%__%\", r"a\%\_\__", '\\', false, false),
        (r"a%__%\", r"a\%\_\_%", '\\', true, false),
        (r"a%__%\", r"a?%?_?_?%\", '?', true, false),
    ];

    for (index, (text, pat, esc, expected_match, expected_err)) in
        eval_cases.into_iter().enumerate()
    {
        let result = eval_sql_like(text, pat, esc);
        assert_eq!(result.is_err(), expected_err, "eval case {index}");
        if let Ok(matched) = result {
            assert_eq!(matched, expected_match, "eval case {index}");
        }
    }
}
