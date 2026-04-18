use minio_rust::internal::s3select::sql::eval_sql_substring;

pub const SOURCE_FILE: &str = "internal/s3select/sql/stringfuncs_contrib_test.go";

#[test]
fn eval_sqlsubstring_matches_reference_cases() {
    let cases = [
        ("abcd", 1, 1, "a"),
        ("abcd", -1, 1, "a"),
        ("abcd", 999, 999, ""),
        ("", 999, 999, ""),
        ("测试abc", 1, 1, "测"),
        ("测试abc", 5, 5, "c"),
    ];

    for (index, (input, start_idx, length, expected)) in cases.into_iter().enumerate() {
        let result = eval_sql_substring(input, start_idx, length)
            .unwrap_or_else(|err| panic!("substring case {index} failed: {err}"));
        assert_eq!(result, expected, "substring case {index}");
    }
}
