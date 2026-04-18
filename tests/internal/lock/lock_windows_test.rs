use minio_rust::internal::lock::fix_long_path;

pub const SOURCE_FILE: &str = "internal/lock/lock_windows_test.go";

#[cfg(windows)]
#[test]
fn fix_long_path_matches_reference_cases() {
    let very_long = format!("l{}ng", "o".repeat(248));
    let cases = [
        (r"C:\short.txt", r"C:\short.txt"),
        (r"C:\", r"C:\"),
        (r"C:", r"C:"),
        (r"C:\long\foo.txt", r"\\?\C:\long\foo.txt"),
        (r"C:/long/foo.txt", r"\\?\C:\long\foo.txt"),
        (r"C:\long\foo\\bar\.\baz\\", r"\\?\C:\long\foo\bar\baz"),
        (r"\\unc\path", r"\\unc\path"),
        (r"long.txt", r"long.txt"),
        (r"C:long.txt", r"C:long.txt"),
        (r"c:\long\..\bar\baz", r"c:\long\..\bar\baz"),
        (r"\\?\c:\long\foo.txt", r"\\?\c:\long\foo.txt"),
        (r"\\?\c:\long/foo.txt", r"\\?\c:\long/foo.txt"),
    ];

    for (input, want) in cases {
        let input = input.replace("long", &very_long);
        let want = want.replace("long", &very_long);
        assert_eq!(fix_long_path(&input), want);
    }
}

#[cfg(not(windows))]
#[test]
fn fix_long_path_non_windows_is_passthrough() {
    assert_eq!(fix_long_path("/tmp/example"), "/tmp/example");
}
