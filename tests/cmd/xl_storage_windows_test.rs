use minio_rust::cmd::normalize_windows_storage_path;

pub const SOURCE_FILE: &str = "cmd/xl-storage_windows_test.go";

#[test]
fn test_uncpaths_line_31() {
    let unc = r"\\server\share\folder\file.txt";
    assert_eq!(normalize_windows_storage_path(unc), unc);
}

#[test]
fn subtest_test_uncpaths_fmt_sprint_i_line_58() {
    let short = r"C:\minio\disk1";
    assert_eq!(normalize_windows_storage_path(short), short);

    let long = format!(r"C:\{}", "deep\\".repeat(80));
    let normalized = normalize_windows_storage_path(&long);
    if cfg!(windows) {
        assert!(normalized.starts_with(r"\\?\C:\"));
    } else {
        assert_eq!(normalized, long);
    }
}

#[test]
fn test_uncpath_enotdir_line_74() {
    let with_parent = r"C:\minio\..\disk1\file.txt";
    assert_eq!(normalize_windows_storage_path(with_parent), with_parent);
}
