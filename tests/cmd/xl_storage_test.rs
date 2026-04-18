// Rust test snapshot derived from cmd/xl-storage_test.go.

use blake2::Blake2b512;
use highway::{HighwayHash, HighwayHasher, Key};
use sha2::{Digest, Sha256};
use std::fs;

use tempfile::{tempdir, NamedTempFile, TempDir};

use minio_rust::cmd::{
    check_path_length, get_disk_info, is_dir_empty, is_valid_volname, BitrotAlgorithm,
    BitrotVerifier, ErasureInfo, FileInfo, LocalXlStorage, ERR_DISK_NOT_DIR, ERR_DISK_NOT_FOUND,
    ERR_EOF, ERR_FILE_ACCESS_DENIED, ERR_FILE_CORRUPT, ERR_FILE_NAME_TOO_LONG, ERR_FILE_NOT_FOUND,
    ERR_FILE_VERSION_NOT_FOUND, ERR_INVALID_ARGUMENT, ERR_IS_NOT_REGULAR, ERR_PATH_NOT_FOUND,
    ERR_UNEXPECTED_EOF, ERR_VOLUME_EXISTS, ERR_VOLUME_NOT_EMPTY, ERR_VOLUME_NOT_FOUND,
    MINIO_META_BUCKET, XL_STORAGE_FORMAT_FILE, XL_STORAGE_FORMAT_FILE_V1,
};

pub const SOURCE_FILE: &str = "cmd/xl-storage_test.go";

fn new_xl_storage_test_setup() -> (LocalXlStorage, TempDir) {
    let temp_dir = tempdir().expect("create tempdir");
    let storage = LocalXlStorage::new(temp_dir.path().to_str().expect("utf-8 path"))
        .expect("create local xl storage");
    storage
        .make_vol(MINIO_META_BUCKET)
        .expect("create meta volume");
    storage
        .write_all(MINIO_META_BUCKET, "format.json", br#"{"version":"1"}"#)
        .expect("seed format");
    (storage, temp_dir)
}

fn digest_for_test(algorithm: BitrotAlgorithm, bytes: &[u8]) -> Vec<u8> {
    match algorithm {
        BitrotAlgorithm::Sha256 => Sha256::digest(bytes).to_vec(),
        BitrotAlgorithm::Blake2b512 => Blake2b512::digest(bytes).to_vec(),
        BitrotAlgorithm::HighwayHash256 | BitrotAlgorithm::HighwayHash256S => {
            let mut hasher = HighwayHasher::new(Key([1, 2, 3, 4]));
            hasher.append(bytes);
            hasher
                .finalize256()
                .into_iter()
                .flat_map(u64::to_le_bytes)
                .collect()
        }
    }
}

fn sample_file_info(volume: &str, path: &str, version_id: &str, mod_time: i64) -> FileInfo {
    FileInfo {
        volume: volume.to_string(),
        name: path.to_string(),
        version_id: version_id.to_string(),
        is_latest: false,
        deleted: false,
        transition_status: String::new(),
        transitioned_obj_name: String::new(),
        transition_tier: String::new(),
        transition_version_id: String::new(),
        expire_restored: false,
        data_dir: String::new(),
        xlv1: false,
        mod_time,
        size: 10_000,
        mode: 0,
        written_by_version: 0,
        metadata: None,
        parts: None,
        erasure: ErasureInfo {
            algorithm: "reedsolomon".to_string(),
            data_blocks: 4,
            parity_blocks: 4,
            block_size: 1024 * 1024,
            index: 1,
            distribution: Some(vec![0, 1, 2, 3, 4, 5, 6, 7]),
        },
        mark_deleted: false,
        replication_state: Default::default(),
        data: None,
        num_versions: 0,
        successor_mod_time: 0,
        fresh: false,
        idx: 0,
        checksum: None,
        versioned: true,
    }
}

#[test]
fn test_check_path_length_line_35() {
    if cfg!(target_os = "macos") || cfg!(target_os = "windows") {
        return;
    }

    let cases = [
        (".", Some(ERR_FILE_ACCESS_DENIED)),
        ("/", Some(ERR_FILE_ACCESS_DENIED)),
        ("..", Some(ERR_FILE_ACCESS_DENIED)),
        (
            "data/G_792/srv-tse/c/users/denis/documents/gestion!20locative/heritier/propri!E9taire/20190101_a2.03!20-!20m.!20heritier!20re!B4mi!20-!20proce!60s-verbal!20de!20livraison!20et!20de!20remise!20des!20cle!B4s!20acque!B4reurs!20-!204-!20livraison!20-!20lp!20promotion!20toulouse!20-!20encre!20et!20plume!20-!205!20de!B4c.!202019!20a!60!2012-49.pdf.ecc",
            Some(ERR_FILE_NAME_TOO_LONG),
        ),
        (
            "data/G_792/srv-tse/c/users/denis/documents/gestionlocative.txt",
            None,
        ),
    ];

    for (path, expected_err) in cases {
        let got = check_path_length(path).err();
        assert_eq!(got.as_deref(), expected_err, "path {path}");
    }
}

#[test]
fn subtest_test_check_path_length_line_54() {
    if cfg!(target_os = "macos") || cfg!(target_os = "windows") {
        return;
    }

    let long_name = "a".repeat(256);
    let path = format!("data/{long_name}");
    assert_eq!(
        check_path_length(&path).err().as_deref(),
        Some(ERR_FILE_NAME_TOO_LONG)
    );
}

#[test]
fn test_is_valid_volname_line_63() {
    let cases = [
        ("lol", true),
        ("1-this-is-valid", true),
        ("1-this-too-is-valid-1", true),
        ("this.works.too.1", true),
        ("1234567", true),
        ("123", true),
        ("s3-eu-west-1.amazonaws.com", true),
        ("ideas-are-more-powerful-than-guns", true),
        ("testbucket", true),
        ("1bucket", true),
        ("bucket1", true),
        ("$this-is-not-valid-too", true),
        ("contains-$-dollar", true),
        ("contains-^-carrot", true),
        (".starts-with-a-dot", true),
        ("ends-with-a-dot.", true),
        ("ends-with-a-dash-", true),
        ("-starts-with-a-dash", true),
        ("THIS-BEINGS-WITH-UPPERCASe", true),
        ("tHIS-ENDS-WITH-UPPERCASE", true),
        ("ThisBeginsAndEndsWithUpperCase", true),
        ("una ñina", true),
        (
            "lalalallalallalalalallalallalala-theString-size-is-greater-than-64",
            true,
        ),
        ("", false),
        ("/", false),
        ("a", false),
        ("ab", false),
        ("ab/", true),
        ("......", true),
    ];

    for (name, should_pass) in cases {
        assert_eq!(is_valid_volname(name), should_pass, "volume name {name}");
    }
}

#[test]
fn test_xlstorage_get_disk_info_line_184() {
    let temp_dir = tempdir().expect("create tempdir");
    let disk_path = temp_dir.path().to_str().expect("utf-8 path");
    assert!(get_disk_info(disk_path).is_ok());

    let missing = temp_dir.path().join("does-not-exist");
    assert_eq!(
        get_disk_info(missing.to_str().expect("utf-8 path"))
            .err()
            .as_deref(),
        Some(ERR_DISK_NOT_FOUND)
    );
}

#[test]
fn test_xlstorage_is_dir_empty_line_203() {
    let temp_dir = tempdir().expect("create tempdir");

    let missing = temp_dir.path().join("non-existent-directory");
    assert!(!is_dir_empty(&missing, true));

    let file = temp_dir.path().join("file");
    fs::write(&file, b"hello").expect("write file");
    assert!(!is_dir_empty(&file, true));

    let empty = temp_dir.path().join("empty");
    fs::create_dir(&empty).expect("create dir");
    assert!(is_dir_empty(&empty, true));
}

#[test]
fn test_xlstorage_read_version_legacy_line_235() {
    let legacy_json = r#"{
        "version":"1.0.1",
        "format":"xl",
        "stat":{"size":2016,"modTime":"2021-10-11T23:40:34.914361617Z"},
        "erasure":{"algorithm":"klauspost/reedsolomon/vandermonde","data":2,"parity":2,"blockSize":10485760,"distribution":[2,3,4,1]},
        "meta":{"content-type":"application/octet-stream","etag":"20000f00cf5e68d3d6b60e44fcd8b9e8-1"},
        "parts":[{"number":1,"etag":"","size":2016,"actualSize":1984}]
    }"#;

    let (storage, _temp_dir) = new_xl_storage_test_setup();
    storage.make_vol("exists-legacy").expect("create volume");
    storage
        .append_file(
            "exists-legacy",
            &format!("as-file/{XL_STORAGE_FORMAT_FILE_V1}"),
            legacy_json.as_bytes(),
        )
        .expect("seed xl.json");

    let fi = storage
        .read_version("exists-legacy", "as-file", "")
        .expect("read legacy version");
    assert!(fi.xlv1);
    assert_eq!(fi.size, 2016);
    assert_eq!(fi.erasure.data_blocks, 2);
    assert_eq!(fi.parts.as_ref().expect("parts")[0].actual_size, 1984);
}

#[test]
fn test_xlstorage_read_version_line_264() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    let version_id = "11111111-1111-1111-1111-111111111111";
    let file_info = sample_file_info("exists", "as-file", version_id, 100);

    storage.make_vol("exists").expect("create volume");
    storage
        .write_metadata("exists", "as-directory/as-file", file_info.clone())
        .expect("seed nested metadata");
    storage
        .write_metadata("exists", "as-file", file_info.clone())
        .expect("seed metadata");
    storage
        .write_metadata("exists", "as-file-parent", file_info)
        .expect("seed parent metadata");

    let cases = [
        ("i-dont-exist", "", None, Some(ERR_VOLUME_NOT_FOUND)),
        (
            "exists",
            "as-file-not-found",
            None,
            Some(ERR_FILE_NOT_FOUND),
        ),
        ("exists", "as-directory", None, Some(ERR_FILE_NOT_FOUND)),
        (
            "exists",
            "as-file-parent/as-file",
            None,
            Some(ERR_FILE_NOT_FOUND),
        ),
        ("exists", "as-file", Some(version_id), None),
        ("ab", "as-file", None, Some(ERR_VOLUME_NOT_FOUND)),
    ];

    for (volume, path, expected_version, expected_err) in cases {
        let result = storage.read_version(volume, path, expected_version.unwrap_or_default());
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "read version volume={volume} path={path}"
        );
        if let Some(expected_version) = expected_version {
            let fi = result.expect("read version");
            assert_eq!(fi.version_id, expected_version);
            assert_eq!(fi.data_dir, "");
            assert_eq!(fi.name, path);
        }
    }

    assert_eq!(
        storage
            .read_version("exists", "as-file", "missing-version")
            .err()
            .as_deref(),
        Some(ERR_FILE_VERSION_NOT_FOUND)
    );
}

#[test]
fn test_xlstorage_read_all_line_351() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    storage.make_vol("exists").expect("create volume");
    storage
        .append_file("exists", "as-directory/as-file", b"Hello, World")
        .expect("append nested file");
    storage
        .append_file("exists", "as-file", b"Hello, World")
        .expect("append file");
    storage
        .append_file("exists", "as-file-parent", b"Hello, World")
        .expect("append parent file");

    let cases = [
        ("i-dont-exist", "", Some(ERR_VOLUME_NOT_FOUND)),
        ("exists", "as-file-not-found", Some(ERR_FILE_NOT_FOUND)),
        ("exists", "as-directory", Some(ERR_FILE_NOT_FOUND)),
        ("exists", "as-file-parent/as-file", Some(ERR_FILE_NOT_FOUND)),
        ("exists", "as-file", None),
        ("ab", "as-file", Some(ERR_VOLUME_NOT_FOUND)),
    ];

    for (volume, path, expected_err) in cases {
        let result = storage.read_all(volume, path);
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "volume={volume} path={path}"
        );
        if expected_err.is_none() {
            assert_eq!(result.expect("read all"), b"Hello, World");
        }
    }
}

#[test]
fn test_new_xlstorage_line_439() {
    assert_eq!(
        LocalXlStorage::new("").err().as_deref(),
        Some(ERR_INVALID_ARGUMENT)
    );

    let temp_root = tempdir().expect("create temp root");
    let disk_path = temp_root.path().join("minio-storage");
    let storage =
        LocalXlStorage::new(disk_path.to_str().expect("utf-8 path")).expect("create storage");
    assert_eq!(storage.disk_path(), disk_path.as_path());

    let temp_file = NamedTempFile::new().expect("create temp file");
    assert_eq!(
        LocalXlStorage::new(temp_file.path().to_str().expect("utf-8 path"))
            .err()
            .as_deref(),
        Some(ERR_DISK_NOT_DIR)
    );
}

#[test]
fn test_xlstorage_make_vol_line_484() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();

    fs::write(storage.disk_path().join("vol-as-file"), b"")
        .expect("create file that shadows volume");
    fs::create_dir(storage.disk_path().join("existing-vol")).expect("create existing volume");

    let cases = [
        ("success-vol", None),
        ("vol-as-file", Some(ERR_VOLUME_EXISTS)),
        ("existing-vol", Some(ERR_VOLUME_EXISTS)),
        ("ab", Some(ERR_INVALID_ARGUMENT)),
    ];

    for (volume, expected_err) in cases {
        let result = storage.make_vol(volume);
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "make vol {volume}"
        );
    }
}

#[test]
fn test_xlstorage_delete_vol_line_570() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    storage
        .make_vol("success-vol")
        .expect("create deletable vol");

    let nonempty = storage.disk_path().join("nonempty-vol");
    fs::create_dir(&nonempty).expect("create nonempty volume");
    fs::write(nonempty.join("test-file"), b"").expect("seed nonempty volume");

    let cases = [
        ("success-vol", None),
        ("nonexistent-vol", Some(ERR_VOLUME_NOT_FOUND)),
        ("nonempty-vol", Some(ERR_VOLUME_NOT_EMPTY)),
        ("ab", Some(ERR_VOLUME_NOT_FOUND)),
    ];

    for (volume, expected_err) in cases {
        let result = storage.delete_vol(volume, false);
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "delete vol {volume}"
        );
    }

    let (deleted_storage, deleted_root) = new_xl_storage_test_setup();
    fs::remove_dir_all(deleted_root.path()).expect("remove backing disk");
    assert_eq!(
        deleted_storage
            .delete_vol("Del-Vol", false)
            .err()
            .as_deref(),
        Some(ERR_DISK_NOT_FOUND)
    );
}

#[test]
fn test_xlstorage_stat_vol_line_685() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    storage.make_vol("success-vol").expect("create volume");

    let cases = [
        ("success-vol", None),
        ("nonexistent-vol", Some(ERR_VOLUME_NOT_FOUND)),
        ("ab", Some(ERR_VOLUME_NOT_FOUND)),
    ];

    for (volume, expected_err) in cases {
        let result = storage.stat_vol(volume);
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "stat vol {volume}"
        );
        if expected_err.is_none() {
            assert_eq!(result.expect("stat vol").name, volume);
        }
    }

    let (deleted_storage, deleted_root) = new_xl_storage_test_setup();
    fs::remove_dir_all(deleted_root.path()).expect("remove backing disk");
    assert_eq!(
        deleted_storage.stat_vol("Stat vol").err().as_deref(),
        Some(ERR_DISK_NOT_FOUND)
    );
}

#[test]
fn test_xlstorage_list_vols_line_749() {
    let (storage, root) = new_xl_storage_test_setup();

    let volumes = storage.list_vols().expect("list empty-ish volumes");
    assert_eq!(volumes.len(), 1);
    assert_eq!(volumes[0].name, MINIO_META_BUCKET);

    storage.make_vol("success-vol").expect("create volume");
    let volumes = storage.list_vols().expect("list volumes");
    assert_eq!(volumes.len(), 2);
    assert!(volumes.iter().any(|info| info.name == "success-vol"));

    fs::remove_dir_all(root.path()).expect("remove backing disk");
    assert_eq!(
        storage.list_vols().err().as_deref(),
        Some(ERR_DISK_NOT_FOUND)
    );
}

#[test]
fn test_xlstorage_list_dir_line_796() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    storage.make_vol("success-vol").expect("create volume");
    storage
        .append_file("success-vol", "abc/def/ghi/success-file", b"Hello, world")
        .expect("seed nested file");
    storage
        .append_file("success-vol", "abc/xyz/ghi/success-file", b"Hello, world")
        .expect("seed second nested file");

    let cases = [
        ("success-vol", "abc", Some(vec!["def/", "xyz/"]), None),
        ("success-vol", "abc/def", Some(vec!["ghi/"]), None),
        (
            "success-vol",
            "abc/def/ghi",
            Some(vec!["success-file"]),
            None,
        ),
        ("success-vol", "abcdef", None, Some(ERR_FILE_NOT_FOUND)),
        ("ab", "success-file", None, Some(ERR_VOLUME_NOT_FOUND)),
        (
            "non-existent-vol",
            "success-file",
            None,
            Some(ERR_VOLUME_NOT_FOUND),
        ),
    ];

    for (volume, path, expected_entries, expected_err) in cases {
        let result = storage.list_dir(volume, path, -1);
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "list dir volume={volume} path={path}"
        );
        if let Some(expected_entries) = expected_entries {
            assert_eq!(result.expect("list dir"), expected_entries);
        }
    }
}

#[test]
fn test_xlstorage_delete_file_line_928() {
    if cfg!(target_os = "windows") {
        return;
    }

    let (storage, root) = new_xl_storage_test_setup();
    storage.make_vol("success-vol").expect("create volume");
    storage
        .append_file("success-vol", "success-file", b"Hello, world")
        .expect("create deletable file");

    let long_name = format!("my-obj-del-{}", "0".repeat(256));
    let cases = [
        ("success-vol", "success-file", None),
        ("success-vol", "success-file", None),
        ("my", "success-file", Some(ERR_VOLUME_NOT_FOUND)),
        (
            "non-existent-vol",
            "success-file",
            Some(ERR_VOLUME_NOT_FOUND),
        ),
        (
            "success-vol",
            long_name.as_str(),
            Some(ERR_FILE_NAME_TOO_LONG),
        ),
    ];

    for (volume, path, expected_err) in cases {
        let result = storage.delete(volume, path);
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "delete file volume={volume} path={path}"
        );
    }

    let (deleted_storage, deleted_root) = new_xl_storage_test_setup();
    fs::remove_dir_all(deleted_root.path()).expect("remove backing disk");
    assert_eq!(
        deleted_storage
            .delete("del-vol", "my-file")
            .err()
            .as_deref(),
        Some(ERR_DISK_NOT_FOUND)
    );

    drop(root);
}

#[test]
fn test_xlstorage_read_file_line_1062() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    let volume = "success-vol";
    storage.make_vol(volume).expect("create volume");
    fs::create_dir(storage.disk_path().join(volume).join("object-as-dir"))
        .expect("create directory sentinel");

    storage
        .append_file(volume, "myobject", b"hello, world")
        .expect("seed object");
    storage
        .append_file(volume, "path/to/my/object", b"hello, world")
        .expect("seed nested object");

    let long_name = format!("path/to/my/object{}", "0".repeat(256));
    let very_long_path = format!(
        "{}/{}",
        "level".to_string() + &"0".repeat(256),
        "object".to_string() + &"1".repeat(256)
    );

    let cases = [
        (
            volume,
            "myobject",
            0_i64,
            5_usize,
            Some(b"hello".as_slice()),
            None,
        ),
        (
            volume,
            "path/to/my/object",
            0_i64,
            5_usize,
            Some(b"hello".as_slice()),
            None,
        ),
        (
            volume,
            "object-as-dir",
            0_i64,
            5_usize,
            None,
            Some(ERR_IS_NOT_REGULAR),
        ),
        (
            volume,
            long_name.as_str(),
            0_i64,
            5_usize,
            None,
            Some(ERR_FILE_NAME_TOO_LONG),
        ),
        (
            volume,
            very_long_path.as_str(),
            0_i64,
            5_usize,
            None,
            Some(ERR_FILE_NAME_TOO_LONG),
        ),
        (
            volume,
            "myobject",
            0_i64,
            16_usize,
            Some(b"hello, world".as_slice()),
            Some(ERR_UNEXPECTED_EOF),
        ),
        (
            volume,
            "myobject",
            7_i64,
            5_usize,
            Some(b"world".as_slice()),
            None,
        ),
        (
            volume,
            "myobject",
            7_i64,
            8_usize,
            Some(b"world".as_slice()),
            Some(ERR_UNEXPECTED_EOF),
        ),
        (volume, "myobject", 14_i64, 1_usize, None, Some(ERR_EOF)),
        (
            "",
            "myobject",
            14_i64,
            1_usize,
            None,
            Some(ERR_VOLUME_NOT_FOUND),
        ),
        (volume, "", 14_i64, 1_usize, None, Some(ERR_IS_NOT_REGULAR)),
        (
            "abcd",
            "",
            14_i64,
            1_usize,
            None,
            Some(ERR_VOLUME_NOT_FOUND),
        ),
        (
            volume,
            "abcd",
            14_i64,
            1_usize,
            None,
            Some(ERR_FILE_NOT_FOUND),
        ),
    ];

    let mut negative_buf = vec![0_u8; 5];
    assert_eq!(
        storage
            .read_file(volume, "myobject", -1, &mut negative_buf)
            .err()
            .as_deref(),
        Some(ERR_INVALID_ARGUMENT)
    );

    for (case_volume, file_name, offset, buf_size, expected_buf, expected_err) in cases {
        let mut buffer = vec![0_u8; buf_size];
        let result = storage.read_file(case_volume, file_name, offset, &mut buffer);
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "read file volume={case_volume} file={file_name} offset={offset} size={buf_size}"
        );

        if let Some(expected) = expected_buf {
            let compare_len = expected.len();
            assert_eq!(&buffer[..compare_len], expected);
        }
        if expected_err.is_none() {
            assert_eq!(result.expect("read file"), buf_size);
        }
    }
}

#[test]
fn test_xlstorage_read_file_with_verify_line_1289() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    let volume = "test-vol";
    let object = "myobject";
    storage.make_vol(volume).expect("create volume");

    let data: Vec<u8> = (0..(8 * 1024)).map(|idx| (idx % 251) as u8).collect();
    storage
        .append_file(volume, object, &data)
        .expect("seed verified object");

    let cases = [
        (0_usize, 100_usize, BitrotAlgorithm::Sha256, false),
        (25_usize, 74_usize, BitrotAlgorithm::Sha256, false),
        (29_usize, 70_usize, BitrotAlgorithm::Sha256, false),
        (100_usize, 0_usize, BitrotAlgorithm::Sha256, false),
        (1_usize, 120_usize, BitrotAlgorithm::Sha256, true),
        (0_usize, 100_usize, BitrotAlgorithm::Blake2b512, false),
        (25_usize, 74_usize, BitrotAlgorithm::Blake2b512, false),
        (29_usize, 70_usize, BitrotAlgorithm::Blake2b512, true),
    ];

    for (offset, length, algorithm, should_corrupt) in cases {
        let slice = &data[offset..offset + length];
        let mut checksum = digest_for_test(algorithm, slice);
        if should_corrupt && !checksum.is_empty() {
            checksum[0] ^= 0xff;
        }

        let verifier = BitrotVerifier::new(algorithm, checksum);
        let mut buffer = vec![0_u8; length];
        let result =
            storage.read_file_with_verifier(volume, object, offset as i64, &mut buffer, &verifier);

        if should_corrupt {
            assert_eq!(result.err().as_deref(), Some(ERR_FILE_CORRUPT));
        } else {
            let n = result.expect("read with verifier");
            assert_eq!(n, length);
            assert_eq!(buffer, slice);
        }
    }
}

#[test]
fn test_xlstorage_format_file_change_line_1330() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    let volume = "fail-vol";
    storage.make_vol(volume).expect("create volume");

    storage
        .write_all(
            MINIO_META_BUCKET,
            "format.json",
            br#"{"version":"1","format":"xl","id":"changed","xl":{"version":"3","this":"randomid"}}"#,
        )
        .expect("rewrite format file");

    assert_eq!(
        storage.make_vol(volume).err().as_deref(),
        Some(ERR_VOLUME_EXISTS)
    );
}

#[test]
fn test_xlstorage_append_file_line_1353() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    storage.make_vol("success-vol").expect("create volume");
    fs::create_dir(
        storage
            .disk_path()
            .join("success-vol")
            .join("object-as-dir"),
    )
    .expect("create directory sentinel");

    let long_name = format!("path/to/my/object{}", "0".repeat(256));
    let cases = [
        ("myobject", None),
        ("path/to/my/object", None),
        ("myobject", None),
        ("path/to/my/testobject", None),
        ("object-as-dir", Some(ERR_IS_NOT_REGULAR)),
        ("myobject/testobject", Some(ERR_FILE_ACCESS_DENIED)),
        (long_name.as_str(), Some(ERR_FILE_NAME_TOO_LONG)),
    ];

    for (path, expected_err) in cases {
        let result = storage.append_file("success-vol", path, b"hello, world");
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "append file path={path}"
        );
    }

    assert_eq!(
        storage
            .append_file("bn", "yes", b"hello, world")
            .err()
            .as_deref(),
        Some(ERR_VOLUME_NOT_FOUND)
    );
}

#[test]
fn test_xlstorage_rename_file_line_1430() {
    let setup = || {
        let (storage, temp_dir) = new_xl_storage_test_setup();
        storage.make_vol("src-vol").expect("create src volume");
        storage.make_vol("dest-vol").expect("create dest volume");
        storage
            .append_file("src-vol", "file1", b"Hello, world")
            .expect("seed file1");
        storage
            .append_file("src-vol", "file2", b"Hello, world")
            .expect("seed file2");
        storage
            .append_file("src-vol", "file4", b"Hello, world")
            .expect("seed file4");
        storage
            .append_file("src-vol", "file5", b"Hello, world")
            .expect("seed file5");
        storage
            .append_file("src-vol", "file6", b"Hello, world")
            .expect("seed file6");
        storage
            .append_file("src-vol", "path/to/file1", b"Hello, world")
            .expect("seed nested file");
        (storage, temp_dir)
    };

    let long_src_name = format!("path/to/my/object{}", "0".repeat(256));
    let long_dest_name = format!("path/to/my/object{}", "1".repeat(256));
    let cases = [
        ("src-vol", "dest-vol", "file1", "file-one", None),
        ("src-vol", "dest-vol", "path/", "new-path/", None),
        ("src-vol", "dest-vol", "file2", "file-one", None),
        (
            "src-vol",
            "dest-vol",
            "non-existent-file",
            "file-three",
            Some(ERR_FILE_NOT_FOUND),
        ),
        (
            "src-vol",
            "dest-vol",
            "path/",
            "file-one",
            Some(ERR_FILE_ACCESS_DENIED),
        ),
        (
            "src-vol",
            "dest-vol",
            "file4",
            "new-path/",
            Some(ERR_FILE_ACCESS_DENIED),
        ),
        (
            "src-vol-non-existent",
            "dest-vol",
            "file4",
            "new-path/",
            Some(ERR_VOLUME_NOT_FOUND),
        ),
        (
            "src-vol",
            "dest-vol-non-existent",
            "file4",
            "new-path/",
            Some(ERR_VOLUME_NOT_FOUND),
        ),
        (
            "ab",
            "dest-vol",
            "file4",
            "new-path/",
            Some(ERR_VOLUME_NOT_FOUND),
        ),
        (
            "src-vol",
            "ef",
            "file4",
            "new-path/",
            Some(ERR_VOLUME_NOT_FOUND),
        ),
        (
            "src-vol",
            "dest-vol",
            long_src_name.as_str(),
            "file-six",
            Some(ERR_FILE_NAME_TOO_LONG),
        ),
        (
            "src-vol",
            "dest-vol",
            "file6",
            long_dest_name.as_str(),
            Some(ERR_FILE_NAME_TOO_LONG),
        ),
    ];

    for (src_vol, dest_vol, src_path, dest_path, expected_err) in cases {
        let (storage, _temp_dir) = setup();
        let result = storage.rename_file(src_vol, src_path, dest_vol, dest_path);
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "rename src={src_vol}:{src_path} dst={dest_vol}:{dest_path}"
        );
    }

    let (storage, _temp_dir) = setup();
    storage
        .append_file("dest-vol", "file-one", b"existing destination")
        .expect("seed overwrite target");
    assert!(storage
        .rename_file("src-vol", "file2", "dest-vol", "file-one")
        .is_ok());

    let (storage, _temp_dir) = setup();
    storage
        .append_file(
            "dest-vol",
            "new-path/existing-file",
            b"seed destination dir",
        )
        .expect("seed destination directory");
    assert_eq!(
        storage
            .rename_file("src-vol", "path/", "dest-vol", "new-path/")
            .err()
            .as_deref(),
        Some(ERR_FILE_ACCESS_DENIED)
    );

    let (storage, _temp_dir) = setup();
    storage
        .append_file("dest-vol", "file-one", b"parent is file")
        .expect("seed parent file");
    assert_eq!(
        storage
            .rename_file("src-vol", "file5", "dest-vol", "file-one/parent-is-file")
            .err()
            .as_deref(),
        Some(ERR_FILE_ACCESS_DENIED)
    );
}

#[test]
fn test_xlstorage_delete_version_line_1647() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    let volume = "myvol-vol";
    let object = "my-object";
    storage.make_vol(volume).expect("create volume");

    let versions: Vec<String> = (0..10)
        .map(|idx| format!("00000000-0000-0000-0000-{:012}", idx + 1))
        .collect();
    for (idx, version_id) in versions.iter().enumerate() {
        storage
            .write_metadata(
                volume,
                object,
                sample_file_info(volume, object, version_id, 1_000 + idx as i64),
            )
            .expect("write version metadata");
    }

    let assert_exists = |storage: &LocalXlStorage, version_id: &str| {
        let fi = storage
            .read_version(volume, object, version_id)
            .expect("version should exist");
        assert_eq!(fi.version_id, version_id);
    };
    let assert_missing = |storage: &LocalXlStorage, version_id: &str| {
        assert_eq!(
            storage
                .read_version(volume, object, version_id)
                .err()
                .as_deref(),
            Some(ERR_FILE_VERSION_NOT_FOUND)
        );
    };

    assert_exists(&storage, &versions[0]);
    storage
        .delete_version(
            volume,
            object,
            &sample_file_info(volume, object, &versions[0], 0),
        )
        .expect("delete first version");
    assert_missing(&storage, &versions[0]);
    assert_exists(&storage, &versions[1]);

    for version_id in &versions[1..4] {
        storage
            .delete_version(
                volume,
                object,
                &sample_file_info(volume, object, version_id, 0),
            )
            .expect("bulk-ish delete");
        assert_missing(&storage, version_id);
    }

    for version_id in &versions[4..] {
        storage
            .delete_version(
                volume,
                object,
                &sample_file_info(volume, object, version_id, 0),
            )
            .expect("delete remaining version");
    }

    assert_eq!(
        storage.read_version(volume, object, "").err().as_deref(),
        Some(ERR_FILE_NOT_FOUND)
    );
}

#[test]
fn test_xlstorage_stat_info_file_line_1738() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    storage.make_vol("success-vol").expect("create volume");

    storage
        .append_file(
            "success-vol",
            &format!("success-file/{XL_STORAGE_FORMAT_FILE}"),
            b"Hello, world",
        )
        .expect("seed xl.meta");
    storage
        .append_file(
            "success-vol",
            &format!("path/to/success-file/{XL_STORAGE_FORMAT_FILE}"),
            b"Hello, world",
        )
        .expect("seed nested xl.meta");
    fs::create_dir_all(
        storage
            .disk_path()
            .join("success-vol")
            .join("path/to")
            .join(XL_STORAGE_FORMAT_FILE),
    )
    .expect("create xl.meta directory");

    let cases = [
        ("success-vol", "success-file", None),
        ("success-vol", "path/to/success-file", None),
        ("success-vol", "nonexistent-file", Some(ERR_PATH_NOT_FOUND)),
        (
            "success-vol",
            "path/2/success-file",
            Some(ERR_PATH_NOT_FOUND),
        ),
        ("success-vol", "path", Some(ERR_PATH_NOT_FOUND)),
        (
            "non-existent-vol",
            "success-file",
            Some(ERR_VOLUME_NOT_FOUND),
        ),
        ("success-vol", "path/to", None),
    ];

    for (volume, base_path, expected_err) in cases {
        let result =
            storage.stat_info_file(volume, &format!("{base_path}/{XL_STORAGE_FORMAT_FILE}"));
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "stat info file volume={volume} path={base_path}"
        );
    }
}

#[test]
fn test_xlstorage_verify_file_line_1827() {
    let (storage, _temp_dir) = new_xl_storage_test_setup();
    let volume = "testvol";
    let file_name = "testfile";
    storage.make_vol(volume).expect("create volume");

    let data: Vec<u8> = (0..(256 * 1024)).map(|idx| (idx % 253) as u8).collect();
    storage
        .write_all(volume, file_name, &data)
        .expect("write test file");

    let sha_sum = digest_for_test(BitrotAlgorithm::Sha256, &data);
    storage
        .verify_file(
            volume,
            file_name,
            data.len(),
            BitrotAlgorithm::Sha256,
            &sha_sum,
        )
        .expect("sha256 verify");

    let highway_sum = digest_for_test(BitrotAlgorithm::HighwayHash256, &data);
    storage
        .verify_file(
            volume,
            file_name,
            data.len(),
            BitrotAlgorithm::HighwayHash256,
            &highway_sum,
        )
        .expect("highway verify");

    let highway_stream_sum = digest_for_test(BitrotAlgorithm::HighwayHash256S, &data);
    storage
        .verify_file(
            volume,
            file_name,
            data.len(),
            BitrotAlgorithm::HighwayHash256S,
            &highway_stream_sum,
        )
        .expect("streaming highway verify");

    storage
        .append_file(volume, file_name, b"a")
        .expect("corrupt file with append");
    assert_eq!(
        storage
            .verify_file(
                volume,
                file_name,
                data.len(),
                BitrotAlgorithm::Sha256,
                &sha_sum
            )
            .err()
            .as_deref(),
        Some(ERR_FILE_CORRUPT)
    );

    let corrupted = storage
        .read_all(volume, file_name)
        .expect("read corrupted file back");
    let blake_sum = digest_for_test(BitrotAlgorithm::Blake2b512, &corrupted);
    storage
        .verify_file(
            volume,
            file_name,
            corrupted.len(),
            BitrotAlgorithm::Blake2b512,
            &blake_sum,
        )
        .expect("blake2 verify on current bytes");
}

#[test]
fn test_xlstorage_read_metadata_line_1930() {
    let volume = "test-vol";
    let object = "A".repeat(257);
    let temp_dir = tempdir().expect("create tempdir");

    let storage = LocalXlStorage::new(temp_dir.path().to_str().expect("utf-8 path"))
        .expect("create local storage");
    storage.make_vol(volume).expect("create volume");

    let item_path = temp_dir.path().join(volume).join(object);
    assert_eq!(
        storage
            .read_metadata(item_path.to_str().expect("utf-8 path"))
            .err()
            .as_deref(),
        Some(ERR_FILE_NAME_TOO_LONG)
    );
}
