use std::fs;
use std::path::PathBuf;

use minio_rust::cmd::{
    check_format_erasure_value, fix_format_erasure_v3, format_erasure_migrate,
    format_erasure_v3_check, format_erasure_v3_this_empty, format_get_backend_erasure_version,
    get_format_erasure_in_quorum, init_storage_disks_with_errors, load_format_erasure_all,
    new_format_erasure_v3, new_heal_format_sets, FormatErasureV1, FormatErasureV1Body,
    FormatMetaV1, ERR_DISK_NOT_FOUND, ERR_ERASURE_READ_QUORUM, ERR_UNFORMATTED_DISK,
    FORMAT_BACKEND_ERASURE, FORMAT_CONFIG_FILE, FORMAT_ERASURE_VERSION_V1,
    FORMAT_ERASURE_VERSION_V3, FORMAT_META_VERSION_V1, MINIO_META_BUCKET,
};
use tempfile::TempDir;

pub const SOURCE_FILE: &str = "cmd/format-erasure_test.go";

fn new_disk_roots(count: usize) -> Vec<TempDir> {
    (0..count)
        .map(|_| TempDir::new().expect("temp disk"))
        .collect::<Vec<_>>()
}

fn disk_paths(roots: &[TempDir]) -> Vec<PathBuf> {
    roots.iter().map(|root| root.path().to_path_buf()).collect()
}

fn format_path(root: &TempDir) -> PathBuf {
    root.path().join(MINIO_META_BUCKET).join(FORMAT_CONFIG_FILE)
}

#[test]
fn test_fix_format_v3_line_30() {
    let roots = new_disk_roots(8);
    let (storage_disks, errs) = init_storage_disks_with_errors(&disk_paths(&roots));
    assert!(errs
        .into_iter()
        .all(|err| err.is_none() || err.as_deref() == Some(ERR_DISK_NOT_FOUND)));

    let base = new_format_erasure_v3(1, 8);
    let mut formats = (0..8)
        .map(|index| {
            let mut format = base.clone();
            format.erasure.this = base.erasure.sets[0][index].clone();
            Some(format)
        })
        .collect::<Vec<_>>();
    formats[1] = None;
    let expected = formats[2].as_ref().expect("format").erasure.this.clone();
    formats[2].as_mut().expect("format").erasure.this.clear();

    fix_format_erasure_v3(&storage_disks, &mut formats).expect("fix format");
    let (loaded, errs) = load_format_erasure_all(&storage_disks);
    assert!(errs
        .into_iter()
        .all(|err| err.is_none() || err.as_deref() == Some(ERR_UNFORMATTED_DISK)));
    assert_eq!(
        loaded[2].as_ref().expect("loaded format").erasure.this,
        expected
    );
}

#[test]
fn test_format_erasure_empty_line_77() {
    let base = new_format_erasure_v3(1, 16);
    let mut formats = (0..16)
        .map(|index| {
            let mut format = base.clone();
            format.erasure.this = base.erasure.sets[0][index].clone();
            Some(format)
        })
        .collect::<Vec<_>>();
    formats[0] = None;
    assert!(!format_erasure_v3_this_empty(&formats));

    formats[2].as_mut().expect("format").erasure.this.clear();
    assert!(format_erasure_v3_this_empty(&formats));
}

#[test]
fn test_format_erasure_migrate_line_103() {
    let root = TempDir::new().expect("temp root");
    let v1 = FormatErasureV1 {
        meta: FormatMetaV1 {
            version: FORMAT_META_VERSION_V1.to_string(),
            format: FORMAT_BACKEND_ERASURE.to_string(),
            id: String::new(),
        },
        erasure: FormatErasureV1Body {
            version: FORMAT_ERASURE_VERSION_V1.to_string(),
            disk: "disk-a".to_string(),
            jbod: vec![
                "disk-a".to_string(),
                "disk-b".to_string(),
                "disk-c".to_string(),
                "disk-d".to_string(),
            ],
        },
    };
    let path = format_path(&root);
    fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    fs::write(&path, serde_json::to_vec(&v1).expect("marshal")).expect("write");

    let (migrated, v3) = format_erasure_migrate(root.path()).expect("migrate");
    assert_eq!(
        format_get_backend_erasure_version(&migrated).expect("version"),
        FORMAT_ERASURE_VERSION_V3
    );
    assert_eq!(v3.erasure.this, v1.erasure.disk);
    assert_eq!(v3.erasure.sets, vec![v1.erasure.jbod.clone()]);

    let mut bad_backend = v1.clone();
    bad_backend.meta.format = "unknown".to_string();
    fs::write(&path, serde_json::to_vec(&bad_backend).expect("marshal")).expect("write");
    assert!(format_erasure_migrate(root.path()).is_err());

    let mut bad_version = v1;
    bad_version.erasure.version = "30".to_string();
    fs::write(&path, serde_json::to_vec(&bad_version).expect("marshal")).expect("write");
    assert!(format_erasure_migrate(root.path()).is_err());
}

#[test]
fn test_check_format_erasure_value_line_201() {
    let mut bad_meta = new_format_erasure_v3(1, 4);
    bad_meta.meta.version = "2".to_string();
    assert!(check_format_erasure_value(&bad_meta, None).is_err());

    let mut bad_format = new_format_erasure_v3(1, 4);
    bad_format.meta.format = "Unknown".to_string();
    assert!(check_format_erasure_value(&bad_format, None).is_err());

    let mut bad_erasure = new_format_erasure_v3(1, 4);
    bad_erasure.erasure.version = "0".to_string();
    assert!(check_format_erasure_value(&bad_erasure, None).is_err());
}

#[test]
fn test_get_format_erasure_in_quorum_check_line_271() {
    let base = new_format_erasure_v3(2, 16);
    let mut formats = (0..32)
        .map(|index| {
            let mut format = base.clone();
            let set = index / 16;
            let drive = index % 16;
            format.erasure.this = base.erasure.sets[set][drive].clone();
            Some(format)
        })
        .collect::<Vec<_>>();

    let quorum = get_format_erasure_in_quorum(&formats).expect("quorum");
    assert!(format_erasure_v3_check(&quorum, formats[0].as_ref().expect("format")).is_ok());
    assert!(format_erasure_v3_check(formats[0].as_ref().expect("format"), &quorum).is_err());

    formats[0] = None;
    let quorum = get_format_erasure_in_quorum(&formats).expect("quorum after nil");

    let mut bad_sets = quorum.clone();
    bad_sets.erasure.sets.clear();
    assert!(format_erasure_v3_check(&quorum, &bad_sets).is_err());

    let mut bad_uuid = quorum.clone();
    bad_uuid.erasure.sets[0][0] = "bad-uuid".to_string();
    assert!(format_erasure_v3_check(&quorum, &bad_uuid).is_err());

    let mut bad_set_size = quorum.clone();
    bad_set_size.erasure.sets[0].clear();
    assert!(format_erasure_v3_check(&quorum, &bad_set_size).is_err());

    for entry in formats.iter_mut().take(17) {
        *entry = None;
    }
    assert_eq!(
        get_format_erasure_in_quorum(&formats),
        Err(ERR_ERASURE_READ_QUORUM.to_string())
    );
}

#[test]
fn benchmark_get_format_erasure_in_quorum_old_line_385() {
    let base = new_format_erasure_v3(4, 15);
    let formats = (0..60)
        .map(|index| {
            let mut format = base.clone();
            let set = index / 15;
            let drive = index % 15;
            format.erasure.this = base.erasure.sets[set][drive].clone();
            Some(format)
        })
        .collect::<Vec<_>>();
    for _ in 0..16 {
        assert!(get_format_erasure_in_quorum(&formats).is_ok());
    }
}

#[test]
fn benchmark_get_format_erasure_in_quorum_line_408() {
    let base = new_format_erasure_v3(4, 15);
    let formats = (0..60)
        .map(|index| {
            let mut format = base.clone();
            let set = index / 15;
            let drive = index % 15;
            format.erasure.this = base.erasure.sets[set][drive].clone();
            Some(format)
        })
        .collect::<Vec<_>>();
    for _ in 0..16 {
        let quorum = get_format_erasure_in_quorum(&formats).expect("quorum");
        assert!(quorum.erasure.this.is_empty());
    }
}

#[test]
fn test_new_format_sets_line_432() {
    let base = new_format_erasure_v3(2, 16);
    let formats = (0..32)
        .map(|index| {
            let mut format = base.clone();
            let set = index / 16;
            let drive = index % 16;
            format.erasure.this = base.erasure.sets[set][drive].clone();
            Some(format)
        })
        .collect::<Vec<_>>();
    let quorum = get_format_erasure_in_quorum(&formats).expect("quorum");
    let mut errs = vec![None; 32];
    errs[15] = Some(ERR_UNFORMATTED_DISK.to_string());

    let healed = new_heal_format_sets(&quorum, 2, 16, &formats, &errs).expect("heal sets");
    for set in healed {
        for entry in set {
            let Some(entry) = entry else {
                continue;
            };
            assert_eq!(entry.meta.id, quorum.meta.id);
        }
    }
}

fn benchmark_init_storage_disks_n(n_disks: usize) {
    let roots = new_disk_roots(n_disks);
    let paths = disk_paths(&roots);
    for _ in 0..4 {
        let (disks, errs) = init_storage_disks_with_errors(&paths);
        assert_eq!(disks.len(), n_disks);
        assert_eq!(errs.len(), n_disks);
    }
}

#[test]
fn benchmark_init_storage_disks256_line_475() {
    benchmark_init_storage_disks_n(256);
}

#[test]
fn benchmark_init_storage_disks1024_line_479() {
    benchmark_init_storage_disks_n(1024);
}

#[test]
fn benchmark_init_storage_disks2048_line_483() {
    benchmark_init_storage_disks_n(2048);
}

#[test]
fn benchmark_init_storage_disks_max_line_487() {
    benchmark_init_storage_disks_n(32 * 32);
}
