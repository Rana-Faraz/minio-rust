use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use minio_rust::cmd::{
    new_object_layer, resolve_server_config_file, MinioServerConfig, DEFAULT_SERVER_CONFIG_FILE,
};
use tempfile::TempDir;

pub const SOURCE_FILE: &str = "cmd/server-main_test.go";

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn test_server_config_file_line_26() {
    assert_eq!(
        resolve_server_config_file(""),
        PathBuf::from(DEFAULT_SERVER_CONFIG_FILE)
    );
    assert_eq!(
        resolve_server_config_file("/tmp/minio"),
        PathBuf::from("/tmp/minio").join("config.json")
    );
    assert_eq!(
        resolve_server_config_file("/tmp/minio/custom.json"),
        PathBuf::from("/tmp/minio/custom.json")
    );
}

#[test]
fn subtest_test_server_config_file_testcase_config_line_55() {
    let cases = [
        ("", PathBuf::from(DEFAULT_SERVER_CONFIG_FILE)),
        (
            "config-dir",
            PathBuf::from("config-dir").join("config.json"),
        ),
        ("config.json", PathBuf::from("config.json")),
    ];

    for (config, expected) in cases {
        assert_eq!(resolve_server_config_file(config), expected);
    }
}

#[test]
fn test_new_object_layer_line_77() {
    let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().expect("temp disk")).collect();
    let disk_paths = dirs
        .iter()
        .map(|dir| dir.path().to_path_buf())
        .collect::<Vec<_>>();

    let object_layer = new_object_layer(disk_paths.clone()).expect("new object layer");
    assert_eq!(object_layer.disk_paths(), disk_paths.as_slice());

    let duplicate_err = new_object_layer(vec![disk_paths[0].clone(), disk_paths[0].clone()])
        .expect_err("duplicates must fail");
    assert!(duplicate_err.contains("duplicate"));

    let empty_err = new_object_layer(Vec::new()).expect_err("empty disks must fail");
    assert!(empty_err.contains("at least one disk"));
}

#[test]
fn test_server_config_reads_secret_file_envs() {
    let _guard = env_lock().lock().expect("env lock");
    let secrets = TempDir::new().expect("temp secrets");
    let access = secrets.path().join("access_key");
    let secret = secrets.path().join("secret_key");
    std::fs::write(&access, "secret-user\n").expect("write access");
    std::fs::write(&secret, "secret-password\n").expect("write secret");

    let old_user = std::env::var("MINIO_ROOT_USER").ok();
    let old_password = std::env::var("MINIO_ROOT_PASSWORD").ok();
    let old_user_file = std::env::var("MINIO_ROOT_USER_FILE").ok();
    let old_password_file = std::env::var("MINIO_ROOT_PASSWORD_FILE").ok();
    unsafe {
        std::env::remove_var("MINIO_ROOT_USER");
        std::env::remove_var("MINIO_ROOT_PASSWORD");
        std::env::set_var("MINIO_ROOT_USER_FILE", access.as_os_str());
        std::env::set_var("MINIO_ROOT_PASSWORD_FILE", secret.as_os_str());
    }

    let config = MinioServerConfig::from_cli_args(&["server".to_string(), "/data".to_string()])
        .expect("config");
    assert_eq!(config.root_user, "secret-user");
    assert_eq!(config.root_password, "secret-password");

    unsafe {
        if let Some(value) = old_user {
            std::env::set_var("MINIO_ROOT_USER", value);
        } else {
            std::env::remove_var("MINIO_ROOT_USER");
        }
        if let Some(value) = old_password {
            std::env::set_var("MINIO_ROOT_PASSWORD", value);
        } else {
            std::env::remove_var("MINIO_ROOT_PASSWORD");
        }
        if let Some(value) = old_user_file {
            std::env::set_var("MINIO_ROOT_USER_FILE", value);
        } else {
            std::env::remove_var("MINIO_ROOT_USER_FILE");
        }
        if let Some(value) = old_password_file {
            std::env::set_var("MINIO_ROOT_PASSWORD_FILE", value);
        } else {
            std::env::remove_var("MINIO_ROOT_PASSWORD_FILE");
        }
    }
}

#[test]
fn test_server_config_reads_legacy_secret_file_envs() {
    let _guard = env_lock().lock().expect("env lock");
    let secrets = TempDir::new().expect("temp secrets");
    let access = secrets.path().join("access_key");
    let secret = secrets.path().join("secret_key");
    std::fs::write(&access, "legacy-user\n").expect("write access");
    std::fs::write(&secret, "legacy-password\n").expect("write secret");

    let old_root_user = std::env::var("MINIO_ROOT_USER").ok();
    let old_root_password = std::env::var("MINIO_ROOT_PASSWORD").ok();
    let old_access_file = std::env::var("MINIO_ACCESS_KEY_FILE").ok();
    let old_secret_file = std::env::var("MINIO_SECRET_KEY_FILE").ok();
    unsafe {
        std::env::remove_var("MINIO_ROOT_USER");
        std::env::remove_var("MINIO_ROOT_PASSWORD");
        std::env::set_var("MINIO_ACCESS_KEY_FILE", access.as_os_str());
        std::env::set_var("MINIO_SECRET_KEY_FILE", secret.as_os_str());
    }

    let config = MinioServerConfig::from_cli_args(&["server".to_string(), "/data".to_string()])
        .expect("config");
    assert_eq!(config.root_user, "legacy-user");
    assert_eq!(config.root_password, "legacy-password");

    unsafe {
        if let Some(value) = old_root_user {
            std::env::set_var("MINIO_ROOT_USER", value);
        } else {
            std::env::remove_var("MINIO_ROOT_USER");
        }
        if let Some(value) = old_root_password {
            std::env::set_var("MINIO_ROOT_PASSWORD", value);
        } else {
            std::env::remove_var("MINIO_ROOT_PASSWORD");
        }
        if let Some(value) = old_access_file {
            std::env::set_var("MINIO_ACCESS_KEY_FILE", value);
        } else {
            std::env::remove_var("MINIO_ACCESS_KEY_FILE");
        }
        if let Some(value) = old_secret_file {
            std::env::set_var("MINIO_SECRET_KEY_FILE", value);
        } else {
            std::env::remove_var("MINIO_SECRET_KEY_FILE");
        }
    }
}

#[test]
fn test_server_config_reads_minio_volumes_env() {
    let _guard = env_lock().lock().expect("env lock");
    let old_volumes = std::env::var("MINIO_VOLUMES").ok();
    unsafe {
        std::env::set_var("MINIO_VOLUMES", "/data1 /data2");
    }

    let config = MinioServerConfig::from_cli_args(&["server".to_string()]).expect("config");
    assert_eq!(
        config.disks,
        vec![PathBuf::from("/data1"), PathBuf::from("/data2")]
    );

    unsafe {
        if let Some(value) = old_volumes {
            std::env::set_var("MINIO_VOLUMES", value);
        } else {
            std::env::remove_var("MINIO_VOLUMES");
        }
    }
}

#[test]
fn test_server_config_loads_config_env_file() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = TempDir::new().expect("config env tempdir");
    let config_env = dir.path().join("config.env");
    std::fs::write(
        &config_env,
        "MINIO_ROOT_USER=env-user\nMINIO_ROOT_PASSWORD=env-password\nMINIO_VOLUMES=/data1 /data2\n",
    )
    .expect("write config env");

    let old_root_user = std::env::var("MINIO_ROOT_USER").ok();
    let old_root_password = std::env::var("MINIO_ROOT_PASSWORD").ok();
    let old_volumes = std::env::var("MINIO_VOLUMES").ok();
    let old_config_env = std::env::var("MINIO_CONFIG_ENV_FILE").ok();
    unsafe {
        std::env::remove_var("MINIO_ROOT_USER");
        std::env::remove_var("MINIO_ROOT_PASSWORD");
        std::env::remove_var("MINIO_VOLUMES");
        std::env::set_var("MINIO_CONFIG_ENV_FILE", config_env.as_os_str());
    }

    let config = MinioServerConfig::from_cli_args(&["server".to_string()]).expect("config");
    assert_eq!(config.root_user, "env-user");
    assert_eq!(config.root_password, "env-password");
    assert_eq!(
        config.disks,
        vec![PathBuf::from("/data1"), PathBuf::from("/data2")]
    );

    unsafe {
        if let Some(value) = old_root_user {
            std::env::set_var("MINIO_ROOT_USER", value);
        } else {
            std::env::remove_var("MINIO_ROOT_USER");
        }
        if let Some(value) = old_root_password {
            std::env::set_var("MINIO_ROOT_PASSWORD", value);
        } else {
            std::env::remove_var("MINIO_ROOT_PASSWORD");
        }
        if let Some(value) = old_volumes {
            std::env::set_var("MINIO_VOLUMES", value);
        } else {
            std::env::remove_var("MINIO_VOLUMES");
        }
        if let Some(value) = old_config_env {
            std::env::set_var("MINIO_CONFIG_ENV_FILE", value);
        } else {
            std::env::remove_var("MINIO_CONFIG_ENV_FILE");
        }
    }
}

#[test]
fn test_server_config_ignores_missing_secret_defaults() {
    let _guard = env_lock().lock().expect("env lock");
    let old_root_user = std::env::var("MINIO_ROOT_USER").ok();
    let old_root_password = std::env::var("MINIO_ROOT_PASSWORD").ok();
    let old_user_file = std::env::var("MINIO_ROOT_USER_FILE").ok();
    let old_password_file = std::env::var("MINIO_ROOT_PASSWORD_FILE").ok();
    unsafe {
        std::env::remove_var("MINIO_ROOT_USER");
        std::env::remove_var("MINIO_ROOT_PASSWORD");
        std::env::set_var("MINIO_ROOT_USER_FILE", "access_key");
        std::env::set_var("MINIO_ROOT_PASSWORD_FILE", "secret_key");
    }

    let config = MinioServerConfig::from_cli_args(&["server".to_string(), "/data".to_string()])
        .expect("config");
    assert_eq!(config.root_user, "minioadmin");
    assert_eq!(config.root_password, "minioadmin");

    unsafe {
        if let Some(value) = old_root_user {
            std::env::set_var("MINIO_ROOT_USER", value);
        } else {
            std::env::remove_var("MINIO_ROOT_USER");
        }
        if let Some(value) = old_root_password {
            std::env::set_var("MINIO_ROOT_PASSWORD", value);
        } else {
            std::env::remove_var("MINIO_ROOT_PASSWORD");
        }
        if let Some(value) = old_user_file {
            std::env::set_var("MINIO_ROOT_USER_FILE", value);
        } else {
            std::env::remove_var("MINIO_ROOT_USER_FILE");
        }
        if let Some(value) = old_password_file {
            std::env::set_var("MINIO_ROOT_PASSWORD_FILE", value);
        } else {
            std::env::remove_var("MINIO_ROOT_PASSWORD_FILE");
        }
    }
}

#[test]
fn test_server_config_accepts_console_address_flag() {
    let config = MinioServerConfig::from_cli_args(&[
        "server".to_string(),
        "--console-address".to_string(),
        ":9001".to_string(),
        "/data".to_string(),
    ])
    .expect("config");
    assert_eq!(config.disks, vec![PathBuf::from("/data")]);
}

#[test]
fn test_server_config_expands_local_ellipses_disks() {
    let root = TempDir::new().expect("temp root");
    let pattern = root
        .path()
        .join("disk{1...4}")
        .to_string_lossy()
        .to_string();

    let config =
        MinioServerConfig::from_cli_args(&["server".to_string(), pattern]).expect("config");
    assert_eq!(config.disks.len(), 4);
    assert!(config.disks[0].ends_with("disk1"));
    assert!(config.disks[1].ends_with("disk2"));
    assert!(config.disks[2].ends_with("disk3"));
    assert!(config.disks[3].ends_with("disk4"));
}

#[test]
fn test_server_config_rejects_distributed_url_endpoints() {
    let err = MinioServerConfig::from_cli_args(&[
        "server".to_string(),
        "http://minio{1...4}/data{1...2}".to_string(),
    ])
    .expect_err("distributed URL endpoints should fail");
    assert!(err.contains("distributed URL endpoints"));
}
