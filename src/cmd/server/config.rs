use super::*;

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:9000";
pub const DEFAULT_ROOT_USER: &str = "minioadmin";
pub const DEFAULT_ROOT_PASSWORD: &str = "minioadmin";
pub const DEFAULT_DOCKER_VOLUME: &str = "/data";

const DOCKER_SECRETS_DIR: &str = "/run/secrets";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinioServerConfig {
    pub address: String,
    pub root_user: String,
    pub root_password: String,
    pub disks: Vec<PathBuf>,
}

impl MinioServerConfig {
    pub fn from_cli_args(args: &[String]) -> Result<Self, String> {
        load_env_vars_from_files()?;

        let mut args = args.to_vec();
        if args.first().is_some_and(|arg| arg == "server") {
            args.remove(0);
        }

        let mut address = env::var("MINIO_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.to_string());
        let (mut root_user, mut root_password) = root_credentials_from_env()?;
        let mut disk_args = Vec::new();

        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--help" | "-h" => return Err(server_usage(None)),
                "--address" => {
                    index += 1;
                    let Some(value) = args.get(index) else {
                        return Err(server_usage(Some("missing value for --address")));
                    };
                    address = value.clone();
                }
                "--console-address" => {
                    index += 1;
                    if args.get(index).is_none() {
                        return Err(server_usage(Some("missing value for --console-address")));
                    }
                }
                "--root-user" => {
                    index += 1;
                    let Some(value) = args.get(index) else {
                        return Err(server_usage(Some("missing value for --root-user")));
                    };
                    root_user = value.clone();
                }
                "--root-password" => {
                    index += 1;
                    let Some(value) = args.get(index) else {
                        return Err(server_usage(Some("missing value for --root-password")));
                    };
                    root_password = value.clone();
                }
                value if value.starts_with("--address=") => {
                    address = value.trim_start_matches("--address=").to_string();
                }
                value if value.starts_with("--console-address=") => {}
                value if value.starts_with("--root-user=") => {
                    root_user = value.trim_start_matches("--root-user=").to_string();
                }
                value if value.starts_with("--root-password=") => {
                    root_password = value.trim_start_matches("--root-password=").to_string();
                }
                value if value.starts_with('-') => {
                    return Err(server_usage(Some(&format!("unknown flag {value}"))));
                }
                value => disk_args.push(value.to_string()),
            }
            index += 1;
        }

        if disk_args.is_empty() {
            disk_args = volumes_from_env()?;
        }
        if disk_args.is_empty() {
            return Err(server_usage(Some("at least one disk path is required")));
        }
        if root_user.is_empty() || root_password.is_empty() {
            return Err("root credentials must not be empty".to_string());
        }

        let disks = expand_disk_args(&address, &disk_args)?;

        Ok(Self {
            address,
            root_user,
            root_password,
            disks,
        })
    }
}

fn server_usage(problem: Option<&str>) -> String {
    let mut usage = String::new();
    if let Some(problem) = problem {
        usage.push_str(problem);
        usage.push('\n');
        usage.push('\n');
    }
    usage.push_str("Usage:\n");
    usage.push_str("  minio-rust server [--address HOST:PORT] [--root-user USER] [--root-password PASS] <disk> [<disk> ...]\n");
    usage.push_str("  minio-rust [--address HOST:PORT] [--root-user USER] [--root-password PASS] <disk> [<disk> ...]\n");
    usage.push_str("\nEnvironment:\n");
    usage.push_str("  MINIO_ADDRESS\n");
    usage.push_str("  MINIO_CONSOLE_ADDRESS\n");
    usage.push_str("  MINIO_ACCESS_KEY\n");
    usage.push_str("  MINIO_SECRET_KEY\n");
    usage.push_str("  MINIO_ROOT_USER\n");
    usage.push_str("  MINIO_ROOT_PASSWORD\n");
    usage.push_str("  MINIO_ACCESS_KEY_FILE\n");
    usage.push_str("  MINIO_SECRET_KEY_FILE\n");
    usage.push_str("  MINIO_ROOT_USER_FILE\n");
    usage.push_str("  MINIO_ROOT_PASSWORD_FILE\n");
    usage.push_str("  MINIO_CONFIG_ENV_FILE\n");
    usage.push_str("  MINIO_VOLUMES\n");
    usage
}

fn resolve_secret_path(value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() || value.contains('/') {
        path
    } else {
        PathBuf::from(DOCKER_SECRETS_DIR).join(value)
    }
}

fn load_env_vars_from_files() -> Result<(), String> {
    set_env_var_from_secret("MINIO_ROOT_USER", "MINIO_ACCESS_KEY_FILE")?;
    set_env_var_from_secret("MINIO_ROOT_PASSWORD", "MINIO_SECRET_KEY_FILE")?;
    set_env_var_from_secret("MINIO_ROOT_USER", "MINIO_ROOT_USER_FILE")?;
    set_env_var_from_secret("MINIO_ROOT_PASSWORD", "MINIO_ROOT_PASSWORD_FILE")?;

    if let Ok(path) = env::var("MINIO_CONFIG_ENV_FILE") {
        for entry in minio_environ_from_file(&path)? {
            unsafe {
                env::set_var(entry.key, entry.value);
            }
        }
    }

    Ok(())
}

fn set_env_var_from_secret(target_key: &str, file_key: &str) -> Result<(), String> {
    let Ok(file) = env::var(file_key) else {
        return Ok(());
    };
    if file.trim().is_empty() {
        return Ok(());
    }

    let path = resolve_secret_path(&file);
    let value = read_from_secret(
        path.to_str()
            .ok_or_else(|| "secret path must be utf8".to_string())?,
    )?;
    if !value.is_empty() {
        unsafe {
            env::set_var(target_key, value);
        }
    }

    Ok(())
}

fn root_credentials_from_env() -> Result<(String, String), String> {
    let root_user = env::var("MINIO_ROOT_USER").ok();
    let root_password = env::var("MINIO_ROOT_PASSWORD").ok();
    if root_user.is_some() ^ root_password.is_some() {
        return Err(
            "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD must be provided together".to_string(),
        );
    }
    if let (Some(user), Some(password)) = (root_user, root_password) {
        return Ok((user, password));
    }

    let access_key = env::var("MINIO_ACCESS_KEY").ok();
    let secret_key = env::var("MINIO_SECRET_KEY").ok();
    if access_key.is_some() ^ secret_key.is_some() {
        return Err("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be provided together".to_string());
    }
    if let (Some(user), Some(password)) = (access_key, secret_key) {
        return Ok((user, password));
    }

    Ok((
        DEFAULT_ROOT_USER.to_string(),
        DEFAULT_ROOT_PASSWORD.to_string(),
    ))
}

fn volumes_from_env() -> Result<Vec<String>, String> {
    let Some(value) = env::var("MINIO_VOLUMES").ok() else {
        return Ok(Vec::new());
    };
    Ok(value
        .split_whitespace()
        .filter(|entry| !entry.is_empty())
        .map(ToString::to_string)
        .collect())
}

pub(super) fn console_address_from_env() -> Option<String> {
    env::var("MINIO_CONSOLE_ADDRESS")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn console_address_from_args_or_env(args: &[String]) -> Result<Option<String>, String> {
    let mut console_address = console_address_from_env();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--console-address" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(server_usage(Some("missing value for --console-address")));
                };
                console_address = Some(value.clone());
            }
            value if value.starts_with("--console-address=") => {
                console_address = Some(value.trim_start_matches("--console-address=").to_string());
            }
            _ => {}
        }
        index += 1;
    }
    Ok(console_address.filter(|value| !value.trim().is_empty()))
}

pub(super) fn validate_console_address(
    api_address: &str,
    console_address: &str,
) -> Result<(), String> {
    if api_address == console_address && !api_address.ends_with(":0") {
        return Err("--console-address cannot be same as --address".to_string());
    }
    let api_port = bind_port(api_address).unwrap_or_default();
    let console_port = bind_port(console_address).unwrap_or_default();
    if !api_port.is_empty() && api_port != "0" && console_port != "0" && api_port == console_port {
        return Err("--console-address port cannot be same as --address port".to_string());
    }
    Ok(())
}

pub(super) fn normalize_bind_address(address: &str) -> String {
    if let Some(port) = bind_port(address) {
        if address.starts_with(':') || address == port {
            return format!("0.0.0.0:{port}");
        }
    }
    address.to_string()
}

fn bind_port(address: &str) -> Option<&str> {
    if let Some(port) = address.strip_prefix(':') {
        return Some(port);
    }
    address.rsplit_once(':').map(|(_, port)| port)
}

pub(super) fn reachable_address(input: &str, bound: &str) -> String {
    let bound_port = bind_port(bound).unwrap_or(bound);
    if input.starts_with(':') {
        return format!("127.0.0.1:{bound_port}");
    }
    if let Some((host, _)) = input.rsplit_once(':') {
        if host == "0.0.0.0" || host == "::" || host == "[::]" {
            return format!("127.0.0.1:{bound_port}");
        }
    }
    bound.to_string()
}

pub(super) fn expand_disk_args(address: &str, args: &[String]) -> Result<Vec<PathBuf>, String> {
    let layout = merge_disks_layout_from_args(args)?;
    let (pools, _) = create_server_endpoints(address, &layout.pools, layout.legacy)?;
    let mut disks = Vec::new();
    for pool in pools {
        for endpoint in pool.resolved_endpoints {
            if endpoint.endpoint_type() == EndpointType::Url {
                return Err(
                    "distributed URL endpoints are not yet supported by minio-rust".to_string(),
                );
            }
            let path = endpoint
                .url
                .to_file_path()
                .map_err(|_| "invalid local endpoint path".to_string())?;
            disks.push(path);
        }
    }
    Ok(disks)
}
