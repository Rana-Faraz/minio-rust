use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::cmd::{BucketOptions, LocalObjectLayer, MakeBucketOptions, ObjectOptions, PutObjReader};

pub const GLOBAL_MINIO_DEFAULT_REGION: &str = "us-east-1";
pub const DEFAULT: &str = "_";
pub const SITE_SUBSYS: &str = "site";
pub const REGION_SUBSYS: &str = "region";
const CONFIG_BUCKET: &str = ".minio.sys";
const CONFIG_OBJECT: &str = "config/config.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SiteConfig {
    pub region: String,
}

impl SiteConfig {
    pub fn region(&self) -> &str {
        &self.region
    }
}

pub type ServerConfig = BTreeMap<String, BTreeMap<String, String>>;

static GLOBAL_SITE: OnceLock<Mutex<SiteConfig>> = OnceLock::new();
static GLOBAL_SERVER_CONFIG: OnceLock<Mutex<ServerConfig>> = OnceLock::new();

fn global_site() -> &'static Mutex<SiteConfig> {
    GLOBAL_SITE.get_or_init(|| Mutex::new(SiteConfig::default()))
}

fn global_server_config() -> &'static Mutex<ServerConfig> {
    GLOBAL_SERVER_CONFIG.get_or_init(|| Mutex::new(ServerConfig::default()))
}

pub fn current_site() -> SiteConfig {
    global_site().lock().expect("lock").clone()
}

pub fn current_server_config() -> ServerConfig {
    global_server_config().lock().expect("lock").clone()
}

pub fn set_region(config: &mut ServerConfig, region: &str) {
    config
        .entry(SITE_SUBSYS.to_string())
        .or_default()
        .insert(DEFAULT.to_string(), region.to_string());
    config
        .entry(REGION_SUBSYS.to_string())
        .or_default()
        .insert(DEFAULT.to_string(), region.to_string());
}

pub fn lookup_site(
    site_value: Option<&String>,
    region_value: Option<&String>,
) -> Result<SiteConfig, String> {
    let region = region_value
        .cloned()
        .or_else(|| site_value.cloned())
        .unwrap_or_else(|| GLOBAL_MINIO_DEFAULT_REGION.to_string());
    Ok(SiteConfig { region })
}

pub fn new_test_config(region: &str, object_layer: &LocalObjectLayer) -> Result<(), String> {
    let bucket_exists = object_layer
        .list_buckets(BucketOptions::default())?
        .into_iter()
        .any(|bucket| bucket.name == CONFIG_BUCKET);
    let system_bucket_exists = object_layer
        .disk_paths()
        .iter()
        .any(|disk| disk.join(CONFIG_BUCKET).exists());

    if !bucket_exists && !system_bucket_exists {
        object_layer.make_bucket(CONFIG_BUCKET, MakeBucketOptions::default())?;
    }

    let mut cfg = ServerConfig::default();
    set_region(&mut cfg, region);
    save_server_config(object_layer, &cfg)?;

    *global_server_config().lock().expect("lock") = cfg;
    *global_site().lock().expect("lock") = SiteConfig {
        region: region.to_string(),
    };
    Ok(())
}

pub fn save_server_config(
    object_layer: &LocalObjectLayer,
    config: &ServerConfig,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(config).map_err(|err| err.to_string())?;
    object_layer.put_object(
        CONFIG_BUCKET,
        CONFIG_OBJECT,
        &PutObjReader {
            data: bytes.clone(),
            declared_size: bytes.len() as i64,
            expected_md5: String::new(),
            expected_sha256: String::new(),
        },
        ObjectOptions::default(),
    )?;
    Ok(())
}

pub fn load_config(object_layer: &LocalObjectLayer) -> Result<(), String> {
    let data = object_layer.get_object(CONFIG_BUCKET, CONFIG_OBJECT)?;
    let cfg: ServerConfig = serde_json::from_slice(&data).map_err(|err| err.to_string())?;
    let site = lookup_site(
        cfg.get(SITE_SUBSYS).and_then(|m| m.get(DEFAULT)),
        cfg.get(REGION_SUBSYS).and_then(|m| m.get(DEFAULT)),
    )?;
    *global_server_config().lock().expect("lock") = cfg;
    *global_site().lock().expect("lock") = site;
    Ok(())
}
