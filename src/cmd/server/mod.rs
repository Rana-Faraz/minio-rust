use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::Engine;
use percent_encoding::percent_decode_str;
use quick_xml::de::from_str;
use serde::{Deserialize, Serialize};
use tiny_http::{Header, Method, Response, Server, StatusCode};
use url::Url;

use super::*;

mod auth;
mod config;
mod handlers;
mod metrics;
mod runtime;
mod state;
mod xml;

pub use config::{
    MinioServerConfig, DEFAULT_ADDRESS, DEFAULT_DOCKER_VOLUME, DEFAULT_ROOT_PASSWORD,
    DEFAULT_ROOT_USER,
};
pub use runtime::{run_cli, run_server, spawn_server};
pub use state::ServerHandle;

#[cfg(test)]
use state::{NOTIFICATION_HISTORY_FILE, REPLICATION_QUEUE_FILE};

#[cfg(test)]
use runtime::{spawn_server_with_replication_targets, spawn_server_with_webhook_targets};

#[cfg(test)]
#[path = "../../../tests/cmd/server_mod_test.rs"]
mod tests;
