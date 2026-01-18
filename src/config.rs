use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct Bootstrap {
    #[serde(default)]
    pub strict: bool,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub pidfile: Option<String>,
    #[serde(default)]
    pub logger: Logger,
    #[serde(default)]
    pub server: Server,
    #[serde(default)]
    pub plugin: Vec<Plugin>,
    #[serde(default)]
    pub upstream: Upstream,
    #[serde(default)]
    pub storage: Storage,
}

impl Bootstrap {
    pub fn validate(&self) -> Result<()> {
        if self.server.addr.trim().is_empty() {
            return Err(anyhow!("server.addr is required"));
        }
        if self.upstream.address.is_empty() {
            return Err(anyhow!("upstream.address must not be empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct Logger {
    #[serde(default)]
    pub level: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub caller: bool,
    #[serde(default)]
    pub traceid: bool,
    #[serde(default)]
    pub max_size: u64,
    #[serde(default)]
    pub max_age: Option<u64>,
    #[serde(default)]
    pub max_backups: u64,
    #[serde(default)]
    pub compress: bool,
    #[serde(default)]
    pub nopid: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct Server {
    #[serde(default)]
    pub addr: String,
    #[serde(default, with = "humantime_serde")]
    pub read_timeout: Duration,
    #[serde(default, with = "humantime_serde")]
    pub write_timeout: Duration,
    #[serde(default, with = "humantime_serde")]
    pub idle_timeout: Duration,
    #[serde(default, with = "humantime_serde")]
    pub read_header_timeout: Duration,
    #[serde(default)]
    pub max_header_bytes: usize,
    #[serde(default)]
    pub server_via_tokens: Option<serde_yaml::Value>,
    #[serde(default)]
    pub middleware: Vec<MiddlewareConfig>,
    #[serde(default)]
    pub pprof: Option<ServerPProf>,
    #[serde(default)]
    pub access_log: Option<ServerAccessLog>,
    #[serde(default)]
    pub local_api_allow_hosts: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ServerPProf {
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct ServerAccessLog {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub encrypt: AccessLogEncrypt,
}

#[derive(Debug, Deserialize, Default)]
pub struct AccessLogEncrypt {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub secret: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct MiddlewareConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub options: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Default)]
pub struct Upstream {
    #[serde(default)]
    pub balancing: String,
    #[serde(default)]
    pub address: Vec<String>,
    #[serde(default)]
    pub max_idle_conns: usize,
    #[serde(default)]
    pub max_idle_conns_per_host: usize,
    #[serde(default)]
    pub max_connections_per_server: usize,
    #[serde(default)]
    pub insecure_skip_verify: bool,
    #[serde(default)]
    pub resolve_addresses: bool,
    #[serde(default)]
    pub features: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Default)]
pub struct Storage {
    #[serde(default)]
    pub driver: String,
    #[serde(default)]
    pub db_type: String,
    #[serde(default)]
    pub db_path: String,
    #[serde(default)]
    pub async_load: bool,
    #[serde(default)]
    pub eviction_policy: String,
    #[serde(default)]
    pub selection_policy: String,
    #[serde(default)]
    pub slice_size: u64,
    #[serde(default)]
    pub buckets: Vec<Bucket>,
}

#[derive(Debug, Deserialize, Default)]
pub struct Bucket {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub driver: String,
    #[serde(default, rename = "type")]
    pub bucket_type: String,
    #[serde(default)]
    pub db_type: String,
    #[serde(default)]
    pub db_path: String,
    #[serde(default)]
    pub async_load: bool,
    #[serde(default)]
    pub slice_size: u64,
    #[serde(default)]
    pub max_object_limit: i64,
    #[serde(default)]
    pub db_config: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Default)]
pub struct Plugin {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub options: HashMap<String, serde_yaml::Value>,
}

pub fn load(path: &Path) -> Result<(Bootstrap, Vec<String>)> {
    let raw = fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let mut ignored = Vec::new();
    let de = serde_yaml::Deserializer::from_str(&raw);
    let cfg: Bootstrap = serde_ignored::deserialize(de, |path| {
        ignored.push(path.to_string());
    })
    .with_context(|| format!("parse config {}", path.display()))?;

    Ok((cfg, ignored))
}
