use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use log::warn;
use serde::Deserialize;

use crate::config;
use crate::constants;
use crate::plugin::Plugin;
use crate::storage;

const METHOD_PURGE: &str = "PURGE";

#[derive(Debug, Deserialize)]
struct PurgeOptions {
    #[serde(default)]
    threshold: Option<i64>,
    #[serde(default)]
    allow_hosts: Vec<String>,
    #[serde(default = "default_header_name")]
    header_name: String,
    #[serde(default)]
    log_path: Option<String>,
}

fn default_header_name() -> String {
    "Purge-Type".to_string()
}

pub struct PurgePlugin {
    options: PurgeOptions,
    allow_hosts: HashSet<String>,
    log_writer: Option<Mutex<std::fs::File>>,
    threshold_window: Mutex<ThresholdWindow>,
}

struct ThresholdWindow {
    start: Instant,
    count: u64,
    threshold: Option<u64>,
}

impl Plugin for PurgePlugin {
    fn name(&self) -> &str {
        "purge"
    }

    fn add_router(&self, router: &mut crate::plugin::Router) {
        router.add("/plugin/purge/tasks", |_req| {
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .header("Content-Length", "0")
                .header("X-Device-Plugin", "purger")
                .body(Full::new(Bytes::new()))
                .unwrap()
        });
    }

    fn handle_request(&self, req: &Request<Incoming>) -> Option<Response<Full<Bytes>>> {
        if req.method().as_str() != METHOD_PURGE {
            return None;
        }

        let addr = req
            .headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.split(',').next().unwrap_or(v).trim().to_string())
            .or_else(|| {
                req.extensions()
                    .get::<crate::net::RemoteAddr>()
                    .map(|v| v.0.clone())
            });

        if let Some(addr) = addr {
            if !self.allow_hosts.is_empty() && !self.allow_hosts.contains(&addr) {
                return Some(empty_response(StatusCode::FORBIDDEN));
            }
        }

        let store_url = req
            .headers()
            .get(constants::INTERNAL_STORE_URL)
            .and_then(|v| v.to_str().ok())
            .filter(|v| !v.is_empty())
            .or_else(|| {
                req.headers()
                    .get("X-Store-Url")
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| !v.is_empty())
            })
            .map(|v| v.to_string())
            .unwrap_or_else(|| req.uri().to_string());

        let is_dir = req
            .headers()
            .get(self.options.header_name.as_str())
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("dir"))
            .unwrap_or(false);

        if is_dir {
            if let Ok(uri) = store_url.parse::<http::Uri>() {
                if let Some(host) = uri.host() {
                    let key = format!("if/domain/{host}");
                    if storage::current().shared_kv().get(key.as_bytes()).is_err() {
                        return Some(empty_response(StatusCode::OK));
                    }
                }
            }
        }

        let result = storage::current().purge(
            &store_url,
            storage::PurgeControl {
                hard: true,
                dir: is_dir,
                mark_expired: false,
            },
        );

        let exceeded = self.bump_threshold();
        let resp = match result {
            Ok(()) => json_response(StatusCode::OK, r#"{"message":"success"}"#),
            Err(err) => {
                if err.to_string().contains("key not found") {
                    empty_response(StatusCode::NOT_FOUND)
                } else {
                    warn!("purge {} failed: {err}", store_url);
                    empty_response(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        };
        self.log_purge(resp.status(), &store_url, is_dir, exceeded);
        Some(resp)
    }
}

pub fn register() {
    crate::plugin::register("purge", new_purge_plugin);
}

fn new_purge_plugin(cfg: &config::Plugin) -> Result<std::sync::Arc<dyn Plugin>> {
    let options = decode_options(cfg)?;
    let allow_hosts = options.allow_hosts.iter().cloned().collect::<HashSet<_>>();
    let log_writer = options
        .log_path
        .as_ref()
        .and_then(|path| {
            if path.trim().is_empty() {
                return None;
            }
            let path = Path::new(path);
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            OpenOptions::new().create(true).append(true).open(path).ok()
        })
        .map(Mutex::new);
    let threshold_window = ThresholdWindow {
        start: Instant::now(),
        count: 0,
        threshold: options.threshold.and_then(|v| if v > 0 { Some(v as u64) } else { None }),
    };
    Ok(std::sync::Arc::new(PurgePlugin {
        options,
        allow_hosts,
        log_writer,
        threshold_window: Mutex::new(threshold_window),
    }))
}

fn decode_options(cfg: &config::Plugin) -> Result<PurgeOptions> {
    if cfg.options.is_empty() {
        return Ok(PurgeOptions {
            threshold: None,
            allow_hosts: Vec::new(),
            header_name: default_header_name(),
            log_path: None,
        });
    }
    let val = serde_yaml::to_value(&cfg.options)?;
    Ok(serde_yaml::from_value(val)?)
}

fn empty_response(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn json_response(status: StatusCode, payload: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Content-Length", payload.len().to_string())
        .body(Full::new(Bytes::from(payload.to_string())))
        .unwrap()
}

impl PurgePlugin {
    fn bump_threshold(&self) -> bool {
        let mut window = match self.threshold_window.lock() {
            Ok(win) => win,
            Err(_) => return false,
        };
        let Some(threshold) = window.threshold else { return false };
        if window.start.elapsed() > Duration::from_secs(60) {
            window.start = Instant::now();
            window.count = 0;
        }
        window.count += 1;
        window.count > threshold
    }

    fn log_purge(&self, status: StatusCode, url: &str, dir: bool, exceeded: bool) {
        let Some(writer) = &self.log_writer else { return };
        let stamp = storage::unix_now();
        let line = format!(
            "{} status={} dir={} exceeded={} url={}\n",
            stamp,
            status.as_u16(),
            dir,
            exceeded,
            url
        );
        if let Ok(mut writer) = writer.lock() {
            let _ = writer.write_all(line.as_bytes());
        }
    }
}
