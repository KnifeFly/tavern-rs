use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http::{HeaderValue, Response, StatusCode};
use http_body_util::Full;
use serde::Deserialize;
use serde::Serialize;

use crate::config;
use crate::plugin::{Plugin, Router};
use crate::storage;
use crate::storage::object::Metadata;

#[derive(Debug, Deserialize, Default, Clone)]
struct ExampleOptions {
    #[serde(default)]
    option1: String,
    #[serde(default)]
    option2: i64,
}

pub struct ExamplePlugin {
    _options: ExampleOptions,
}

impl Plugin for ExamplePlugin {
    fn name(&self) -> &str {
        "example-plugin"
    }

    fn add_router(&self, router: &mut Router) {
        let opts = self._options.clone();
        router.add("/plugin/store/disk", move |_req| {
            let store = storage::current();
            let mut buckets = std::collections::HashMap::new();
            for bucket in store.buckets() {
                buckets.insert(bucket.id().to_string(), bucket.objects());
            }
            json_response_with_opts(&buckets, &opts)
        });

        let opts = self._options.clone();
        router.add("/plugin/store/object/simple", move |req| {
            let store = storage::current();
            let use_hash = req
                .uri()
                .query()
                .map(|q| q.split('&').any(|kv| kv.split('=').next() == Some("hash")))
                .unwrap_or(false);
            let mut objects = Vec::new();
            for bucket in store.buckets() {
                let _ = bucket.iterate(&mut |meta| {
                    objects.push(SimpleMetadata::from_meta(meta, use_hash));
                    Ok(())
                });
            }
            json_response_with_opts(&objects, &opts)
        });

        let opts = self._options.clone();
        router.add("/plugin/store/service-domains", move |_req| {
            let store = storage::current();
            let mut domain_map = std::collections::HashMap::<String, u32>::new();
            let _ = store.shared_kv().iterate_prefix(b"if/domain", &mut |key, val| {
                if key.len() > 10 && val.len() >= 4 {
                    let domain = String::from_utf8_lossy(&key[10..]).to_string();
                    let count = u32::from_be_bytes([val[0], val[1], val[2], val[3]]);
                    domain_map.insert(domain, count);
                }
                Ok(())
            });
            json_response_with_opts(&domain_map, &opts)
        });
    }
}

pub fn register() {
    crate::plugin::register("example-plugin", new_example_plugin);
}

fn new_example_plugin(cfg: &config::Plugin) -> Result<Arc<dyn Plugin>> {
    let options = decode_options(cfg)?;
    Ok(Arc::new(ExamplePlugin { _options: options }))
}

fn decode_options(cfg: &config::Plugin) -> Result<ExampleOptions> {
    if cfg.options.is_empty() {
        return Ok(ExampleOptions::default());
    }
    let val = serde_yaml::to_value(&cfg.options)?;
    Ok(serde_yaml::from_value(val)?)
}

#[derive(Serialize)]
struct SimpleMetadata {
    id: String,
    chunks: String,
    code: i32,
    size: u64,
    resp_unix: i64,
    expired: i64,
    flags: String,
    cache_ref: i64,
    vd: Vec<String>,
}

impl SimpleMetadata {
    fn from_meta(meta: &Metadata, use_hash: bool) -> Self {
        Self {
            id: if use_hash { meta.id.hash_str() } else { meta.id.key() },
            chunks: conv_range(&meta.chunks),
            code: meta.code,
            size: meta.size,
            resp_unix: meta.resp_unix,
            expired: meta.expires_at,
            flags: format!("{:?}", meta.flags),
            cache_ref: meta.refs,
            vd: meta.virtual_key.clone(),
        }
    }
}

fn conv_range(chunks: &crate::storage::object::ChunkSet) -> String {
    let mut nums: Vec<u32> = chunks.iter().copied().collect();
    nums.sort_unstable();
    if nums.is_empty() {
        return String::new();
    }
    let mut ranges = Vec::new();
    let mut start = nums[0];
    let mut prev = nums[0];
    for n in nums.iter().skip(1) {
        if *n != prev + 1 {
            if start == prev {
                ranges.push(format!("{start}"));
            } else {
                ranges.push(format!("{start}-{prev}"));
            }
            start = *n;
        }
        prev = *n;
    }
    if start == prev {
        ranges.push(format!("{start}"));
    } else {
        ranges.push(format!("{start}-{prev}"));
    }
    ranges.join(",")
}

fn json_response<T: Serialize>(payload: &T) -> Response<Full<Bytes>> {
    match serde_json::to_vec(payload) {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .header("Content-Length", body.len().to_string())
            .body(Full::new(Bytes::from(body)))
            .unwrap(),
        Err(_) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Full::new(Bytes::new()))
            .unwrap(),
    }
}

fn json_response_with_opts<T: Serialize>(
    payload: &T,
    opts: &ExampleOptions,
) -> Response<Full<Bytes>> {
    let mut resp = json_response(payload);
    if !opts.option1.is_empty() {
        if let Ok(val) = HeaderValue::from_str(&opts.option1) {
            resp.headers_mut().insert("X-Example-Option1", val);
        }
    }
    if opts.option2 != 0 {
        if let Ok(val) = HeaderValue::from_str(&opts.option2.to_string()) {
            resp.headers_mut().insert("X-Example-Option2", val);
        }
    }
    resp
}
