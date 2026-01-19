use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use crc32fast::Hasher;
use http::{Request, StatusCode};
use http_body_util::Full;
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use serde::Serialize;

use crate::event::{self, CacheCompletedPayload};
use crate::config;
use crate::plugin::Plugin;

#[derive(Debug, Deserialize)]
struct VerifierOptions {
    #[serde(default = "default_endpoint")]
    endpoint: String,
    #[serde(default = "default_timeout")]
    timeout: u64,
    #[serde(default = "default_report_ratio")]
    report_ratio: i32,
    #[serde(default)]
    api_key: String,
}

fn default_endpoint() -> String {
    "http://verifier.default.svc.cluster.local:8080/report".to_string()
}

fn default_timeout() -> u64 {
    5
}

fn default_report_ratio() -> i32 {
    1
}

#[derive(Clone)]
pub struct VerifierPlugin {
    options: Arc<VerifierOptions>,
    client: Client<HttpConnector, Full<Bytes>>,
}

impl Plugin for VerifierPlugin {
    fn name(&self) -> &str {
        "verifier"
    }

    fn start(&self) -> Result<()> {
        let options = Arc::clone(&self.options);
        let client = self.client.clone();
        let topic = event::new_topic_key::<CacheCompletedPayload>(event::CACHE_COMPLETED_KEY);
        event::subscribe(&topic, move |_ctx, payload| {
            let payload = payload.clone();
            let options = Arc::clone(&options);
            let client = client.clone();
            tokio::spawn(async move {
                if let Err(err) = handle_event(&client, &options, payload).await {
                    log::error!("verifier report failed: {err}");
                }
            });
        })?;
        Ok(())
    }
}

pub fn register() {
    crate::plugin::register("verifier", new_verifier_plugin);
}

fn new_verifier_plugin(cfg: &config::Plugin) -> Result<Arc<dyn Plugin>> {
    let options = decode_options(cfg)?;
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(Duration::from_secs(options.timeout)));
    connector.set_keepalive(Some(Duration::from_secs(30)));
    let client = Client::builder(TokioExecutor::new()).build(connector);
    Ok(Arc::new(VerifierPlugin {
        options: Arc::new(options),
        client,
    }))
}

fn decode_options(cfg: &config::Plugin) -> Result<VerifierOptions> {
    if cfg.options.is_empty() {
        return Ok(VerifierOptions {
            endpoint: default_endpoint(),
            timeout: default_timeout(),
            report_ratio: default_report_ratio(),
            api_key: String::new(),
        });
    }
    let val = serde_yaml::to_value(&cfg.options)?;
    Ok(serde_yaml::from_value(val)?)
}

async fn handle_event(
    client: &Client<HttpConnector, Full<Bytes>>,
    options: &VerifierOptions,
    payload: CacheCompletedPayload,
) -> Result<()> {
    let ratio = hash_ratio(&payload.store_key);
    let mut report_ratio = payload.report_ratio();
    if report_ratio == -1 {
        return Ok(());
    }
    if report_ratio == 0 {
        report_ratio = options.report_ratio;
    }
    if ratio >= report_ratio {
        return Ok(());
    }

    let hash = read_and_sum_hash(
        PathBuf::from(&payload.store_path),
        &payload.store_key,
        payload.chunk_count,
        payload.chunk_size,
    )?;

    let report = ReportPayload {
        url: payload.store_url,
        hash,
        lm: payload.last_modified,
        cl: payload.content_length as u64,
    };

    send_report(client, options, report).await
}

fn hash_ratio(key: &str) -> i32 {
    let mut hasher = Hasher::new();
    hasher.update(key.as_bytes());
    let crc = hasher.finalize();
    (crc % 100) as i32
}

#[derive(Serialize)]
struct ReportPayload {
    url: String,
    hash: String,
    lm: String,
    cl: u64,
}

async fn send_report(
    client: &Client<HttpConnector, Full<Bytes>>,
    options: &VerifierOptions,
    payload: ReportPayload,
) -> Result<()> {
    let body = serde_json::to_vec(&payload)?;
    let uri: http::Uri = options.endpoint.parse().map_err(|_| anyhow!("invalid endpoint"))?;

    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header(CONTENT_TYPE, "application/json")
        .header(AUTHORIZATION, options.api_key.clone())
        .header("Content-Length", body.len().to_string())
        .body(Full::new(Bytes::from(body)))?;

    let resp = match tokio::time::timeout(Duration::from_secs(options.timeout), client.request(req))
        .await
    {
        Ok(Ok(resp)) => resp,
        Ok(Err(err)) => {
            crate::metrics::record_verifier("0");
            return Err(err.into());
        }
        Err(err) => {
            crate::metrics::record_verifier("0");
            return Err(anyhow!("report verifier timeout: {err}"));
        }
    };
    if resp.status() == StatusCode::CONFLICT {
        crate::metrics::record_verifier("409");
        return Err(anyhow!("report verifier CRC hash conflict"));
    }
    if resp.status() != StatusCode::OK {
        crate::metrics::record_verifier(&resp.status().as_u16().to_string());
        return Err(anyhow!("report verifier failed status {}", resp.status()));
    }
    crate::metrics::record_verifier("200");
    Ok(())
}

fn read_and_sum_hash(basepath: PathBuf, cache_key: &str, count: usize, chunk_size: u64) -> Result<String> {
    let mut ctx = md5::Context::new();
    for i in 0..count {
        let path = basepath.join(format!("{cache_key}-{i:06}"));
        let mut file = File::open(&path)?;
        let mut buf = Vec::new();
        let n = file.read_to_end(&mut buf)?;
        if i + 1 < count && n as u64 != chunk_size {
            return Err(anyhow!(
                "chunk size mismatch {} expected {}",
                n,
                chunk_size
            ));
        }
        ctx.consume(&buf);
    }
    Ok(format!("{:x}", ctx.compute()))
}
