use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use http::{HeaderMap, StatusCode};
use rand::rngs::OsRng;
use rand::RngCore;

use crate::constants;
use crate::storage;
use nix::sys::statvfs;
use tokio::task_local;

#[derive(Clone, Debug)]
pub struct RequestMetric {
    pub start_at: Instant,
    pub request_id: String,
    pub recv_req: u64,
    pub sent_resp: u64,
    pub store_url: String,
    pub cache_status: String,
    pub remote_addr: String,
}

impl RequestMetric {
    pub fn new(headers: &HeaderMap) -> Self {
        Self {
            start_at: Instant::now(),
            request_id: request_id_from_headers(headers),
            recv_req: 0,
            sent_resp: 0,
            store_url: String::new(),
            cache_status: String::new(),
            remote_addr: String::new(),
        }
    }
}

#[derive(Default)]
struct Metrics {
    requests_total: u64,
    requests_by_status: HashMap<u16, u64>,
    requests_code: HashMap<(String, String), u64>,
    unexpected_closed: HashMap<(String, String), u64>,
    verifier_requests: HashMap<String, u64>,
    disk_io: HashMap<(String, String), u64>,
}

fn metrics() -> &'static Mutex<Metrics> {
    static METRICS: OnceLock<Mutex<Metrics>> = OnceLock::new();
    METRICS.get_or_init(|| {
        let mut m = Metrics::default();
        for code in ["200", "206", "400", "404", "500"] {
            m.requests_code
                .insert(("HTTP/1.1".to_string(), code.to_string()), 0);
        }
        for method in ["GET", "HEAD"] {
            m.unexpected_closed
                .insert(("HTTP/1.1".to_string(), method.to_string()), 0);
        }
        for code in ["200", "409", "0"] {
            m.verifier_requests.insert(code.to_string(), 0);
        }
        Mutex::new(m)
    })
}

pub fn record(status: StatusCode) {
    let mut m = metrics().lock().expect("metrics");
    m.requests_total += 1;
    let code = status.as_u16();
    *m.requests_by_status.entry(code).or_insert(0) += 1;
    let protocol = current_protocol().unwrap_or_else(|| "HTTP/1.1".to_string());
    let key = (protocol, code.to_string());
    *m.requests_code.entry(key).or_insert(0) += 1;
}

pub fn record_unexpected_closed(protocol: &str, method: &str) {
    let mut m = metrics().lock().expect("metrics");
    let key = (protocol.to_string(), method.to_string());
    *m.unexpected_closed.entry(key).or_insert(0) += 1;
}

pub fn record_verifier(code: &str) {
    let mut m = metrics().lock().expect("metrics");
    *m.verifier_requests.entry(code.to_string()).or_insert(0) += 1;
}

pub fn record_disk_io(dev: &str, path: &str) {
    let mut m = metrics().lock().expect("metrics");
    let key = (dev.to_string(), path.to_string());
    *m.disk_io.entry(key).or_insert(0) += 1;
}

pub fn render() -> String {
    let m = metrics().lock().expect("metrics");
    let mut out = String::new();
    out.push_str("# TYPE tavern_requests_total counter\n");
    out.push_str(&format!("tavern_requests_total {}\n", m.requests_total));
    out.push_str("# TYPE tavern_requests_status_total counter\n");
    for (code, count) in m.requests_by_status.iter() {
        out.push_str(&format!(
            "tavern_requests_status_total{{code=\"{}\"}} {}\n",
            code, count
        ));
    }
    out.push_str("# TYPE tr_tavern_requests_code_total counter\n");
    for ((protocol, code), count) in m.requests_code.iter() {
        out.push_str(&format!(
            "tr_tavern_requests_code_total{{protocol=\"{}\",code=\"{}\"}} {}\n",
            protocol, code, count
        ));
    }
    out.push_str("# TYPE tr_tavern_requests_unexpected_closed_total counter\n");
    for ((protocol, method), count) in m.unexpected_closed.iter() {
        out.push_str(&format!(
            "tr_tavern_requests_unexpected_closed_total{{protocol=\"{}\",method=\"{}\"}} {}\n",
            protocol, method, count
        ));
    }
    out.push_str("# TYPE tr_tavern_verifier_requests_total counter\n");
    for (code, count) in m.verifier_requests.iter() {
        out.push_str(&format!(
            "tr_tavern_verifier_requests_total{{code=\"{}\"}} {}\n",
            code, count
        ));
    }
    out.push_str("# TYPE tr_tavern_disk_usage gauge\n");
    for metric in disk_usage_metrics() {
        out.push_str(&format!(
            "tr_tavern_disk_usage{{dev=\"{}\",path=\"{}\"}} {}\n",
            metric.dev, metric.path, metric.used_bytes
        ));
    }
    out.push_str("# TYPE tr_tavern_disk_io counter\n");
    for ((dev, path), count) in m.disk_io.iter() {
        out.push_str(&format!(
            "tr_tavern_disk_io{{dev=\"{}\",path=\"{}\"}} {}\n",
            dev, path, count
        ));
    }
    out
}

pub fn request_id_from_headers(headers: &HeaderMap) -> String {
    headers
        .get(constants::PROTOCOL_REQUEST_ID_KEY)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .unwrap_or_else(generate_request_id)
}

pub fn generate_request_id() -> String {
    let mut buf = [0u8; 16];
    OsRng.fill_bytes(&mut buf);
    hex::encode(buf)
}

#[derive(Clone, Debug)]
struct RequestContext {
    protocol: String,
    method: String,
}

task_local! {
    static REQUEST_CONTEXT: RequestContext;
}

pub async fn with_request_context<F, T>(protocol: String, method: String, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    REQUEST_CONTEXT.scope(RequestContext { protocol, method }, fut).await
}

fn current_protocol() -> Option<String> {
    REQUEST_CONTEXT.try_with(|ctx| ctx.protocol.clone()).ok()
}

#[derive(Clone)]
struct DiskUsageMetric {
    dev: String,
    path: String,
    used_bytes: u64,
}

fn disk_usage_metrics() -> Vec<DiskUsageMetric> {
    let mut out = Vec::new();
    for bucket in storage::current().buckets() {
        if bucket.store_type() == "memory" {
            continue;
        }
        let path = bucket.path().to_string_lossy().to_string();
        if let Ok(stat) = statvfs::statvfs(bucket.path()) {
            let used = (stat.blocks() - stat.blocks_available()) as u64
                * stat.block_size() as u64;
            let dev = std::fs::metadata(bucket.path())
                .map(|m| {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        return m.dev().to_string();
                    }
                    #[cfg(not(unix))]
                    {
                        return "-".to_string();
                    }
                })
                .unwrap_or_else(|_| "-".to_string());
            out.push(DiskUsageMetric {
                dev,
                path,
                used_bytes: used,
            });
        }
    }
    out
}
