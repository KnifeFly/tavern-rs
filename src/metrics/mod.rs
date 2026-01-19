use std::sync::OnceLock;
use std::time::Instant;

use http::{HeaderMap, StatusCode};
use prometheus::{Encoder, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};
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

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::new)
}

fn requests_total() -> &'static IntCounter {
    static METRIC: OnceLock<IntCounter> = OnceLock::new();
    METRIC.get_or_init(|| {
        let counter = IntCounter::new("tavern_requests_total", "Total requests").unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    })
}

fn requests_status_total() -> &'static IntCounterVec {
    static METRIC: OnceLock<IntCounterVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new("tavern_requests_status_total", "Requests by status"),
            &["code"],
        )
        .unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    })
}

fn requests_code_total() -> &'static IntCounterVec {
    static METRIC: OnceLock<IntCounterVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new(
                "tr_tavern_requests_code_total",
                "Total processed requests",
            ),
            &["protocol", "code"],
        )
        .unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    })
}

fn unexpected_closed_total() -> &'static IntCounterVec {
    static METRIC: OnceLock<IntCounterVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new(
                "tr_tavern_requests_unexpected_closed_total",
                "Unexpected closed requests",
            ),
            &["protocol", "method"],
        )
        .unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    })
}

fn verifier_requests_total() -> &'static IntCounterVec {
    static METRIC: OnceLock<IntCounterVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new("tr_tavern_verifier_requests_total", "Verifier requests"),
            &["code"],
        )
        .unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    })
}

fn disk_usage_gauge() -> &'static IntGaugeVec {
    static METRIC: OnceLock<IntGaugeVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let gauge = IntGaugeVec::new(
            Opts::new("tr_tavern_disk_usage", "Disk usage"),
            &["dev", "path"],
        )
        .unwrap();
        registry().register(Box::new(gauge.clone())).unwrap();
        gauge
    })
}

fn disk_io_counter() -> &'static IntCounterVec {
    static METRIC: OnceLock<IntCounterVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new("tr_tavern_disk_io", "Disk IO"),
            &["dev", "path"],
        )
        .unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    })
}

fn tiered_event_counter() -> &'static IntCounterVec {
    static METRIC: OnceLock<IntCounterVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new("tr_tavern_tiered_events_total", "Tiered migration events"),
            &["event", "result"],
        )
        .unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    })
}

fn tiered_queue_gauge() -> &'static IntGaugeVec {
    static METRIC: OnceLock<IntGaugeVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let gauge = IntGaugeVec::new(
            Opts::new("tr_tavern_tiered_queue_depth", "Tiered migration inflight"),
            &[],
        )
        .unwrap();
        registry().register(Box::new(gauge.clone())).unwrap();
        gauge
    })
}

fn init_metrics() {
    let _ = requests_total();
    let _ = requests_status_total();
    let _ = disk_usage_gauge();
    let _ = disk_io_counter();
    let _ = tiered_event_counter();
    let _ = tiered_queue_gauge();
    let codes = ["200", "206", "400", "404", "500"];
    for code in codes {
        requests_code_total()
            .with_label_values(&["HTTP/1.1", code])
            .inc_by(0);
    }
    for method in ["GET", "HEAD"] {
        unexpected_closed_total()
            .with_label_values(&["HTTP/1.1", method])
            .inc_by(0);
    }
    for code in ["200", "409", "0"] {
        verifier_requests_total()
            .with_label_values(&[code])
            .inc_by(0);
    }
}

pub fn record(status: StatusCode) {
    init_metrics();
    requests_total().inc();
    let code = status.as_u16().to_string();
    requests_status_total().with_label_values(&[code.as_str()]).inc();
    let protocol = current_protocol().unwrap_or_else(|| "HTTP/1.1".to_string());
    requests_code_total()
        .with_label_values(&[protocol.as_str(), code.as_str()])
        .inc();
}

pub fn record_unexpected_closed(protocol: &str, method: &str) {
    init_metrics();
    unexpected_closed_total()
        .with_label_values(&[protocol, method])
        .inc();
}

pub fn record_verifier(code: &str) {
    init_metrics();
    verifier_requests_total().with_label_values(&[code]).inc();
}

pub fn record_disk_io(dev: &str, path: &str) {
    init_metrics();
    disk_io_counter()
        .with_label_values(&[dev, path])
        .inc();
}

pub fn record_tiered_event(event: &str, ok: bool) {
    init_metrics();
    let result = if ok { "ok" } else { "error" };
    tiered_event_counter()
        .with_label_values(&[event, result])
        .inc();
}

pub fn set_tiered_queue_depth(depth: i64) {
    init_metrics();
    tiered_queue_gauge().with_label_values(&[]).set(depth);
}

pub fn render() -> String {
    init_metrics();
    for metric in disk_usage_metrics() {
        disk_usage_gauge()
            .with_label_values(&[metric.dev.as_str(), metric.path.as_str()])
            .set(metric.used_bytes as i64);
    }
    let families = registry().gather();
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&families, &mut buf).unwrap_or(());
    String::from_utf8(buf).unwrap_or_default()
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
