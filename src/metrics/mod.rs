use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use http::{HeaderMap, StatusCode};
use rand::rngs::OsRng;
use rand::RngCore;

use crate::constants;

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
}

fn metrics() -> &'static Mutex<Metrics> {
    static METRICS: OnceLock<Mutex<Metrics>> = OnceLock::new();
    METRICS.get_or_init(|| Mutex::new(Metrics::default()))
}

pub fn record(status: StatusCode) {
    let mut m = metrics().lock().expect("metrics");
    m.requests_total += 1;
    let code = status.as_u16();
    *m.requests_by_status.entry(code).or_insert(0) += 1;
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
