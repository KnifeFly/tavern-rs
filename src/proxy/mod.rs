use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::{HeaderMap, Method, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use rand::Rng;

pub mod singleflight;

#[derive(Clone, Debug)]
pub struct Node {
    pub scheme: String,
    pub address: String,
    pub weight: usize,
}

impl Node {
    pub fn new(scheme: &str, address: &str, weight: usize) -> Self {
        let weight = weight.max(1);
        Self {
            scheme: scheme.to_string(),
            address: address.to_string(),
            weight,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum BalancePolicy {
    RoundRobin,
    Random,
    WeightedRoundRobin,
}

impl BalancePolicy {
    pub fn from_str(raw: &str) -> Self {
        match raw.to_ascii_lowercase().as_str() {
            "random" => BalancePolicy::Random,
            "wrr" | "weighted" | "weighted_round_robin" => BalancePolicy::WeightedRoundRobin,
            _ => BalancePolicy::RoundRobin,
        }
    }
}

pub struct ReverseProxy {
    nodes: Arc<Vec<Node>>,
    cursor: AtomicUsize,
    clients: Arc<tokio::sync::Mutex<HashMap<String, Client<HttpConnector, Full<Bytes>>>>>,
    policy: BalancePolicy,
    wrr_state: Mutex<WrrState>,
}

impl ReverseProxy {
    pub fn new(nodes: Vec<Node>, policy: BalancePolicy) -> Self {
        let wrr_state = WrrState::new(&nodes);
        Self {
            nodes: Arc::new(nodes),
            cursor: AtomicUsize::new(0),
            clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            policy,
            wrr_state: Mutex::new(wrr_state),
        }
    }

    pub fn apply(&mut self, nodes: Vec<Node>) {
        self.nodes = Arc::new(nodes);
        self.cursor.store(0, Ordering::SeqCst);
        if let Ok(mut state) = self.wrr_state.lock() {
            *state = WrrState::new(&self.nodes);
        }
    }

    pub async fn do_request(
        &self,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        _collapsed: bool,
        _wait_timeout: Duration,
    ) -> Result<(http::StatusCode, HeaderMap, Bytes)> {
        self.direct_request(method, uri, headers).await
    }

    async fn direct_request(
        &self,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
    ) -> Result<(http::StatusCode, HeaderMap, Bytes)> {
        let node = self.select_node().ok_or_else(|| anyhow!("no upstream nodes"))?;
        let client = self.client_for(&node.address).await;
        let mut req = http::Request::builder()
            .method(method)
            .uri(uri.clone());
        for (k, v) in headers.iter() {
            req = req.header(k, v);
        }
        let req = req.body(Full::new(Bytes::new()))?;
        let resp = client.request(req).await?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = collect_body(resp).await?;
        Ok((status, headers, body))
    }

    fn select_node(&self) -> Option<Node> {
        let nodes = self.nodes.as_ref();
        if nodes.is_empty() {
            return None;
        }
        match self.policy {
            BalancePolicy::Random => {
                let idx = rand::thread_rng().gen_range(0..nodes.len());
                Some(nodes[idx].clone())
            }
            BalancePolicy::WeightedRoundRobin => {
                let mut state = self.wrr_state.lock().ok()?;
                if state.current.len() != nodes.len() {
                    *state = WrrState::new(nodes);
                }
                let mut best = 0usize;
                for (idx, node) in nodes.iter().enumerate() {
                    state.current[idx] += node.weight as i32;
                    if state.current[idx] > state.current[best] {
                        best = idx;
                    }
                }
                if state.total == 0 {
                    let idx = self.cursor.fetch_add(1, Ordering::SeqCst) % nodes.len();
                    return Some(nodes[idx].clone());
                }
                state.current[best] -= state.total;
                Some(nodes[best].clone())
            }
            BalancePolicy::RoundRobin => {
                let idx = self.cursor.fetch_add(1, Ordering::SeqCst) % nodes.len();
                Some(nodes[idx].clone())
            }
        }
    }

    pub fn next_node(&self) -> Option<Node> {
        self.select_node()
    }

    async fn client_for(&self, addr: &str) -> Client<HttpConnector, Full<Bytes>> {
        let mut map = self.clients.lock().await;
        if let Some(client) = map.get(addr) {
            return client.clone();
        }
        let connector = HttpConnector::new();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        map.insert(addr.to_string(), client.clone());
        client
    }
}

#[derive(Debug)]
struct WrrState {
    current: Vec<i32>,
    total: i32,
}

impl WrrState {
    fn new(nodes: &[Node]) -> Self {
        let total = nodes.iter().map(|n| n.weight as i32).sum();
        Self {
            current: vec![0; nodes.len()],
            total,
        }
    }
}

async fn collect_body(resp: http::Response<Incoming>) -> Result<Bytes> {
    let body = resp.into_body().collect().await?;
    Ok(body.to_bytes())
}
