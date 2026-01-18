use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use rand::Rng;

use crate::config::MiddlewareConfig;
use crate::constants;
use crate::middleware::{Cleanup, Middleware, RoundTripper};
use crate::middleware::registry::register;
use crate::proxy::ReverseProxy;
use crate::upstream::UpstreamClient;

#[derive(Debug, serde::Deserialize, Default, Clone)]
struct MultirangeOptions {
    #[serde(default)]
    merge: bool,
}

pub fn register_middleware() {
    register("multirange", build);
}

pub fn build(cfg: &MiddlewareConfig) -> Result<(Middleware, Cleanup)> {
    let opts = parse_options(cfg)?;
    let middleware: Middleware = Arc::new(move |next: Arc<dyn RoundTripper>| {
        let opts = opts.clone();
        Arc::new(MultirangeMiddleware { next, opts, state: None }) as Arc<dyn RoundTripper>
    });
    Ok((middleware, crate::middleware::empty_cleanup))
}

#[derive(Clone)]
pub struct MultirangeState {
    upstream: UpstreamClient,
    proxy: Arc<ReverseProxy>,
    upstream_addrs: Vec<String>,
}

impl MultirangeState {
    pub fn new(
        upstream: UpstreamClient,
        proxy: Arc<ReverseProxy>,
        upstream_addrs: Vec<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            upstream,
            proxy,
            upstream_addrs,
        })
    }
}

pub fn build_with_state(
    cfg: &MiddlewareConfig,
    state: Arc<MultirangeState>,
) -> Result<(Middleware, Cleanup)> {
    let opts = parse_options(cfg)?;
    let middleware: Middleware = Arc::new(move |next: Arc<dyn RoundTripper>| {
        let opts = opts.clone();
        let state = Arc::clone(&state);
        Arc::new(MultirangeMiddleware {
            next,
            opts,
            state: Some(state),
        }) as Arc<dyn RoundTripper>
    });
    Ok((middleware, crate::middleware::empty_cleanup))
}

fn parse_options(cfg: &MiddlewareConfig) -> Result<MultirangeOptions> {
    if cfg.options.is_empty() {
        return Ok(MultirangeOptions::default());
    }
    let val = serde_yaml::to_value(&cfg.options)?;
    let opts: MultirangeOptions = serde_yaml::from_value(val)?;
    Ok(opts)
}

struct MultirangeMiddleware {
    next: Arc<dyn RoundTripper>,
    opts: MultirangeOptions,
    state: Option<Arc<MultirangeState>>,
}

impl RoundTripper for MultirangeMiddleware {
    fn round_trip(
        &self,
        mut req: Request<Incoming>,
    ) -> crate::middleware::BoxFuture<Result<Response<Full<Bytes>>>> {
        let next = Arc::clone(&self.next);
        let opts = self.opts.clone();
        let state = self.state.clone();
        Box::pin(async move {
            let range_header = req
                .headers()
                .get("Range")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.to_string());
            if let Some(range) = range_header {
                if range.contains(',') {
                    if opts.merge || state.is_none() {
                        let first = range.split(',').next().unwrap_or(&range).trim();
                        req.headers_mut()
                            .insert("Range", first.parse().unwrap());
                        return next.round_trip(req).await;
                    }
                    return handle_multirange(req, state.expect("state")).await;
                }
            }
            next.round_trip(req).await
        })
    }
}

async fn handle_multirange(
    req: Request<Incoming>,
    state: Arc<MultirangeState>,
) -> Result<Response<Full<Bytes>>> {
    let raw_range = req
        .headers()
        .get("Range")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let parsed = match parse_multi_range(raw_range) {
        Ok(v) => v,
        Err(err) => {
            return Ok(Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header("X-Error", err.to_string())
                .body(Full::new(Bytes::new()))
                .unwrap());
        }
    };

    let head = prefetch_resource(&req, &state).await?;
    let obj_size = content_range_size(&head.headers)
        .or_else(|| content_length_from_headers(&head.headers).ok())
        .ok_or_else(|| anyhow!("missing content length"))?;

    let ranges = resolve_ranges(parsed, obj_size);
    if ranges.is_empty() {
        return Ok(Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header("Content-Range", format!("bytes */{}", obj_size))
            .body(Full::new(Bytes::new()))
            .unwrap());
    }
    if ranges.len() <= 1 {
        return Ok(fetch_single_range(req, &state, ranges.first().copied()).await?);
    }

    let ctype = head
        .headers
        .get("Content-Type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");
    let mut headers = strip_hop_headers(&head.headers);
    headers.remove("Content-Length");
    headers.remove("Content-Range");
    headers.remove("Content-Type");

    let boundary = format!("tavern-{}", rand::thread_rng().gen::<u64>());
    let mut body = Vec::new();
    for range in &ranges {
        let part = fetch_range_body(&req, &state, *range).await?;
        append_part(&mut body, &boundary, ctype, obj_size, *range, &part);
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    headers.insert("Accept-Ranges", "bytes".parse().unwrap());
    headers.insert(
        "Content-Type",
        format!("multipart/byteranges; boundary={boundary}")
            .parse()
            .unwrap(),
    );
    headers.insert("Content-Length", body.len().to_string().parse().unwrap());

    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .body(Full::new(Bytes::from(body)))
        .unwrap())
}

struct PrefetchResponse {
    headers: HeaderMap,
}

async fn prefetch_resource(req: &Request<Incoming>, state: &MultirangeState) -> Result<PrefetchResponse> {
    let mut headers = HeaderMap::new();
    copy_headers(req.headers(), &mut headers);
    headers.insert("Range", "bytes=0-0".parse().unwrap());
    let uri = build_upstream_uri(req, state)?;
    let resp = match state
        .upstream
        .fetch(Method::HEAD, uri.clone(), headers.clone())
        .await
    {
        Ok(resp) => resp,
        Err(_) => state.upstream.fetch(Method::GET, uri, headers).await?,
    };
    Ok(PrefetchResponse { headers: resp.1 })
}

async fn fetch_single_range(
    mut req: Request<Incoming>,
    state: &MultirangeState,
    range: Option<RangeSpec>,
) -> Result<Response<Full<Bytes>>> {
    if let Some(range) = range {
        req.headers_mut()
            .insert("Range", format!("bytes={}-{}", range.start, range.end).parse().unwrap());
    }
    let uri = build_upstream_uri(&req, state)?;
    let mut headers = HeaderMap::new();
    copy_headers(req.headers(), &mut headers);
    headers.remove(constants::INTERNAL_UPSTREAM_ADDR);
    let (status, headers, body) = state
        .upstream
        .fetch(req.method().clone(), uri, headers)
        .await?;
    let mut response_headers = strip_hop_headers(&headers);
    response_headers.insert("Content-Length", body.len().to_string().parse().unwrap());
    let mut builder = Response::builder().status(status);
    for (k, v) in response_headers.iter() {
        builder = builder.header(k, v);
    }
    Ok(builder.body(Full::new(body)).unwrap())
}

async fn fetch_range_body(
    req: &Request<Incoming>,
    state: &MultirangeState,
    range: RangeSpec,
) -> Result<Bytes> {
    let uri = build_upstream_uri(req, state)?;
    let mut headers = HeaderMap::new();
    copy_headers(req.headers(), &mut headers);
    headers.remove(constants::INTERNAL_UPSTREAM_ADDR);
    headers.insert(
        "Range",
        format!("bytes={}-{}", range.start, range.end).parse().unwrap(),
    );
    let (status, _headers, body) = state
        .upstream
        .fetch(Method::GET, uri, headers)
        .await?;
    if status != StatusCode::PARTIAL_CONTENT && status != StatusCode::OK {
        return Err(anyhow::anyhow!("upstream range fetch failed"));
    }
    Ok(body)
}

fn append_part(
    buf: &mut Vec<u8>,
    boundary: &str,
    content_type: &str,
    total_size: u64,
    range: RangeSpec,
    body: &Bytes,
) {
    buf.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    buf.extend_from_slice(format!("Content-Type: {content_type}\r\n").as_bytes());
    buf.extend_from_slice(
        format!(
            "Content-Range: bytes {}-{}/{}\r\n\r\n",
            range.start, range.end, total_size
        )
        .as_bytes(),
    );
    buf.extend_from_slice(body);
    buf.extend_from_slice(b"\r\n");
}

fn content_length_from_headers(headers: &HeaderMap) -> Result<u64> {
    headers
        .get("Content-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| anyhow::anyhow!("missing content length"))
}

fn content_range_size(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("Content-Range")
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| crate::http_range::parse_content_range(raw))
        .map(|cr| cr.size)
}

fn strip_hop_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (k, v) in headers.iter() {
        if is_hop_header(k.as_str()) {
            continue;
        }
        out.insert(k, v.clone());
    }
    out
}

fn is_hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn select_upstream_addr(req: &Request<Incoming>, state: &MultirangeState) -> Result<String> {
    if let Some(val) = req.headers().get(constants::INTERNAL_UPSTREAM_ADDR) {
        if let Ok(addr) = val.to_str() {
            return Ok(addr.to_string());
        }
    }
    if let Some(node) = state.proxy.next_node() {
        return Ok(format!("{}://{}", node.scheme, node.address));
    }
    state
        .upstream_addrs
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("upstream.address is empty"))
}

fn build_upstream_uri(req: &Request<Incoming>, state: &MultirangeState) -> Result<http::Uri> {
    let upstream_addr = select_upstream_addr(req, state)?;
    let base = if upstream_addr.starts_with("http://") || upstream_addr.starts_with("https://") {
        upstream_addr
    } else {
        format!("http://{}", upstream_addr)
    };
    let path = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let full = format!("{}{}", base, path);
    Ok(full.parse()?)
}

fn copy_headers(src: &HeaderMap, dst: &mut HeaderMap) {
    for (k, v) in src.iter() {
        dst.insert(k, v.clone());
    }
}

#[derive(Clone, Copy)]
struct RangeSpec {
    start: u64,
    end: u64,
}

enum RawRange {
    StartEnd { start: u64, end: Option<u64> },
    Suffix(u64),
}

fn parse_multi_range(header: &str) -> Result<Vec<RawRange>> {
    let header = header.trim();
    if !header.starts_with("bytes=") {
        return Err(anyhow::anyhow!("invalid range"));
    }
    let ranges = &header[6..];
    let mut out = Vec::new();
    for part in ranges.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut iter = part.splitn(2, '-');
        let start_str = iter.next().unwrap_or("");
        let end_str = iter.next().unwrap_or("");
        if start_str.is_empty() {
            let suffix: u64 = end_str.parse()?;
            out.push(RawRange::Suffix(suffix));
        } else {
            let start: u64 = start_str.parse()?;
            let end = if end_str.is_empty() {
                None
            } else {
                Some(end_str.parse()?)
            };
            out.push(RawRange::StartEnd { start, end });
        }
    }
    if out.is_empty() {
        return Err(anyhow::anyhow!("invalid range"));
    }
    Ok(out)
}

fn resolve_ranges(ranges: Vec<RawRange>, size: u64) -> Vec<RangeSpec> {
    let mut resolved = Vec::new();
    for raw in ranges {
        match raw {
            RawRange::StartEnd { start, end } => {
                if start >= size {
                    continue;
                }
                let mut end_val = end.unwrap_or_else(|| size.saturating_sub(1));
                if end_val >= size {
                    end_val = size.saturating_sub(1);
                }
                if end_val >= start {
                    resolved.push(RangeSpec {
                        start,
                        end: end_val,
                    });
                }
            }
            RawRange::Suffix(suffix) => {
                if suffix == 0 || size == 0 {
                    continue;
                }
                let start = size.saturating_sub(suffix);
                resolved.push(RangeSpec {
                    start,
                    end: size.saturating_sub(1),
                });
            }
        }
    }
    resolved.sort_by_key(|r| r.start);
    resolved
}
