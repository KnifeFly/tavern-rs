use std::collections::HashSet;
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http::header::HOST;
use http_body_util::Full;
use hyper::body::Incoming;
use rand::Rng;

use crate::cache::{CacheEntry, CacheStatus, CacheStore};
use crate::config::MiddlewareConfig;
use crate::constants;
use crate::event::{CacheCompletedPayload, EventContext};
use crate::http_range::{build_content_range, parse_content_range, parse_range, RangeError, RangeSpec};
use crate::metrics;
use crate::middleware::{Cleanup, Middleware, RoundTripper};
use crate::middleware::registry::register;
use crate::proxy::singleflight::Group;
use crate::proxy::ReverseProxy;
use crate::storage;
use crate::storage::object::{CacheFlag, ChunkSet, Id, Metadata};
use crate::upstream::UpstreamClient;

const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024;

#[derive(Clone)]
pub struct CachingState {
    cache: Arc<CacheStore>,
    upstream: UpstreamClient,
    proxy: Arc<ReverseProxy>,
    upstream_addrs: Vec<String>,
    cache_completed_pub: Arc<dyn Fn(&EventContext, CacheCompletedPayload) + Send + Sync>,
    chunk_size: u64,
    include_query: bool,
    singleflight: Arc<Group<UpstreamOutcome>>,
}

impl CachingState {
    pub fn new(
        cache: Arc<CacheStore>,
        upstream: UpstreamClient,
        proxy: Arc<ReverseProxy>,
        upstream_addrs: Vec<String>,
        cache_completed_pub: Arc<dyn Fn(&EventContext, CacheCompletedPayload) + Send + Sync>,
        chunk_size: u64,
        include_query: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            cache,
            upstream,
            proxy,
            upstream_addrs,
            cache_completed_pub,
            chunk_size,
            include_query,
            singleflight: Arc::new(Group::new()),
        })
    }

    pub fn upstream(&self) -> UpstreamClient {
        self.upstream.clone()
    }

    pub fn proxy(&self) -> Arc<ReverseProxy> {
        Arc::clone(&self.proxy)
    }

    pub fn upstream_addrs(&self) -> Vec<String> {
        self.upstream_addrs.clone()
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
struct CachingOptions {
    #[serde(default)]
    fuzzy_refresh: bool,
    #[serde(default)]
    fuzzy_refresh_rate: f64,
    #[serde(default)]
    collapsed_request: bool,
    #[serde(default, with = "humantime_serde")]
    collapsed_request_wait_timeout: Duration,
    #[serde(default)]
    include_query_in_cache_key: Option<bool>,
    #[serde(default)]
    fill_range_percent: Option<u64>,
    #[serde(default)]
    object_pool_enabled: Option<bool>,
    #[serde(default, alias = "object_poll_size")]
    object_pool_size: Option<usize>,
    #[serde(default)]
    async_flush_chunk: Option<bool>,
    #[serde(default)]
    vary_limit: Option<usize>,
    #[serde(default)]
    vary_ignore_key: Vec<String>,
    #[serde(default)]
    slice_size: Option<u64>,
}

#[derive(Clone)]
struct Config {
    include_query: bool,
    chunk_size: u64,
    fuzzy_refresh: bool,
    fuzzy_rate: f64,
    collapsed_request: bool,
    collapsed_timeout: Duration,
    fill_range_percent: u64,
    object_pool_enabled: bool,
    object_pool_size: usize,
    async_flush_chunk: bool,
    vary_limit: usize,
    vary_ignore: HashSet<String>,
}

impl Config {
    fn from_options(state: &CachingState, opts: &CachingOptions) -> Self {
        let include_query = opts
            .include_query_in_cache_key
            .unwrap_or(state.include_query);
        let mut chunk_size = opts.slice_size.unwrap_or(state.chunk_size);
        if chunk_size == 0 {
            chunk_size = DEFAULT_CHUNK_SIZE;
        }
        let vary_limit = opts.vary_limit.unwrap_or(100);
        let vary_ignore = opts
            .vary_ignore_key
            .iter()
            .map(|v| v.to_ascii_lowercase())
            .collect();
        let fill_range_percent = opts
            .fill_range_percent
            .unwrap_or(100)
            .min(100);
        let object_pool_enabled = opts.object_pool_enabled.unwrap_or(false);
        let object_pool_size = opts.object_pool_size.unwrap_or(0);
        let async_flush_chunk = opts.async_flush_chunk.unwrap_or(false);
        Self {
            include_query,
            chunk_size,
            fuzzy_refresh: opts.fuzzy_refresh,
            fuzzy_rate: if opts.fuzzy_refresh_rate <= 0.0 {
                0.0
            } else {
                opts.fuzzy_refresh_rate
            },
            collapsed_request: opts.collapsed_request,
            collapsed_timeout: opts.collapsed_request_wait_timeout,
            fill_range_percent,
            object_pool_enabled,
            object_pool_size,
            async_flush_chunk,
            vary_limit,
            vary_ignore,
        }
    }
}

#[derive(Clone)]
struct UpstreamOutcome {
    ok: bool,
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone)]
struct RequestSnapshot {
    method: Method,
    uri: http::Uri,
    headers: HeaderMap,
}

pub fn register_middleware() {
    register("caching", build);
}

fn build(cfg: &MiddlewareConfig) -> Result<(Middleware, Cleanup)> {
    let opts = parse_options(cfg)?;
    let middleware: Middleware = Arc::new(move |next: Arc<dyn RoundTripper>| {
        let opts = opts.clone();
        Arc::new(CachingMiddleware { next, opts }) as Arc<dyn RoundTripper>
    });
    Ok((middleware, crate::middleware::empty_cleanup))
}

pub fn build_with_state(
    cfg: &MiddlewareConfig,
    state: Arc<CachingState>,
) -> Result<(Middleware, Cleanup)> {
    let opts = parse_options(cfg)?;
    let middleware: Middleware = Arc::new(move |next: Arc<dyn RoundTripper>| {
        let state = Arc::clone(&state);
        let opts = opts.clone();
        Arc::new(CachingMiddlewareWithState {
            next,
            state,
            opts,
        }) as Arc<dyn RoundTripper>
    });
    Ok((middleware, crate::middleware::empty_cleanup))
}

fn parse_options(cfg: &MiddlewareConfig) -> Result<CachingOptions> {
    if cfg.options.is_empty() {
        return Ok(CachingOptions::default());
    }
    let val = serde_yaml::to_value(&cfg.options)?;
    let opts: CachingOptions = serde_yaml::from_value(val)?;
    Ok(opts)
}

struct CachingMiddleware {
    next: Arc<dyn RoundTripper>,
    opts: CachingOptions,
}

impl RoundTripper for CachingMiddleware {
    fn round_trip(&self, req: Request<Incoming>) -> crate::middleware::BoxFuture<Result<Response<Full<Bytes>>>> {
        let next = Arc::clone(&self.next);
        let _opts = self.opts.clone();
        Box::pin(async move { next.round_trip(req).await })
    }
}

struct CachingMiddlewareWithState {
    next: Arc<dyn RoundTripper>,
    state: Arc<CachingState>,
    opts: CachingOptions,
}

impl RoundTripper for CachingMiddlewareWithState {
    fn round_trip(&self, req: Request<Incoming>) -> crate::middleware::BoxFuture<Result<Response<Full<Bytes>>>> {
        let next = Arc::clone(&self.next);
        let state = Arc::clone(&self.state);
        let opts = self.opts.clone();
        Box::pin(async move { handle_request(req, next, state, opts).await })
    }
}

async fn handle_request(
    req: Request<Incoming>,
    _next: Arc<dyn RoundTripper>,
    state: Arc<CachingState>,
    opts: CachingOptions,
) -> Result<Response<Full<Bytes>>> {
    let cfg = Config::from_options(&state, &opts);
    touch_object_pool_config(&cfg);
    let method = req.method().clone();
    if method != Method::GET && method != Method::HEAD {
        return Ok(text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"));
    }

    let base_key = build_cache_key(&req, cfg.include_query)
        .ok_or_else(|| anyhow!("invalid host"))?;
    let range_header = req.headers().get("Range").and_then(|v| v.to_str().ok());
    let prefetch = req.headers().contains_key(constants::PREFETCH_CACHE_KEY);

    let (cache_key, entry_opt) =
        resolve_cache_entry(&req, &state, &cfg, base_key.clone()).await?;

    if let Some(entry) = entry_opt {
        if !entry.is_expired() {
            maybe_trigger_fuzzy_refresh(&req, &state, &cfg, &cache_key, &entry);
            return Ok(handle_cache_hit(&req, &state, &cfg, &cache_key, entry, range_header).await);
        }
        return Ok(handle_revalidate(&req, &state, &cfg, &cache_key, entry, range_header).await);
    }

    if let Some(resp) = handle_storage_hit(&req, &state, &cache_key, range_header).await {
        return Ok(resp);
    }

    Ok(handle_cache_miss(
        &req,
        Arc::clone(&state),
        &cfg,
        &cache_key,
        range_header,
        prefetch,
    )
    .await)
}

async fn resolve_cache_entry(
    req: &Request<Incoming>,
    state: &CachingState,
    cfg: &Config,
    base_key: String,
) -> Result<(String, Option<CacheEntry>)> {
    if let Some(entry) = state.cache.get(&base_key).await {
        if entry.is_vary_index {
            let vary_key = build_vary_key(req, &base_key, &entry, cfg)?;
            if let Some(hit) = state.cache.get(&vary_key).await {
                return Ok((vary_key, Some(hit)));
            }
            return Ok((vary_key, None));
        }
        return Ok((base_key, Some(entry)));
    }
    Ok((base_key, None))
}

fn build_vary_key(
    req: &Request<Incoming>,
    base_key: &str,
    entry: &CacheEntry,
    cfg: &Config,
) -> Result<String> {
    let vary_headers = entry
        .vary_headers
        .as_ref()
        .ok_or_else(|| anyhow!("missing vary headers"))?;
    let mut parts = Vec::new();
    for name in vary_headers {
        let lower = name.to_ascii_lowercase();
        if cfg.vary_ignore.contains(&lower) {
            continue;
        }
        let value = req
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        parts.push(format!("{}={}", lower, value));
    }
    Ok(format!("{base_key}#vary:{}", parts.join("|")))
}

async fn handle_cache_miss(
    req: &Request<Incoming>,
    state: Arc<CachingState>,
    cfg: &Config,
    cache_key: &str,
    range_header: Option<&str>,
    prefetch: bool,
) -> Response<Full<Bytes>> {
    let mut snapshot = snapshot_request(req);
    snapshot.headers.remove(constants::PREFETCH_CACHE_KEY);
    if prefetch {
        snapshot.headers.remove("Range");
    }
    let keep_range = range_header.is_some() && !prefetch;
    let upstream = if cfg.collapsed_request {
        let cache_key = cache_key.to_string();
        if cfg.collapsed_timeout > Duration::from_millis(0) {
            let cfg_timeout = cfg.clone();
            let state_timeout = Arc::clone(&state);
            let snapshot_timeout = snapshot.clone();
            let keep_range = keep_range;
            let call = state.singleflight.do_call(cache_key.clone(), move || {
                let cfg = cfg_timeout.clone();
                let state = Arc::clone(&state_timeout);
                let snapshot = snapshot_timeout.clone();
                async move {
                    if keep_range {
                        fetch_upstream_snapshot_keep_range(&snapshot, &state, None, &cfg)
                            .await
                            .unwrap_or(UpstreamOutcome {
                                ok: false,
                                status: StatusCode::BAD_GATEWAY,
                                headers: HeaderMap::new(),
                                body: Bytes::new(),
                            })
                    } else {
                        fetch_upstream_snapshot(&snapshot, &state, None, &cfg)
                            .await
                            .unwrap_or(UpstreamOutcome {
                                ok: false,
                                status: StatusCode::BAD_GATEWAY,
                                headers: HeaderMap::new(),
                                body: Bytes::new(),
                            })
                    }
                }
            });
            match tokio::time::timeout(cfg.collapsed_timeout, call).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    if keep_range {
                        fetch_upstream_snapshot_keep_range(&snapshot, &state, None, cfg)
                            .await
                            .unwrap_or(UpstreamOutcome {
                                ok: false,
                                status: StatusCode::BAD_GATEWAY,
                                headers: HeaderMap::new(),
                                body: Bytes::new(),
                            })
                    } else {
                        fetch_upstream_snapshot(&snapshot, &state, None, cfg)
                            .await
                            .unwrap_or(UpstreamOutcome {
                                ok: false,
                                status: StatusCode::BAD_GATEWAY,
                                headers: HeaderMap::new(),
                                body: Bytes::new(),
                            })
                    }
                }
            }
        } else {
            let cfg_now = cfg.clone();
            let state_now = Arc::clone(&state);
            let snapshot_now = snapshot.clone();
            let keep_range = keep_range;
            state
                .singleflight
                .do_call(cache_key, move || {
                    let cfg = cfg_now.clone();
                    let state = Arc::clone(&state_now);
                    let snapshot = snapshot_now.clone();
                    async move {
                        if keep_range {
                            fetch_upstream_snapshot_keep_range(&snapshot, &state, None, &cfg)
                                .await
                                .unwrap_or(UpstreamOutcome {
                                    ok: false,
                                    status: StatusCode::BAD_GATEWAY,
                                    headers: HeaderMap::new(),
                                    body: Bytes::new(),
                                })
                        } else {
                            fetch_upstream_snapshot(&snapshot, &state, None, &cfg)
                                .await
                                .unwrap_or(UpstreamOutcome {
                                    ok: false,
                                    status: StatusCode::BAD_GATEWAY,
                                    headers: HeaderMap::new(),
                                    body: Bytes::new(),
                                })
                        }
                    }
                })
                .await
        }
    } else {
        if keep_range {
            fetch_upstream_snapshot_keep_range(&snapshot, &state, None, cfg)
                .await
                .unwrap_or(UpstreamOutcome {
                    ok: false,
                    status: StatusCode::BAD_GATEWAY,
                    headers: HeaderMap::new(),
                    body: Bytes::new(),
                })
        } else {
            fetch_upstream_snapshot(&snapshot, &state, None, cfg)
                .await
                .unwrap_or(UpstreamOutcome {
                    ok: false,
                    status: StatusCode::BAD_GATEWAY,
                    headers: HeaderMap::new(),
                    body: Bytes::new(),
                })
        }
    };

    if !upstream.ok {
        return text_response(StatusCode::BAD_GATEWAY, "upstream error");
    }

    let status = upstream.status;
    let headers = upstream.headers;
    let body = upstream.body;

    let ttl = cache_ttl(&headers);
    let cacheable = is_cacheable(status, &headers, ttl);

    let mut response_headers = strip_hop_headers(headers.clone());
    let mut resp_status = status;
    let mut resp_body = body.clone();
    let mut content_range_header: Option<String> = None;
    let content_range = content_range_from_headers(&headers);
    if range_header.is_some() && status == StatusCode::PARTIAL_CONTENT {
        if let Some(cr) = content_range.as_ref() {
            let raw_range = range_header.and_then(|raw| parse_range(raw, cr.size).ok());
            if let Some(raw_range) = raw_range {
                if raw_range.start >= cr.start && raw_range.end <= cr.end {
                    let offset = (raw_range.start - cr.start) as usize;
                    let end = (raw_range.end - cr.start) as usize;
                    if end < resp_body.len() && offset <= end {
                        resp_body = resp_body.slice(offset..=end);
                    }
                }
                content_range_header =
                    Some(build_content_range(raw_range.start, raw_range.end, cr.size));
            } else {
                content_range_header = Some(build_content_range(cr.start, cr.end, cr.size));
            }
            resp_status = StatusCode::PARTIAL_CONTENT;
        }
    } else {
        let (built_status, built_body, built_range) =
            build_body_with_range(status, range_header, body.clone());
        resp_status = built_status;
        resp_body = built_body;
        content_range_header = built_range;
    }
    if let Some(cr) = content_range_header {
        response_headers.insert("Content-Range", cr.parse().unwrap());
    }
    response_headers.insert("Content-Length", resp_body.len().to_string().parse().unwrap());
    response_headers.insert(
        constants::PROTOCOL_CACHE_STATUS_KEY,
        CacheStatus::Miss.as_str().parse().unwrap(),
    );

    let mut entry_size = body.len() as u64;
    if let Some(cr) = content_range.as_ref() {
        entry_size = cr.size;
    } else if let Some(len) = headers
        .get("Content-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
    {
        entry_size = len;
    }
    let mut store_range_start = 0u64;
    let store_body = body.clone();
    if let Some(cr) = content_range.as_ref() {
        store_range_start = cr.start;
    }
    let chunk_hint = if let Some(cr) = content_range.as_ref() {
        Some(chunk_range_from_spec(
            RangeSpec {
                start: cr.start,
                end: cr.end,
            },
            cfg.chunk_size,
        ))
    } else {
        chunk_hint_for_range(
            range_header,
            entry_size,
            cfg.chunk_size,
            cfg.fill_range_percent,
        )
    };

    if cacheable {
        if let Some(vary) = headers.get("Vary").and_then(|v| v.to_str().ok()) {
            if let Some(vary_entry) = build_vary_index(vary, cache_key, req, &response_headers, status, ttl, cfg)
            {
                let vary_key = vary_entry.0;
                let vary_index = vary_entry.1;
                let mut entry = build_cache_entry(
                    &response_headers,
                    status,
                    &store_body,
                    Some(entry_size),
                    ttl,
                    cfg.chunk_size,
                    chunk_hint.as_deref(),
                );
                entry.is_vary_index = false;
                state.cache.insert(vary_key.clone(), entry).await;
                state.cache.insert(cache_key.to_string(), vary_index).await;
                if let Some(payload) = store_to_storage(
                    &vary_key,
                    status,
                    &headers,
                    &response_headers,
                    &store_body,
                    store_range_start,
                    entry_size,
                    ttl,
                    cfg.chunk_size,
                    cfg.async_flush_chunk,
                ) {
                    (state.cache_completed_pub)(&EventContext, payload);
                }
            }
        } else {
            let mut entry = build_cache_entry(
                &response_headers,
                status,
                &store_body,
                Some(entry_size),
                ttl,
                cfg.chunk_size,
                chunk_hint.as_deref(),
            );
            entry.is_vary_index = false;
            state.cache.insert(cache_key.to_string(), entry).await;
            if let Some(payload) = store_to_storage(
                cache_key,
                status,
                &headers,
                &response_headers,
                &store_body,
                store_range_start,
                entry_size,
                ttl,
                cfg.chunk_size,
                cfg.async_flush_chunk,
            ) {
                (state.cache_completed_pub)(&EventContext, payload);
            }
        }
    }

    if req.method() == Method::HEAD {
        return empty_with_headers(resp_status, response_headers);
    }

    response_with_headers(resp_status, response_headers, resp_body)
}

fn build_cache_entry(
    response_headers: &HeaderMap,
    status: StatusCode,
    body: &Bytes,
    size_override: Option<u64>,
    ttl: Option<Duration>,
    chunk_size: u64,
    chunk_hint: Option<&[u32]>,
) -> CacheEntry {
    let size = size_override.unwrap_or(body.len() as u64);
    let created_at = Instant::now();
    let mut entry = CacheEntry {
        headers: response_headers.clone(),
        status,
        body: body.clone(),
        size,
        created_at,
        expires_at: cache_expiry_at(created_at, ttl),
        etag: response_headers
            .get("ETag")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string()),
        last_modified: response_headers
            .get("Last-Modified")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string()),
        chunk_size,
        chunks: HashSet::new(),
        is_vary_index: false,
        vary_headers: None,
    };
    if entry.size > 0 {
        if let Some(chunks) = chunk_hint {
            entry.mark_chunks(chunks);
        } else {
            let chunks = entry.chunk_range(0, entry.size.saturating_sub(1));
            entry.mark_chunks(&chunks);
        }
    }
    entry
}

fn build_vary_index(
    vary: &str,
    base_key: &str,
    req: &Request<Incoming>,
    response_headers: &HeaderMap,
    status: StatusCode,
    ttl: Option<Duration>,
    cfg: &Config,
) -> Option<(String, CacheEntry)> {
    if vary.trim() == "*" {
        return None;
    }
    let headers: Vec<String> = vary
        .split(',')
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect();
    if headers.is_empty() || headers.len() > cfg.vary_limit {
        return None;
    }
    let created_at = Instant::now();
    let mut index_entry = CacheEntry {
        headers: response_headers.clone(),
        status,
        body: Bytes::new(),
        size: 0,
        created_at,
        expires_at: cache_expiry_at(created_at, ttl),
        etag: None,
        last_modified: None,
        chunk_size: cfg.chunk_size,
        chunks: HashSet::new(),
        is_vary_index: true,
        vary_headers: Some(headers.clone()),
    };
    let vary_key = build_vary_key(req, base_key, &index_entry, cfg).ok()?;
    index_entry.is_vary_index = true;
    Some((vary_key, index_entry))
}

async fn handle_cache_hit(
    req: &Request<Incoming>,
    state: &CachingState,
    cfg: &Config,
    cache_key: &str,
    entry: CacheEntry,
    range_header: Option<&str>,
) -> Response<Full<Bytes>> {
    let mut status = CacheStatus::Hit;
    let range = parse_range_header(range_header, entry.size);
    let has_full_body = entry.body.len() as u64 == entry.size || entry.size == 0;

    if let Some(range) = range {
        let chunks = entry.chunk_range(range.start, range.end);
        status = if entry.has_all_chunks(&chunks) {
            CacheStatus::Hit
        } else if entry.has_any_chunks(&chunks) {
            CacheStatus::PartHit
        } else {
            CacheStatus::PartMiss
        };

        if has_full_body {
            state.cache.update(cache_key, |e| e.mark_chunks(&chunks)).await;
        }

        if range_header.is_some() {
            if let Ok(changed) = validate_upstream(req, state, &entry).await {
                if changed {
                    let _ = state.cache.remove(cache_key).await;
                    let _ = storage::current().purge(
                        cache_key,
                        storage::PurgeControl {
                            hard: true,
                            dir: false,
                            mark_expired: false,
                        },
                    );
                }
            }
        }

        if !has_full_body {
            if status == CacheStatus::Hit {
                if let Some(resp) = serve_storage_range(req, cache_key, &entry, range, status) {
                    return resp;
                }
                status = CacheStatus::PartMiss;
            }
            return fetch_range_and_store(req, state, cfg, cache_key, &entry, range, status).await;
        }
    }

    let mut headers = entry.headers.clone();
    headers.insert(
        constants::PROTOCOL_CACHE_STATUS_KEY,
        status.as_str().parse().unwrap(),
    );
    let (resp_status, resp_body, content_range) =
        build_body_with_range(entry.status, range_header, entry.body.clone());
    if let Some(cr) = content_range {
        headers.insert("Content-Range", cr.parse().unwrap());
    }
    headers.insert("Content-Length", resp_body.len().to_string().parse().unwrap());

    if req.method() == Method::HEAD {
        return empty_with_headers(resp_status, headers);
    }

    response_with_headers(resp_status, headers, resp_body)
}

pub(crate) async fn prefetch_url(state: &CachingState, url: &str) -> Result<()> {
    let uri: http::Uri = url.parse().map_err(|_| anyhow!("invalid url"))?;
    let cache_key = cache_key_from_uri(&uri, state.include_query)
        .ok_or_else(|| anyhow!("invalid url"))?;
    if let Some(entry) = state.cache.get(&cache_key).await {
        if !entry.is_expired() {
            return Ok(());
        }
    }

    let mut headers = HeaderMap::new();
    if let Some(authority) = uri.authority() {
        headers.insert(HOST, authority.as_str().parse().unwrap());
    }
    let store_val = http::HeaderValue::from_str(url).map_err(|_| anyhow!("invalid url"))?;
    headers.insert("X-Store-Url", store_val);

    let snapshot = RequestSnapshot {
        method: Method::GET,
        uri: uri.clone(),
        headers,
    };
    let cfg = Config::from_options(state, &CachingOptions::default());
    let upstream = fetch_upstream_snapshot(&snapshot, state, None, &cfg).await?;
    if !upstream.ok {
        return Err(anyhow!("upstream error"));
    }
    let ttl = cache_ttl(&upstream.headers);
    if !is_cacheable(upstream.status, &upstream.headers, ttl) {
        return Err(anyhow!("response not cacheable"));
    }
    let mut response_headers = strip_hop_headers(upstream.headers.clone());
    response_headers.insert(
        "Content-Length",
        upstream.body.len().to_string().parse().unwrap(),
    );
    let entry = build_cache_entry(
        &response_headers,
        upstream.status,
        &upstream.body,
        None,
        ttl,
        state.chunk_size,
        None,
    );
    state.cache.insert(cache_key.clone(), entry).await;

    if let Some(payload) = store_to_storage(
        &cache_key,
        upstream.status,
        &upstream.headers,
        &response_headers,
        &upstream.body,
        0,
        upstream.body.len() as u64,
        ttl,
        state.chunk_size,
        false,
    ) {
        (state.cache_completed_pub)(&EventContext, payload);
    }
    Ok(())
}

fn maybe_trigger_fuzzy_refresh(
    req: &Request<Incoming>,
    state: &Arc<CachingState>,
    cfg: &Config,
    cache_key: &str,
    entry: &CacheEntry,
) {
    if !cfg.fuzzy_refresh || cfg.fuzzy_rate <= 0.0 {
        return;
    }
    let Some((soft_ttl, hard_ttl)) = fuzzy_refresh_window(entry, cfg.fuzzy_rate) else {
        return;
    };
    let now = Instant::now();
    if !should_trigger_fuzzy_refresh(now, soft_ttl, hard_ttl) {
        return;
    }
    if !entry_complete(entry) {
        return;
    }
    if !entry_has_condition(entry) {
        return;
    }
    let state_cloned = Arc::clone(state);
    let cfg_cloned = cfg.clone();
    let snapshot = snapshot_request(req);
    let cache_key_cloned = cache_key.to_string();
    tokio::spawn(async move {
        let _ = refresh_entry(snapshot, state_cloned, &cfg_cloned, &cache_key_cloned).await;
    });
}

fn fuzzy_refresh_window(entry: &CacheEntry, rate: f64) -> Option<(Instant, Instant)> {
    let mut rate = rate;
    if rate <= 0.0 || rate > 1.0 {
        rate = 0.8;
    }
    let ttl = entry.expires_at.checked_duration_since(entry.created_at)?;
    if ttl.as_nanos() == 0 {
        return None;
    }
    let soft = entry.created_at + ttl.mul_f64(rate);
    Some((soft, entry.expires_at))
}

fn should_trigger_fuzzy_refresh(now: Instant, soft_ttl: Instant, hard_ttl: Instant) -> bool {
    if now < soft_ttl || now >= hard_ttl {
        return false;
    }
    let total = hard_ttl.duration_since(soft_ttl).as_secs_f64();
    if total <= 0.0 {
        return false;
    }
    let elapsed = now.duration_since(soft_ttl).as_secs_f64();
    let probability = elapsed / total;
    let roll: f64 = rand::thread_rng().gen();
    roll < probability
}

fn entry_complete(entry: &CacheEntry) -> bool {
    entry.size == 0 || entry.body.len() as u64 == entry.size
}

fn entry_has_condition(entry: &CacheEntry) -> bool {
    entry.etag.is_some() || entry.last_modified.is_some()
}

fn serve_storage_range(
    req: &Request<Incoming>,
    cache_key: &str,
    entry: &CacheEntry,
    range: RangeSpec,
    status: CacheStatus,
) -> Option<Response<Full<Bytes>>> {
    let id = Id::new(cache_key);
    let storage = storage::current();
    let bucket = storage.selector().select(&id)?;
    let meta = bucket.lookup(&id).ok().flatten()?;
    let chunk_size = if meta.block_size > 0 {
        meta.block_size
    } else {
        entry.chunk_size
    };
    let body = read_storage_range(bucket.as_ref(), &meta, range, chunk_size)?;

    let mut headers = header_map_from_meta(&meta);
    if headers.is_empty() {
        headers = entry.headers.clone();
    }
    headers.insert(
        constants::PROTOCOL_CACHE_STATUS_KEY,
        status.as_str().parse().ok()?,
    );
    headers.insert(
        "Content-Range",
        build_content_range(range.start, range.end, meta.size)
            .parse()
            .ok()?,
    );
    headers.insert(
        "Content-Length",
        body.len().to_string().parse().ok()?,
    );

    if req.method() == Method::HEAD {
        return Some(empty_with_headers(StatusCode::PARTIAL_CONTENT, headers));
    }
    Some(response_with_headers(
        StatusCode::PARTIAL_CONTENT,
        headers,
        body,
    ))
}

fn read_storage_range(
    bucket: &dyn storage::Bucket,
    meta: &Metadata,
    range: RangeSpec,
    chunk_size: u64,
) -> Option<Bytes> {
    if chunk_size == 0 {
        return None;
    }
    let first = range.start / chunk_size;
    let last = range.end / chunk_size;
    let mut buf = Vec::with_capacity((range.end - range.start + 1) as usize);
    for idx in first..=last {
        let idx_u32: u32 = idx.try_into().ok()?;
        let (mut reader, _) = bucket.read_chunk_file(&meta.id, idx_u32).ok()?;
        let mut chunk = Vec::new();
        reader.read_to_end(&mut chunk).ok()?;
        if idx == first {
            let start_offset = (range.start - first * chunk_size) as usize;
            if start_offset >= chunk.len() {
                return None;
            }
            chunk = chunk[start_offset..].to_vec();
        }
        if idx == last {
            let end_offset = (range.end - last * chunk_size) as usize;
            if end_offset >= chunk.len() {
                return None;
            }
            chunk.truncate(end_offset + 1);
        }
        buf.extend_from_slice(&chunk);
    }
    Some(Bytes::from(buf))
}

async fn fetch_range_and_store(
    req: &Request<Incoming>,
    state: &CachingState,
    cfg: &Config,
    cache_key: &str,
    entry: &CacheEntry,
    raw_range: RangeSpec,
    cache_status: CacheStatus,
) -> Response<Full<Bytes>> {
    let filled = fill_range_spec(
        raw_range,
        entry.size,
        cfg.chunk_size,
        cfg.fill_range_percent,
    );
    let upstream = fetch_upstream_with_range(req, state, filled).await.unwrap_or(UpstreamOutcome {
        ok: false,
        status: StatusCode::BAD_GATEWAY,
        headers: HeaderMap::new(),
        body: Bytes::new(),
    });
    if !upstream.ok {
        return text_response(StatusCode::BAD_GATEWAY, "upstream error");
    }

    let status = upstream.status;
    let headers = upstream.headers;
    let body = upstream.body;
    let ttl = cache_ttl(&headers);
    let cacheable = is_cacheable(status, &headers, ttl);

    let mut response_headers = strip_hop_headers(headers.clone());
    let mut resp_body = body.clone();
    let mut total_size = entry.size;
    let resp_status: StatusCode;
    let store_start: u64;
    let mut store_body = body.clone();
    if let Some(cr) = content_range_from_headers(&headers) {
        total_size = cr.size;
        store_start = cr.start;
        if raw_range.start >= cr.start && raw_range.end <= cr.end {
            let offset = (raw_range.start - cr.start) as usize;
            let end = (raw_range.end - cr.start) as usize;
            if end < resp_body.len() && offset <= end {
                resp_body = resp_body.slice(offset..=end);
            }
        }
        response_headers.insert(
            "Content-Range",
            build_content_range(raw_range.start, raw_range.end, cr.size)
                .parse()
                .unwrap(),
        );
        resp_status = StatusCode::PARTIAL_CONTENT;
    } else {
        if total_size == 0 {
            total_size = body.len() as u64;
        }
        let raw_start = raw_range.start as usize;
        let raw_end = raw_range.end as usize;
        if raw_end < body.len() && raw_start <= raw_end {
            resp_body = body.slice(raw_start..=raw_end);
            resp_status = StatusCode::PARTIAL_CONTENT;
            response_headers.insert(
                "Content-Range",
                build_content_range(raw_range.start, raw_range.end, total_size)
                    .parse()
                    .unwrap(),
            );
        } else {
            resp_body = Bytes::new();
            resp_status = StatusCode::RANGE_NOT_SATISFIABLE;
            response_headers.insert(
                "Content-Range",
                format!("bytes */{}", total_size).parse().unwrap(),
            );
        }
        store_start = 0;
        store_body = body.clone();
    }
    response_headers.insert(
        constants::PROTOCOL_CACHE_STATUS_KEY,
        cache_status.as_str().parse().unwrap(),
    );
    response_headers.insert(
        "Content-Length",
        resp_body.len().to_string().parse().unwrap(),
    );

    if cacheable {
        let hint = chunk_range_from_spec(filled, cfg.chunk_size);
        let mut new_entry = build_cache_entry(
            &response_headers,
            status,
            &store_body,
            Some(total_size),
            ttl,
            cfg.chunk_size,
            Some(&hint),
        );
        new_entry.chunks.extend(entry.chunks.iter().copied());
        new_entry.is_vary_index = false;
        state.cache.insert(cache_key.to_string(), new_entry).await;
        if let Some(payload) = store_to_storage(
            cache_key,
            status,
            &headers,
            &response_headers,
            &store_body,
            store_start,
            total_size,
            ttl,
            cfg.chunk_size,
            cfg.async_flush_chunk,
        ) {
            (state.cache_completed_pub)(&EventContext, payload);
        }
    }

    if req.method() == Method::HEAD {
        return empty_with_headers(resp_status, response_headers);
    }
    response_with_headers(resp_status, response_headers, resp_body)
}

async fn handle_storage_hit(
    req: &Request<Incoming>,
    _state: &CachingState,
    cache_key: &str,
    range_header: Option<&str>,
) -> Option<Response<Full<Bytes>>> {
    let id = Id::new(cache_key);
    let storage = storage::current();
    let bucket = storage.selector().select(&id)?;
    let meta = bucket.lookup(&id).ok().flatten()?;
    if !meta.has_complete() {
        return None;
    }
    let body = read_storage_body(bucket.as_ref(), &meta).ok()?;
    let status = StatusCode::from_u16(meta.code as u16).unwrap_or(StatusCode::OK);
    let mut headers = header_map_from_meta(&meta);
    headers.insert(
        constants::PROTOCOL_CACHE_STATUS_KEY,
        CacheStatus::Hit.as_str().parse().ok()?,
    );

    let (resp_status, resp_body, content_range) =
        build_body_with_range(status, range_header, body);
    if let Some(cr) = content_range {
        headers.insert("Content-Range", cr.parse().ok()?);
    }
    headers.insert("Content-Length", resp_body.len().to_string().parse().ok()?);

    if req.method() == Method::HEAD {
        return Some(empty_with_headers(resp_status, headers));
    }
    Some(response_with_headers(resp_status, headers, resp_body))
}

async fn handle_revalidate(
    req: &Request<Incoming>,
    state: &CachingState,
    cfg: &Config,
    cache_key: &str,
    entry: CacheEntry,
    range_header: Option<&str>,
) -> Response<Full<Bytes>> {
    let mut conditional = HeaderMap::new();
    if let Some(etag) = &entry.etag {
        conditional.insert("If-None-Match", etag.parse().unwrap());
    }
    if let Some(lm) = &entry.last_modified {
        conditional.insert("If-Modified-Since", lm.parse().unwrap());
    }

    match fetch_upstream_full(req, state, Some(conditional), cfg).await {
        Ok(upstream) => {
            let status = upstream.status;
            let headers = upstream.headers;
            let body = upstream.body;
            if status == StatusCode::NOT_MODIFIED {
                let ttl = cache_ttl(&headers).or_else(|| cache_ttl(&entry.headers));
                if let Some(ttl) = ttl {
                    let now = Instant::now();
                    state
                        .cache
                        .update(cache_key, |e| {
                            e.created_at = now;
                            e.expires_at = cache_expiry_at(now, Some(ttl));
                        })
                        .await;
                }
                return serve_cached_with_status(
                    req,
                    entry,
                    CacheStatus::RevalidateHit,
                    range_header,
                );
            }

            let ttl = cache_ttl(&headers);
            let cacheable = is_cacheable(status, &headers, ttl);
            let mut response_headers = strip_hop_headers(headers.clone());
            let (resp_status, resp_body, content_range) =
                build_body_with_range(status, range_header, body.clone());
            if let Some(cr) = content_range {
                response_headers.insert("Content-Range", cr.parse().unwrap());
            }
            response_headers.insert("Content-Length", resp_body.len().to_string().parse().unwrap());
            response_headers.insert(
                constants::PROTOCOL_CACHE_STATUS_KEY,
                CacheStatus::RevalidateMiss.as_str().parse().unwrap(),
            );

            if cacheable {
                let mut new_entry = build_cache_entry(
                    &response_headers,
                    status,
                    &body,
                    None,
                    ttl,
                    cfg.chunk_size,
                    None,
                );
                new_entry.is_vary_index = false;
                state.cache.insert(cache_key.to_string(), new_entry).await;
            } else {
                state.cache.remove(cache_key).await;
            }

            if req.method() == Method::HEAD {
                return empty_with_headers(resp_status, response_headers);
            }
            response_with_headers(resp_status, response_headers, resp_body)
        }
        Err(err) => {
            log::warn!("revalidate upstream error: {err}");
            serve_cached_with_status(req, entry, CacheStatus::RevalidateHit, range_header)
        }
    }
}

async fn refresh_entry(
    snapshot: RequestSnapshot,
    state: Arc<CachingState>,
    cfg: &Config,
    cache_key: &str,
) -> Result<()> {
    let mut conditional = HeaderMap::new();
    if let Some(entry) = state.cache.get(cache_key).await {
        if let Some(etag) = entry.etag {
            conditional.insert("If-None-Match", etag.parse().unwrap());
        }
        if let Some(lm) = entry.last_modified {
            conditional.insert("If-Modified-Since", lm.parse().unwrap());
        }
    }
    let upstream = fetch_upstream_snapshot(&snapshot, &state, Some(conditional), cfg).await?;
    if upstream.status == StatusCode::NOT_MODIFIED {
        return Ok(());
    }
    let ttl = cache_ttl(&upstream.headers);
    if !is_cacheable(upstream.status, &upstream.headers, ttl) {
        return Ok(());
    }
    let mut response_headers = strip_hop_headers(upstream.headers.clone());
    response_headers.insert(
        "Content-Length",
        upstream.body.len().to_string().parse().unwrap(),
    );
    let entry = build_cache_entry(
        &response_headers,
        upstream.status,
        &upstream.body,
        None,
        ttl,
        cfg.chunk_size,
        None,
    );
    state.cache.insert(cache_key.to_string(), entry).await;
    Ok(())
}

fn serve_cached_with_status(
    req: &Request<Incoming>,
    entry: CacheEntry,
    status: CacheStatus,
    range_header: Option<&str>,
) -> Response<Full<Bytes>> {
    let mut headers = entry.headers.clone();
    headers.insert(constants::PROTOCOL_CACHE_STATUS_KEY, status.as_str().parse().unwrap());
    let (resp_status, resp_body, content_range) =
        build_body_with_range(entry.status, range_header, entry.body.clone());
    if let Some(cr) = content_range {
        headers.insert("Content-Range", cr.parse().unwrap());
    }
    headers.insert("Content-Length", resp_body.len().to_string().parse().unwrap());
    if req.method() == Method::HEAD {
        return empty_with_headers(resp_status, headers);
    }
    response_with_headers(resp_status, headers, resp_body)
}

async fn fetch_upstream_full(
    req: &Request<Incoming>,
    state: &CachingState,
    extra_headers: Option<HeaderMap>,
    _cfg: &Config,
) -> Result<UpstreamOutcome> {
    let upstream_addr = select_upstream_addr(req, state)?;
    let uri = build_upstream_uri(req, &upstream_addr)?;
    let mut headers = HeaderMap::new();
    copy_headers(req.headers(), &mut headers);
    if let Some(extra) = extra_headers {
        for (k, v) in extra.iter() {
            headers.insert(k, v.clone());
        }
    }
    headers.remove(constants::INTERNAL_UPSTREAM_ADDR);
    headers.remove("Range");
    match state
        .upstream
        .fetch(req.method().clone(), uri, headers)
        .await
    {
        Ok((status, headers, body)) => Ok(UpstreamOutcome {
            ok: true,
            status,
            headers,
            body,
        }),
        Err(err) => {
            log::warn!("upstream fetch failed: {err}");
            Ok(UpstreamOutcome {
                ok: false,
                status: StatusCode::BAD_GATEWAY,
                headers: HeaderMap::new(),
                body: Bytes::new(),
            })
        }
    }
}

async fn fetch_upstream_with_range(
    req: &Request<Incoming>,
    state: &CachingState,
    range: RangeSpec,
) -> Result<UpstreamOutcome> {
    let upstream_addr = select_upstream_addr(req, state)?;
    let uri = build_upstream_uri(req, &upstream_addr)?;
    let mut headers = HeaderMap::new();
    copy_headers(req.headers(), &mut headers);
    headers.remove(constants::INTERNAL_UPSTREAM_ADDR);
    headers.insert(
        "Range",
        format!("bytes={}-{}", range.start, range.end)
            .parse()
            .unwrap(),
    );
    match state
        .upstream
        .fetch(req.method().clone(), uri, headers)
        .await
    {
        Ok((status, headers, body)) => Ok(UpstreamOutcome {
            ok: true,
            status,
            headers,
            body,
        }),
        Err(err) => {
            log::warn!("upstream fetch failed: {err}");
            Ok(UpstreamOutcome {
                ok: false,
                status: StatusCode::BAD_GATEWAY,
                headers: HeaderMap::new(),
                body: Bytes::new(),
            })
        }
    }
}

async fn fetch_upstream_snapshot(
    snapshot: &RequestSnapshot,
    state: &CachingState,
    extra_headers: Option<HeaderMap>,
    _cfg: &Config,
) -> Result<UpstreamOutcome> {
    let upstream_addr = select_upstream_addr_from_headers(&snapshot.headers, state)?;
    let uri = build_upstream_uri_from_snapshot(snapshot, &upstream_addr)?;
    let mut headers = HeaderMap::new();
    copy_headers(&snapshot.headers, &mut headers);
    if let Some(extra) = extra_headers {
        for (k, v) in extra.iter() {
            headers.insert(k, v.clone());
        }
    }
    headers.remove(constants::INTERNAL_UPSTREAM_ADDR);
    headers.remove("Range");
    match state.upstream.fetch(snapshot.method.clone(), uri, headers).await {
        Ok((status, headers, body)) => Ok(UpstreamOutcome {
            ok: true,
            status,
            headers,
            body,
        }),
        Err(err) => {
            log::warn!("upstream fetch failed: {err}");
            Ok(UpstreamOutcome {
                ok: false,
                status: StatusCode::BAD_GATEWAY,
                headers: HeaderMap::new(),
                body: Bytes::new(),
            })
        }
    }
}

async fn fetch_upstream_snapshot_keep_range(
    snapshot: &RequestSnapshot,
    state: &CachingState,
    extra_headers: Option<HeaderMap>,
    _cfg: &Config,
) -> Result<UpstreamOutcome> {
    let upstream_addr = select_upstream_addr_from_headers(&snapshot.headers, state)?;
    let uri = build_upstream_uri_from_snapshot(snapshot, &upstream_addr)?;
    let mut headers = HeaderMap::new();
    copy_headers(&snapshot.headers, &mut headers);
    if let Some(extra) = extra_headers {
        for (k, v) in extra.iter() {
            headers.insert(k, v.clone());
        }
    }
    headers.remove(constants::INTERNAL_UPSTREAM_ADDR);
    match state.upstream.fetch(snapshot.method.clone(), uri, headers).await {
        Ok((status, headers, body)) => Ok(UpstreamOutcome {
            ok: true,
            status,
            headers,
            body,
        }),
        Err(err) => {
            log::warn!("upstream fetch failed: {err}");
            Ok(UpstreamOutcome {
                ok: false,
                status: StatusCode::BAD_GATEWAY,
                headers: HeaderMap::new(),
                body: Bytes::new(),
            })
        }
    }
}

async fn validate_upstream(
    req: &Request<Incoming>,
    state: &CachingState,
    entry: &CacheEntry,
) -> Result<bool> {
    let upstream_addr = select_upstream_addr(req, state)?;
    let uri = build_upstream_uri(req, &upstream_addr)?;
    let mut headers = HeaderMap::new();
    if let Some(etag) = &entry.etag {
        headers.insert("If-None-Match", etag.parse().unwrap());
    }
    if let Some(lm) = &entry.last_modified {
        headers.insert("If-Modified-Since", lm.parse().unwrap());
    }

    let (status, resp_headers, _body) = state.upstream.fetch(Method::HEAD, uri, headers).await?;
    if status == StatusCode::NOT_MODIFIED {
        return Ok(false);
    }
    Ok(has_metadata_changed(entry, &resp_headers))
}

fn has_metadata_changed(entry: &CacheEntry, headers: &HeaderMap) -> bool {
    if let Some(cl) = headers.get("Content-Length") {
        if let Ok(val) = cl.to_str() {
            if let Ok(new_len) = val.parse::<u64>() {
                if new_len != entry.size {
                    return true;
                }
            }
        }
    }
    if let Some(new_etag) = headers.get("ETag").and_then(|v| v.to_str().ok()) {
        if let Some(old_etag) = &entry.etag {
            if !new_etag.eq_ignore_ascii_case(old_etag) {
                return true;
            }
        }
    }
    if let Some(new_lm) = headers.get("Last-Modified").and_then(|v| v.to_str().ok()) {
        if let Some(old_lm) = &entry.last_modified {
            if !new_lm.eq_ignore_ascii_case(old_lm) {
                return true;
            }
        }
    }
    false
}

fn build_body_with_range(
    base_status: StatusCode,
    range_header: Option<&str>,
    body: Bytes,
) -> (StatusCode, Bytes, Option<String>) {
    if base_status.is_success() {
        if let Some(raw) = range_header {
            let size = body.len() as u64;
            match parse_range(raw, size) {
                Ok(range) => {
                    let slice = body.slice(range.start as usize..=range.end as usize);
                    let cr = build_content_range(range.start, range.end, size);
                    return (StatusCode::PARTIAL_CONTENT, slice, Some(cr));
                }
                Err(RangeError::Unsatisfiable) => {
                    let cr = format!("bytes */{}", size);
                    return (StatusCode::RANGE_NOT_SATISFIABLE, Bytes::new(), Some(cr));
                }
                Err(RangeError::Invalid) => {
                    return (StatusCode::BAD_REQUEST, Bytes::new(), None);
                }
            }
        }
    }
    (base_status, body, None)
}

fn parse_range_header(range_header: Option<&str>, size: u64) -> Option<RangeSpec> {
    let header = range_header?;
    parse_range(header, size).ok()
}

fn chunk_range_from_spec(range: RangeSpec, chunk_size: u64) -> Vec<u32> {
    let first = range.start / chunk_size;
    let last = range.end / chunk_size;
    (first..=last).map(|v| v as u32).collect()
}

fn content_range_from_headers(headers: &HeaderMap) -> Option<crate::http_range::ContentRange> {
    headers
        .get("Content-Range")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_content_range)
}

fn chunk_hint_for_range(
    range_header: Option<&str>,
    size: u64,
    chunk_size: u64,
    fill_percent: u64,
) -> Option<Vec<u32>> {
    if chunk_size == 0 {
        return None;
    }
    let raw = range_header?;
    let range = parse_range(raw, size).ok()?;
    let filled = fill_range_spec(range, size, chunk_size, fill_percent);
    Some(chunk_range_from_spec(filled, chunk_size))
}

fn fill_range_spec(
    range: RangeSpec,
    size: u64,
    chunk_size: u64,
    fill_percent: u64,
) -> RangeSpec {
    if chunk_size == 0 || fill_percent == 0 || size == 0 {
        return range;
    }
    let fp = fill_percent.min(100);
    let max_fill = chunk_size.saturating_mul(fp) / 100;
    let min_fill = chunk_size.saturating_mul(100 - fp) / 100;

    let raw_start = range.start;
    let raw_end = range.end;

    let mut new_start = (raw_start / chunk_size) * chunk_size;
    if raw_start.saturating_sub(new_start) > max_fill {
        new_start = raw_start;
    }

    let mut new_end = (raw_end / chunk_size)
        .saturating_add(1)
        .saturating_mul(chunk_size)
        .saturating_sub(1);
    if new_end >= size {
        new_end = size.saturating_sub(1);
    }
    if new_end.saturating_sub(raw_end) > max_fill {
        new_end = raw_end;
    }

    if new_end < new_start {
        return range;
    }

    let raw_len = raw_end.saturating_sub(raw_start).saturating_add(1);
    let expanded_len = new_end.saturating_sub(new_start).saturating_add(1);
    let extra = raw_start.saturating_sub(new_start) + new_end.saturating_sub(raw_end);
    if (expanded_len <= chunk_size && extra > max_fill) || raw_len < min_fill {
        return range;
    }

    RangeSpec {
        start: new_start,
        end: new_end,
    }
}

fn touch_object_pool_config(cfg: &Config) {
    if cfg.object_pool_enabled && cfg.object_pool_size == 0 {
        log::debug!("caching object_pool_enabled without object_pool_size");
    }
}

fn cache_ttl(headers: &HeaderMap) -> Option<Duration> {
    if let Some(val) = headers.get(constants::CACHE_TIME).and_then(|v| v.to_str().ok()) {
        if let Ok(secs) = val.parse::<u64>() {
            if secs > 0 {
                return Some(Duration::from_secs(secs));
            }
        }
    }
    if let Some(val) = headers.get("Cache-Control").and_then(|v| v.to_str().ok()) {
        for part in val.split(',') {
            let part = part.trim();
            if let Some(raw) = part.strip_prefix("max-age=") {
                if let Ok(secs) = raw.parse::<u64>() {
                    return Some(Duration::from_secs(secs));
                }
            }
        }
    }
    None
}

fn cache_expiry_at(now: Instant, ttl: Option<Duration>) -> Instant {
    if let Some(ttl) = ttl {
        now + ttl
    } else {
        now
    }
}


fn is_cacheable(status: StatusCode, headers: &HeaderMap, ttl: Option<Duration>) -> bool {
    if ttl.is_none() {
        return false;
    }
    if status.as_u16() >= 400 {
        return headers
            .get(constants::INTERNAL_CACHE_ERR_CODE)
            .and_then(|v| v.to_str().ok())
            .map(|v| v == "1")
            .unwrap_or(false);
    }
    true
}

fn build_cache_key(req: &Request<Incoming>, include_query: bool) -> Option<String> {
    if let Some(val) = req.headers().get("X-Store-Url").and_then(|v| v.to_str().ok()) {
        if let Ok(uri) = val.parse::<http::Uri>() {
            let scheme = uri.scheme_str().unwrap_or("http");
            if let Some(authority) = uri.authority() {
                let path = if include_query {
                    uri.path_and_query().map(|v| v.as_str()).unwrap_or("/")
                } else {
                    uri.path()
                };
                return Some(format!("{}://{}{}", scheme, authority.as_str(), path));
            }
        }
    }
    let scheme = req.uri().scheme_str().unwrap_or("http");
    let host = req
        .uri()
        .authority()
        .map(|a| a.as_str().to_string())
        .or_else(|| req.headers().get("host").and_then(|v| v.to_str().ok()).map(|v| v.to_string()))?;
    let path = if include_query {
        req.uri().path_and_query().map(|v| v.as_str()).unwrap_or("/")
    } else {
        req.uri().path()
    };
    Some(format!("{}://{}{}", scheme, host, path))
}

fn cache_key_from_uri(uri: &http::Uri, include_query: bool) -> Option<String> {
    let scheme = uri.scheme_str().unwrap_or("http");
    let authority = uri.authority()?;
    let path = if include_query {
        uri.path_and_query().map(|v| v.as_str()).unwrap_or("/")
    } else {
        uri.path()
    };
    Some(format!("{}://{}{}", scheme, authority.as_str(), path))
}

fn select_upstream_addr(req: &Request<Incoming>, state: &CachingState) -> Result<String> {
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
        .ok_or_else(|| anyhow!("upstream.address is empty"))
}

fn select_upstream_addr_from_headers(headers: &HeaderMap, state: &CachingState) -> Result<String> {
    if let Some(val) = headers.get(constants::INTERNAL_UPSTREAM_ADDR) {
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
        .ok_or_else(|| anyhow!("upstream.address is empty"))
}

fn build_upstream_uri(req: &Request<Incoming>, addr: &str) -> Result<http::Uri> {
    let base = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    };
    let path = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let full = format!("{}{}", base, path);
    full.parse::<http::Uri>().context("parse upstream uri")
}

fn build_upstream_uri_from_snapshot(snapshot: &RequestSnapshot, addr: &str) -> Result<http::Uri> {
    let base = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    };
    let path = snapshot
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let full = format!("{}{}", base, path);
    full.parse::<http::Uri>().context("parse upstream uri")
}

fn copy_headers(src: &HeaderMap, dst: &mut HeaderMap) {
    for (k, v) in src.iter() {
        dst.insert(k, v.clone());
    }
}

fn strip_hop_headers(headers: HeaderMap) -> HeaderMap {
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

fn store_to_storage(
    cache_key: &str,
    status: StatusCode,
    headers: &HeaderMap,
    response_headers: &HeaderMap,
    body: &Bytes,
    range_start: u64,
    total_size: u64,
    ttl: Option<Duration>,
    chunk_size: u64,
    async_flush_chunk: bool,
) -> Option<CacheCompletedPayload> {
    let storage = storage::current();
    let id = Id::new(cache_key);
    let bucket = storage.selector().select(&id)?;
    let body_size = body.len() as u64;
    let size = total_size;
    let mut chunks = ChunkSet::default();
    let mut parts = ChunkSet::default();
    let mut first_path = None;
    let now = storage::unix_now();
    let expires_at = ttl.map(|d| now + d.as_secs() as i64).unwrap_or(0);
    let mut header_pairs = Vec::new();
    let mut has_length = false;
    for (k, v) in headers.iter() {
        if let Ok(val) = v.to_str() {
            if k.as_str().eq_ignore_ascii_case("Content-Length") {
                has_length = true;
            }
            header_pairs.push((k.to_string(), val.to_string()));
        }
    }
    if !has_length {
        header_pairs.push(("Content-Length".to_string(), size.to_string()));
    }

    let mut meta = Metadata {
        flags: CacheFlag::CACHE,
        id: id.clone(),
        block_size: chunk_size,
        chunks: ChunkSet::default(),
        parts: ChunkSet::default(),
        code: status.as_u16() as i32,
        size,
        resp_unix: now,
        last_ref_unix: now,
        refs: 1,
        expires_at,
        headers: header_pairs,
        virtual_key: Vec::new(),
    };

    if body_size > 0 {
        let mut idx = 0u32;
        let mut offset = 0usize;
        let start_index = if chunk_size == 0 {
            0
        } else {
            (range_start / chunk_size) as u32
        };
        while offset < body.len() {
            let end = std::cmp::min(offset + chunk_size as usize, body.len());
            let slice = &body[offset..end];
            let chunk_index = start_index.saturating_add(idx);
            if let Ok((mut writer, path)) = bucket.write_chunk_file(&id, chunk_index) {
                let _ = std::io::Write::write_all(&mut writer, slice);
                first_path.get_or_insert(path);
            }
            chunks.insert(chunk_index);
            parts.insert(chunk_index);
            meta.chunks.insert(chunk_index);
            meta.parts.insert(chunk_index);
            if async_flush_chunk {
                let _ = bucket.store(&meta);
            }
            idx = idx.saturating_add(1);
            offset = end;
        }
    }

    if !async_flush_chunk {
        meta.chunks = chunks;
        meta.parts = parts;
    }
    let _ = bucket.store(&meta);

    if let Ok(uri) = cache_key.parse::<http::Uri>() {
        if let Some(host) = uri.host() {
            let key = format!("if/domain/{host}");
            let _ = storage.shared_kv().incr(key.as_bytes(), 1);
        }
    }
    let ix_key = format!("ix/{}/{}", bucket.id(), cache_key);
    let _ = storage.shared_kv().set(ix_key.as_bytes(), &id.hash().0);

    let store_path = first_path
        .and_then(|p| p.parent().map(|p| p.to_string_lossy().to_string()))
        .unwrap_or_else(|| bucket.path().to_string_lossy().to_string());
    if bucket.store_type() == "memory" {
        return None;
    }
    Some(CacheCompletedPayload {
        store_url: id.key(),
        store_key: id.hash_str(),
        store_path,
        content_length: size as i64,
        last_modified: response_headers
            .get("Last-Modified")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string(),
        chunk_count: meta.chunks.count(),
        chunk_size,
        report_ratio: 0,
    })
}

fn header_map_from_meta(meta: &Metadata) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (k, v) in &meta.headers {
        if let (Ok(name), Ok(value)) = (
            http::header::HeaderName::from_bytes(k.as_bytes()),
            http::HeaderValue::from_str(v),
        ) {
            headers.insert(name, value);
        }
    }
    headers
}

fn read_storage_body(bucket: &dyn storage::Bucket, meta: &Metadata) -> anyhow::Result<Bytes> {
    let mut indices: Vec<u32> = meta.chunks.iter().copied().collect();
    indices.sort_unstable();
    let mut buf = Vec::with_capacity(meta.size as usize);
    for idx in indices {
        let (mut reader, _) = bucket.read_chunk_file(&meta.id, idx)?;
        let mut chunk = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut chunk)?;
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

fn snapshot_request(req: &Request<Incoming>) -> RequestSnapshot {
    let mut headers = HeaderMap::new();
    for (k, v) in req.headers().iter() {
        headers.insert(k, v.clone());
    }
    RequestSnapshot {
        method: req.method().clone(),
        uri: req.uri().clone(),
        headers,
    }
}

fn response_with_headers(status: StatusCode, headers: HeaderMap, body: Bytes) -> Response<Full<Bytes>> {
    metrics::record(status);
    let mut builder = Response::builder().status(status);
    for (k, v) in headers.iter() {
        builder = builder.header(k, v);
    }
    builder.body(Full::new(body)).unwrap()
}

fn empty_with_headers(status: StatusCode, headers: HeaderMap) -> Response<Full<Bytes>> {
    response_with_headers(status, headers, Bytes::new())
}

fn text_response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    metrics::record(status);
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
