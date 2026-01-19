use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::{env, io::Write};
use std::io::Read;

use base64::Engine;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Body, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::{TcpListener, UnixListener};
use tokio::time::Duration;
use pprof::protos::Message;
use chrono::Local;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use tokio::sync::watch;

use crate::cache::CacheStore;
use crate::config::{Bootstrap, MiddlewareConfig};
use crate::constants;
use crate::access_log::AccessLogger;
use crate::event::{self, CacheCompletedPayload, EventContext};
use crate::middleware::{self, RoundTripper};
use crate::middleware::caching::CachingState;
use crate::metrics;
use crate::plugin::{Plugin, Router};
use crate::proxy::{self, Node};
use crate::runtime;
use crate::storage;
use crate::upstream::UpstreamClient;
use crate::net::RemoteAddr;

const DEFAULT_LOCAL_HOSTS: &[&str] = &["localhost", "127.0.0.1", "127.1"];
const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024; // 1MB

pub async fn run(cfg: Arc<Bootstrap>) -> Result<()> {
    let store = storage::native::NativeStorage::new(&cfg.storage)?;
    storage::set_default(store);

    crate::middleware::caching::register_middleware();
    crate::middleware::multirange::register_middleware();
    crate::middleware::recovery::register_middleware();
    crate::middleware::rewrite::register_middleware();

    crate::plugin::register_builtin();
    let plugins = load_plugins(&cfg);
    for plugin in &plugins {
        if let Err(err) = plugin.start() {
            log::warn!("plugin {} start failed: {err}", plugin.name());
        }
    }

    let local_hosts = build_local_hosts(&cfg);
    let cache_cfg = build_cache_config(&cfg);
    let plugin_router = build_plugin_router(&plugins);
    let cache_completed_pub = event::new_publisher(&event::new_topic_key::<CacheCompletedPayload>(
        event::CACHE_COMPLETED_KEY,
    ));
    let proxy_policy = proxy::BalancePolicy::from_str(&cfg.upstream.balancing);
    let proxy = Arc::new(proxy::ReverseProxy::new(build_upstream_nodes(&cfg), proxy_policy));
    let access_logger = build_access_logger(&cfg);
    let pprof_auth = cfg
        .server
        .pprof
        .as_ref()
        .map(|p| (p.username.clone(), p.password.clone()));
    let cache = if cache_cfg.object_pool_enabled {
        Arc::new(CacheStore::new_with_limit(cache_cfg.object_pool_size))
    } else {
        Arc::new(CacheStore::new())
    };
    let upstream = UpstreamClient::new();
    let cache_completed_pub: Arc<dyn Fn(&EventContext, CacheCompletedPayload) + Send + Sync> =
        Arc::new(cache_completed_pub);
    let cache_state = CachingState::new(
        Arc::clone(&cache),
        upstream.clone(),
        Arc::clone(&proxy),
        cfg.upstream.address.clone(),
        Arc::clone(&cache_completed_pub),
        cache_cfg.chunk_size,
        cache_cfg.include_query,
    );

    let proxy_chain = build_proxy_chain(&cfg, Arc::clone(&cache_state))?;

    let state = Arc::new(AppState {
        cfg,
        cache,
        upstream,
        local_hosts,
        cache_include_query: cache_cfg.include_query,
        plugins,
        plugin_router,
        cache_completed_pub,
        proxy,
        access_logger,
        pprof_auth,
        proxy_chain,
    });

    let addr = state.cfg.server.addr.clone();
    let inherited = inherited_listener(&addr);
    let (listener, meta) = bind_listener(&addr, inherited)?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    spawn_signal_handlers(meta, Arc::clone(&state.cfg), shutdown_tx);
    notify_ready();
    let result = match listener {
        ListenerKind::Tcp(listener) => run_tcp(listener, shutdown_rx, Arc::clone(&state)).await,
        ListenerKind::Unix { listener, .. } => run_unix(listener, shutdown_rx, Arc::clone(&state)).await,
    };
    if let Err(err) = result {
        return Err(err);
    }
    for plugin in &state.plugins {
        if let Err(err) = plugin.stop() {
            log::warn!("plugin {} stop failed: {err}", plugin.name());
        }
    }
    Ok(())
}

struct AppState {
    cfg: Arc<Bootstrap>,
    cache: Arc<CacheStore>,
    upstream: UpstreamClient,
    local_hosts: Arc<HashSet<String>>,
    cache_include_query: bool,
    plugins: Vec<Arc<dyn Plugin>>,
    plugin_router: Arc<Router>,
    cache_completed_pub: Arc<dyn Fn(&EventContext, CacheCompletedPayload) + Send + Sync>,
    proxy: Arc<proxy::ReverseProxy>,
    access_logger: Option<Arc<AccessLogger>>,
    pprof_auth: Option<(String, String)>,
    proxy_chain: Arc<dyn RoundTripper>,
}

#[derive(Clone, Copy)]
struct CacheConfig {
    include_query: bool,
    chunk_size: u64,
    object_pool_enabled: bool,
    object_pool_size: Option<usize>,
}

fn build_cache_config(cfg: &Bootstrap) -> CacheConfig {
    let mut include_query = true;
    let mut chunk_size = cfg.storage.slice_size;
    let mut object_pool_enabled = false;
    let mut object_pool_size = None;
    if chunk_size == 0 {
        chunk_size = DEFAULT_CHUNK_SIZE;
    }

    if let Some(opts) = find_caching_options(&cfg.server.middleware) {
        if let Some(val) = opts.include_query_in_cache_key {
            include_query = val;
        }
        if let Some(val) = opts.slice_size {
            chunk_size = val;
        }
        if let Some(enabled) = opts.object_pool_enabled {
            object_pool_enabled = enabled;
        }
        if let Some(size) = opts.object_pool_size {
            if size > 0 {
                object_pool_size = Some(size as usize);
            }
        }
    }

    CacheConfig {
        include_query,
        chunk_size,
        object_pool_enabled,
        object_pool_size,
    }
}

fn build_proxy_chain(
    cfg: &Bootstrap,
    cache_state: Arc<CachingState>,
) -> Result<Arc<dyn RoundTripper>> {
    let multirange_state = crate::middleware::multirange::MultirangeState::new(
        cache_state.upstream(),
        cache_state.proxy(),
        cache_state.upstream_addrs(),
    );
    let mut middlewares = Vec::new();
    for mw in &cfg.server.middleware {
        let name = mw.name.to_lowercase();
        let (mw_fn, _cleanup) = match name.as_str() {
            "caching" => crate::middleware::caching::build_with_state(mw, Arc::clone(&cache_state))?,
            "rewrite" => crate::middleware::rewrite::build(mw)?,
            "recovery" => crate::middleware::recovery::build(mw)?,
            "multirange" => {
                crate::middleware::multirange::build_with_state(mw, Arc::clone(&multirange_state))?
            }
            _ => crate::middleware::registry::create_or_empty(mw),
        };
        middlewares.push(mw_fn);
    }

    let base = Arc::new(UpstreamRoundTripper {
        upstream: cache_state.upstream(),
        proxy: cache_state.proxy(),
        upstream_addrs: cache_state.upstream_addrs(),
    });

    Ok(middleware::chain(&middlewares, base))
}

#[derive(Debug, serde::Deserialize)]
struct CachingOptions {
    #[serde(default)]
    include_query_in_cache_key: Option<bool>,
    #[serde(default)]
    slice_size: Option<u64>,
    #[serde(default)]
    object_pool_enabled: Option<bool>,
    #[serde(default, rename = "object_pool_size", alias = "object_poll_size")]
    object_pool_size: Option<u64>,
}

fn find_caching_options(middlewares: &[MiddlewareConfig]) -> Option<CachingOptions> {
    for mw in middlewares {
        if mw.name == "caching" {
            if mw.options.is_empty() {
                return Some(CachingOptions {
                    include_query_in_cache_key: None,
                    slice_size: None,
                    object_pool_enabled: None,
                    object_pool_size: None,
                });
            }
            let val = serde_yaml::to_value(&mw.options).ok()?;
            let opts: CachingOptions = serde_yaml::from_value(val).ok()?;
            return Some(opts);
        }
    }
    None
}


fn build_local_hosts(cfg: &Bootstrap) -> Arc<HashSet<String>> {
    let mut set = HashSet::new();
    for host in DEFAULT_LOCAL_HOSTS {
        set.insert((*host).to_string());
    }
    for host in &cfg.server.local_api_allow_hosts {
        set.insert(host.to_string());
    }
    Arc::new(set)
}

fn load_plugins(cfg: &Bootstrap) -> Vec<Arc<dyn Plugin>> {
    let mut plugins = Vec::new();
    for plug in &cfg.plugin {
        match crate::plugin::create(plug) {
            Ok(instance) => plugins.push(instance),
            Err(err) => log::warn!("plugin {} load failed: {err}", plug.name),
        }
    }
    plugins
}

fn build_plugin_router(plugins: &[Arc<dyn Plugin>]) -> Arc<Router> {
    let mut router = Router::new();
    for plugin in plugins {
        plugin.add_router(&mut router);
    }
    Arc::new(router)
}

fn build_upstream_nodes(cfg: &Bootstrap) -> Vec<Node> {
    let mut nodes = Vec::new();
    for addr in &cfg.upstream.address {
        if addr.trim().is_empty() {
            continue;
        }
        if addr.contains("://") {
            if let Ok(uri) = addr.parse::<http::Uri>() {
                let scheme = uri.scheme_str().unwrap_or("http");
                if let Some(authority) = uri.authority() {
                    nodes.push(Node::new(scheme, authority.as_str(), 1));
                    continue;
                }
            }
        }
        nodes.push(Node::new("http", addr, 1));
    }
    nodes
}

struct UpstreamRoundTripper {
    upstream: UpstreamClient,
    proxy: Arc<proxy::ReverseProxy>,
    upstream_addrs: Vec<String>,
}

impl RoundTripper for UpstreamRoundTripper {
    fn round_trip(
        &self,
        req: Request<Incoming>,
    ) -> crate::middleware::BoxFuture<Result<Response<Full<Bytes>>>> {
        let upstream = self.upstream.clone();
        let proxy = Arc::clone(&self.proxy);
        let upstream_addrs = self.upstream_addrs.clone();
        Box::pin(async move {
            let upstream_addr = select_upstream_addr_from_headers(req.headers(), &proxy, &upstream_addrs)?;
            let uri = build_upstream_uri(&req, &upstream_addr)?;
            let mut headers = HeaderMap::new();
            copy_headers(req.headers(), &mut headers);
            headers.remove(constants::INTERNAL_UPSTREAM_ADDR);
            let (status, headers, body) = upstream.fetch(req.method().clone(), uri, headers).await?;
            Ok(response_with_headers(status, headers, body))
        })
    }
}

fn select_upstream_addr_from_headers(
    headers: &HeaderMap,
    proxy: &proxy::ReverseProxy,
    upstream_addrs: &[String],
) -> Result<String> {
    if let Some(val) = headers.get(constants::INTERNAL_UPSTREAM_ADDR) {
        if let Ok(addr) = val.to_str() {
            return Ok(addr.to_string());
        }
    }
    if let Some(node) = proxy.next_node() {
        return Ok(format!("{}://{}", node.scheme, node.address));
    }
    upstream_addrs
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

fn copy_headers(src: &HeaderMap, dst: &mut HeaderMap) {
    for (k, v) in src.iter() {
        dst.insert(k, v.clone());
    }
}

fn build_access_logger(cfg: &Bootstrap) -> Option<Arc<AccessLogger>> {
    let access = cfg.server.access_log.as_ref()?;
    if !access.enabled {
        return None;
    }
    match AccessLogger::new(Some(&access.path)) {
        Ok(logger) => Some(Arc::new(logger)),
        Err(err) => {
            log::warn!("failed to init access log: {err}");
            None
        }
    }
}

fn is_unix_addr(addr: &str) -> bool {
    addr.starts_with("unix://") || addr.ends_with(".sock") || addr.starts_with('/')
}

enum ListenerKind {
    Tcp(TcpListener),
    Unix { listener: UnixListener, path: String },
}

struct ListenerMeta {
    fd: RawFd,
    kind: &'static str,
    addr: String,
}

fn inherited_listener(addr: &str) -> Option<ListenerMeta> {
    let fd = env::var("TAVERN_GRACEFUL_FD").ok()?.parse::<RawFd>().ok()?;
    let kind = env::var("TAVERN_GRACEFUL_TYPE")
        .unwrap_or_else(|_| if is_unix_addr(addr) { "unix".into() } else { "tcp".into() });
    let addr = env::var("TAVERN_GRACEFUL_ADDR").unwrap_or_else(|_| addr.to_string());
    let kind = if kind == "unix" { "unix" } else { "tcp" };
    Some(ListenerMeta { fd, kind, addr })
}

fn bind_listener(addr: &str, inherited: Option<ListenerMeta>) -> Result<(ListenerKind, ListenerMeta)> {
    if let Some(mut meta) = inherited {
        if meta.kind == "unix" {
            let listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(meta.fd) };
            listener.set_nonblocking(true)?;
            let tokio_listener = UnixListener::from_std(listener)?;
            meta.fd = tokio_listener.as_raw_fd();
            return Ok((
                ListenerKind::Unix {
                    listener: tokio_listener,
                    path: meta.addr.clone(),
                },
                meta,
            ));
        }
        let listener = unsafe { std::net::TcpListener::from_raw_fd(meta.fd) };
        listener.set_nonblocking(true)?;
        let tokio_listener = TcpListener::from_std(listener)?;
        meta.fd = tokio_listener.as_raw_fd();
        return Ok((ListenerKind::Tcp(tokio_listener), meta));
    }

    if is_unix_addr(addr) {
        let path = addr.strip_prefix("unix://").unwrap_or(addr);
        let path_ref = Path::new(path);
        if path_ref.exists() {
            std::fs::remove_file(path_ref).ok();
        }
        let listener = std::os::unix::net::UnixListener::bind(path_ref).context("bind unix socket")?;
        listener.set_nonblocking(true)?;
        let tokio_listener = UnixListener::from_std(listener)?;
        let meta = ListenerMeta {
            fd: tokio_listener.as_raw_fd(),
            kind: "unix",
            addr: path.to_string(),
        };
        return Ok((
            ListenerKind::Unix {
                listener: tokio_listener,
                path: path.to_string(),
            },
            meta,
        ));
    }

    let bind_addr = if addr.starts_with(':') {
        format!("0.0.0.0{}", addr)
    } else {
        addr.to_string()
    };
    let socket_addr: SocketAddr = bind_addr.parse().context("parse server.addr")?;
    let listener = std::net::TcpListener::bind(socket_addr).context("bind tcp")?;
    listener.set_nonblocking(true)?;
    let tokio_listener = TcpListener::from_std(listener)?;
    let meta = ListenerMeta {
        fd: tokio_listener.as_raw_fd(),
        kind: "tcp",
        addr: bind_addr,
    };
    Ok((ListenerKind::Tcp(tokio_listener), meta))
}

fn spawn_signal_handlers(meta: ListenerMeta, cfg: Arc<Bootstrap>, shutdown: watch::Sender<bool>) {
    let upgrade_meta = ListenerMeta {
        fd: meta.fd,
        kind: meta.kind,
        addr: meta.addr.clone(),
    };
    let shutdown_upgrade = shutdown.clone();
    tokio::spawn(async move {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
        {
            while sig.recv().await.is_some() {
                if let Err(err) = spawn_upgrade(&upgrade_meta, &cfg, shutdown_upgrade.clone()).await {
                    log::error!("graceful upgrade failed: {err}");
                }
            }
        }
    });

    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
        loop {
            tokio::select! {
                _ = async {
                    if let Some(sig) = sigterm.as_mut() {
                        let _ = sig.recv().await;
                    }
                } => {
                    let _ = shutdown.send(true);
                    break;
                }
                _ = async {
                    if let Some(sig) = sigint.as_mut() {
                        let _ = sig.recv().await;
                    }
                } => {
                    let _ = shutdown.send(true);
                    break;
                }
            }
        }
    });
}

async fn spawn_upgrade(meta: &ListenerMeta, cfg: &Bootstrap, shutdown: watch::Sender<bool>) -> Result<()> {
    clear_cloexec(meta.fd)?;
    let (read_fd, write_fd) = nix::unistd::pipe().context("pipe")?;
    clear_cloexec(write_fd)?;
    let exe = env::current_exe().context("current exe")?;
    let mut cmd = std::process::Command::new(exe);
    cmd.args(env::args().skip(1));
    cmd.env("TAVERN_GRACEFUL_FD", meta.fd.to_string());
    cmd.env("TAVERN_GRACEFUL_TYPE", meta.kind);
    cmd.env("TAVERN_GRACEFUL_ADDR", &meta.addr);
    cmd.env("TAVERN_GRACEFUL_READY_FD", write_fd.to_string());
    if let Some(pidfile) = &cfg.pidfile {
        cmd.env("TAVERN_GRACEFUL_PIDFILE", pidfile);
    }
    cmd.spawn().context("spawn upgrade")?;
    nix::unistd::close(write_fd).ok();
    let ready = tokio::time::timeout(Duration::from_secs(30), wait_ready(read_fd)).await;
    match ready {
        Ok(Ok(())) => {
            let _ = shutdown.send(true);
        }
        Ok(Err(err)) => {
            log::warn!("upgrade readiness failed: {err}");
        }
        Err(_) => {
            log::warn!("upgrade readiness timeout");
        }
    }
    Ok(())
}

async fn wait_ready(fd: RawFd) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf).context("read ready")
    })
    .await
    .unwrap_or_else(|err| Err(anyhow!("join error: {err}")))
}

fn notify_ready() {
    let fd = match env::var("TAVERN_GRACEFUL_READY_FD") {
        Ok(val) => val.parse::<RawFd>().ok(),
        Err(_) => None,
    };
    let Some(fd) = fd else { return };
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    let _ = file.write_all(b"1");
}

fn clear_cloexec(fd: RawFd) -> Result<()> {
    use nix::fcntl::{fcntl, FcntlArg, FdFlag};
    let flags = fcntl(fd, FcntlArg::F_GETFD).context("fcntl getfd")?;
    let mut flags = FdFlag::from_bits_truncate(flags);
    flags.remove(FdFlag::FD_CLOEXEC);
    fcntl(fd, FcntlArg::F_SETFD(flags)).context("fcntl setfd")?;
    Ok(())
}

async fn run_tcp(
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
    state: Arc<AppState>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            res = listener.accept() => {
                let (stream, _) = res.context("accept tcp")?;
                let peer = stream.peer_addr().ok().map(|addr| addr.to_string());
                let io = TokioIo::new(stream);
                let state = Arc::clone(&state);

                tokio::spawn(async move {
                    let state = Arc::clone(&state);
                    let service = service_fn(move |mut req| {
                        if let Some(peer) = &peer {
                            req.extensions_mut().insert(RemoteAddr(peer.clone()));
                        }
                        handle(req, Arc::clone(&state))
                    });
                    let builder = ConnBuilder::new(TokioExecutor::new());
                    if let Err(err) = builder.serve_connection(io, service).await {
                        metrics::record_unexpected_closed("HTTP/1.1", "UNKNOWN");
                        log::error!("http connection error: {err}");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn run_unix(
    listener: UnixListener,
    mut shutdown: watch::Receiver<bool>,
    state: Arc<AppState>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            res = listener.accept() => {
                let (stream, _) = res.context("accept unix")?;
                let io = TokioIo::new(stream);
                let peer = Some("unix".to_string());
                let state = Arc::clone(&state);

                tokio::spawn(async move {
                    let state = Arc::clone(&state);
                    let service = service_fn(move |mut req| {
                        if let Some(peer) = &peer {
                            req.extensions_mut().insert(RemoteAddr(peer.clone()));
                        }
                        handle(req, Arc::clone(&state))
                    });
                    let builder = ConnBuilder::new(TokioExecutor::new());
                    if let Err(err) = builder.serve_connection(io, service).await {
                        metrics::record_unexpected_closed("HTTP/1.1", "UNKNOWN");
                        log::error!("http connection error: {err}");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn handle(req: Request<Incoming>, state: Arc<AppState>) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let req_info = RequestInfo::from_request(&req);
    let host = extract_host(&req);
    let is_local = host
        .as_ref()
        .and_then(|h| h.split_once(':').map(|(h, _)| h).or_else(|| Some(h.as_str())))
        .map(|h| state.local_hosts.contains(h))
        .unwrap_or(false);

    let protocol = req_info.protocol.clone();
    let method = req_info.method.to_string();
    let mut resp = metrics::with_request_context(protocol, method, async {
        if is_local {
            handle_internal(req, &state).await
        } else {
            handle_proxy(req, Arc::clone(&state)).await
        }
    })
    .await;
    if let Ok(val) = req_info.request_id.parse() {
        resp.headers_mut()
            .insert(constants::PROTOCOL_REQUEST_ID_KEY, val);
    }
    log_access(&state, &req_info, &resp);
    Ok(resp)
}

async fn handle_internal(req: Request<Incoming>, state: &AppState) -> Response<Full<Bytes>> {
    let path = req.uri().path().to_string();
    if path.starts_with("/debug/pprof") {
        return handle_pprof(&req, state).await;
    }
    if let Some(resp) = state.plugin_router.handle_path(&path, req) {
        return resp;
    }
    match path.as_str() {
        "/healthz/startup-probe" => text_response(StatusCode::OK, "ok"),
        "/healthz/liveness-probe" => empty_response(StatusCode::OK),
        "/healthz/readiness-probe" => empty_response(StatusCode::OK),
        "/version" => json_response(&runtime::build_info()),
        "/metrics" => text_response(StatusCode::OK, &metrics::render()),
        _ => not_found(),
    }
}

async fn handle_proxy(req: Request<Incoming>, state: Arc<AppState>) -> Response<Full<Bytes>> {
    for plugin in &state.plugins {
        if let Some(resp) = plugin.handle_request(&req) {
            return resp;
        }
    }
    let method = req.method().clone();
    if method.as_str() == "PURGE" {
        return handle_purge(&req, &state).await;
    }
    match state.proxy_chain.round_trip(req).await {
        Ok(resp) => resp,
        Err(err) => text_response(StatusCode::BAD_GATEWAY, &format!("proxy error: {err}")),
    }
}

async fn handle_purge(req: &Request<Incoming>, state: &AppState) -> Response<Full<Bytes>> {
    let cache_key = match build_cache_key(req, state.cache_include_query) {
        Some(key) => key,
        None => return text_response(StatusCode::BAD_REQUEST, "invalid host"),
    };

    let existed = state.cache.remove(&cache_key).await;
    let purge_type = req
        .headers()
        .get("Purge-Type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let dir = purge_type.eq_ignore_ascii_case("dir");
    let _ = storage::current().purge(
        req.uri().to_string().as_str(),
        storage::PurgeControl {
            hard: true,
            dir,
            mark_expired: false,
        },
    );
    if existed {
        text_response(StatusCode::OK, "")
    } else {
        text_response(StatusCode::NOT_FOUND, "")
    }
}

fn build_cache_key(req: &Request<Incoming>, include_query: bool) -> Option<String> {
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

fn extract_host(req: &Request<Incoming>) -> Option<String> {
    if let Some(host) = req.headers().get("host") {
        return host.to_str().ok().map(|v| v.to_string());
    }
    req.uri().host().map(|v| v.to_string())
}

struct RequestInfo {
    method: Method,
    uri: String,
    remote_addr: String,
    client_ip: String,
    host: String,
    referer: String,
    user_agent: String,
    content_length: String,
    range: String,
    x_forwarded_for: String,
    request_id: String,
    protocol: String,
    start_at: std::time::Instant,
}

impl RequestInfo {
    fn from_request(req: &Request<Incoming>) -> Self {
        let remote_addr = req
            .extensions()
            .get::<RemoteAddr>()
            .map(|v| v.0.clone())
            .unwrap_or_else(|| "-".to_string());
        let client_ip = client_ip(&remote_addr, req.headers());
        let host = extract_host(req).unwrap_or_else(|| "-".to_string());
        let host = host.split(':').next().unwrap_or(&host).to_string();
        let referer = req
            .headers()
            .get("Referer")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-")
            .to_string();
        let user_agent = req
            .headers()
            .get("User-Agent")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-")
            .to_string();
        let content_length = req
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-")
            .to_string();
        let range = req
            .headers()
            .get("Range")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-")
            .to_string();
        let x_forwarded_for = req
            .headers()
            .get("X-Forwarded-For")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-")
            .to_string();
        let request_id = metrics::request_id_from_headers(req.headers());
        let protocol = format!("{:?}", req.version())
            .replace("HTTP/", "HTTP/")
            .replace("_", ".")
            .replace("HTTP.", "HTTP/");
        Self {
            method: req.method().clone(),
            uri: req.uri().to_string(),
            remote_addr,
            client_ip,
            host,
            referer,
            user_agent,
            content_length,
            range,
            x_forwarded_for,
            request_id,
            protocol,
            start_at: std::time::Instant::now(),
        }
    }
}

fn not_found() -> Response<Full<Bytes>> {
    text_response(StatusCode::NOT_FOUND, "not found")
}

fn empty_response(status: StatusCode) -> Response<Full<Bytes>> {
    metrics::record(status);
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .unwrap()
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

fn json_response<T: serde::Serialize>(payload: &T) -> Response<Full<Bytes>> {
    match serde_json::to_vec(payload) {
        Ok(bytes) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json; charset=utf-8")
            .body(Full::new(Bytes::from(bytes)))
            .unwrap(),
        Err(_) => text_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to encode"),
    }
}

#[allow(dead_code)]
fn invalid_request(msg: &str) -> Response<Full<Bytes>> {
    text_response(StatusCode::BAD_REQUEST, msg)
}

#[allow(dead_code)]
fn internal_error(err: &dyn std::error::Error) -> Response<Full<Bytes>> {
    let body = format!("internal error: {err}");
    text_response(StatusCode::INTERNAL_SERVER_ERROR, &body)
}

fn _ensure_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Bootstrap>();
}

fn log_access(state: &AppState, req: &RequestInfo, resp: &Response<Full<Bytes>>) {
    let logger = match &state.access_logger {
        Some(logger) => logger,
        None => return,
    };
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get("Content-Type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let cache_status = resp
        .headers()
        .get(constants::PROTOCOL_CACHE_STATUS_KEY)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    let body_len = resp.body().size_hint().exact().unwrap_or(0);
    let bytes_sent = response_header_size(resp.status(), resp.headers()) + body_len;
    let duration_ms = req.start_at.elapsed().as_millis().to_string();
    let request_line = format!("{} {} {}", req.method, req.uri, req.protocol);
    let fields = [
        normalize_field(&req.client_ip),
        normalize_field(&req.host),
        normalize_field_replace(content_type),
        normalize_field(&format_access_time()),
        normalize_field_replace(&request_line),
        normalize_field(&status.to_string()),
        normalize_field(&bytes_sent.to_string()),
        normalize_field_replace(&req.referer),
        normalize_field_replace(&req.user_agent),
        normalize_field(&duration_ms),
        normalize_field(&body_len.to_string()),
        normalize_field_replace(&req.content_length),
        normalize_field_replace(&req.range),
        normalize_field_replace(&req.x_forwarded_for),
        normalize_field(cache_status),
        normalize_field(&req.request_id),
    ];
    let line = format!("{}\n", fields.join(" "));
    logger.log_line(&line);
}

fn normalize_field(s: &str) -> String {
    if s.is_empty() {
        "-".to_string()
    } else {
        s.to_string()
    }
}

fn normalize_field_replace(s: &str) -> String {
    if s.is_empty() {
        "-".to_string()
    } else {
        s.replace(' ', "+")
    }
}

fn format_access_time() -> String {
    Local::now().format("[%d/%b/%Y:%H:%M:%S %z]").to_string()
}

fn response_header_size(status: StatusCode, headers: &HeaderMap) -> u64 {
    let reason_len = status.canonical_reason().map(|r| r.len()).unwrap_or(0);
    let mut n = (reason_len + 15) as u64;
    for (k, v) in headers.iter() {
        n += (k.as_str().len() + 4) as u64;
        if let Ok(val) = v.to_str() {
            n += val.len() as u64;
        }
    }
    n + 2
}

fn client_ip(remote_addr: &str, headers: &HeaderMap) -> String {
    let direct = headers
        .get("Client-Ip")
        .or_else(|| headers.get("X-Real-IP"))
        .or_else(|| headers.get("X-Forwarded-For"))
        .and_then(|v| v.to_str().ok());
    direct.unwrap_or(remote_addr).to_string()
}

async fn handle_pprof(req: &Request<Incoming>, state: &AppState) -> Response<Full<Bytes>> {
    let (user, pass) = match &state.pprof_auth {
        Some(val) => val,
        None => return not_found(),
    };
    if !basic_auth_ok(req, user, pass) {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("WWW-Authenticate", r#"Basic realm="restricted", charset="UTF-8""#)
            .body(Full::new(Bytes::from("Unauthorized")))
            .unwrap();
    }
    let path = req.uri().path();
    if path == "/debug/pprof" || path == "/debug/pprof/" {
        let body = "profile\n";
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; charset=utf-8")
            .header("Content-Length", body.len().to_string())
            .body(Full::new(Bytes::from(body)))
            .unwrap();
    }

    if path == "/debug/pprof/profile" {
        let mut seconds = 30u64;
        if let Some(query) = req.uri().query() {
            for part in query.split('&') {
                let mut iter = part.splitn(2, '=');
                if let (Some(key), Some(val)) = (iter.next(), iter.next()) {
                    if key == "seconds" {
                        if let Ok(parsed) = val.parse::<u64>() {
                            seconds = parsed;
                        }
                    }
                }
            }
        }
        seconds = seconds.clamp(1, 120);
        let guard = match pprof::ProfilerGuard::new(100) {
            Ok(guard) => guard,
            Err(err) => {
                return text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("pprof guard error: {err}"),
                );
            }
        };
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        let report = match guard.report().build() {
            Ok(report) => report,
            Err(err) => {
                return text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("pprof report error: {err}"),
                );
            }
        };
        let profile = match report.pprof() {
            Ok(profile) => profile,
            Err(err) => {
                return text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("pprof encode error: {err}"),
                );
            }
        };
        let mut body = Vec::new();
        if let Err(err) = profile.write_to_vec(&mut body) {
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("pprof encode error: {err}"),
            );
        }
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", body.len().to_string())
            .body(Full::new(Bytes::from(body)))
            .unwrap();
    }

    text_response(StatusCode::NOT_FOUND, "pprof endpoint not found")
}

fn basic_auth_ok(req: &Request<Incoming>, user: &str, pass: &str) -> bool {
    let header = match req.headers().get("authorization") {
        Some(val) => val,
        None => return false,
    };
    let raw = match header.to_str() {
        Ok(val) => val,
        Err(_) => return false,
    };
    let encoded = raw.strip_prefix("Basic ").unwrap_or("");
    if encoded.is_empty() {
        return false;
    }
    let decoded = match base64::engine::general_purpose::STANDARD.decode(encoded) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    let creds = match std::str::from_utf8(&decoded) {
        Ok(val) => val,
        Err(_) => return false,
    };
    creds == format!("{user}:{pass}")
}
