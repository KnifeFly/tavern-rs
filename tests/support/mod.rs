use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use rand::RngCore;
use tavern::config::{Bootstrap, Logger, MiddlewareConfig, Server, Storage, Upstream};
use tavern::constants;
use tokio::net::TcpListener;
use tokio::sync::OnceCell;

static START: OnceCell<()> = OnceCell::const_new();

pub async fn ensure_server() {
    START
        .get_or_init(|| async {
            let cfg = test_config();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                runtime.block_on(async move {
                    let _ = tavern::server::run(Arc::new(cfg)).await;
                });
            });
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;
}

fn test_config() -> Bootstrap {
    let mut middleware = Vec::new();
    let mut opts = HashMap::new();
    opts.insert("include_query_in_cache_key".to_string(), serde_yaml::Value::Bool(true));
    let caching = MiddlewareConfig {
        name: "caching".to_string(),
        options: opts,
    };
    middleware.push(caching);

    Bootstrap {
        strict: false,
        hostname: None,
        pidfile: None,
        logger: Logger::default(),
        server: Server {
            addr: "127.0.0.1:18080".to_string(),
            read_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(60),
            idle_timeout: Duration::from_secs(90),
            read_header_timeout: Duration::from_secs(30),
            max_header_bytes: 1024 * 1024,
            middleware,
            pprof: None,
            access_log: None,
            local_api_allow_hosts: Vec::new(),
            server_via_tokens: None,
        },
        plugin: Vec::new(),
        upstream: Upstream {
            balancing: "wrr".to_string(),
            address: vec!["http://127.0.0.1:1".to_string()],
            max_idle_conns: 1000,
            max_idle_conns_per_host: 500,
            max_connections_per_server: 100,
            insecure_skip_verify: true,
            resolve_addresses: false,
            features: HashMap::new(),
        },
        storage: Storage {
            driver: "native".to_string(),
            db_type: "pebble".to_string(),
            db_path: "index".to_string(),
            async_load: true,
            eviction_policy: "fifo".to_string(),
            selection_policy: "hashring".to_string(),
            slice_size: 524_288,
            buckets: Vec::new(),
        },
    }
}

#[derive(Clone)]
pub struct TestResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl TestResponse {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }
}

#[derive(Clone)]
pub struct TestClient {
    proxy_addr: SocketAddr,
}

impl TestClient {
    pub fn new(proxy: &str) -> Self {
        let proxy = proxy.trim_start_matches("http://");
        let proxy_addr = proxy.parse().expect("proxy addr");
        Self { proxy_addr }
    }

    pub async fn send(&self, method: Method, url: &str, headers: HeaderMap) -> TestResponse {
        let stream = tokio::net::TcpStream::connect(self.proxy_addr)
            .await
            .expect("connect proxy");
        let io = TokioIo::new(stream);
        let (mut sender, conn) = http1::handshake(io).await.expect("handshake");
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let uri: http::Uri = url.parse().expect("uri");
        let mut builder = Request::builder().method(method).uri(uri.clone());
        if !headers.contains_key(http::header::HOST) {
            if let Some(authority) = uri.authority() {
                builder = builder.header(http::header::HOST, authority.as_str());
            }
        }
        for (k, v) in headers.iter() {
            builder = builder.header(k, v);
        }
        let req = builder
            .body(Full::new(Bytes::new()))
            .expect("request");

        let resp = sender.send_request(req).await.expect("send request");
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();

        TestResponse {
            status,
            headers,
            body,
        }
    }
}

pub struct MockServer {
    addr: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockServer {
    pub async fn start<F>(handler: F) -> Self
    where
        F: Fn(http::Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("local addr");
        let (shutdown, mut rx) = tokio::sync::oneshot::channel();
        let handler = Arc::new(handler);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut rx => break,
                    res = listener.accept() => {
                        let (stream, _) = match res { Ok(v) => v, Err(_) => break };
                        let io = TokioIo::new(stream);
                        let handler = Arc::clone(&handler);
                        tokio::spawn(async move {
                            let service = service_fn(move |req| {
                                let resp = handler(req);
                                async move { Ok::<_, hyper::Error>(resp) }
                            });
                            let builder = ConnBuilder::new(TokioExecutor::new());
                            let _ = builder.serve_connection(io, service).await;
                        });
                    }
                }
            }
        });

        Self {
            addr,
            shutdown: Some(shutdown),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[derive(Clone)]
pub struct MockFile {
    pub path: PathBuf,
    pub md5: String,
    pub size: usize,
    _temp_dir: Arc<tempfile::TempDir>,
}

pub fn gen_file(size: usize) -> MockFile {
    let mut buf = vec![0u8; size];
    rand::thread_rng().fill_bytes(&mut buf);
    let dir = Arc::new(tempfile::tempdir().expect("tempdir"));
    let path = dir.path().join(format!("file-{size}.bin"));
    std::fs::write(&path, &buf).expect("write file");
    let digest = format!("{:x}", md5::compute(&buf));
    MockFile {
        path,
        md5: digest,
        size,
        _temp_dir: dir,
    }
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", md5::compute(bytes))
}

pub fn read_range(path: &PathBuf, start: usize, length: usize) -> Vec<u8> {
    let mut file = std::fs::File::open(path).expect("open file");
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(start as u64)).expect("seek");
    let mut buf = vec![0u8; length];
    file.read_exact(&mut buf).expect("read range");
    buf
}

pub struct E2E {
    pub case_url: String,
    pub upstream: MockServer,
    pub client: TestClient,
}

impl E2E {
    pub async fn new<F>(case_url: &str, handler: F) -> Self
    where
        F: Fn(http::Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync + 'static,
    {
        ensure_server().await;
        let upstream = MockServer::start(handler).await;
        let client = TestClient::new("http://127.0.0.1:18080");
        Self {
            case_url: case_url.to_string(),
            upstream,
            client,
        }
    }

    pub async fn do_request<F>(&self, edit: F) -> TestResponse
    where
        F: FnOnce(&mut Method, &mut HeaderMap),
    {
        let mut method = Method::GET;
        let mut headers = HeaderMap::new();
        headers.insert(
            constants::INTERNAL_UPSTREAM_ADDR,
            self.upstream.addr().to_string().parse().unwrap(),
        );
        headers.insert(
            "X-Store-Url",
            http::HeaderValue::from_str(&self.case_url).unwrap(),
        );
        edit(&mut method, &mut headers);
        self.client.send(method, &self.case_url, headers).await
    }

    pub async fn purge(&self) -> TestResponse {
        let mut headers = HeaderMap::new();
        headers.insert(
            constants::INTERNAL_UPSTREAM_ADDR,
            self.upstream.addr().to_string().parse().unwrap(),
        );
        self.client
            .send(Method::from_bytes(b"PURGE").unwrap(), &self.case_url, headers)
            .await
    }
}

pub fn resp_simple_file(file: &MockFile) -> impl Fn(http::Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync {
    let file = file.clone();
    move |_req: http::Request<Incoming>| {
        let bytes = std::fs::read(&file.path).expect("read file");
        let mut headers = HeaderMap::new();
        headers.insert("Cache-Control", "max-age=10".parse().unwrap());
        headers.insert("Content-MD5", file.md5.parse().unwrap());
        headers.insert("ETag", file.md5.parse().unwrap());
        headers.insert(
            "Last-Modified",
            httpdate::fmt_http_date(std::time::SystemTime::now())
                .parse()
                .unwrap(),
        );
        headers.insert("X-Server", "tavern-e2e/1.0.0".parse().unwrap());
        headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
        build_response(StatusCode::OK, headers, Bytes::from(bytes))
    }
}

pub fn resp_callback_file<F>(file: &MockFile, cb: F) -> impl Fn(http::Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync
where
    F: Fn(&http::Request<Incoming>, &mut HeaderMap) + Send + Sync + 'static,
{
    let file = file.clone();
    let cb = Arc::new(cb);
    move |req: http::Request<Incoming>| {
        let bytes = std::fs::read(&file.path).expect("read file");
        let mut headers = HeaderMap::new();
        headers.insert("Cache-Control", "max-age=10".parse().unwrap());
        headers.insert("Content-MD5", file.md5.parse().unwrap());
        headers.insert("ETag", file.md5.parse().unwrap());
        headers.insert(
            "Last-Modified",
            httpdate::fmt_http_date(std::time::SystemTime::now())
                .parse()
                .unwrap(),
        );
        headers.insert("X-Server", "tavern-e2e/1.0.0".parse().unwrap());
        cb(&req, &mut headers);
        headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
        build_response(StatusCode::OK, headers, Bytes::from(bytes))
    }
}

pub fn resp_callback<F>(cb: F) -> impl Fn(http::Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync
where
    F: Fn(&http::Request<Incoming>, &mut HeaderMap) + Send + Sync + 'static,
{
    let cb = Arc::new(cb);
    move |req: http::Request<Incoming>| {
        let mut headers = HeaderMap::new();
        cb(&req, &mut headers);
        build_response(StatusCode::OK, headers, Bytes::new())
    }
}

pub fn wrong_hit() -> impl Fn(http::Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync {
    move |_req: http::Request<Incoming>| {
        build_response(StatusCode::BAD_GATEWAY, HeaderMap::new(), Bytes::new())
    }
}

fn build_response(status: StatusCode, headers: HeaderMap, body: Bytes) -> Response<Full<Bytes>> {
    let mut builder = Response::builder().status(status);
    for (k, v) in headers.iter() {
        builder = builder.header(k, v);
    }
    builder.body(Full::new(body)).unwrap()
}
