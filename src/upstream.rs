use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use http::{HeaderMap, Method, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use hyper_rustls::HttpsConnector;
use hyper_rustls::HttpsConnectorBuilder;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};

use crate::config;

#[derive(Clone)]
pub struct UpstreamClient {
    client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
    max_conns_per_host: Option<usize>,
    host_limits: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    fd_limit: Option<Arc<Semaphore>>,
}

impl UpstreamClient {
    pub fn new(cfg: &config::Upstream) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let https = if cfg.insecure_skip_verify {
            let tls = insecure_tls_config();
            HttpsConnectorBuilder::new()
                .with_tls_config(tls)
                .https_or_http()
                .enable_http1()
                .enable_http2()
                .wrap_connector(connector)
        } else {
            HttpsConnectorBuilder::new()
                .with_native_roots()
                .unwrap()
                .https_or_http()
                .enable_http1()
                .enable_http2()
                .wrap_connector(connector)
        };
        let mut builder = Client::builder(TokioExecutor::new());
        let mut max_idle_per_host = if cfg.max_idle_conns_per_host > 0 {
            cfg.max_idle_conns_per_host
        } else {
            usize::MAX
        };
        if cfg.max_idle_conns > 0 {
            max_idle_per_host = max_idle_per_host.min(cfg.max_idle_conns);
        }
        if max_idle_per_host != usize::MAX {
            builder.pool_max_idle_per_host(max_idle_per_host);
        }
        let client = builder.build(https);
        let max_conns_per_host = if cfg.max_connections_per_server > 0 {
            Some(cfg.max_connections_per_server)
        } else {
            None
        };
        let fd_limit = if feature_enabled(&cfg.features, "limit_rate_by_fd") {
            load_fd_limit().map(Arc::new)
        } else {
            None
        };
        Self {
            client,
            max_conns_per_host,
            host_limits: Arc::new(Mutex::new(HashMap::new())),
            fd_limit,
        }
    }

    pub async fn fetch(
        &self,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
    ) -> Result<(http::StatusCode, HeaderMap, Bytes)> {
        let _fd_permit = self.acquire_fd_limit().await;
        let _host_permit = self.acquire_host_limit(&uri).await;
        let mut req = http::Request::builder().method(method).uri(uri);
        for (k, v) in headers.iter() {
            req = req.header(k, v);
        }
        let req = req.body(Full::new(Bytes::new())).context("build upstream request")?;
        let resp = self.client.request(req).await.context("upstream request")?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let body_bytes = collect_body(resp).await?;
        Ok((status, headers, body_bytes))
    }

    async fn acquire_host_limit(&self, uri: &Uri) -> Option<OwnedSemaphorePermit> {
        let Some(limit) = self.max_conns_per_host else { return None };
        let Some(authority) = uri.authority() else { return None };
        let key = authority.as_str().to_string();
        let sem = {
            let mut map = self.host_limits.lock().expect("host limits");
            map.entry(key)
                .or_insert_with(|| Arc::new(Semaphore::new(limit)))
                .clone()
        };
        Some(sem.acquire_owned().await.expect("semaphore"))
    }

    async fn acquire_fd_limit(&self) -> Option<OwnedSemaphorePermit> {
        let sem = self.fd_limit.as_ref()?.clone();
        Some(sem.acquire_owned().await.expect("fd limit"))
    }
}

async fn collect_body(resp: http::Response<Incoming>) -> Result<Bytes> {
    let body = resp.into_body().collect().await.context("read upstream body")?;
    Ok(body.to_bytes())
}

fn insecure_tls_config() -> rustls::ClientConfig {
    let provider = rustls::crypto::ring::default_provider();
    let builder = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_safe_default_protocol_versions()
        .expect("tls versions");
    let verifier = Arc::new(NoVerifier {});
    builder
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth()
}

#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn feature_enabled(features: &HashMap<String, serde_yaml::Value>, key: &str) -> bool {
    features
        .get(key)
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

fn load_fd_limit() -> Option<Semaphore> {
    let limit = match nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE) {
        Ok((soft, _)) => soft as usize,
        Err(_) => return None,
    };
    let budget = limit.saturating_sub(256).max(1);
    Some(Semaphore::new(budget))
}
