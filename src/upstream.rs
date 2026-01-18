use anyhow::{Context, Result};
use bytes::Bytes;
use http::{HeaderMap, Method, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;

#[derive(Clone)]
pub struct UpstreamClient {
    client: Client<HttpConnector, Full<Bytes>>,
}

impl UpstreamClient {
    pub fn new() -> Self {
        let connector = HttpConnector::new();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }

    pub async fn fetch(
        &self,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
    ) -> Result<(http::StatusCode, HeaderMap, Bytes)> {
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
}

async fn collect_body(resp: http::Response<Incoming>) -> Result<Bytes> {
    let body = resp.into_body().collect().await.context("read upstream body")?;
    Ok(body.to_bytes())
}
