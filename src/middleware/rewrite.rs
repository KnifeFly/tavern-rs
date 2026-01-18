use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http::{HeaderMap, Request, Response};
use http_body_util::Full;
use hyper::body::Incoming;

use crate::config::MiddlewareConfig;
use crate::middleware::{Cleanup, Middleware, RoundTripper};
use crate::middleware::registry::register;

#[derive(Debug, serde::Deserialize, Default, Clone)]
struct HeadersPolicy {
    #[serde(default)]
    set: HashMap<String, String>,
    #[serde(default)]
    add: HashMap<String, String>,
    #[serde(default)]
    remove: Vec<String>,
}

#[derive(Debug, serde::Deserialize, Default, Clone)]
struct RewriteOptions {
    #[serde(default)]
    request_headers_rewrite: Option<HeadersPolicy>,
    #[serde(default)]
    response_headers_rewrite: Option<HeadersPolicy>,
}

pub fn register_middleware() {
    register("rewrite", build);
}

pub fn build(cfg: &MiddlewareConfig) -> Result<(Middleware, Cleanup)> {
    let opts = parse_options(cfg)?;
    let middleware: Middleware = Arc::new(move |next: Arc<dyn RoundTripper>| {
        let opts = opts.clone();
        Arc::new(RewriteMiddleware { next, opts }) as Arc<dyn RoundTripper>
    });
    Ok((middleware, crate::middleware::empty_cleanup))
}

fn parse_options(cfg: &MiddlewareConfig) -> Result<RewriteOptions> {
    if cfg.options.is_empty() {
        return Ok(RewriteOptions::default());
    }
    let val = serde_yaml::to_value(&cfg.options)?;
    let opts: RewriteOptions = serde_yaml::from_value(val)?;
    Ok(opts)
}

struct RewriteMiddleware {
    next: Arc<dyn RoundTripper>,
    opts: RewriteOptions,
}

impl RoundTripper for RewriteMiddleware {
    fn round_trip(
        &self,
        mut req: Request<Incoming>,
    ) -> crate::middleware::BoxFuture<Result<Response<Full<Bytes>>>> {
        let next = Arc::clone(&self.next);
        let opts = self.opts.clone();
        Box::pin(async move {
            if let Some(policy) = &opts.request_headers_rewrite {
                apply_header_policy(req.headers_mut(), policy);
            }
            let mut resp = next.round_trip(req).await?;
            if let Some(policy) = &opts.response_headers_rewrite {
                apply_header_policy(resp.headers_mut(), policy);
            }
            Ok(resp)
        })
    }
}

fn apply_header_policy(headers: &mut HeaderMap, policy: &HeadersPolicy) {
    for (k, v) in policy.set.iter() {
        if let Ok(name) = http::header::HeaderName::from_bytes(k.as_bytes()) {
            if let Ok(val) = http::HeaderValue::from_str(v) {
                headers.insert(name, val);
            }
        }
    }
    for (k, v) in policy.add.iter() {
        if let Ok(name) = http::header::HeaderName::from_bytes(k.as_bytes()) {
            if let Ok(val) = http::HeaderValue::from_str(v) {
                headers.append(name, val);
            }
        }
    }
    for k in policy.remove.iter() {
        if let Ok(name) = http::header::HeaderName::from_bytes(k.as_bytes()) {
            headers.remove(name);
        }
    }
}
