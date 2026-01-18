use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures::FutureExt;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use std::panic::AssertUnwindSafe;

use crate::config::MiddlewareConfig;
use crate::middleware::{Cleanup, Middleware, RoundTripper};
use crate::middleware::registry::register;

#[derive(Debug, serde::Deserialize, Default, Clone)]
struct RecoveryOptions {
    #[serde(default)]
    fail_count_threshold: Option<u64>,
    #[serde(default)]
    fail_window: Option<u64>,
}

pub fn register_middleware() {
    register("recovery", build);
}

pub fn build(cfg: &MiddlewareConfig) -> Result<(Middleware, Cleanup)> {
    let opts = parse_options(cfg)?;
    let fail_count = Arc::new(AtomicU64::new(0));
    let threshold = opts.fail_count_threshold.unwrap_or(0);
    if let Some(window) = opts.fail_window {
        if window > 0 {
            let fail_count = Arc::clone(&fail_count);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(window));
                loop {
                    ticker.tick().await;
                    fail_count.store(0, Ordering::Relaxed);
                }
            });
        }
    }

    let middleware: Middleware = Arc::new(move |next: Arc<dyn RoundTripper>| {
        Arc::new(RecoveryMiddleware {
            next,
            fail_count: Arc::clone(&fail_count),
            threshold,
        }) as Arc<dyn RoundTripper>
    });

    Ok((middleware, crate::middleware::empty_cleanup))
}

fn parse_options(cfg: &MiddlewareConfig) -> Result<RecoveryOptions> {
    if cfg.options.is_empty() {
        return Ok(RecoveryOptions::default());
    }
    let val = serde_yaml::to_value(&cfg.options)?;
    let opts: RecoveryOptions = serde_yaml::from_value(val)?;
    Ok(opts)
}

struct RecoveryMiddleware {
    next: Arc<dyn RoundTripper>,
    fail_count: Arc<AtomicU64>,
    threshold: u64,
}

impl RoundTripper for RecoveryMiddleware {
    fn round_trip(
        &self,
        req: Request<Incoming>,
    ) -> crate::middleware::BoxFuture<Result<Response<Full<Bytes>>>> {
        let next = Arc::clone(&self.next);
        let fail_count = Arc::clone(&self.fail_count);
        let threshold = self.threshold;
        Box::pin(async move {
            match AssertUnwindSafe(next.round_trip(req)).catch_unwind().await {
                Ok(result) => result,
                Err(_) => {
                    let count = fail_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if threshold > 0 && count >= threshold {
                        log::error!(
                            "middleware recovery reached fail count threshold {}",
                            threshold
                        );
                    }
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Full::new(Bytes::from("internal server error")))
                        .unwrap())
                }
            }
        })
    }
}
