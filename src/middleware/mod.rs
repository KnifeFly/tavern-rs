use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use hyper::body::Incoming;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub trait RoundTripper: Send + Sync {
    fn round_trip(&self, req: Request<Incoming>) -> BoxFuture<Result<Response<Full<Bytes>>>>;
}

pub struct RoundTripperFn<F>(pub F);

impl<F> RoundTripper for RoundTripperFn<F>
where
    F: Fn(Request<Incoming>) -> BoxFuture<Result<Response<Full<Bytes>>>> + Send + Sync,
{
    fn round_trip(&self, req: Request<Incoming>) -> BoxFuture<Result<Response<Full<Bytes>>>> {
        (self.0)(req)
    }
}

pub type Middleware =
    Arc<dyn Fn(Arc<dyn RoundTripper>) -> Arc<dyn RoundTripper> + Send + Sync>;
pub type Cleanup = fn();

pub fn chain(middlewares: &[Middleware], next: Arc<dyn RoundTripper>) -> Arc<dyn RoundTripper> {
    let mut current = next;
    for mw in middlewares.iter().rev() {
        current = mw(current);
    }
    current
}

pub fn empty_middleware(next: Arc<dyn RoundTripper>) -> Arc<dyn RoundTripper> {
    next
}

pub fn empty_cleanup() {}

pub mod registry;
pub mod caching;
pub mod multirange;
pub mod recovery;
pub mod rewrite;
