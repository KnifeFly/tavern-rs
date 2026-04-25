use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use http::{Request, Response};
use hyper::body::Incoming;

use crate::body::ResponseBody;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub trait RoundTripper: Send + Sync {
    fn round_trip(&self, req: Request<Incoming>) -> BoxFuture<Result<Response<ResponseBody>>>;
}

pub struct RoundTripperFn<F>(pub F);

impl<F> RoundTripper for RoundTripperFn<F>
where
    F: Fn(Request<Incoming>) -> BoxFuture<Result<Response<ResponseBody>>> + Send + Sync,
{
    fn round_trip(&self, req: Request<Incoming>) -> BoxFuture<Result<Response<ResponseBody>>> {
        (self.0)(req)
    }
}

pub type Middleware = Arc<dyn Fn(Arc<dyn RoundTripper>) -> Arc<dyn RoundTripper> + Send + Sync>;
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

pub mod caching;
pub mod multirange;
pub mod recovery;
pub mod registry;
pub mod rewrite;
