use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;

pub type ResponseBody = UnsyncBoxBody<Bytes, hyper::Error>;

pub fn boxed_full(body: impl Into<Bytes>) -> ResponseBody {
    Full::new(body.into())
        .map_err(never_to_hyper)
        .boxed_unsync()
}

pub fn boxed_incoming(body: Incoming) -> ResponseBody {
    body.boxed_unsync()
}

pub fn empty_body() -> ResponseBody {
    boxed_full(Bytes::new())
}

fn never_to_hyper(err: Infallible) -> hyper::Error {
    match err {}
}
