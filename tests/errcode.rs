mod support;

use bytes::Bytes;
use http::StatusCode;
use support::*;
use tavern::constants;

#[tokio::test]
async fn test_errcode_cache_behavior() {
    // no cache
    let case1 = E2E::new(
        "http://example.com.gslb.com/errcode/no-cache",
        |_req| {
            let body = Bytes::from_static(b"Bad Gateway");
            let mut headers = http::HeaderMap::new();
            headers.insert("Content-Length", body.len().to_string().parse().unwrap());
            headers.insert("Content-Type", "text/plain".parse().unwrap());
            let mut builder = http::Response::builder().status(StatusCode::BAD_GATEWAY);
            for (k, v) in headers.iter() {
                builder = builder.header(k, v);
            }
            builder.body(http_body_util::Full::new(body)).unwrap()
        },
    )
    .await;
    let resp = case1.do_request(|_, _| {}).await;
    assert_eq!(resp.status, StatusCode::BAD_GATEWAY);

    // cache errcode when enabled
    let case2 = E2E::new(
        "http://example.com.gslb.com/errcode/cache",
        |_req| {
            let body = Bytes::from_static(b"Bad Gateway");
            let mut headers = http::HeaderMap::new();
            headers.insert(constants::CACHE_TIME, "30".parse().unwrap());
            headers.insert(constants::INTERNAL_CACHE_ERR_CODE, "1".parse().unwrap());
            headers.insert("Content-Length", body.len().to_string().parse().unwrap());
            headers.insert("Content-Type", "text/plain".parse().unwrap());
            let mut builder = http::Response::builder().status(StatusCode::BAD_GATEWAY);
            for (k, v) in headers.iter() {
                builder = builder.header(k, v);
            }
            builder.body(http_body_util::Full::new(body)).unwrap()
        },
    )
    .await;
    let resp = case2.do_request(|_, _| {}).await;
    assert_eq!(resp.status, StatusCode::BAD_GATEWAY);

    let case3 = E2E::new(
        "http://example.com.gslb.com/errcode/cache",
        wrong_hit(),
    )
    .await;
    let resp = case3.do_request(|_, _| {}).await;
    assert!(
        resp.headers
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("HIT")
    );

    let purge_resp = case3.purge().await;
    assert!(
        purge_resp.status == StatusCode::OK || purge_resp.status == StatusCode::NOT_FOUND
    );
}
