mod support;

use http::{Method, StatusCode};
use support::*;

#[tokio::test]
async fn test_cache_method_allow_get_or_head() {
    let file = gen_file(1 << 20);

    // GET miss
    let case1 = E2E::new(
        "http://example.com.gslb.com/cache-method/get",
        resp_simple_file(&file),
    )
    .await;
    let resp = case1
        .do_request(|method, _headers| {
            *method = Method::GET;
        })
        .await;

    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body.len(), file.size);
    assert_eq!(resp.headers.get("ETag").unwrap(), file.md5.as_str());
    assert!(
        resp.headers
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("MISS")
    );

    // GET hit
    let case2 = E2E::new(
        "http://example.com.gslb.com/cache-method/get",
        wrong_hit(),
    )
    .await;
    let resp = case2
        .do_request(|method, _headers| {
            *method = Method::GET;
        })
        .await;

    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body.len(), file.size);
    assert_eq!(resp.headers.get("ETag").unwrap(), file.md5.as_str());
    assert!(
        resp.headers
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("HIT")
    );

    // HEAD hit
    let case3 = E2E::new(
        "http://example.com.gslb.com/cache-method/get",
        wrong_hit(),
    )
    .await;
    let resp = case3
        .do_request(|method, _headers| {
            *method = Method::HEAD;
        })
        .await;

    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.headers.get("ETag").unwrap(), file.md5.as_str());
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
