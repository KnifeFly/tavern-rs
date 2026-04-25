mod support;

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use support::*;

async fn push_action(client: &TestClient, action: &str, url: &str) -> TestResponse {
    let body = serde_json::json!({
        "action": action,
        "url": url,
    });
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    client
        .send_body(
            Method::POST,
            "http://localhost/cache/push",
            headers,
            Bytes::from(body.to_string()),
        )
        .await
}

#[tokio::test]
async fn test_push_delete_removes_memory_cache() {
    let old_file = gen_file(64 * 1024);
    let new_file = gen_file(64 * 1024);
    let case_url = "http://example.com.gslb.com/push/delete-memory";

    let case1 = E2E::new(case_url, resp_simple_file(&old_file)).await;
    let resp = case1.do_request(|_, _| {}).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("ETag").unwrap(), old_file.md5.as_str());

    let push_resp = push_action(&case1.client, "delete", case_url).await;
    assert_eq!(push_resp.status(), StatusCode::OK);

    let case2 = E2E::new(case_url, resp_simple_file(&new_file)).await;
    let resp = case2.do_request(|_, _| {}).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("ETag").unwrap(), new_file.md5.as_str());
    assert!(resp
        .headers()
        .get("X-Cache")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("MISS"));

    let purge_resp = case2.purge().await;
    assert!(purge_resp.status() == StatusCode::OK || purge_resp.status() == StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_push_expire_marks_memory_cache_stale() {
    let old_file = gen_file(64 * 1024);
    let new_file = gen_file(64 * 1024);
    let case_url = "http://example.com.gslb.com/push/expire-memory";

    let case1 = E2E::new(case_url, resp_simple_file(&old_file)).await;
    let resp = case1.do_request(|_, _| {}).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("ETag").unwrap(), old_file.md5.as_str());

    let push_resp = push_action(&case1.client, "expire", case_url).await;
    assert_eq!(push_resp.status(), StatusCode::OK);

    let case2 = E2E::new(case_url, resp_simple_file(&new_file)).await;
    let resp = case2.do_request(|_, _| {}).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("ETag").unwrap(), new_file.md5.as_str());
    assert!(resp
        .headers()
        .get("X-Cache")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("REVALIDATE_MISS"));

    let purge_resp = case2.purge().await;
    assert!(purge_resp.status() == StatusCode::OK || purge_resp.status() == StatusCode::NOT_FOUND);
}
