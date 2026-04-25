mod support;

use bytes::Bytes;
use http::HeaderMap;
use http::{Method, StatusCode};
use http_body_util::{BodyExt, Full};
use std::sync::Arc;
use support::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
    assert!(resp
        .headers
        .get("X-Cache")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("MISS"));

    // GET hit
    let case2 = E2E::new("http://example.com.gslb.com/cache-method/get", wrong_hit()).await;
    let resp = case2
        .do_request(|method, _headers| {
            *method = Method::GET;
        })
        .await;

    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body.len(), file.size);
    assert_eq!(resp.headers.get("ETag").unwrap(), file.md5.as_str());
    assert!(resp
        .headers
        .get("X-Cache")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("HIT"));

    // HEAD hit
    let case3 = E2E::new("http://example.com.gslb.com/cache-method/get", wrong_hit()).await;
    let resp = case3
        .do_request(|method, _headers| {
            *method = Method::HEAD;
        })
        .await;

    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.headers.get("ETag").unwrap(), file.md5.as_str());
    assert!(resp
        .headers
        .get("X-Cache")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("HIT"));

    let purge_resp = case3.purge().await;
    assert!(purge_resp.status == StatusCode::OK || purge_resp.status == StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_non_get_bypasses_cache_and_forwards_body() {
    let case_url = "http://example.com.gslb.com/cache-method/post-bypass";
    let seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let seen_handler = Arc::clone(&seen);
    let case = E2E::new_async(case_url, move |req| {
        let seen = Arc::clone(&seen_handler);
        async move {
            let method = req.method().clone();
            let body = req
                .into_body()
                .collect()
                .await
                .expect("upstream body")
                .to_bytes();
            seen.lock().await.push((method, body.clone()));
            http::Response::builder()
                .status(StatusCode::CREATED)
                .header("Cache-Control", "max-age=60")
                .header("Content-Length", body.len().to_string())
                .body(Full::new(body))
                .unwrap()
        }
    })
    .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        tavern::constants::INTERNAL_UPSTREAM_ADDR,
        case.upstream.addr().to_string().parse().unwrap(),
    );
    headers.insert("X-Store-Url", case_url.parse().unwrap());
    let resp = case
        .client
        .send_body(
            Method::POST,
            case_url,
            headers,
            Bytes::from_static(b"forward-me"),
        )
        .await;

    assert_eq!(resp.status, StatusCode::CREATED);
    assert_eq!(resp.body, Bytes::from_static(b"forward-me"));
    assert_eq!(
        resp.headers
            .get(tavern::constants::PROTOCOL_CACHE_STATUS_KEY)
            .and_then(|v| v.to_str().ok()),
        Some("BYPASS")
    );
    let seen = seen.lock().await;
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, Method::POST);
    assert_eq!(seen[0].1, Bytes::from_static(b"forward-me"));
    drop(seen);

    let miss_case = E2E::new(case_url, wrong_hit()).await;
    let resp = miss_case
        .do_request(|method, _headers| {
            *method = Method::GET;
        })
        .await;
    assert_eq!(resp.status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn test_bypass_response_streams_before_upstream_body_completes() {
    ensure_server().await;
    let upstream = start_delayed_raw_upstream("max-age=60").await;
    let mut stream = tokio::net::TcpStream::connect("127.0.0.1:18080")
        .await
        .expect("connect proxy");
    let request = format!(
        "POST http://stream.example.test/bypass HTTP/1.1\r\n\
         Host: stream.example.test\r\n\
         {}: {}\r\n\
         Content-Length: 4\r\n\
         Connection: close\r\n\
         \r\n\
         ping",
        tavern::constants::INTERNAL_UPSTREAM_ADDR,
        upstream
    );
    stream.write_all(request.as_bytes()).await.expect("write");

    let mut buf = vec![0u8; 1024];
    let n = tokio::time::timeout(std::time::Duration::from_millis(250), stream.read(&mut buf))
        .await
        .expect("stream did not produce first bytes")
        .expect("read");
    let first = String::from_utf8_lossy(&buf[..n]);
    assert!(first.contains("200 OK"));
    assert!(first.contains("hello"));
    assert!(!first.contains("world"));
}

#[tokio::test]
async fn test_uncacheable_get_response_streams_before_upstream_body_completes() {
    ensure_server().await;
    let upstream = start_delayed_raw_upstream("no-store").await;
    let mut stream = tokio::net::TcpStream::connect("127.0.0.1:18080")
        .await
        .expect("connect proxy");
    let request = format!(
        "GET http://stream.example.test/no-store HTTP/1.1\r\n\
         Host: stream.example.test\r\n\
         {}: {}\r\n\
         Connection: close\r\n\
         \r\n",
        tavern::constants::INTERNAL_UPSTREAM_ADDR,
        upstream
    );
    stream.write_all(request.as_bytes()).await.expect("write");

    let mut buf = vec![0u8; 1024];
    let n = tokio::time::timeout(std::time::Duration::from_millis(250), stream.read(&mut buf))
        .await
        .expect("stream did not produce first bytes")
        .expect("read");
    let first = String::from_utf8_lossy(&buf[..n]);
    assert!(first.contains("200 OK"));
    assert!(first.contains("hello"));
    assert!(!first.contains("world"));
}

async fn start_delayed_raw_upstream(cache_control: &'static str) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nCache-Control: {cache_control}\r\n\r\nhello"
                )
                .as_bytes(),
            )
            .await
            .expect("write first chunk");
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
        let _ = socket.write_all(b"world").await;
    });
    addr
}
