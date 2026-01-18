mod support;

use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use support::*;

fn resp_range_file(file: &MockFile) -> impl Fn(http::Request<Incoming>) -> http::Response<Full<Bytes>> + Send + Sync {
    let file = file.clone();
    move |req: http::Request<Incoming>| {
        let bytes = std::fs::read(&file.path).expect("read file");
        let size = bytes.len() as u64;
        let mut headers = HeaderMap::new();
        headers.insert("Cache-Control", "max-age=10".parse().unwrap());
        headers.insert("Content-MD5", file.md5.parse().unwrap());
        headers.insert("ETag", file.md5.parse().unwrap());
        headers.insert(
            "Last-Modified",
            httpdate::fmt_http_date(std::time::SystemTime::now())
                .parse()
                .unwrap(),
        );
        let range = req.headers().get("Range").and_then(|v| v.to_str().ok());
        if let Some(raw) = range.and_then(|v| v.strip_prefix("bytes=")) {
            let parts: Vec<&str> = raw.splitn(2, '-').collect();
            let start = parts.get(0).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
            let end = parts
                .get(1)
                .and_then(|v| if v.is_empty() { None } else { v.parse::<u64>().ok() })
                .unwrap_or_else(|| size.saturating_sub(1));
            if start >= size || end < start {
                let mut builder = http::Response::builder().status(StatusCode::RANGE_NOT_SATISFIABLE);
                builder = builder.header("Content-Range", format!("bytes */{size}"));
                return builder.body(Full::new(Bytes::new())).unwrap();
            }
            let end = end.min(size.saturating_sub(1));
            let slice = Bytes::from(bytes[start as usize..=end as usize].to_vec());
            let mut builder = http::Response::builder().status(StatusCode::PARTIAL_CONTENT);
            builder = builder.header("Content-Range", format!("bytes {start}-{end}/{size}"));
            builder = builder.header("Content-Length", slice.len().to_string());
            for (k, v) in headers.iter() {
                builder = builder.header(k, v);
            }
            return builder.body(Full::new(slice)).unwrap();
        }

        let mut builder = http::Response::builder().status(StatusCode::OK);
        builder = builder.header("Content-Length", bytes.len().to_string());
        for (k, v) in headers.iter() {
            builder = builder.header(k, v);
        }
        builder.body(Full::new(Bytes::from(bytes))).unwrap()
    }
}

#[tokio::test]
async fn test_prefetch_range_fills_cache() {
    let file = gen_file(128 * 1024);
    let case_url = "http://sendya.me.gslb.com/cases/prefetch/file1";

    let case1 = E2E::new(case_url, resp_range_file(&file)).await;
    let resp = case1
        .do_request(|_, headers| {
            headers.insert("Range", "bytes=0-1023".parse().unwrap());
            headers.insert("X-Prefetch", "1".parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body().len(), 1024);

    let case2 = E2E::new(case_url, wrong_hit()).await;
    let resp = case2
        .do_request(|_, headers| {
            headers.insert("Range", "bytes=4096-8191".parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let expected = read_range(&file.path, 4096, 4096);
    assert_eq!(hash_bytes(resp.body()), hash_bytes(&expected));

    let purge_resp = case2.purge().await;
    assert!(
        purge_resp.status() == StatusCode::OK || purge_resp.status() == StatusCode::NOT_FOUND
    );
}
