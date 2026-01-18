mod support;

use http::StatusCode;
use support::*;

fn range_header(start: u64, end: Option<u64>) -> String {
    match end {
        Some(end) => format!("bytes={}-{}", start, end),
        None => format!("bytes={}-", start),
    }
}

fn content_range(start: u64, end: u64, size: u64) -> String {
    format!("bytes {}-{}/{}", start, end, size)
}

#[tokio::test]
async fn test_content_length_changed() {
    let f1 = gen_file(1 << 20);
    let f2 = gen_file(2 << 20);

    // old file
    let case1 = E2E::new(
        "http://sendya.me.gslb.com/cases/cl/file1.apk",
        resp_callback_file(&f1, |_req, headers| {
            headers.insert("X-Case", "old-file".parse().unwrap());
        }),
    )
    .await;
    let resp = case1
        .do_request(|_, headers| {
            headers.insert("Range", range_header(1, Some(400_000)).parse().unwrap());
        })
        .await;
    let body_hash = hash_bytes(resp.body());
    let expected = read_range(&f1.path, 1, 400_000);
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get("Content-Range").unwrap(),
        content_range(1, 400_000, f1.size as u64).as_str()
    );
    assert_eq!(resp.headers().get("X-Case").unwrap(), "old-file");
    assert_eq!(resp.headers().get("ETag").unwrap(), f1.md5.as_str());
    assert_eq!(body_hash, hash_bytes(&expected));

    // new file, should still see old data
    let case2 = E2E::new(
        "http://sendya.me.gslb.com/cases/cl/file1.apk",
        resp_callback_file(&f2, |_req, headers| {
            headers.insert("X-Case", "new-file".parse().unwrap());
        }),
    )
    .await;
    let resp = case2
        .do_request(|_, headers| {
            headers.insert("Range", range_header(600_000, None).parse().unwrap());
        })
        .await;
    let body_hash = hash_bytes(resp.body());
    let expected = read_range(&f1.path, 600_000, f1.size - 600_000);
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers().get("X-Case").unwrap(), "old-file");
    assert_eq!(resp.headers().get("ETag").unwrap(), f1.md5.as_str());
    assert_eq!(body_hash, hash_bytes(&expected));

    // new file should be served after discard
    let case3 = E2E::new(
        "http://sendya.me.gslb.com/cases/cl/file1.apk",
        resp_callback_file(&f2, |_req, headers| {
            headers.insert("X-Case", "new-file".parse().unwrap());
        }),
    )
    .await;
    let resp = case3
        .do_request(|_, headers| {
            headers.insert("Range", range_header(600_000, None).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers().get("X-Case").unwrap(), "new-file");
    assert_eq!(resp.headers().get("ETag").unwrap(), f2.md5.as_str());

    let purge_resp = case3.purge().await;
    assert!(
        purge_resp.status() == StatusCode::OK || purge_resp.status() == StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn test_last_modified_changed() {
    let f1 = gen_file(1 << 20);
    let f2 = gen_file(1 << 20);

    let old_lm = httpdate::fmt_http_date(std::time::SystemTime::now());
    let new_lm = httpdate::fmt_http_date(
        std::time::SystemTime::now() + std::time::Duration::from_secs(2),
    );
    let old_lm_header = old_lm.clone();
    let new_lm_case2 = new_lm.clone();
    let new_lm_case3 = new_lm.clone();

    let case1 = E2E::new(
        "http://sendya.me.gslb.com/cases/lm/file1.apk",
        resp_callback_file(&f1, move |_req, headers| {
            headers.insert("X-Case", "old-file".parse().unwrap());
            headers.insert("Last-Modified", old_lm_header.parse().unwrap());
        }),
    )
    .await;
    let resp = case1
        .do_request(|_, headers| {
            headers.insert("Range", range_header(0, Some(32_767)).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers().get("X-Case").unwrap(), "old-file");

    let case2 = E2E::new(
        "http://sendya.me.gslb.com/cases/lm/file1.apk",
        resp_callback_file(&f2, move |_req, headers| {
            headers.insert("X-Case", "new-file".parse().unwrap());
            headers.insert("Last-Modified", new_lm_case2.parse().unwrap());
        }),
    )
    .await;
    let resp = case2
        .do_request(|_, headers| {
            headers.insert("Range", range_header(600_000, None).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers().get("X-Case").unwrap(), "old-file");

    let case3 = E2E::new(
        "http://sendya.me.gslb.com/cases/lm/file1.apk",
        resp_callback_file(&f2, move |_req, headers| {
            headers.insert("X-Case", "new-file".parse().unwrap());
            headers.insert("Last-Modified", new_lm_case3.parse().unwrap());
        }),
    )
    .await;
    let resp = case3
        .do_request(|_, headers| {
            headers.insert("Range", range_header(600_000, None).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers().get("X-Case").unwrap(), "new-file");

    let purge_resp = case3.purge().await;
    assert!(
        purge_resp.status() == StatusCode::OK || purge_resp.status() == StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn test_etag_changed() {
    let f1 = gen_file(1 << 20);
    let f2 = gen_file(1 << 20);

    let case1 = E2E::new(
        "http://sendya.me.gslb.com/cases/etag/file1",
        resp_callback_file(&f1, |_req, headers| {
            headers.insert("X-Case", "old-file".parse().unwrap());
        }),
    )
    .await;
    let resp = case1
        .do_request(|_, headers| {
            headers.insert("Range", range_header(1, Some(400_000)).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers().get("X-Case").unwrap(), "old-file");
    assert_eq!(resp.headers().get("ETag").unwrap(), f1.md5.as_str());

    // new file still returns old headers on partial request
    let case2 = E2E::new(
        "http://sendya.me.gslb.com/cases/etag/file1",
        resp_callback_file(&f2, |_req, headers| {
            headers.insert("X-Case", "new-file".parse().unwrap());
        }),
    )
    .await;
    let resp = case2
        .do_request(|_, headers| {
            headers.insert("Range", range_header(600_000, None).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers().get("X-Case").unwrap(), "old-file");
    assert_eq!(resp.headers().get("ETag").unwrap(), f1.md5.as_str());

    // new file after discard
    let case3 = E2E::new(
        "http://sendya.me.gslb.com/cases/etag/file1",
        resp_callback_file(&f2, |_req, headers| {
            headers.insert("X-Case", "new-file".parse().unwrap());
        }),
    )
    .await;
    let resp = case3.do_request(|_, _| {}).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("X-Case").unwrap(), "new-file");
    assert_eq!(resp.headers().get("ETag").unwrap(), f2.md5.as_str());

    let purge_resp = case3.purge().await;
    assert!(
        purge_resp.status() == StatusCode::OK || purge_resp.status() == StatusCode::NOT_FOUND
    );
}
