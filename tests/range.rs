mod support;

use bytes::Bytes;
use http::StatusCode;
use support::*;

fn range_header(start: u64, end: Option<u64>) -> String {
    match end {
        Some(end) => format!("bytes={}-{}", start, end),
        None => format!("bytes={}-", start),
    }
}

#[tokio::test]
async fn test_range_offset() {
    let file = gen_file(2 << 20);

    // MISS
    let case1 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/normal/1-1",
        resp_simple_file(&file),
    )
    .await;
    let resp = case1
        .do_request(|_, headers| {
            headers.insert("Range", range_header(0, Some(524_287)).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body().len(), 524_288);
    assert!(
        resp.headers()
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("MISS")
    );

    // HIT
    let case2 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/normal/1-1",
        wrong_hit(),
    )
    .await;
    let resp = case2
        .do_request(|_, headers| {
            headers.insert("Range", range_header(0, Some(524_287)).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body().len(), 524_288);
    assert!(
        resp.headers()
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("HIT")
    );

    // PART_MISS
    let case3 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/normal/1-1",
        resp_simple_file(&file),
    )
    .await;
    let resp = case3
        .do_request(|_, headers| {
            headers.insert(
                "Range",
                range_header(524_288, Some(1_048_575)).parse().unwrap(),
            );
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body().len(), 524_288);
    assert!(
        resp.headers()
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("PART_MISS")
    );

    // PART_HIT
    let case4 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/normal/1-1",
        resp_simple_file(&file),
    )
    .await;
    let resp = case4
        .do_request(|_, headers| {
            headers.insert(
                "Range",
                range_header(524_288, Some(1_572_863)).parse().unwrap(),
            );
        })
        .await;
    let hash_body = hash_bytes(resp.body());
    let hash_file = hash_bytes(&read_range(&file.path, 524_288, 1_048_576));
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert!(
        resp.headers()
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("PART_HIT")
    );
    assert_eq!(hash_body, hash_file);

    let purge_resp = case4.purge().await;
    assert!(
        purge_resp.status() == StatusCode::OK || purge_resp.status() == StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn test_range_overflow() {
    let file = gen_file(5 << 20);

    let case1 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/overflow/1-1",
        resp_simple_file(&file),
    )
    .await;
    let resp = case1
        .do_request(|_, headers| {
            headers.insert("Range", range_header(0, Some(524_287)).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body().len(), 524_288);

    let case2 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/overflow/1-1",
        resp_simple_file(&file),
    )
    .await;
    let resp = case2
        .do_request(|_, headers| {
            headers.insert("Range", range_header(5_242_880, None).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);

    let case3 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/overflow/1-1",
        resp_simple_file(&file),
    )
    .await;
    let resp = case3
        .do_request(|_, headers| {
            headers.insert("Range", range_header(5_242_878, None).parse().unwrap());
        })
        .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body().len(), 2);

    let purge_resp = case3.purge().await;
    assert!(
        purge_resp.status() == StatusCode::OK || purge_resp.status() == StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn test_range_revalidate() {
    let mut file = gen_file(5 << 20);
    let file_md5 = file.md5.clone();

    // initial fill
    let case1 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/full-cache/1-1",
        resp_callback_file(&file, move |_req, headers| {
            headers.insert("Cache-Control", "max-age=2".parse().unwrap());
            headers.insert("ETag", file_md5.parse().unwrap());
        }),
    )
    .await;
    let resp = case1.do_request(|_, _| {}).await;
    assert!(
        resp.headers()
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("MISS")
    );

    // HIT
    let case2 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/full-cache/1-1",
        wrong_hit(),
    )
    .await;
    let resp = case2
        .do_request(|_, headers| {
            headers.insert("Range", range_header(0, Some(524_287)).parse().unwrap());
        })
        .await;
    assert!(
        resp.headers()
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("HIT")
    );

    // Revalidate HIT (304)
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let file_md5 = file.md5.clone();
    let case3 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/full-cache/1-1",
        move |req| {
            let et = req.headers().get("If-None-Match").unwrap().to_str().unwrap();
            assert_eq!(et, file_md5);
            let lm = req.headers().get("If-Modified-Since");
            assert!(lm.is_some());
            let mut headers = http::HeaderMap::new();
            headers.insert("Cache-Control", "max-age=2".parse().unwrap());
            headers.insert("ETag", file_md5.parse().unwrap());
            http::Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .body(http_body_util::Full::new(Bytes::new()))
                .unwrap()
        },
    )
    .await;
    let resp = case3
        .do_request(|_, headers| {
            headers.insert("Range", range_header(0, Some(524_287)).parse().unwrap());
        })
        .await;
    assert!(
        resp.headers()
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("REVALIDATE_HIT")
    );

    // Revalidate MISS (file changed)
    file = gen_file(5 << 20);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let new_md5 = file.md5.clone();
    let file_path = file.path.clone();
    let case4 = E2E::new(
        "http://sendya.me.gslb.com/cases/range/full-cache/1-1",
        move |req| {
            let et = req.headers().get("If-None-Match").unwrap().to_str().unwrap();
            assert_ne!(et, new_md5);
            let mut headers = http::HeaderMap::new();
            headers.insert("Cache-Control", "max-age=2".parse().unwrap());
            headers.insert("ETag", new_md5.parse().unwrap());
            let body = Bytes::from(std::fs::read(&file_path).unwrap());
            http::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Length", body.len().to_string())
                .body(http_body_util::Full::new(body))
                .unwrap()
        },
    )
    .await;
    let resp = case4
        .do_request(|_, headers| {
            headers.insert("Range", range_header(0, Some(524_287)).parse().unwrap());
        })
        .await;
    assert!(
        resp.headers()
            .get("X-Cache")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("REVALIDATE_MISS")
    );

    let purge_resp = case4.purge().await;
    assert!(
        purge_resp.status() == StatusCode::OK || purge_resp.status() == StatusCode::NOT_FOUND
    );
}
