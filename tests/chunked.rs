use http::Request;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

#[tokio::test]
async fn test_chunked_interrupted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let _ = stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\r\n",
            )
            .await;
        let _ = stream.write_all(b"c\r\npartial-body\r\n").await;
        let _ = stream.shutdown().await;
    });

    let connector = HttpConnector::new();
    let client: Client<_, Full<bytes::Bytes>> =
        Client::builder(TokioExecutor::new()).build(connector);
    let uri: http::Uri = format!("http://{}/", addr).parse().unwrap();
    let req = Request::builder().uri(uri).body(Full::new(bytes::Bytes::new())).unwrap();
    let resp = client.request(req).await.expect("response");
    assert_eq!(resp.status(), http::StatusCode::OK);
    let body = resp.into_body().collect().await;
    assert!(body.is_err());
}
