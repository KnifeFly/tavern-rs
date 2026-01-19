use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use serde::Deserialize;
use serde::Serialize;

use crate::middleware::caching;
use crate::server::AppState;
use crate::storage;

#[derive(Debug, Deserialize)]
struct PushRequest {
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    urls: Vec<String>,
    #[serde(default)]
    dir: Option<bool>,
    #[serde(default)]
    mark_expired: Option<bool>,
    #[serde(default)]
    delete: Option<bool>,
}

#[derive(Debug, Serialize)]
struct PushResponse {
    action: String,
    success: usize,
    failed: usize,
    results: Vec<PushResult>,
}

#[derive(Debug, Serialize)]
struct PushResult {
    url: String,
    ok: bool,
    message: String,
}

#[derive(Clone, Copy, Debug)]
enum PushAction {
    Prefetch,
    Expire,
    Delete,
}

impl PushAction {
    fn as_str(self) -> &'static str {
        match self {
            PushAction::Prefetch => "prefetch",
            PushAction::Expire => "expire",
            PushAction::Delete => "delete",
        }
    }
}

pub(crate) async fn handle(req: Request<Incoming>, state: &AppState) -> Response<Full<Bytes>> {
    if req.method() != Method::POST {
        return text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    let (parts, body) = req.into_parts();
    let body_bytes = match body.collect().await {
        Ok(data) => data.to_bytes(),
        Err(_) => return text_response(StatusCode::BAD_REQUEST, "invalid body"),
    };
    let push = if body_bytes.is_empty() {
        parse_query(&parts.uri)
    } else {
        serde_json::from_slice::<PushRequest>(&body_bytes).unwrap_or_else(|_| parse_query(&parts.uri))
    };
    let Some(action) = resolve_action(&push) else {
        return text_response(StatusCode::BAD_REQUEST, "missing action");
    };
    let urls = resolve_urls(&push);
    if urls.is_empty() {
        return text_response(StatusCode::BAD_REQUEST, "missing urls");
    }
    let dir = push.dir.unwrap_or(false);
    let mut results = Vec::new();
    for url in urls {
        let result = match action {
            PushAction::Prefetch => {
                match caching::prefetch_url(&state.cache_state, &url).await {
                    Ok(()) => PushResult {
                        url,
                        ok: true,
                        message: "prefetch ok".to_string(),
                    },
                    Err(err) => PushResult {
                        url,
                        ok: false,
                        message: err.to_string(),
                    },
                }
            }
            PushAction::Expire => {
                let res = storage::current().purge(
                    &url,
                    storage::PurgeControl {
                        hard: false,
                        dir,
                        mark_expired: true,
                    },
                );
                match res {
                    Ok(()) => PushResult {
                        url,
                        ok: true,
                        message: "expired".to_string(),
                    },
                    Err(err) => PushResult {
                        url,
                        ok: false,
                        message: err.to_string(),
                    },
                }
            }
            PushAction::Delete => {
                let res = storage::current().purge(
                    &url,
                    storage::PurgeControl {
                        hard: true,
                        dir,
                        mark_expired: false,
                    },
                );
                match res {
                    Ok(()) => PushResult {
                        url,
                        ok: true,
                        message: "deleted".to_string(),
                    },
                    Err(err) => PushResult {
                        url,
                        ok: false,
                        message: err.to_string(),
                    },
                }
            }
        };
        results.push(result);
    }
    let success = results.iter().filter(|r| r.ok).count();
    let failed = results.len() - success;
    json_response(
        StatusCode::OK,
        &PushResponse {
            action: action.as_str().to_string(),
            success,
            failed,
            results,
        },
    )
}

fn parse_query(uri: &http::Uri) -> PushRequest {
    let mut action = None;
    let mut url = None;
    let mut dir = None;
    if let Some(query) = uri.query() {
        for pair in query.split('&') {
            let mut it = pair.splitn(2, '=');
            let key = it.next().unwrap_or("").trim();
            let val = it.next().unwrap_or("").trim();
            match key {
                "action" => {
                    if !val.is_empty() {
                        action = Some(val.to_string());
                    }
                }
                "url" => {
                    if !val.is_empty() {
                        url = Some(val.to_string());
                    }
                }
                "dir" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        dir = Some(parsed);
                    }
                }
                _ => {}
            }
        }
    }
    PushRequest {
        action,
        url,
        urls: Vec::new(),
        dir,
        mark_expired: None,
        delete: None,
    }
}

fn resolve_action(req: &PushRequest) -> Option<PushAction> {
    if req.mark_expired == Some(true) {
        return Some(PushAction::Expire);
    }
    if req.delete == Some(true) {
        return Some(PushAction::Delete);
    }
    match req.action.as_deref().unwrap_or("prefetch") {
        "prefetch" | "push" => Some(PushAction::Prefetch),
        "expire" | "expired" => Some(PushAction::Expire),
        "delete" | "purge" => Some(PushAction::Delete),
        _ => None,
    }
}

fn resolve_urls(req: &PushRequest) -> Vec<String> {
    let mut urls = Vec::new();
    if let Some(url) = &req.url {
        if !url.trim().is_empty() {
            urls.push(url.trim().to_string());
        }
    }
    for url in &req.urls {
        if !url.trim().is_empty() {
            urls.push(url.trim().to_string());
        }
    }
    urls
}

fn text_response(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "text/plain; charset=utf-8")
        .header("Content-Length", msg.len().to_string())
        .body(Full::new(Bytes::from(msg.to_string())))
        .unwrap()
}

fn json_response<T: Serialize>(status: StatusCode, body: &T) -> Response<Full<Bytes>> {
    let payload = serde_json::to_vec(body).unwrap_or_default();
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Content-Length", payload.len().to_string())
        .body(Full::new(Bytes::from(payload)))
        .unwrap()
}
