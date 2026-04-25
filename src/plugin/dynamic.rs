use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use hyper::body::Incoming;
use libloading::Library;
use serde::Serialize;

use crate::body::{boxed_full, ResponseBody};
use crate::config;
use crate::plugin::Plugin;

const ABI_VERSION: u32 = 1;
const CREATE_SYMBOL: &[u8] = b"tavern_plugin_create_v1\0";

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TavernPluginApiV1 {
    pub abi_version: u32,
    pub plugin: *mut c_void,
    pub name: Option<unsafe extern "C" fn(*mut c_void) -> *const c_char>,
    pub start: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    pub stop: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    pub handle_request:
        Option<unsafe extern "C" fn(*mut c_void, *const c_char, *mut *mut c_char) -> i32>,
    pub free_string: Option<unsafe extern "C" fn(*mut c_char)>,
    pub destroy: Option<unsafe extern "C" fn(*mut c_void)>,
}

impl Default for TavernPluginApiV1 {
    fn default() -> Self {
        Self {
            abi_version: 0,
            plugin: ptr::null_mut(),
            name: None,
            start: None,
            stop: None,
            handle_request: None,
            free_string: None,
            destroy: None,
        }
    }
}

type CreatePluginV1 = unsafe extern "C" fn(*const c_char, *mut TavernPluginApiV1) -> i32;

pub fn load_plugin(cfg: &config::Plugin) -> Result<Arc<dyn Plugin>> {
    let library = cfg
        .library
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("dynamic plugin {} missing library", cfg.name))?;
    let lib = unsafe { Library::new(library) }
        .with_context(|| format!("load dynamic plugin library {library}"))?;
    let mut api = TavernPluginApiV1::default();
    let config_json = serde_json::to_string(&DynamicPluginConfig {
        name: &cfg.name,
        options: &cfg.options,
    })?;
    let config_json = CString::new(config_json).context("dynamic plugin config contains nul")?;
    unsafe {
        let create = lib
            .get::<CreatePluginV1>(CREATE_SYMBOL)
            .with_context(|| format!("load symbol tavern_plugin_create_v1 from {library}"))?;
        let code = create(config_json.as_ptr(), &mut api as *mut TavernPluginApiV1);
        if code != 0 {
            return Err(anyhow!("dynamic plugin {} create failed: {code}", cfg.name));
        }
    }
    if api.abi_version != ABI_VERSION {
        return Err(anyhow!(
            "dynamic plugin {} ABI version {} is unsupported, expected {}",
            cfg.name,
            api.abi_version,
            ABI_VERSION
        ));
    }
    let name = read_plugin_name(&api).unwrap_or_else(|| cfg.name.clone());
    Ok(Arc::new(DynamicPlugin {
        name,
        _library: lib,
        api,
    }))
}

#[derive(Serialize)]
struct DynamicPluginConfig<'a> {
    name: &'a str,
    options: &'a HashMap<String, serde_yaml::Value>,
}

pub struct DynamicPlugin {
    name: String,
    _library: Library,
    api: TavernPluginApiV1,
}

unsafe impl Send for DynamicPlugin {}
unsafe impl Sync for DynamicPlugin {}

impl Plugin for DynamicPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn start(&self) -> Result<()> {
        call_status(self.api.start, self.api.plugin, "start")
    }

    fn stop(&self) -> Result<()> {
        call_status(self.api.stop, self.api.plugin, "stop")
    }

    fn handle_request(&self, req: &Request<Incoming>) -> Option<Response<ResponseBody>> {
        let handle = self.api.handle_request?;
        let request = DynamicRequest::from_request(req);
        let request_json = match serde_json::to_string(&request) {
            Ok(value) => value,
            Err(err) => {
                log::warn!("dynamic plugin {} request encode failed: {err}", self.name);
                return None;
            }
        };
        let request_json = match CString::new(request_json) {
            Ok(value) => value,
            Err(err) => {
                log::warn!("dynamic plugin {} request contains nul: {err}", self.name);
                return None;
            }
        };
        let mut out: *mut c_char = ptr::null_mut();
        let code = unsafe { handle(self.api.plugin, request_json.as_ptr(), &mut out) };
        if code == 0 {
            return None;
        }
        if code < 0 {
            log::warn!("dynamic plugin {} handle_request failed: {code}", self.name);
            return None;
        }
        let raw = unsafe { take_plugin_string(self.api.free_string, out) };
        let raw = match raw {
            Some(value) => value,
            None => {
                log::warn!("dynamic plugin {} returned null response", self.name);
                return None;
            }
        };
        let response: DynamicResponse = match serde_json::from_slice(&raw) {
            Ok(value) => value,
            Err(err) => {
                log::warn!("dynamic plugin {} response decode failed: {err}", self.name);
                return None;
            }
        };
        Some(response.into_http_response())
    }
}

impl Drop for DynamicPlugin {
    fn drop(&mut self) {
        if let Some(destroy) = self.api.destroy {
            unsafe { destroy(self.api.plugin) };
        }
    }
}

#[derive(Serialize)]
struct DynamicRequest {
    method: String,
    uri: String,
    headers: Vec<(String, String)>,
}

impl DynamicRequest {
    fn from_request(req: &Request<Incoming>) -> Self {
        let headers = req
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect();
        Self {
            method: req.method().as_str().to_string(),
            uri: req.uri().to_string(),
            headers,
        }
    }
}

#[derive(serde::Deserialize)]
struct DynamicResponse {
    status: Option<u16>,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    body: String,
    #[serde(default)]
    body_base64: Option<String>,
}

impl DynamicResponse {
    fn into_http_response(self) -> Response<ResponseBody> {
        let status = self
            .status
            .and_then(|status| StatusCode::from_u16(status).ok())
            .unwrap_or(StatusCode::OK);
        let body = self
            .body_base64
            .as_deref()
            .and_then(|raw| {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, raw).ok()
            })
            .map(Bytes::from)
            .unwrap_or_else(|| Bytes::from(self.body));
        let mut builder = Response::builder().status(status);
        let mut has_length = false;
        for (name, value) in self.headers {
            if name.eq_ignore_ascii_case("content-length") {
                has_length = true;
            }
            builder = builder.header(name, value);
        }
        if !has_length {
            builder = builder.header("Content-Length", body.len().to_string());
        }
        builder.body(boxed_full(body)).unwrap()
    }
}

fn call_status(
    func: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    plugin: *mut c_void,
    name: &str,
) -> Result<()> {
    let Some(func) = func else {
        return Ok(());
    };
    let code = unsafe { func(plugin) };
    if code == 0 {
        Ok(())
    } else {
        Err(anyhow!("dynamic plugin {name} failed: {code}"))
    }
}

fn read_plugin_name(api: &TavernPluginApiV1) -> Option<String> {
    let name = api.name?;
    let raw = unsafe { name(api.plugin) };
    if raw.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(raw) }
        .to_str()
        .ok()
        .map(|value| value.to_string())
}

unsafe fn take_plugin_string(
    free_string: Option<unsafe extern "C" fn(*mut c_char)>,
    raw: *mut c_char,
) -> Option<Vec<u8>> {
    if raw.is_null() {
        return None;
    }
    let bytes = CStr::from_ptr(raw).to_bytes().to_vec();
    if let Some(free_string) = free_string {
        free_string(raw);
    }
    Some(bytes)
}
