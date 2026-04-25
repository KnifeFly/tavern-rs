use std::path::PathBuf;
use std::process::Command;

use tavern::config::Plugin as PluginConfig;

struct CompiledPlugin {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

#[test]
fn loads_dynamic_plugin_with_c_abi_v1() {
    let plugin = compile_cdylib("success", format!("{ABI_PREAMBLE}{SUCCESS_PLUGIN}"));
    let cfg = PluginConfig {
        name: "dynamic".to_string(),
        library: Some(plugin.path.to_string_lossy().to_string()),
        options: Default::default(),
    };

    let instance = tavern::plugin::create(&cfg).expect("load plugin");

    assert_eq!(instance.name(), "ffi-plugin");
    instance.start().expect("start");
    instance.stop().expect("stop");
}

#[test]
fn rejects_dynamic_plugin_missing_create_symbol() {
    let plugin = compile_cdylib("missing_symbol", MISSING_SYMBOL_PLUGIN);
    let cfg = PluginConfig {
        name: "dynamic".to_string(),
        library: Some(plugin.path.to_string_lossy().to_string()),
        options: Default::default(),
    };

    let err = match tavern::plugin::create(&cfg) {
        Ok(plugin) => panic!("expected missing symbol error, got {}", plugin.name()),
        Err(err) => err,
    };

    assert!(err.to_string().contains("tavern_plugin_create_v1"));
}

#[test]
fn rejects_dynamic_plugin_abi_mismatch() {
    let plugin = compile_cdylib(
        "abi_mismatch",
        format!("{ABI_PREAMBLE}{ABI_MISMATCH_PLUGIN}"),
    );
    let cfg = PluginConfig {
        name: "dynamic".to_string(),
        library: Some(plugin.path.to_string_lossy().to_string()),
        options: Default::default(),
    };

    let err = match tavern::plugin::create(&cfg) {
        Ok(plugin) => panic!("expected ABI mismatch error, got {}", plugin.name()),
        Err(err) => err,
    };

    assert!(err.to_string().contains("ABI version"));
}

fn compile_cdylib(name: &str, source: impl AsRef<str>) -> CompiledPlugin {
    let dir = tempfile::tempdir().expect("tempdir");
    let source_path = dir.path().join(format!("{name}.rs"));
    let lib_path = dir.path().join(dylib_filename(name));
    std::fs::write(&source_path, source.as_ref()).expect("write plugin source");
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let output = Command::new(rustc)
        .arg("--edition=2021")
        .arg("--crate-type")
        .arg("cdylib")
        .arg(&source_path)
        .arg("-o")
        .arg(&lib_path)
        .output()
        .expect("run rustc");
    assert!(
        output.status.success(),
        "rustc failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    CompiledPlugin {
        _dir: dir,
        path: lib_path,
    }
}

fn dylib_filename(name: &str) -> String {
    if cfg!(target_os = "macos") {
        format!("lib{name}.dylib")
    } else if cfg!(target_os = "windows") {
        format!("{name}.dll")
    } else {
        format!("lib{name}.so")
    }
}

const ABI_PREAMBLE: &str = r##"
use std::ffi::CString;
use std::os::raw::{c_char, c_void};

#[repr(C)]
pub struct TavernPluginApiV1 {
    pub abi_version: u32,
    pub plugin: *mut c_void,
    pub name: Option<unsafe extern "C" fn(*mut c_void) -> *const c_char>,
    pub start: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    pub stop: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    pub handle_request: Option<unsafe extern "C" fn(*mut c_void, *const c_char, *mut *mut c_char) -> i32>,
    pub free_string: Option<unsafe extern "C" fn(*mut c_char)>,
    pub destroy: Option<unsafe extern "C" fn(*mut c_void)>,
}

static NAME: &[u8] = b"ffi-plugin\0";

unsafe extern "C" fn plugin_name(_plugin: *mut c_void) -> *const c_char {
    NAME.as_ptr() as *const c_char
}

unsafe extern "C" fn ok(_plugin: *mut c_void) -> i32 {
    0
}

unsafe extern "C" fn handle(_plugin: *mut c_void, _request: *const c_char, out: *mut *mut c_char) -> i32 {
    if out.is_null() {
        return -1;
    }
    let response = r#"{"status":202,"headers":{"X-Dynamic-Plugin":"ok"},"body":"accepted"}"#;
    *out = CString::new(response).unwrap().into_raw();
    1
}

unsafe extern "C" fn free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        let _ = CString::from_raw(ptr);
    }
}

unsafe extern "C" fn destroy(_plugin: *mut c_void) {}
"##;

const SUCCESS_PLUGIN: &str = r#"
#[no_mangle]
pub unsafe extern "C" fn tavern_plugin_create_v1(_config: *const c_char, out: *mut TavernPluginApiV1) -> i32 {
    if out.is_null() {
        return -1;
    }
    *out = TavernPluginApiV1 {
        abi_version: 1,
        plugin: std::ptr::null_mut(),
        name: Some(plugin_name),
        start: Some(ok),
        stop: Some(ok),
        handle_request: Some(handle),
        free_string: Some(free_string),
        destroy: Some(destroy),
    };
    0
}
"#;

const ABI_MISMATCH_PLUGIN: &str = r#"
#[no_mangle]
pub unsafe extern "C" fn tavern_plugin_create_v1(_config: *const c_char, out: *mut TavernPluginApiV1) -> i32 {
    if out.is_null() {
        return -1;
    }
    *out = TavernPluginApiV1 {
        abi_version: 999,
        plugin: std::ptr::null_mut(),
        name: Some(plugin_name),
        start: Some(ok),
        stop: Some(ok),
        handle_request: Some(handle),
        free_string: Some(free_string),
        destroy: Some(destroy),
    };
    0
}
"#;

const MISSING_SYMBOL_PLUGIN: &str = r#"
#[no_mangle]
pub unsafe extern "C" fn not_the_create_symbol() -> i32 {
    0
}
"#;
