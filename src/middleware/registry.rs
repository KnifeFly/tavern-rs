use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{anyhow, Result};

use crate::config::MiddlewareConfig;
use crate::middleware::{empty_cleanup, empty_middleware, Cleanup, Middleware};

pub type Factory = fn(&MiddlewareConfig) -> Result<(Middleware, Cleanup)>;

fn registry() -> &'static Mutex<HashMap<String, Factory>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, Factory>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register(name: &str, factory: Factory) {
    let mut map = registry().lock().expect("middleware registry");
    map.insert(format!("tavern.middleware.{name}"), factory);
}

pub fn create(cfg: &MiddlewareConfig) -> Result<(Middleware, Cleanup)> {
    let name = format!("tavern.middleware.{}", cfg.name.to_lowercase());
    let map = registry().lock().expect("middleware registry");
    let factory = map.get(&name).ok_or_else(|| anyhow!("middleware not found"))?;
    let (mw, cleanup) = factory(cfg)?;
    Ok((mw, cleanup))
}

pub fn create_or_empty(cfg: &MiddlewareConfig) -> (Middleware, Cleanup) {
    match create(cfg) {
        Ok(val) => val,
        Err(_) => (Arc::new(empty_middleware), empty_cleanup),
    }
}
