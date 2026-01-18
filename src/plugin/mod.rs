use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use hyper::body::Incoming;

use crate::config;

pub trait Plugin: Send + Sync {
    fn name(&self) -> &str;
    fn add_router(&self, _router: &mut Router) {}
    fn handle_request(&self, _req: &Request<Incoming>) -> Option<Response<Full<Bytes>>> {
        None
    }
    fn start(&self) -> Result<()> {
        Ok(())
    }
    fn stop(&self) -> Result<()> {
        Ok(())
    }
}

pub type PluginCtor = fn(&config::Plugin) -> Result<Arc<dyn Plugin>>;

fn registry() -> &'static Mutex<HashMap<String, PluginCtor>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, PluginCtor>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register(name: &str, ctor: PluginCtor) {
    let mut map = registry().lock().expect("plugin registry");
    map.insert(name.to_string(), ctor);
}

pub fn create(cfg: &config::Plugin) -> Result<Arc<dyn Plugin>> {
    let map = registry().lock().expect("plugin registry");
    let ctor = map
        .get(&cfg.name)
        .ok_or_else(|| anyhow!("plugin {} not registered", cfg.name))?;
    ctor(cfg)
}

pub fn register_builtin() {
    crate::plugin::purge::register();
    crate::plugin::verifier::register();
    crate::plugin::example::register();
}

pub struct Router {
    routes: HashMap<String, Arc<dyn Fn(Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
        }
    }

    pub fn add<F>(&mut self, path: &str, handler: F)
    where
        F: Fn(Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync + 'static,
    {
        self.routes.insert(path.to_string(), Arc::new(handler));
    }

    pub fn handle(&self, req: Request<Incoming>) -> Option<Response<Full<Bytes>>> {
        let path = req.uri().path().to_string();
        let handler = self.routes.get(&path)?;
        Some(handler(req))
    }

    pub fn handle_path(
        &self,
        path: &str,
        req: Request<Incoming>,
    ) -> Option<Response<Full<Bytes>>> {
        let handler = self.routes.get(path)?;
        Some(handler(req))
    }
}

pub mod example;
pub mod purge;
pub mod verifier;
