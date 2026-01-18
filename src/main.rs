use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clap::Parser;

use tavern::config;
use tavern::logging;
use tavern::server;

#[derive(Parser, Debug)]
#[command(name = "tavern", about = "Rust rewrite of Tavern", version)]
struct Cli {
    /// Config file path
    #[arg(short = 'c', default_value = "config.yaml")]
    config: PathBuf,

    /// Enable verbose logging
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let (mut cfg, ignored) = config::load(&cli.config)?;

    if cfg.hostname.is_none() {
        cfg.hostname = std::env::var("HOSTNAME").ok();
    }

    logging::init(&cfg.logger, cli.verbose)?;

    if cfg.strict && !ignored.is_empty() {
        return Err(anyhow!("unknown config fields: {}", ignored.join(", ")));
    }

    if !ignored.is_empty() {
        log::warn!("ignoring unknown config fields: {}", ignored.join(", "));
    }

    if let Some(pidfile) = &cfg.pidfile {
        write_pid(pidfile)?;
    }

    cfg.validate()?;

    log::info!("tavern starting with config {}", cli.config.display());

    server::run(Arc::new(cfg)).await
}

fn write_pid(path: &str) -> Result<()> {
    let pid = std::process::id();
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create pid dir {}", parent.display()))?;
    }
    std::fs::write(&path, pid.to_string()).with_context(|| format!("write pid file {}", path.display()))?;
    Ok(())
}
