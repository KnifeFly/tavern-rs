use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    start_config_watcher(cli.config.clone());

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

fn start_config_watcher(path: PathBuf) {
    #[cfg(unix)]
    {
        use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
        use std::sync::mpsc::channel;

        let dir = path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
        let filename = path.file_name().map(|s| s.to_os_string());
        std::thread::spawn(move || {
            let (tx, rx) = channel();
            let mut watcher = match RecommendedWatcher::new(tx, notify::Config::default()) {
                Ok(watcher) => watcher,
                Err(err) => {
                    log::warn!("config watcher init failed: {err}");
                    return;
                }
            };
            if let Err(err) = watcher.watch(&dir, RecursiveMode::NonRecursive) {
                log::warn!("config watcher start failed: {err}");
                return;
            }
            let mut last = Instant::now() - Duration::from_secs(1);
            for res in rx {
                let event = match res {
                    Ok(event) => event,
                    Err(err) => {
                        log::warn!("config watcher error: {err}");
                        continue;
                    }
                };
                if let Some(name) = filename.as_ref() {
                    if !event.paths.iter().any(|p| p.file_name() == Some(name.as_ref())) {
                        continue;
                    }
                }
                match event.kind {
                    EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_) => {}
                    _ => continue,
                }
                if last.elapsed() < Duration::from_millis(300) {
                    continue;
                }
                last = Instant::now();
                log::info!("config changed, triggering graceful reload");
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::this(),
                    nix::sys::signal::Signal::SIGUSR1,
                );
            }
        });
    }
}
