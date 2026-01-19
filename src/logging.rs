use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use flexi_logger::{Cleanup, Criterion, Duplicate, FileSpec, Logger as FlexiLogger, Naming, WriteMode};
use log::LevelFilter;
use tokio::task_local;

use crate::config::Logger;

const TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3fZ";

#[derive(Clone, Copy)]
struct FormatConfig {
    include_pid: bool,
    include_caller: bool,
    include_trace_id: bool,
}

static FORMAT_CONFIG: OnceLock<FormatConfig> = OnceLock::new();

task_local! {
    static TRACE_ID: String;
}

pub fn init(config: &Logger, verbose: bool) -> Result<()> {
    let level = if verbose {
        LevelFilter::Debug
    } else {
        parse_level(&config.level)
    };

    FORMAT_CONFIG.get_or_init(|| FormatConfig {
        include_pid: !config.nopid,
        include_caller: config.caller,
        include_trace_id: config.traceid,
    });

    let mut logger = FlexiLogger::try_with_str(level.as_str())
        .context("init logger")?
        .duplicate_to_stderr(Duplicate::Warn)
        .format_for_stdout(log_format);

    if !config.path.trim().is_empty() {
        let spec = FileSpec::try_from(Path::new(&config.path))?;
        let mut file_logger = logger.log_to_file(spec).format(log_format);

        if config.max_size > 0 {
            let naming = Naming::Numbers;
            let cleanup = cleanup_policy(config);
            file_logger = file_logger.rotate(Criterion::Size(config.max_size * 1024 * 1024), naming, cleanup);
        }

        logger = file_logger.write_mode(WriteMode::BufferAndFlush);
    }

    logger.start()?;

    if config.max_age.unwrap_or(0) > 0 && !config.path.trim().is_empty() {
        start_age_cleanup(Path::new(&config.path), config.max_age.unwrap_or(0));
    }
    Ok(())
}

fn parse_level(raw: &str) -> LevelFilter {
    match raw.to_ascii_lowercase().as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "warn" | "warning" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    }
}

fn cleanup_policy(config: &Logger) -> Cleanup {
    if config.max_backups > 0 {
        if config.compress {
            #[cfg(feature = "compress")]
            {
                return Cleanup::KeepCompressedFiles(config.max_backups as usize);
            }
        }
        return Cleanup::KeepLogFiles(config.max_backups as usize);
    }
    Cleanup::Never
}

fn log_format(
    writer: &mut dyn Write,
    now: &mut flexi_logger::DeferredNow,
    record: &log::Record,
) -> std::io::Result<()> {
    let cfg = FORMAT_CONFIG.get().copied().unwrap_or(FormatConfig {
        include_pid: true,
        include_caller: false,
        include_trace_id: false,
    });
    let ts = now.now_utc_owned().format(TIMESTAMP_FORMAT);
    write!(writer, "{} [{}]", ts, record.level())?;
    if cfg.include_pid {
        write!(writer, " pid={}", std::process::id())?;
    }
    if cfg.include_trace_id {
        let trace_id = current_trace_id().unwrap_or_else(|| "-".to_string());
        write!(writer, " trace_id={}", trace_id)?;
    }
    if cfg.include_caller {
        let file = record.file().unwrap_or("-");
        let line = record.line().unwrap_or(0);
        write!(writer, " {}:{}", file, line)?;
    }
    writeln!(writer, " {}", record.args())
}

pub async fn with_trace_id<T>(trace_id: String, fut: impl std::future::Future<Output = T>) -> T {
    TRACE_ID.scope(trace_id, fut).await
}

fn current_trace_id() -> Option<String> {
    TRACE_ID.try_with(|val| val.clone()).ok()
}

fn start_age_cleanup(path: &Path, max_age_days: u64) {
    let path = path.to_path_buf();
    std::thread::spawn(move || loop {
        cleanup_old_logs(&path, max_age_days);
        std::thread::sleep(Duration::from_secs(24 * 60 * 60));
    });
}

fn cleanup_old_logs(path: &Path, max_age_days: u64) {
    let Some(parent) = path.parent() else { return };
    let basename = match path.file_name().and_then(|v| v.to_str()) {
        Some(name) => name.to_string(),
        None => return,
    };
    let max_age = Duration::from_secs(max_age_days * 24 * 60 * 60);
    let now = SystemTime::now();
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let file_name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        if !file_name.starts_with(&basename) {
            continue;
        }
        let meta = match entry.metadata() {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        let modified = match meta.modified() {
            Ok(ts) => ts,
            Err(_) => continue,
        };
        if now.duration_since(modified).unwrap_or(Duration::ZERO) > max_age {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
