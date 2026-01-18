use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use flexi_logger::{Cleanup, Criterion, Duplicate, FileSpec, Logger as FlexiLogger, Naming, WriteMode};
use log::LevelFilter;

use crate::config::Logger;

const TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3fZ";

#[derive(Clone, Copy)]
struct FormatConfig {
    include_pid: bool,
    include_caller: bool,
}

static FORMAT_CONFIG: OnceLock<FormatConfig> = OnceLock::new();

pub fn init(config: &Logger, verbose: bool) -> Result<()> {
    let level = if verbose {
        LevelFilter::Debug
    } else {
        parse_level(&config.level)
    };

    FORMAT_CONFIG.get_or_init(|| FormatConfig {
        include_pid: !config.nopid,
        include_caller: config.caller,
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
    });
    let ts = now.now_utc_owned().format(TIMESTAMP_FORMAT);
    write!(writer, "{} [{}]", ts, record.level())?;
    if cfg.include_pid {
        write!(writer, " pid={}", std::process::id())?;
    }
    if cfg.include_caller {
        let file = record.file().unwrap_or("-");
        let line = record.line().unwrap_or(0);
        write!(writer, " {}:{}", file, line)?;
    }
    writeln!(writer, " {}", record.args())
}
