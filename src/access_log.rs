use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;

#[derive(Debug)]
pub struct AccessLogger {
    writer: Mutex<AccessWriter>,
}

#[derive(Debug)]
enum AccessWriter {
    File(std::fs::File),
    Stdout(std::io::Stdout),
}

impl AccessLogger {
    pub fn new(path: Option<&str>) -> Result<Self> {
        let writer = if let Some(path) = path.filter(|p| !p.is_empty()) {
            let path = Path::new(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            AccessWriter::File(file)
        } else {
            AccessWriter::Stdout(std::io::stdout())
        };
        Ok(Self {
            writer: Mutex::new(writer),
        })
    }

    pub fn log_line(&self, line: &str) {
        if let Ok(mut writer) = self.writer.lock() {
            match &mut *writer {
                AccessWriter::File(file) => {
                    let _ = file.write_all(line.as_bytes());
                }
                AccessWriter::Stdout(stdout) => {
                    let _ = stdout.write_all(line.as_bytes());
                }
            }
        }
    }
}
