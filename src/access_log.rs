use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::Result;
use chrono::Local;

#[derive(Debug)]
pub struct AccessLogger {
    writer: Mutex<AccessWriter>,
    path: Option<PathBuf>,
    last_stamp: Mutex<Option<String>>,
}

#[derive(Debug)]
enum AccessWriter {
    File(std::fs::File),
    Stdout(std::io::Stdout),
}

impl AccessLogger {
    pub fn new(path: Option<&str>) -> Result<Self> {
        let (writer, path) = if let Some(path) = path.filter(|p| !p.is_empty()) {
            let path = Path::new(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            (AccessWriter::File(file), Some(path.to_path_buf()))
        } else {
            (AccessWriter::Stdout(std::io::stdout()), None)
        };
        Ok(Self {
            writer: Mutex::new(writer),
            path,
            last_stamp: Mutex::new(None),
        })
    }

    pub fn log_line(&self, line: &str) {
        self.rotate_if_needed();
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

    fn rotate_if_needed(&self) {
        let path = match &self.path {
            Some(path) => path,
            None => return,
        };
        let stamp = Local::now().format("%Y%m%d%H%M").to_string();
        let mut last = match self.last_stamp.lock() {
            Ok(val) => val,
            Err(_) => return,
        };
        if last.as_ref() == Some(&stamp) {
            return;
        }
        let rotated = PathBuf::from(format!("{}.{}", path.display(), stamp));
        let _ = fs::rename(path, rotated);
        if let Ok(mut writer) = self.writer.lock() {
            if let Ok(file) = OpenOptions::new().create(true).append(true).open(path) {
                *writer = AccessWriter::File(file);
                *last = Some(stamp);
            }
        }
    }
}
