use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::Result;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use chrono::Local;
use rand::RngCore;
use sha2::{Digest, Sha256};

pub struct AccessLogger {
    writer: Mutex<AccessWriter>,
    path: Option<PathBuf>,
    last_stamp: Mutex<Option<String>>,
    encryptor: Option<AccessEncryptor>,
}

enum AccessWriter {
    File(std::fs::File),
    Stdout(std::io::Stdout),
}

impl AccessLogger {
    pub fn new(path: Option<&str>, encrypt: Option<AccessEncryptor>) -> Result<Self> {
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
            encryptor: encrypt,
        })
    }

    pub fn log_line(&self, line: &str) {
        self.rotate_if_needed();
        let payload = if let Some(encryptor) = &self.encryptor {
            encryptor.encrypt_line(line)
        } else {
            line.to_string()
        };
        if let Ok(mut writer) = self.writer.lock() {
            match &mut *writer {
                AccessWriter::File(file) => {
                    let _ = file.write_all(payload.as_bytes());
                }
                AccessWriter::Stdout(stdout) => {
                    let _ = stdout.write_all(payload.as_bytes());
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

#[derive(Clone)]
pub struct AccessEncryptor {
    cipher: Aes256Gcm,
}

impl AccessEncryptor {
    pub fn new(secret: &str) -> Option<Self> {
        if secret.trim().is_empty() {
            return None;
        }
        let mut hasher = Sha256::new();
        hasher.update(secret.as_bytes());
        let key = hasher.finalize();
        let cipher = Aes256Gcm::new_from_slice(&key).ok()?;
        Some(Self { cipher })
    }

    fn encrypt_line(&self, line: &str) -> String {
        let raw = line.trim_end_matches('\n');
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(nonce, raw.as_bytes())
            .unwrap_or_else(|_| raw.as_bytes().to_vec());
        let mut out = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        format!("{}\n", base64::engine::general_purpose::STANDARD.encode(out))
    }
}
