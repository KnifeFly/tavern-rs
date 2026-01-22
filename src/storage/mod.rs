use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};

use crate::config;

pub mod bucket;
pub mod indexdb;
pub mod sharedkv;
pub mod object;
pub mod selector;
pub mod native;
pub mod tiered;

pub trait StorageReader: Read + Seek + Send {}
impl<T: Read + Seek + Send> StorageReader for T {}

pub type BoxedReader = Box<dyn StorageReader>;
pub type BoxedWriter = Box<dyn Write + Send>;

#[derive(Clone, Copy)]
pub struct PurgeControl {
    pub hard: bool,
    pub dir: bool,
    pub mark_expired: bool,
}

pub trait Selector: Send + Sync {
    fn select(&self, id: &object::Id) -> Option<Arc<dyn Bucket>>;
    fn rebuild(&self, _buckets: Vec<Arc<dyn Bucket>>) -> Result<()> {
        Ok(())
    }
}

pub trait Bucket: Send + Sync {
    fn id(&self) -> &str;
    fn weight(&self) -> i32;
    fn allow(&self) -> i32;
    fn use_allow(&self) -> bool;
    fn objects(&self) -> u64;
    fn has_bad(&self) -> bool;
    fn bucket_type(&self) -> &str;
    fn store_type(&self) -> &str;
    fn path(&self) -> &Path;

    fn lookup(&self, id: &object::Id) -> Result<Option<object::Metadata>>;
    fn store(&self, meta: &object::Metadata) -> Result<()>;
    fn exist(&self, hash: &[u8]) -> bool;
    fn remove(&self, id: &object::Id) -> Result<()>;
    fn discard(&self, id: &object::Id) -> Result<()>;
    fn discard_with_hash(&self, hash: object::IdHash) -> Result<()>;
    fn discard_with_metadata(&self, meta: &object::Metadata) -> Result<()> {
        self.discard(&meta.id)
    }
    fn iterate(&self, f: &mut dyn FnMut(&object::Metadata) -> Result<()>) -> Result<()>;
    fn expired(&self, id: &object::Id, meta: &object::Metadata) -> bool;
    fn write_chunk_file(&self, id: &object::Id, index: u32) -> Result<(BoxedWriter, PathBuf)>;
    fn read_chunk_file(&self, id: &object::Id, index: u32) -> Result<(BoxedReader, PathBuf)>;
}

pub trait SharedKV: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>>;
    fn set(&self, key: &[u8], val: &[u8]) -> Result<()>;
    fn incr(&self, key: &[u8], delta: u32) -> Result<u32>;
    fn decr(&self, key: &[u8], delta: u32) -> Result<u32>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn drop_prefix(&self, prefix: &[u8]) -> Result<()>;
    fn iterate(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()>;
    fn iterate_prefix(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>)
        -> Result<()>;
}

pub trait Storage: Send + Sync {
    fn buckets(&self) -> Vec<Arc<dyn Bucket>>;
    fn shared_kv(&self) -> Arc<dyn SharedKV>;
    fn selector(&self) -> Arc<dyn Selector>;
    fn purge(&self, store_url: &str, control: PurgeControl) -> Result<()>;
    fn bucket_by_id(&self, _id: &str) -> Option<Arc<dyn Bucket>> {
        None
    }
    fn hot_buckets(&self) -> Vec<Arc<dyn Bucket>> {
        Vec::new()
    }
    fn cold_buckets(&self) -> Vec<Arc<dyn Bucket>> {
        Vec::new()
    }
    fn select_hot(&self, id: &object::Id) -> Option<Arc<dyn Bucket>> {
        self.selector().select(id)
    }
    fn select_cold(&self, _id: &object::Id) -> Option<Arc<dyn Bucket>> {
        None
    }
}

struct IoLimiter {
    read: Option<Arc<Mutex<RateLimiter>>>,
    write: Option<Arc<Mutex<RateLimiter>>>,
}

struct RateLimiter {
    rate: u64,
    burst: u64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    fn new(rate: u64, burst: u64) -> Self {
        let burst = burst.max(rate).max(1);
        Self {
            rate: rate.max(1),
            burst,
            tokens: burst as f64,
            last: Instant::now(),
        }
    }

    fn acquire(&mut self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        let added = elapsed * self.rate as f64;
        self.tokens = (self.tokens + added).min(self.burst as f64);
        self.last = now;
        if self.tokens >= bytes as f64 {
            self.tokens -= bytes as f64;
            return;
        }
        let needed = bytes as f64 - self.tokens;
        let wait = needed / self.rate as f64;
        self.tokens = 0.0;
        std::thread::sleep(Duration::from_secs_f64(wait));
        self.last = Instant::now();
    }
}

struct RateLimitedReader {
    inner: BoxedReader,
    limiter: Arc<Mutex<RateLimiter>>,
}

impl Read for RateLimitedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            let mut limiter = self.limiter.lock().expect("io limiter");
            limiter.acquire(n as u64);
        }
        Ok(n)
    }
}

impl Seek for RateLimitedReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

struct RateLimitedWriter {
    inner: BoxedWriter,
    limiter: Arc<Mutex<RateLimiter>>,
}

impl Write for RateLimitedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        if n > 0 {
            let mut limiter = self.limiter.lock().expect("io limiter");
            limiter.acquire(n as u64);
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

static IO_LIMITER: OnceLock<IoLimiter> = OnceLock::new();

pub fn set_io_limits(cfg: &config::Storage) {
    let burst = if cfg.io_burst_bytes > 0 {
        cfg.io_burst_bytes
    } else {
        cfg.io_read_limit.max(cfg.io_write_limit)
    };
    let read = if cfg.io_read_limit > 0 {
        Some(Arc::new(Mutex::new(RateLimiter::new(
            cfg.io_read_limit,
            burst,
        ))))
    } else {
        None
    };
    let write = if cfg.io_write_limit > 0 {
        Some(Arc::new(Mutex::new(RateLimiter::new(
            cfg.io_write_limit,
            burst,
        ))))
    } else {
        None
    };
    let _ = IO_LIMITER.set(IoLimiter { read, write });
}

pub fn rate_limit_reader(reader: BoxedReader) -> BoxedReader {
    let limiter = IO_LIMITER.get().and_then(|l| l.read.as_ref().cloned());
    if let Some(limiter) = limiter {
        Box::new(RateLimitedReader { inner: reader, limiter })
    } else {
        reader
    }
}

pub fn rate_limit_writer(writer: BoxedWriter) -> BoxedWriter {
    let limiter = IO_LIMITER.get().and_then(|l| l.write.as_ref().cloned());
    if let Some(limiter) = limiter {
        Box::new(RateLimitedWriter { inner: writer, limiter })
    } else {
        writer
    }
}

#[derive(Clone, Copy)]
pub enum CacheStatus {
    Miss,
    Hit,
    ParentHit,
    PartHit,
    RevalidateHit,
    RevalidateMiss,
    PartMiss,
    HotHit,
    Bypass,
}

impl CacheStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            CacheStatus::Miss => "MISS",
            CacheStatus::Hit => "HIT",
            CacheStatus::ParentHit => "PARENT_HIT",
            CacheStatus::PartHit => "PART_HIT",
            CacheStatus::RevalidateHit => "REVALIDATE_HIT",
            CacheStatus::RevalidateMiss => "REVALIDATE_MISS",
            CacheStatus::PartMiss => "PART_MISS",
            CacheStatus::HotHit => "HOT_HIT",
            CacheStatus::Bypass => "BYPASS",
        }
    }
}

pub struct MemSharedKV {
    map: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl MemSharedKV {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            map: Mutex::new(HashMap::new()),
        })
    }
}

impl SharedKV for MemSharedKV {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let map = self.map.lock().expect("sharedkv");
        map.get(key)
            .cloned()
            .ok_or_else(|| anyhow!("key not found"))
    }

    fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let mut map = self.map.lock().expect("sharedkv");
        map.insert(key.to_vec(), val.to_vec());
        Ok(())
    }

    fn incr(&self, key: &[u8], delta: u32) -> Result<u32> {
        let mut map = self.map.lock().expect("sharedkv");
        let cur = map
            .get(key)
            .and_then(|v| v.get(0..4))
            .map(|v| u32::from_be_bytes([v[0], v[1], v[2], v[3]]))
            .unwrap_or(0);
        let next = cur.saturating_add(delta);
        map.insert(key.to_vec(), next.to_be_bytes().to_vec());
        Ok(next)
    }

    fn decr(&self, key: &[u8], delta: u32) -> Result<u32> {
        let mut map = self.map.lock().expect("sharedkv");
        let cur = map
            .get(key)
            .and_then(|v| v.get(0..4))
            .map(|v| u32::from_be_bytes([v[0], v[1], v[2], v[3]]))
            .unwrap_or(0);
        let next = cur.saturating_sub(delta);
        map.insert(key.to_vec(), next.to_be_bytes().to_vec());
        Ok(next)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let mut map = self.map.lock().expect("sharedkv");
        map.remove(key);
        Ok(())
    }

    fn drop_prefix(&self, prefix: &[u8]) -> Result<()> {
        let mut map = self.map.lock().expect("sharedkv");
        let keys: Vec<Vec<u8>> = map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        for key in keys {
            map.remove(&key);
        }
        Ok(())
    }

    fn iterate(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
        let map = self.map.lock().expect("sharedkv");
        for (k, v) in map.iter() {
            f(k, v)?;
        }
        Ok(())
    }

    fn iterate_prefix(
        &self,
        prefix: &[u8],
        f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        let map = self.map.lock().expect("sharedkv");
        for (k, v) in map.iter() {
            if k.starts_with(prefix) {
                f(k, v)?;
            }
        }
        Ok(())
    }
}

pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64
}

static DEFAULT_STORAGE: OnceLock<Arc<dyn Storage>> = OnceLock::new();

pub fn set_default(storage: Arc<dyn Storage>) {
    let _ = DEFAULT_STORAGE.set(storage);
}

pub fn current() -> Arc<dyn Storage> {
    DEFAULT_STORAGE
        .get()
        .expect("storage not initialized")
        .clone()
}
