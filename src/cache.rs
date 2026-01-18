use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::HeaderMap;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStatus {
    Miss,
    Hit,
    PartHit,
    PartMiss,
    RevalidateHit,
    RevalidateMiss,
}

impl CacheStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            CacheStatus::Miss => "MISS",
            CacheStatus::Hit => "HIT",
            CacheStatus::PartHit => "PART_HIT",
            CacheStatus::PartMiss => "PART_MISS",
            CacheStatus::RevalidateHit => "REVALIDATE_HIT",
            CacheStatus::RevalidateMiss => "REVALIDATE_MISS",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub headers: HeaderMap,
    pub status: http::StatusCode,
    pub body: Bytes,
    pub size: u64,
    pub expires_at: Instant,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub chunk_size: u64,
    pub chunks: HashSet<u32>,
    pub is_vary_index: bool,
    pub vary_headers: Option<Vec<String>>,
}

impl CacheEntry {
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    pub fn chunk_range(&self, start: u64, end: u64) -> Vec<u32> {
        let first = start / self.chunk_size;
        let last = end / self.chunk_size;
        (first..=last).map(|v| v as u32).collect()
    }

    pub fn mark_chunks(&mut self, chunks: &[u32]) {
        for c in chunks {
            self.chunks.insert(*c);
        }
    }

    pub fn has_all_chunks(&self, chunks: &[u32]) -> bool {
        chunks.iter().all(|c| self.chunks.contains(c))
    }

    pub fn has_any_chunks(&self, chunks: &[u32]) -> bool {
        chunks.iter().any(|c| self.chunks.contains(c))
    }
}

#[derive(Debug, Default)]
pub struct CacheStore {
    inner: RwLock<HashMap<String, CacheEntry>>,
}

impl CacheStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, key: &str) -> Option<CacheEntry> {
        let map = self.inner.read().await;
        map.get(key).cloned()
    }

    pub async fn insert(&self, key: String, entry: CacheEntry) {
        let mut map = self.inner.write().await;
        map.insert(key, entry);
    }

    pub async fn update<F>(&self, key: &str, updater: F)
    where
        F: FnOnce(&mut CacheEntry),
    {
        let mut map = self.inner.write().await;
        if let Some(entry) = map.get_mut(key) {
            updater(entry);
        }
    }

    pub async fn remove(&self, key: &str) -> bool {
        let mut map = self.inner.write().await;
        map.remove(key).is_some()
    }
}

pub fn compute_expiry(ttl: Duration) -> Instant {
    Instant::now() + ttl
}
