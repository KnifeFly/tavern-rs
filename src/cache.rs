use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::HeaderMap;
use tokio::sync::RwLock;
use std::collections::VecDeque;

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
    pub created_at: Instant,
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
struct CacheInner {
    map: HashMap<String, CacheEntry>,
    order: VecDeque<String>,
    max_entries: Option<usize>,
}

#[derive(Debug)]
pub struct CacheStore {
    inner: RwLock<CacheInner>,
}

impl CacheStore {
    pub fn new() -> Self {
        Self::new_with_limit(None)
    }

    pub fn new_with_limit(max_entries: Option<usize>) -> Self {
        Self {
            inner: RwLock::new(CacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
                max_entries: max_entries.filter(|v| *v > 0),
            }),
        }
    }

    pub async fn get(&self, key: &str) -> Option<CacheEntry> {
        let inner = self.inner.read().await;
        inner.map.get(key).cloned()
    }

    pub async fn insert(&self, key: String, entry: CacheEntry) {
        let mut inner = self.inner.write().await;
        if !inner.map.contains_key(&key) {
            if let Some(max) = inner.max_entries {
                while inner.map.len() >= max {
                    if let Some(old) = inner.order.pop_front() {
                        inner.map.remove(&old);
                    } else {
                        break;
                    }
                }
            }
            inner.order.push_back(key.clone());
        }
        inner.map.insert(key, entry);
    }

    pub async fn update<F>(&self, key: &str, updater: F)
    where
        F: FnOnce(&mut CacheEntry),
    {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.map.get_mut(key) {
            updater(entry);
        }
    }

    pub async fn remove(&self, key: &str) -> bool {
        let mut inner = self.inner.write().await;
        if inner.map.remove(key).is_some() {
            if let Some(pos) = inner.order.iter().position(|k| k == key) {
                inner.order.remove(pos);
            }
            return true;
        }
        false
    }
}

pub fn compute_expiry(ttl: Duration) -> Instant {
    Instant::now() + ttl
}
