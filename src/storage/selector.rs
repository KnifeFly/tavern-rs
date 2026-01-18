use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use crc32fast::Hasher;

use crate::storage::{object, Bucket, Selector};

const DEFAULT_REPLICAS: usize = 20;

#[derive(Clone)]
struct RingEntry {
    hash: u32,
    bucket_index: usize,
}

struct RingState {
    buckets: Vec<Arc<dyn Bucket>>,
    ring: Vec<RingEntry>,
}

pub struct HashRingSelector {
    replicas: usize,
    state: RwLock<RingState>,
}

impl HashRingSelector {
    pub fn new(buckets: Vec<Arc<dyn Bucket>>) -> Self {
        Self::with_replicas(buckets, DEFAULT_REPLICAS)
    }

    pub fn with_replicas(buckets: Vec<Arc<dyn Bucket>>, replicas: usize) -> Self {
        let state = RingState {
            ring: build_ring(&buckets, replicas),
            buckets,
        };
        Self {
            replicas,
            state: RwLock::new(state),
        }
    }
}

impl Selector for HashRingSelector {
    fn select(&self, id: &object::Id) -> Option<Arc<dyn Bucket>> {
        let state = self.state.read().expect("selector");
        if state.ring.is_empty() {
            return None;
        }
        let hash = hash_bytes(&id.hash().0);
        let mut idx = match state
            .ring
            .binary_search_by(|entry| entry.hash.cmp(&hash))
        {
            Ok(pos) => pos,
            Err(pos) => {
                if pos >= state.ring.len() {
                    0
                } else {
                    pos
                }
            }
        };

        for _ in 0..state.ring.len() {
            let entry = &state.ring[idx];
            let bucket = &state.buckets[entry.bucket_index];
            if !bucket.use_allow() {
                idx = (idx + 1) % state.ring.len();
                continue;
            }
            if bucket.has_bad() {
                idx = (idx + 1) % state.ring.len();
                continue;
            }
            return Some(Arc::clone(bucket));
        }

        None
    }

    fn rebuild(&self, buckets: Vec<Arc<dyn Bucket>>) -> anyhow::Result<()> {
        let mut state = self.state.write().expect("selector");
        state.ring = build_ring(&buckets, self.replicas);
        state.buckets = buckets;
        Ok(())
    }
}

pub struct RoundRobinSelector {
    buckets: RwLock<Vec<Arc<dyn Bucket>>>,
    cursor: AtomicUsize,
}

impl RoundRobinSelector {
    pub fn new(buckets: Vec<Arc<dyn Bucket>>) -> Self {
        Self {
            buckets: RwLock::new(buckets),
            cursor: AtomicUsize::new(0),
        }
    }
}

impl Selector for RoundRobinSelector {
    fn select(&self, _id: &object::Id) -> Option<Arc<dyn Bucket>> {
        let buckets = self.buckets.read().expect("selector");
        if buckets.is_empty() {
            return None;
        }
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % buckets.len();
        Some(Arc::clone(&buckets[idx]))
    }

    fn rebuild(&self, buckets: Vec<Arc<dyn Bucket>>) -> anyhow::Result<()> {
        let mut guard = self.buckets.write().expect("selector");
        *guard = buckets;
        self.cursor.store(0, Ordering::Relaxed);
        Ok(())
    }
}

fn build_ring(buckets: &[Arc<dyn Bucket>], replicas: usize) -> Vec<RingEntry> {
    let mut ring = Vec::new();
    for (idx, bucket) in buckets.iter().enumerate() {
        for replica in 0..replicas {
            let key = format!("{}-{}", bucket.id(), replica);
            let hash = hash_bytes(key.as_bytes());
            ring.push(RingEntry {
                hash,
                bucket_index: idx,
            });
        }
    }
    ring.sort_by(|a, b| a.hash.cmp(&b.hash));
    ring
}

fn hash_bytes(input: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(input);
    hasher.finalize()
}
