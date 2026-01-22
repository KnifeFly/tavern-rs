use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::config;
use crate::storage::bucket::{disk::DiskBucket, empty::EmptyBucket, memory::MemoryBucket};
use crate::storage::bucket::lru::EvictionPolicy;
use crate::storage::indexdb;
use crate::storage::object::Id;
use crate::storage::selector::{HashRingSelector, RoundRobinSelector};
use crate::storage::sharedkv;
use crate::storage::{Bucket, PurgeControl, Selector, SharedKV, Storage};

pub struct NativeStorage {
    buckets: Vec<Arc<dyn crate::storage::Bucket>>,
    shared_kv: Arc<dyn SharedKV>,
    selector: Arc<dyn Selector>,
    bucket_by_id: HashMap<String, Arc<dyn crate::storage::Bucket>>,
    hot_buckets: Vec<Arc<dyn crate::storage::Bucket>>,
    cold_buckets: Vec<Arc<dyn crate::storage::Bucket>>,
    hot_selector: Arc<dyn Selector>,
    cold_selector: Option<Arc<dyn Selector>>,
}

impl NativeStorage {
    pub fn new(cfg: &config::Storage) -> Result<Arc<Self>> {
        crate::storage::set_io_limits(cfg);
        let shared_kv = build_shared_kv(cfg);
        let _ = shared_kv.drop_prefix(b"if/domain/");
        let _ = shared_kv.drop_prefix(b"ix/");
        let mut buckets: Vec<Arc<dyn crate::storage::Bucket>> = Vec::new();
        let mut bucket_by_id = HashMap::new();
        let policy = parse_eviction_policy(&cfg.eviction_policy);

        if cfg.buckets.is_empty() {
            let bucket: Arc<dyn Bucket> = MemoryBucket::new(
                "memory",
                Arc::clone(&shared_kv),
                default_max_objects(),
                policy,
                "memory".to_string(),
            );
            bucket_by_id.insert(bucket.id().to_string(), Arc::clone(&bucket));
            buckets.push(bucket);
        } else {
            for (idx, bucket_cfg) in cfg.buckets.iter().enumerate() {
                let db_type = if bucket_cfg.db_type.is_empty() {
                    cfg.db_type.as_str()
                } else {
                    bucket_cfg.db_type.as_str()
                };
                let max_objects = normalize_max_objects(bucket_cfg.max_object_limit);
                let store_type = if bucket_cfg.bucket_type.trim().is_empty() {
                    "normal".to_string()
                } else {
                    bucket_cfg.bucket_type.clone()
                };
                let async_load = bucket_cfg.async_load || cfg.async_load;
                let driver = if bucket_cfg.driver.trim().is_empty() {
                    cfg.driver.as_str()
                } else {
                    bucket_cfg.driver.as_str()
                };
                let bucket: Arc<dyn Bucket> = match driver {
                    "memory" | "mem" | "fastmemory" => MemoryBucket::new(
                        &format!("memory-{idx}"),
                        Arc::clone(&shared_kv),
                        max_objects,
                        policy,
                        store_type,
                    ),
                    "empty" => EmptyBucket::new(&format!("empty-{idx}")),
                    _ => {
                        if driver == "custom-driver" {
                            log::warn!("custom-driver not supported, fallback to native disk bucket");
                        }
                        let path = if bucket_cfg.path.is_empty() {
                            PathBuf::from(format!("bucket-{idx}"))
                        } else {
                            PathBuf::from(&bucket_cfg.path)
                        };
                        let db_path = if bucket_cfg.db_path.is_empty() {
                            if cfg.db_path.is_empty() {
                                path.join(".indexdb")
                            } else {
                                PathBuf::from(&cfg.db_path).join(format!("bucket-{idx}"))
                            }
                        } else {
                            PathBuf::from(&bucket_cfg.db_path)
                        };
                        let indexdb = indexdb::open(&db_path, db_type)?;
                        DiskBucket::new(
                            path,
                            &format!("disk-{idx}"),
                            indexdb,
                            Arc::clone(&shared_kv),
                            async_load,
                            max_objects,
                            policy,
                            store_type,
                        )?
                    }
                };
                bucket_by_id.insert(bucket.id().to_string(), Arc::clone(&bucket));
                buckets.push(bucket);
            }
        }

        let selector = build_selector(cfg.selection_policy.as_str(), buckets.clone());
        let (hot_buckets, cold_buckets) = split_tiers(&buckets);
        let hot_selector = build_selector(cfg.selection_policy.as_str(), hot_buckets.clone());
        let cold_selector = if cold_buckets.is_empty() {
            None
        } else {
            Some(build_selector(cfg.selection_policy.as_str(), cold_buckets.clone()))
        };

        Ok(Arc::new(Self {
            buckets,
            shared_kv,
            selector,
            bucket_by_id,
            hot_buckets,
            cold_buckets,
            hot_selector,
            cold_selector,
        }))
    }

    fn purge_single(&self, store_url: &str) -> Result<()> {
        let id = Id::new(store_url);
        if let Some(bucket) = self.hot_selector.select(&id) {
            if bucket.exist(&id.hash().0) {
                bucket.discard(&id)?;
                let ix_key = format!("ix/{}/{}", bucket.id(), id.key());
                let _ = self.shared_kv.delete(ix_key.as_bytes());
                if let Ok(uri) = store_url.parse::<http::Uri>() {
                    if let Some(host) = uri.host() {
                        let key = format!("if/domain/{host}");
                        let _ = self.shared_kv.decr(key.as_bytes(), 1);
                    }
                }
                return Ok(());
            }
        }
        if let Some(selector) = &self.cold_selector {
            if let Some(bucket) = selector.select(&id) {
                if bucket.exist(&id.hash().0) {
                    bucket.discard(&id)?;
                    let ix_key = format!("ix/{}/{}", bucket.id(), id.key());
                    let _ = self.shared_kv.delete(ix_key.as_bytes());
                    if let Ok(uri) = store_url.parse::<http::Uri>() {
                        if let Some(host) = uri.host() {
                            let key = format!("if/domain/{host}");
                            let _ = self.shared_kv.decr(key.as_bytes(), 1);
                        }
                    }
                    return Ok(());
                }
            }
        }
        Err(anyhow!("bucket not found"))
    }

    fn purge_dir(&self, store_url: &str, control: PurgeControl) -> Result<()> {
        let mut processed = 0usize;
        if !control.mark_expired {
            for bucket in &self.buckets {
                let prefix = format!("ix/{}/{store_url}", bucket.id());
                let _ = self.shared_kv.iterate_prefix(
                    prefix.as_bytes(),
                    &mut |key, val| {
                        if val.len() < crate::storage::object::ID_HASH_SIZE {
                            return Ok(());
                        }
                        let mut raw = [0u8; crate::storage::object::ID_HASH_SIZE];
                        raw.copy_from_slice(&val[..crate::storage::object::ID_HASH_SIZE]);
                        let hash = crate::storage::object::IdHash(raw);
                        if control.hard || !control.mark_expired {
                            let _ = bucket.discard_with_hash(hash);
                            processed += 1;
                        }
                        let _ = self.shared_kv.delete(key);
                        Ok(())
                    },
                );
            }
        }

        if processed == 0 {
            for bucket in &self.buckets {
                bucket.iterate(&mut |meta| {
                    if meta.id.path().starts_with(store_url) {
                        if control.hard || !control.mark_expired {
                            let _ = bucket.discard_with_metadata(meta);
                            let ix_key = format!("ix/{}/{}", bucket.id(), meta.id.key());
                            let _ = self.shared_kv.delete(ix_key.as_bytes());
                        } else {
                            let mut meta = meta.clone();
                            meta.expires_at = crate::storage::unix_now() - 1;
                            let _ = bucket.store(&meta);
                        }
                        processed += 1;
                    }
                    Ok(())
                })?;
            }
        }

        if processed == 0 {
            return Err(anyhow!("key not found"));
        }
        Ok(())
    }
}

fn build_shared_kv(cfg: &config::Storage) -> Arc<dyn SharedKV> {
    let base = if cfg.db_path.trim().is_empty() {
        PathBuf::from(".indexdb")
    } else {
        PathBuf::from(&cfg.db_path)
    };
    let path = base.join("sharedkv");
    match sharedkv::open(&path, cfg.db_type.as_str()) {
        Ok(kv) => kv,
        Err(err) => {
            log::warn!("sharedkv open failed: {err}, fallback to memory");
            crate::storage::MemSharedKV::new()
        }
    }
}

fn default_max_objects() -> Option<usize> {
    Some(10_000_000)
}

fn normalize_max_objects(limit: i64) -> Option<usize> {
    if limit <= 0 {
        default_max_objects()
    } else {
        Some(limit as usize)
    }
}

fn parse_eviction_policy(raw: &str) -> EvictionPolicy {
    match raw.to_ascii_lowercase().as_str() {
        "fifo" => EvictionPolicy::Fifo,
        "lfu" => EvictionPolicy::Lfu,
        _ => EvictionPolicy::Lru,
    }
}

impl Storage for NativeStorage {
    fn buckets(&self) -> Vec<Arc<dyn crate::storage::Bucket>> {
        self.buckets.clone()
    }

    fn shared_kv(&self) -> Arc<dyn SharedKV> {
        Arc::clone(&self.shared_kv)
    }

    fn selector(&self) -> Arc<dyn Selector> {
        Arc::clone(&self.selector)
    }

    fn purge(&self, store_url: &str, control: PurgeControl) -> Result<()> {
        if control.dir {
            return self.purge_dir(store_url, control);
        }
        self.purge_single(store_url)
    }

    fn bucket_by_id(&self, id: &str) -> Option<Arc<dyn Bucket>> {
        self.bucket_by_id.get(id).cloned()
    }

    fn hot_buckets(&self) -> Vec<Arc<dyn Bucket>> {
        self.hot_buckets.clone()
    }

    fn cold_buckets(&self) -> Vec<Arc<dyn Bucket>> {
        self.cold_buckets.clone()
    }

    fn select_hot(&self, id: &Id) -> Option<Arc<dyn Bucket>> {
        self.hot_selector.select(id)
    }

    fn select_cold(&self, id: &Id) -> Option<Arc<dyn Bucket>> {
        self.cold_selector.as_ref().and_then(|s| s.select(id))
    }
}

fn build_selector(policy: &str, buckets: Vec<Arc<dyn Bucket>>) -> Arc<dyn Selector> {
    match policy {
        "roundrobin" => Arc::new(RoundRobinSelector::new(buckets)),
        _ => Arc::new(HashRingSelector::new(buckets)),
    }
}

fn split_tiers(buckets: &[Arc<dyn Bucket>]) -> (Vec<Arc<dyn Bucket>>, Vec<Arc<dyn Bucket>>) {
    let mut hot = Vec::new();
    let mut cold = Vec::new();
    for bucket in buckets {
        if bucket.store_type().eq_ignore_ascii_case("cold") {
            cold.push(Arc::clone(bucket));
        } else {
            hot.push(Arc::clone(bucket));
        }
    }
    if hot.is_empty() {
        return (buckets.to_vec(), Vec::new());
    }
    (hot, cold)
}
