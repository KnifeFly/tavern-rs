use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::config;
use crate::storage::bucket::lru::EvictionPolicy;
use crate::storage::bucket::{disk::DiskBucket, empty::EmptyBucket, memory::MemoryBucket};
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
                            log::warn!(
                                "custom-driver not supported, fallback to native disk bucket"
                            );
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
            Some(build_selector(
                cfg.selection_policy.as_str(),
                cold_buckets.clone(),
            ))
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

    fn purge_single(&self, store_url: &str, control: PurgeControl) -> Result<()> {
        let primary = self.purge_single_object(store_url, control);
        let variants = crate::storage::read_vary_variants(self.shared_kv.as_ref(), store_url);
        let mut removed_variant = false;
        for variant in variants {
            if self.purge_single_object(&variant, control).is_ok() {
                removed_variant = true;
            }
        }
        let _ = self
            .shared_kv
            .delete(crate::storage::vary_index_key(store_url).as_bytes());
        if primary.is_ok() || removed_variant {
            return Ok(());
        }
        primary
    }

    fn purge_single_object(&self, store_url: &str, control: PurgeControl) -> Result<()> {
        let id = Id::new(store_url);
        if let Some(bucket) = self.hot_selector.select(&id) {
            if bucket.exist(&id.hash().0) {
                if control.mark_expired && !control.hard {
                    if let Some(mut meta) = bucket.lookup(&id)? {
                        meta.expires_at = crate::storage::unix_now() - 1;
                        bucket.store(&meta)?;
                    }
                } else {
                    bucket.discard(&id)?;
                    let ix_key = format!("ix/{}/{}", bucket.id(), id.key());
                    let _ = self.shared_kv.delete(ix_key.as_bytes());
                    if let Ok(uri) = store_url.parse::<http::Uri>() {
                        if let Some(host) = uri.host() {
                            let key = format!("if/domain/{host}");
                            let _ = self.shared_kv.decr(key.as_bytes(), 1);
                        }
                    }
                    if let Some((base, _)) = store_url.split_once("#vary:") {
                        let _ = crate::storage::remove_vary_variant(
                            self.shared_kv.as_ref(),
                            base,
                            store_url,
                        );
                    }
                }
                return Ok(());
            }
        }
        if let Some(selector) = &self.cold_selector {
            if let Some(bucket) = selector.select(&id) {
                if bucket.exist(&id.hash().0) {
                    if control.mark_expired && !control.hard {
                        if let Some(mut meta) = bucket.lookup(&id)? {
                            meta.expires_at = crate::storage::unix_now() - 1;
                            bucket.store(&meta)?;
                        }
                    } else {
                        bucket.discard(&id)?;
                        let ix_key = format!("ix/{}/{}", bucket.id(), id.key());
                        let _ = self.shared_kv.delete(ix_key.as_bytes());
                        if let Ok(uri) = store_url.parse::<http::Uri>() {
                            if let Some(host) = uri.host() {
                                let key = format!("if/domain/{host}");
                                let _ = self.shared_kv.decr(key.as_bytes(), 1);
                            }
                        }
                        if let Some((base, _)) = store_url.split_once("#vary:") {
                            let _ = crate::storage::remove_vary_variant(
                                self.shared_kv.as_ref(),
                                base,
                                store_url,
                            );
                        }
                    }
                    return Ok(());
                }
            }
        }
        if let Some((base, _)) = store_url.split_once("#vary:") {
            let _ = crate::storage::remove_vary_variant(self.shared_kv.as_ref(), base, store_url);
        }
        Err(anyhow!("bucket not found"))
    }

    fn purge_dir(&self, store_url: &str, control: PurgeControl) -> Result<()> {
        let mut processed = 0usize;
        if !control.mark_expired {
            for bucket in &self.buckets {
                let prefix = format!("ix/{}/{store_url}", bucket.id());
                let _ = self
                    .shared_kv
                    .iterate_prefix(prefix.as_bytes(), &mut |key, val| {
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
                    });
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
        let vary_prefix = format!("vary/{store_url}");
        let _ = self.shared_kv.drop_prefix(vary_prefix.as_bytes());
        Ok(())
    }

    fn remove_meta_indexes(&self, bucket: &dyn Bucket, meta: &crate::storage::object::Metadata) {
        let ix_key = format!("ix/{}/{}", bucket.id(), meta.id.key());
        let _ = self.shared_kv.delete(ix_key.as_bytes());
        if let Ok(uri) = meta.id.path().parse::<http::Uri>() {
            if let Some(host) = uri.host() {
                let key = format!("if/domain/{host}");
                let _ = self.shared_kv.decr(key.as_bytes(), 1);
            }
        }
        if let Some((base, _)) = meta.id.key().split_once("#vary:") {
            let _ =
                crate::storage::remove_vary_variant(self.shared_kv.as_ref(), base, &meta.id.key());
        }
    }

    fn gc_bucket(&self, bucket: Arc<dyn Bucket>, batch_size: usize) -> Result<usize> {
        let now = crate::storage::unix_now();
        let mut expired = Vec::new();
        bucket.iterate(&mut |meta| {
            if meta.expires_at > 0
                && meta.expires_at <= now
                && (batch_size == 0 || expired.len() < batch_size)
            {
                expired.push(meta.clone());
            }
            Ok(())
        })?;

        let mut removed = 0usize;
        for meta in expired {
            let base_key = meta.id.key();
            let variants = crate::storage::read_vary_variants(self.shared_kv.as_ref(), &base_key);
            for variant in variants {
                if self
                    .purge_single_object(
                        &variant,
                        PurgeControl {
                            hard: true,
                            dir: false,
                            mark_expired: false,
                        },
                    )
                    .is_ok()
                {
                    removed += 1;
                }
            }
            let _ = self
                .shared_kv
                .delete(crate::storage::vary_index_key(&base_key).as_bytes());
            bucket.discard_with_metadata(&meta)?;
            self.remove_meta_indexes(bucket.as_ref(), &meta);
            removed += 1;
        }
        Ok(removed)
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
        self.purge_single(store_url, control)
    }

    fn gc_expired(&self, batch_size: usize) -> Result<usize> {
        let mut removed = 0usize;
        for bucket in &self.buckets {
            if batch_size > 0 && removed >= batch_size {
                break;
            }
            let remaining = if batch_size == 0 {
                0
            } else {
                batch_size.saturating_sub(removed)
            };
            removed += self.gc_bucket(Arc::clone(bucket), remaining)?;
        }
        Ok(removed)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::{Bucket as BucketConfig, Storage as StorageConfig, StorageGc};
    use crate::storage::object::{CacheFlag, ChunkSet, Id, Metadata};

    fn memory_config() -> StorageConfig {
        StorageConfig {
            driver: "memory".to_string(),
            db_type: "memory".to_string(),
            db_path: String::new(),
            async_load: false,
            eviction_policy: "lru".to_string(),
            selection_policy: "hashring".to_string(),
            slice_size: 1024,
            io_read_limit: 0,
            io_write_limit: 0,
            io_burst_bytes: 0,
            gc: StorageGc::default(),
            buckets: vec![BucketConfig {
                path: String::new(),
                driver: "memory".to_string(),
                bucket_type: "hot".to_string(),
                db_type: "memory".to_string(),
                db_path: String::new(),
                async_load: false,
                slice_size: 0,
                max_object_limit: 100,
                db_config: HashMap::new(),
            }],
        }
    }

    fn metadata(
        key: &str,
        flags: CacheFlag,
        expires_at: i64,
        virtual_key: Vec<String>,
    ) -> Metadata {
        Metadata {
            flags,
            id: Id::new(key),
            block_size: 1024,
            chunks: ChunkSet::default(),
            parts: ChunkSet::default(),
            code: 200,
            size: 0,
            resp_unix: crate::storage::unix_now(),
            last_ref_unix: crate::storage::unix_now(),
            refs: 1,
            expires_at,
            headers: Vec::new(),
            virtual_key,
        }
    }

    #[test]
    fn gc_expired_removes_vary_variants_and_index() {
        let store = NativeStorage::new(&memory_config()).expect("storage");
        let bucket = store.buckets().pop().expect("bucket");
        let base_key = "http://example.com/object";
        let variant_key = "http://example.com/object#vary:accept-encoding=gzip";
        let now = crate::storage::unix_now();

        bucket
            .store(&metadata(
                base_key,
                CacheFlag::VARY_INDEX,
                now - 1,
                Vec::new(),
            ))
            .expect("store base");
        bucket
            .store(&metadata(
                variant_key,
                CacheFlag::VARY_CACHE,
                now + 60,
                vec![base_key.to_string()],
            ))
            .expect("store variant");
        crate::storage::add_vary_variant(store.shared_kv().as_ref(), base_key, variant_key)
            .expect("vary index");

        let removed = store.gc_expired(10).expect("gc");

        assert_eq!(removed, 2);
        assert!(bucket.lookup(&Id::new(base_key)).unwrap().is_none());
        assert!(bucket.lookup(&Id::new(variant_key)).unwrap().is_none());
        assert!(
            crate::storage::read_vary_variants(store.shared_kv().as_ref(), base_key).is_empty()
        );
    }
}
