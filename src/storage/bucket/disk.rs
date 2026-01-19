use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::os::unix::fs::MetadataExt;

use anyhow::Result;

use crate::storage::bucket::lru::{EvictionPolicy, EvictionTracker};
use crate::storage::indexdb::IndexDB;
use crate::storage::object::{Id, IdHash, Metadata};
use crate::storage::{BoxedReader, BoxedWriter, Bucket, SharedKV};
use crate::metrics;

pub struct DiskBucket {
    id: String,
    weight: i32,
    allow: i32,
    path: PathBuf,
    indexdb: Arc<dyn IndexDB>,
    shared_kv: Arc<dyn SharedKV>,
    eviction: Mutex<EvictionTracker>,
    store_type: String,
}

impl DiskBucket {
    pub fn new(
        path: PathBuf,
        id: &str,
        indexdb: Arc<dyn IndexDB>,
        shared_kv: Arc<dyn SharedKV>,
        async_load: bool,
        max_objects: Option<usize>,
        policy: EvictionPolicy,
        store_type: String,
    ) -> Result<Arc<Self>> {
        fs::create_dir_all(&path)?;
        let bucket = Arc::new(Self {
            id: id.to_string(),
            weight: 100,
            allow: 100,
            path,
            indexdb,
            shared_kv,
            eviction: Mutex::new(EvictionTracker::new(policy, max_objects)),
            store_type,
        });
        let bucket_clone = Arc::clone(&bucket);
        if async_load {
            std::thread::spawn(move || bucket_clone.load_metadata());
        } else {
            bucket.load_metadata();
        }
        Ok(bucket)
    }

    fn load_metadata(&self) {
        let _ = self.indexdb.iterate(None, &mut |_, meta| {
            {
                let mut eviction = self.eviction.lock().expect("eviction");
                eviction.touch_no_evict(meta.id.hash());
            }
            self.update_shared_kv(meta);
            true
        });
        let evicted = {
            let mut eviction = self.eviction.lock().expect("eviction");
            eviction.evict_overflow()
        };
        for hash in evicted {
            let _ = self.evict_hash(hash);
        }
    }

    fn update_shared_kv(&self, meta: &Metadata) {
        if let Ok(uri) = meta.id.path().parse::<http::Uri>() {
            if let Some(host) = uri.host() {
                let key = format!("if/domain/{host}");
                let _ = self.shared_kv.incr(key.as_bytes(), 1);
            }
        }
        let ix_key = format!("ix/{}/{}", self.id, meta.id.key());
        let _ = self.shared_kv.set(ix_key.as_bytes(), &meta.id.hash().0);
    }

    fn remove_shared_kv(&self, meta: &Metadata) {
        let ix_key = format!("ix/{}/{}", self.id, meta.id.key());
        let _ = self.shared_kv.delete(ix_key.as_bytes());
        if let Ok(uri) = meta.id.path().parse::<http::Uri>() {
            if let Some(host) = uri.host() {
                let key = format!("if/domain/{host}");
                let _ = self.shared_kv.decr(key.as_bytes(), 1);
            }
        }
    }

    fn touch_and_evict(&self, hash: IdHash) -> Result<()> {
        let evicted = {
            let mut eviction = self.eviction.lock().expect("eviction");
            eviction.touch(hash)
        };
        for hash in evicted {
            let _ = self.evict_hash(hash);
        }
        Ok(())
    }

    fn evict_hash(&self, hash: IdHash) -> Result<()> {
        if let Some(meta) = self.indexdb.get(&hash.0)? {
            self.remove_shared_kv(&meta);
            self.discard_with_metadata(&meta)?;
        }
        Ok(())
    }

    fn remove_from_lru(&self, hash: IdHash) {
        let mut eviction = self.eviction.lock().expect("eviction");
        eviction.remove(&hash);
    }
}

impl Bucket for DiskBucket {
    fn id(&self) -> &str {
        &self.id
    }

    fn weight(&self) -> i32 {
        self.weight
    }

    fn allow(&self) -> i32 {
        self.allow
    }

    fn use_allow(&self) -> bool {
        true
    }

    fn objects(&self) -> u64 {
        self.indexdb.len().unwrap_or(0) as u64
    }

    fn has_bad(&self) -> bool {
        false
    }

    fn bucket_type(&self) -> &str {
        "disk"
    }

    fn store_type(&self) -> &str {
        &self.store_type
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn lookup(&self, id: &Id) -> Result<Option<Metadata>> {
        let meta = self.indexdb.get(&id.hash().0)?;
        if meta.is_some() {
            let _ = self.touch_and_evict(id.hash());
        }
        Ok(meta)
    }

    fn store(&self, meta: &Metadata) -> Result<()> {
        self.indexdb.set(&meta.id.hash().0, meta)?;
        self.touch_and_evict(meta.id.hash())?;
        Ok(())
    }

    fn exist(&self, hash: &[u8]) -> bool {
        self.indexdb.exist(hash).unwrap_or(false)
    }

    fn remove(&self, id: &Id) -> Result<()> {
        self.indexdb.delete(&id.hash().0)
    }

    fn discard(&self, id: &Id) -> Result<()> {
        if let Some(meta) = self.indexdb.get(&id.hash().0)? {
            self.discard_with_metadata(&meta)?;
        }
        Ok(())
    }

    fn discard_with_hash(&self, hash: IdHash) -> Result<()> {
        if let Some(meta) = self.indexdb.get(&hash.0)? {
            self.discard_with_metadata(&meta)?;
        }
        Ok(())
    }

    fn discard_with_metadata(&self, meta: &Metadata) -> Result<()> {
        self.remove_from_lru(meta.id.hash());
        self.indexdb.delete(&meta.id.hash().0)?;
        for idx in meta.chunks.iter() {
            let path = meta.id.wpath_slice(&self.path, *idx);
            let _ = fs::remove_file(path);
        }
        Ok(())
    }

    fn iterate(&self, f: &mut dyn FnMut(&Metadata) -> Result<()>) -> Result<()> {
        self.indexdb.iterate(None, &mut |_, meta| f(meta).is_ok())
    }

    fn expired(&self, _id: &Id, meta: &Metadata) -> bool {
        let now = crate::storage::unix_now();
        meta.expires_at > 0 && meta.expires_at <= now
    }

    fn write_chunk_file(&self, id: &Id, index: u32) -> Result<(BoxedWriter, PathBuf)> {
        let path = id.wpath_slice(&self.path, index);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(&path)?;
        let dev = fs::metadata(&self.path)
            .map(|m| m.dev().to_string())
            .unwrap_or_else(|_| "-".to_string());
        metrics::record_disk_io(&dev, &self.path.to_string_lossy());
        Ok((Box::new(file), path))
    }

    fn read_chunk_file(&self, id: &Id, index: u32) -> Result<(BoxedReader, PathBuf)> {
        let path = id.wpath_slice(&self.path, index);
        let file = File::open(&path)?;
        let dev = fs::metadata(&self.path)
            .map(|m| m.dev().to_string())
            .unwrap_or_else(|_| "-".to_string());
        metrics::record_disk_io(&dev, &self.path.to_string_lossy());
        Ok((Box::new(BufReader::new(file)), path))
    }
}
