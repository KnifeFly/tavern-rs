use std::collections::HashMap;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};

use crate::storage::bucket::lru::{EvictionPolicy, EvictionTracker};
use crate::storage::indexdb::{IndexDB, SledIndexDB};
use crate::storage::object::{Id, IdHash, Metadata};
use crate::storage::{BoxedReader, BoxedWriter, Bucket, SharedKV};

pub struct MemoryBucket {
    id: String,
    weight: i32,
    allow: i32,
    path: PathBuf,
    indexdb: Arc<dyn IndexDB>,
    shared_kv: Arc<dyn SharedKV>,
    eviction: Mutex<EvictionTracker>,
    store_type: String,
    chunks: Arc<Mutex<HashMap<(IdHash, u32), Vec<u8>>>>,
}

impl MemoryBucket {
    pub fn new(
        id: &str,
        shared_kv: Arc<dyn SharedKV>,
        max_objects: Option<usize>,
        policy: EvictionPolicy,
        store_type: String,
    ) -> Arc<Self> {
        let indexdb = SledIndexDB::temporary().expect("indexdb");
        Arc::new(Self {
            id: id.to_string(),
            weight: 100,
            allow: 100,
            path: PathBuf::from("/"),
            indexdb,
            shared_kv,
            eviction: Mutex::new(EvictionTracker::new(policy, max_objects)),
            store_type,
            chunks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn new_with_indexdb(
        id: &str,
        indexdb: Arc<dyn IndexDB>,
        shared_kv: Arc<dyn SharedKV>,
        max_objects: Option<usize>,
        policy: EvictionPolicy,
        store_type: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            id: id.to_string(),
            weight: 100,
            allow: 100,
            path: PathBuf::from("/"),
            indexdb,
            shared_kv,
            eviction: Mutex::new(EvictionTracker::new(policy, max_objects)),
            store_type,
            chunks: Arc::new(Mutex::new(HashMap::new())),
        })
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

impl Bucket for MemoryBucket {
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
        "memory"
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
        let mut chunks = self.chunks.lock().expect("chunks");
        for idx in meta.chunks.iter() {
            chunks.remove(&(meta.id.hash(), *idx));
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
        let key = (id.hash(), index);
        let chunks = Arc::clone(&self.chunks);
        let writer = MemoryChunkWriter { key, chunks };
        let path = PathBuf::from(format!("mem://{}-{:06}", id.hash_str(), index));
        Ok((Box::new(writer), path))
    }

    fn read_chunk_file(&self, id: &Id, index: u32) -> Result<(BoxedReader, PathBuf)> {
        let chunks = self.chunks.lock().expect("chunks");
        let data = chunks
            .get(&(id.hash(), index))
            .ok_or_else(|| anyhow!("chunk not found"))?
            .clone();
        let path = PathBuf::from(format!("mem://{}-{:06}", id.hash_str(), index));
        Ok((Box::new(Cursor::new(data)), path))
    }
}

struct MemoryChunkWriter {
    key: (IdHash, u32),
    chunks: Arc<Mutex<HashMap<(IdHash, u32), Vec<u8>>>>,
}

impl Write for MemoryChunkWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut map = self.chunks.lock().expect("chunks");
        let entry = map.entry(self.key).or_default();
        entry.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for MemoryChunkWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut map = self.chunks.lock().expect("chunks");
        let entry = map.entry(self.key).or_default();
        let mut cursor = Cursor::new(entry);
        cursor.seek(pos)
    }
}
