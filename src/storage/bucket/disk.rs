use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;

use crate::storage::indexdb::IndexDB;
use crate::storage::object::{Id, IdHash, Metadata};
use crate::storage::{BoxedReader, BoxedWriter, Bucket};

#[derive(Clone)]
pub struct DiskBucket {
    id: String,
    weight: i32,
    allow: i32,
    path: PathBuf,
    indexdb: Arc<dyn IndexDB>,
}

impl DiskBucket {
    pub fn new(path: PathBuf, id: &str, indexdb: Arc<dyn IndexDB>) -> Result<Arc<Self>> {
        fs::create_dir_all(&path)?;
        Ok(Arc::new(Self {
            id: id.to_string(),
            weight: 100,
            allow: 100,
            path,
            indexdb,
        }))
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
        "normal"
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn lookup(&self, id: &Id) -> Result<Option<Metadata>> {
        self.indexdb.get(&id.hash().0)
    }

    fn store(&self, meta: &Metadata) -> Result<()> {
        self.indexdb.set(&meta.id.hash().0, meta)
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
        Ok((Box::new(file), path))
    }

    fn read_chunk_file(&self, id: &Id, index: u32) -> Result<(BoxedReader, PathBuf)> {
        let path = id.wpath_slice(&self.path, index);
        let file = File::open(&path)?;
        Ok((Box::new(BufReader::new(file)), path))
    }
}
