use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::storage::object::{Id, IdHash, Metadata};
use crate::storage::{BoxedReader, BoxedWriter, Bucket};

#[derive(Clone)]
pub struct EmptyBucket {
    id: String,
    path: PathBuf,
}

impl EmptyBucket {
    pub fn new(id: &str) -> Arc<Self> {
        Arc::new(Self {
            id: id.to_string(),
            path: PathBuf::from("/"),
        })
    }
}

impl Bucket for EmptyBucket {
    fn id(&self) -> &str {
        &self.id
    }

    fn weight(&self) -> i32 {
        0
    }

    fn allow(&self) -> i32 {
        0
    }

    fn use_allow(&self) -> bool {
        false
    }

    fn objects(&self) -> u64 {
        0
    }

    fn has_bad(&self) -> bool {
        false
    }

    fn bucket_type(&self) -> &str {
        "empty"
    }

    fn store_type(&self) -> &str {
        "empty"
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn lookup(&self, _id: &Id) -> Result<Option<Metadata>> {
        Ok(None)
    }

    fn store(&self, _meta: &Metadata) -> Result<()> {
        Ok(())
    }

    fn exist(&self, _hash: &[u8]) -> bool {
        false
    }

    fn remove(&self, _id: &Id) -> Result<()> {
        Ok(())
    }

    fn discard(&self, _id: &Id) -> Result<()> {
        Ok(())
    }

    fn discard_with_hash(&self, _hash: IdHash) -> Result<()> {
        Ok(())
    }

    fn iterate(&self, _f: &mut dyn FnMut(&Metadata) -> Result<()>) -> Result<()> {
        Ok(())
    }

    fn expired(&self, _id: &Id, _meta: &Metadata) -> bool {
        false
    }

    fn write_chunk_file(&self, _id: &Id, _index: u32) -> Result<(BoxedWriter, PathBuf)> {
        Err(anyhow!("empty bucket"))
    }

    fn read_chunk_file(&self, _id: &Id, _index: u32) -> Result<(BoxedReader, PathBuf)> {
        Err(anyhow!("empty bucket"))
    }
}
