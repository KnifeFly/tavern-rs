use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use sled::Db;

use crate::storage::object::Metadata;

pub trait IndexDB: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Metadata>>;
    fn set(&self, key: &[u8], val: &Metadata) -> Result<()>;
    fn exist(&self, key: &[u8]) -> Result<bool>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn iterate(&self, prefix: Option<&[u8]>, f: &mut dyn FnMut(&[u8], &Metadata) -> bool)
        -> Result<()>;
    fn expired(&self, now: i64, f: &mut dyn FnMut(&[u8], &Metadata) -> bool) -> Result<()>;
    fn gc(&self) -> Result<()>;
    fn len(&self) -> Result<usize>;
}

pub struct SledIndexDB {
    db: Db,
}

impl SledIndexDB {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        let db = sled::open(path)?;
        Ok(Arc::new(Self { db }))
    }

    pub fn temporary() -> Result<Arc<Self>> {
        let db = sled::Config::new().temporary(true).open()?;
        Ok(Arc::new(Self { db }))
    }

    fn encode(meta: &Metadata) -> Result<Vec<u8>> {
        serde_json::to_vec(meta).map_err(|err| anyhow!("encode metadata: {err}"))
    }

    fn decode(raw: &[u8]) -> Result<Metadata> {
        serde_json::from_slice(raw).map_err(|err| anyhow!("decode metadata: {err}"))
    }
}

impl IndexDB for SledIndexDB {
    fn get(&self, key: &[u8]) -> Result<Option<Metadata>> {
        let val = match self.db.get(key)? {
            Some(val) => val,
            None => return Ok(None),
        };
        Ok(Some(Self::decode(&val)?))
    }

    fn set(&self, key: &[u8], val: &Metadata) -> Result<()> {
        let raw = Self::encode(val)?;
        self.db.insert(key, raw)?;
        Ok(())
    }

    fn exist(&self, key: &[u8]) -> Result<bool> {
        Ok(self.db.contains_key(key)?)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.remove(key)?;
        Ok(())
    }

    fn iterate(
        &self,
        prefix: Option<&[u8]>,
        f: &mut dyn FnMut(&[u8], &Metadata) -> bool,
    ) -> Result<()> {
        let iter: Box<dyn Iterator<Item = Result<(sled::IVec, sled::IVec), sled::Error>>> =
            match prefix {
                Some(prefix) => Box::new(self.db.scan_prefix(prefix)),
                None => Box::new(self.db.iter()),
            };
        for item in iter {
            let (key, val) = item?;
            let meta = Self::decode(&val)?;
            if !f(&key, &meta) {
                break;
            }
        }
        Ok(())
    }

    fn expired(&self, now: i64, f: &mut dyn FnMut(&[u8], &Metadata) -> bool) -> Result<()> {
        for item in self.db.iter() {
            let (key, val) = item?;
            let meta = Self::decode(&val)?;
            if meta.expires_at > 0 && meta.expires_at <= now {
                if !f(&key, &meta) {
                    break;
                }
            }
        }
        Ok(())
    }

    fn gc(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }

    fn len(&self) -> Result<usize> {
        Ok(self.db.len())
    }
}

pub fn open(path: &Path, db_type: &str) -> Result<Arc<dyn IndexDB>> {
    match db_type {
        "memory" | "mem" => Ok(SledIndexDB::temporary()?),
        "pebble" | "nutsdb" | "sled" | "" => Ok(SledIndexDB::open(path)?),
        _ => Ok(SledIndexDB::open(path)?),
    }
}
