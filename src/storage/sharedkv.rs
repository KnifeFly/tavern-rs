use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use sled::Db;

use crate::storage::SharedKV;

pub struct SledSharedKV {
    db: Db,
}

impl SledSharedKV {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        let db = sled::open(path)?;
        Ok(Arc::new(Self { db }))
    }

    fn read_u32(val: &[u8]) -> u32 {
        if val.len() >= 4 {
            u32::from_be_bytes([val[0], val[1], val[2], val[3]])
        } else {
            0
        }
    }

    fn write_u32(val: u32) -> [u8; 4] {
        val.to_be_bytes()
    }
}

impl SharedKV for SledSharedKV {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let val = self
            .db
            .get(key)?
            .ok_or_else(|| anyhow!("key not found"))?;
        Ok(val.to_vec())
    }

    fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.db.insert(key, val)?;
        Ok(())
    }

    fn incr(&self, key: &[u8], delta: u32) -> Result<u32> {
        self.db.fetch_and_update(key, |old| {
            let cur = old.as_ref().map(|v| Self::read_u32(v)).unwrap_or(0);
            let next = cur.saturating_add(delta);
            Some(Self::write_u32(next).to_vec())
        })?;
        let val = self.db.get(key)?.ok_or_else(|| anyhow!("key not found"))?;
        Ok(Self::read_u32(&val))
    }

    fn decr(&self, key: &[u8], delta: u32) -> Result<u32> {
        self.db.fetch_and_update(key, |old| {
            let cur = old.as_ref().map(|v| Self::read_u32(v)).unwrap_or(0);
            let next = cur.saturating_sub(delta);
            Some(Self::write_u32(next).to_vec())
        })?;
        let val = self.db.get(key)?.ok_or_else(|| anyhow!("key not found"))?;
        Ok(Self::read_u32(&val))
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.remove(key)?;
        Ok(())
    }

    fn drop_prefix(&self, prefix: &[u8]) -> Result<()> {
        let keys: Vec<Vec<u8>> = self
            .db
            .scan_prefix(prefix)
            .keys()
            .filter_map(|res| res.ok().map(|k| k.to_vec()))
            .collect();
        for key in keys {
            let _ = self.db.remove(key);
        }
        Ok(())
    }

    fn iterate(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
        for item in self.db.iter() {
            let (key, val) = item?;
            f(&key, &val)?;
        }
        Ok(())
    }

    fn iterate_prefix(
        &self,
        prefix: &[u8],
        f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        for item in self.db.scan_prefix(prefix) {
            let (key, val) = item?;
            f(&key, &val)?;
        }
        Ok(())
    }
}
