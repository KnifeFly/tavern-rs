use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once, OnceLock};

use anyhow::{anyhow, Result};
use heed::types::Bytes;
use heed::EnvOpenOptions;
use redb::ReadableTable;
use rocksdb::{IteratorMode, Options, DB};
use sled::Db;

use crate::storage::{MemSharedKV, SharedKV};

type SharedKvOpener = fn(&Path) -> Result<Arc<dyn SharedKV>>;

fn registry() -> &'static Mutex<HashMap<String, SharedKvOpener>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, SharedKvOpener>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

pub fn register_shared_kv(name: &str, opener: SharedKvOpener) {
    let mut reg = registry().lock().expect("sharedkv registry");
    reg.insert(normalize_name(name), opener);
}

fn ensure_registry() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        register_shared_kv("sled", open_sled);
        register_shared_kv("rocksdb", open_rocks);
        register_shared_kv("redb", open_redb);
        register_shared_kv("lmdb", open_lmdb);
    });
}

pub fn open(path: &Path, db_type: &str) -> Result<Arc<dyn SharedKV>> {
    ensure_registry();
    let name = normalize_name(db_type);
    if name.is_empty() || name == "sled" {
        return open_sled(path);
    }
    if name == "memory" || name == "mem" {
        return Ok(MemSharedKV::new());
    }
    let reg = registry().lock().expect("sharedkv registry");
    if let Some(opener) = reg.get(&name) {
        return opener(path);
    }
    if let Some(opener) = reg.get("sled") {
        log::warn!("sharedkv type {name} not supported, fallback to sled");
        return opener(path);
    }
    Err(anyhow!("sharedkv registry not initialized"))
}

fn open_sled(path: &Path) -> Result<Arc<dyn SharedKV>> {
    SledSharedKV::open(path).map(|kv| kv as Arc<dyn SharedKV>)
}

fn open_rocks(path: &Path) -> Result<Arc<dyn SharedKV>> {
    RocksSharedKV::open(path).map(|kv| kv as Arc<dyn SharedKV>)
}

fn open_redb(path: &Path) -> Result<Arc<dyn SharedKV>> {
    RedbSharedKV::open(path).map(|kv| kv as Arc<dyn SharedKV>)
}

fn open_lmdb(path: &Path) -> Result<Arc<dyn SharedKV>> {
    LmdbSharedKV::open(path).map(|kv| kv as Arc<dyn SharedKV>)
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

pub struct SledSharedKV {
    db: Db,
}

impl SledSharedKV {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        let db = sled::open(path)?;
        Ok(Arc::new(Self { db }))
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
            let cur = old.as_ref().map(|v| read_u32(v)).unwrap_or(0);
            let next = cur.saturating_add(delta);
            Some(write_u32(next).to_vec())
        })?;
        let val = self.db.get(key)?.ok_or_else(|| anyhow!("key not found"))?;
        Ok(read_u32(&val))
    }

    fn decr(&self, key: &[u8], delta: u32) -> Result<u32> {
        self.db.fetch_and_update(key, |old| {
            let cur = old.as_ref().map(|v| read_u32(v)).unwrap_or(0);
            let next = cur.saturating_sub(delta);
            Some(write_u32(next).to_vec())
        })?;
        let val = self.db.get(key)?.ok_or_else(|| anyhow!("key not found"))?;
        Ok(read_u32(&val))
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

pub struct RocksSharedKV {
    db: DB,
    io_lock: Mutex<()>,
}

impl RocksSharedKV {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        std::fs::create_dir_all(path)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(Arc::new(Self {
            db,
            io_lock: Mutex::new(()),
        }))
    }
}

impl SharedKV for RocksSharedKV {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let val = self
            .db
            .get(key)?
            .ok_or_else(|| anyhow!("key not found"))?;
        Ok(val.to_vec())
    }

    fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.db.put(key, val)?;
        Ok(())
    }

    fn incr(&self, key: &[u8], delta: u32) -> Result<u32> {
        let _guard = self.io_lock.lock().expect("sharedkv");
        let cur = self.db.get(key)?.map(|v| read_u32(&v)).unwrap_or(0);
        let next = cur.saturating_add(delta);
        self.db.put(key, write_u32(next))?;
        Ok(next)
    }

    fn decr(&self, key: &[u8], delta: u32) -> Result<u32> {
        let _guard = self.io_lock.lock().expect("sharedkv");
        let cur = self.db.get(key)?.map(|v| read_u32(&v)).unwrap_or(0);
        let next = cur.saturating_sub(delta);
        self.db.put(key, write_u32(next))?;
        Ok(next)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key)?;
        Ok(())
    }

    fn drop_prefix(&self, prefix: &[u8]) -> Result<()> {
        let mut keys = Vec::new();
        for item in self.db.iterator(IteratorMode::Start) {
            let (key, _) = item?;
            if key.starts_with(prefix) {
                keys.push(key.to_vec());
            }
        }
        for key in keys {
            let _ = self.db.delete(key);
        }
        Ok(())
    }

    fn iterate(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
        for item in self.db.iterator(IteratorMode::Start) {
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
        for item in self.db.iterator(IteratorMode::Start) {
            let (key, val) = item?;
            if key.starts_with(prefix) {
                f(&key, &val)?;
            }
        }
        Ok(())
    }
}

const REDB_SHARED_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("shared_kv");

pub struct RedbSharedKV {
    db: redb::Database,
}

impl RedbSharedKV {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        let path = redb_path(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = redb::Database::open(&path).or_else(|_| redb::Database::create(&path))?;
        {
            let write_txn = db.begin_write()?;
            let _ = write_txn.open_table(REDB_SHARED_TABLE)?;
            write_txn.commit()?;
        }
        Ok(Arc::new(Self { db }))
    }
}

impl SharedKV for RedbSharedKV {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_SHARED_TABLE)?;
        let val = table.get(key)?.ok_or_else(|| anyhow!("key not found"))?;
        Ok(val.value().to_vec())
    }

    fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REDB_SHARED_TABLE)?;
            table.insert(key, val)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn incr(&self, key: &[u8], delta: u32) -> Result<u32> {
        let write_txn = self.db.begin_write()?;
        let next = {
            let mut table = write_txn.open_table(REDB_SHARED_TABLE)?;
            let cur = table
                .get(key)?
                .map(|v| read_u32(v.value()))
                .unwrap_or(0);
            let next = cur.saturating_add(delta);
            table.insert(key, write_u32(next).as_slice())?;
            next
        };
        write_txn.commit()?;
        Ok(next)
    }

    fn decr(&self, key: &[u8], delta: u32) -> Result<u32> {
        let write_txn = self.db.begin_write()?;
        let next = {
            let mut table = write_txn.open_table(REDB_SHARED_TABLE)?;
            let cur = table
                .get(key)?
                .map(|v| read_u32(v.value()))
                .unwrap_or(0);
            let next = cur.saturating_sub(delta);
            table.insert(key, write_u32(next).as_slice())?;
            next
        };
        write_txn.commit()?;
        Ok(next)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REDB_SHARED_TABLE)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn drop_prefix(&self, prefix: &[u8]) -> Result<()> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_SHARED_TABLE)?;
        let mut keys = Vec::new();
        for item in table.iter()? {
            let (key, _) = item?;
            if key.value().starts_with(prefix) {
                keys.push(key.value().to_vec());
            }
        }
        drop(table);
        drop(read_txn);
        if keys.is_empty() {
            return Ok(());
        }
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REDB_SHARED_TABLE)?;
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    fn iterate(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_SHARED_TABLE)?;
        for item in table.iter()? {
            let (key, val) = item?;
            f(key.value(), val.value())?;
        }
        Ok(())
    }

    fn iterate_prefix(
        &self,
        prefix: &[u8],
        f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_SHARED_TABLE)?;
        for item in table.iter()? {
            let (key, val) = item?;
            let key = key.value();
            if key.starts_with(prefix) {
                f(key, val.value())?;
            }
        }
        Ok(())
    }
}

pub struct LmdbSharedKV {
    env: heed::Env,
    db: heed::Database<Bytes, Bytes>,
}

impl LmdbSharedKV {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        std::fs::create_dir_all(path)?;
        let env = unsafe { EnvOpenOptions::new().max_dbs(4).open(path)? };
        let db = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database(&mut wtxn, Some("shared_kv"))?;
            wtxn.commit()?;
            db
        };
        Ok(Arc::new(Self { env, db }))
    }
}

impl SharedKV for LmdbSharedKV {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let rtxn = self.env.read_txn()?;
        let val = self
            .db
            .get(&rtxn, key)?
            .ok_or_else(|| anyhow!("key not found"))?;
        Ok(val.to_vec())
    }

    fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        self.db.put(&mut wtxn, key, val)?;
        wtxn.commit()?;
        Ok(())
    }

    fn incr(&self, key: &[u8], delta: u32) -> Result<u32> {
        let mut wtxn = self.env.write_txn()?;
        let cur = self.db.get(&wtxn, key)?.map(read_u32).unwrap_or(0);
        let next = cur.saturating_add(delta);
        self.db.put(&mut wtxn, key, write_u32(next).as_slice())?;
        wtxn.commit()?;
        Ok(next)
    }

    fn decr(&self, key: &[u8], delta: u32) -> Result<u32> {
        let mut wtxn = self.env.write_txn()?;
        let cur = self.db.get(&wtxn, key)?.map(read_u32).unwrap_or(0);
        let next = cur.saturating_sub(delta);
        self.db.put(&mut wtxn, key, write_u32(next).as_slice())?;
        wtxn.commit()?;
        Ok(next)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        self.db.delete(&mut wtxn, key)?;
        wtxn.commit()?;
        Ok(())
    }

    fn drop_prefix(&self, prefix: &[u8]) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        let mut iter = self.db.iter(&rtxn)?;
        let mut keys = Vec::new();
        while let Some(item) = iter.next() {
            let (key, _) = item?;
            if key.starts_with(prefix) {
                keys.push(key.to_vec());
            }
        }
        drop(iter);
        drop(rtxn);
        if keys.is_empty() {
            return Ok(());
        }
        let mut wtxn = self.env.write_txn()?;
        for key in keys {
            let _ = self.db.delete(&mut wtxn, key.as_slice());
        }
        wtxn.commit()?;
        Ok(())
    }

    fn iterate(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        let mut iter = self.db.iter(&rtxn)?;
        while let Some(item) = iter.next() {
            let (key, val) = item?;
            f(key, val)?;
        }
        Ok(())
    }

    fn iterate_prefix(
        &self,
        prefix: &[u8],
        f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        let mut iter = self.db.iter(&rtxn)?;
        while let Some(item) = iter.next() {
            let (key, val) = item?;
            if key.starts_with(prefix) {
                f(key, val)?;
            }
        }
        Ok(())
    }
}

fn redb_path(path: &Path) -> PathBuf {
    if path.extension().is_some() {
        path.to_path_buf()
    } else {
        path.join("shared_kv.redb")
    }
}
