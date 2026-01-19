use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once, OnceLock};

use anyhow::{anyhow, Result};
use heed::types::Bytes;
use heed::EnvOpenOptions;
use redb::{ReadableTable, ReadableTableMetadata};
use rocksdb::{FlushOptions, IteratorMode, Options, DB};
use sled::Db;

use crate::storage::object::Metadata;

pub trait IndexDB: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Metadata>>;
    fn set(&self, key: &[u8], val: &Metadata) -> Result<()>;
    fn exist(&self, key: &[u8]) -> Result<bool>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn iterate(
        &self,
        prefix: Option<&[u8]>,
        f: &mut dyn FnMut(&[u8], &Metadata) -> bool,
    ) -> Result<()>;
    fn expired(&self, now: i64, f: &mut dyn FnMut(&[u8], &Metadata) -> bool) -> Result<()>;
    fn gc(&self) -> Result<()>;
    fn len(&self) -> Result<usize>;
}

type IndexDbOpener = fn(&Path) -> Result<Arc<dyn IndexDB>>;

fn registry() -> &'static Mutex<HashMap<String, IndexDbOpener>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, IndexDbOpener>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

pub fn register_indexdb(name: &str, opener: IndexDbOpener) {
    let mut reg = registry().lock().expect("indexdb registry");
    reg.insert(normalize_name(name), opener);
}

fn ensure_registry() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        register_indexdb("sled", open_sled);
        register_indexdb("rocksdb", open_rocks);
        register_indexdb("redb", open_redb);
        register_indexdb("lmdb", open_lmdb);
    });
}

pub fn open(path: &Path, db_type: &str) -> Result<Arc<dyn IndexDB>> {
    ensure_registry();
    let name = normalize_name(db_type);
    if name.is_empty() || name == "sled" {
        return open_sled(path);
    }
    if name == "memory" || name == "mem" {
        return SledIndexDB::temporary().map(|db| db as Arc<dyn IndexDB>);
    }
    let reg = registry().lock().expect("indexdb registry");
    if let Some(opener) = reg.get(&name) {
        return opener(path);
    }
    if let Some(opener) = reg.get("sled") {
        log::warn!("indexdb type {name} not supported, fallback to sled");
        return opener(path);
    }
    Err(anyhow!("indexdb registry not initialized"))
}

fn open_sled(path: &Path) -> Result<Arc<dyn IndexDB>> {
    SledIndexDB::open(path).map(|db| db as Arc<dyn IndexDB>)
}

fn open_rocks(path: &Path) -> Result<Arc<dyn IndexDB>> {
    RocksIndexDB::open(path).map(|db| db as Arc<dyn IndexDB>)
}

fn open_redb(path: &Path) -> Result<Arc<dyn IndexDB>> {
    RedbIndexDB::open(path).map(|db| db as Arc<dyn IndexDB>)
}

fn open_lmdb(path: &Path) -> Result<Arc<dyn IndexDB>> {
    LmdbIndexDB::open(path).map(|db| db as Arc<dyn IndexDB>)
}

fn encode_meta(meta: &Metadata) -> Result<Vec<u8>> {
    serde_json::to_vec(meta).map_err(|err| anyhow!("encode metadata: {err}"))
}

fn decode_meta(raw: &[u8]) -> Result<Metadata> {
    serde_json::from_slice(raw).map_err(|err| anyhow!("decode metadata: {err}"))
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
}

impl IndexDB for SledIndexDB {
    fn get(&self, key: &[u8]) -> Result<Option<Metadata>> {
        let val = match self.db.get(key)? {
            Some(val) => val,
            None => return Ok(None),
        };
        Ok(Some(decode_meta(&val)?))
    }

    fn set(&self, key: &[u8], val: &Metadata) -> Result<()> {
        let raw = encode_meta(val)?;
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
            let meta = decode_meta(&val)?;
            if !f(&key, &meta) {
                break;
            }
        }
        Ok(())
    }

    fn expired(&self, now: i64, f: &mut dyn FnMut(&[u8], &Metadata) -> bool) -> Result<()> {
        for item in self.db.iter() {
            let (key, val) = item?;
            let meta = decode_meta(&val)?;
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

pub struct RocksIndexDB {
    db: DB,
}

impl RocksIndexDB {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        std::fs::create_dir_all(path)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(Arc::new(Self { db }))
    }
}

impl IndexDB for RocksIndexDB {
    fn get(&self, key: &[u8]) -> Result<Option<Metadata>> {
        let val = match self.db.get(key)? {
            Some(val) => val,
            None => return Ok(None),
        };
        Ok(Some(decode_meta(&val)?))
    }

    fn set(&self, key: &[u8], val: &Metadata) -> Result<()> {
        let raw = encode_meta(val)?;
        self.db.put(key, raw)?;
        Ok(())
    }

    fn exist(&self, key: &[u8]) -> Result<bool> {
        Ok(self.db.get(key)?.is_some())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key)?;
        Ok(())
    }

    fn iterate(
        &self,
        prefix: Option<&[u8]>,
        f: &mut dyn FnMut(&[u8], &Metadata) -> bool,
    ) -> Result<()> {
        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            let (key, val) = item?;
            if let Some(prefix) = prefix {
                if !key.starts_with(prefix) {
                    continue;
                }
            }
            let meta = decode_meta(&val)?;
            if !f(&key, &meta) {
                break;
            }
        }
        Ok(())
    }

    fn expired(&self, now: i64, f: &mut dyn FnMut(&[u8], &Metadata) -> bool) -> Result<()> {
        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            let (key, val) = item?;
            let meta = decode_meta(&val)?;
            if meta.expires_at > 0 && meta.expires_at <= now {
                if !f(&key, &meta) {
                    break;
                }
            }
        }
        Ok(())
    }

    fn gc(&self) -> Result<()> {
        let mut opts = FlushOptions::default();
        opts.set_wait(true);
        self.db.flush_opt(&opts)?;
        Ok(())
    }

    fn len(&self) -> Result<usize> {
        if let Ok(Some(val)) = self.db.property_int_value("rocksdb.estimate-num-keys") {
            return Ok(val as usize);
        }
        let mut count = 0usize;
        for _ in self.db.iterator(IteratorMode::Start) {
            count += 1;
        }
        Ok(count)
    }
}

const REDB_METADATA_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("metadata");

pub struct RedbIndexDB {
    db: redb::Database,
}

impl RedbIndexDB {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        let path = redb_path(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = redb::Database::open(&path).or_else(|_| redb::Database::create(&path))?;
        {
            let write_txn = db.begin_write()?;
            let _ = write_txn.open_table(REDB_METADATA_TABLE)?;
            write_txn.commit()?;
        }
        Ok(Arc::new(Self { db }))
    }
}

impl IndexDB for RedbIndexDB {
    fn get(&self, key: &[u8]) -> Result<Option<Metadata>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_METADATA_TABLE)?;
        if let Some(val) = table.get(key)? {
            return Ok(Some(decode_meta(val.value())?));
        }
        Ok(None)
    }

    fn set(&self, key: &[u8], val: &Metadata) -> Result<()> {
        let raw = encode_meta(val)?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REDB_METADATA_TABLE)?;
            table.insert(key, raw.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn exist(&self, key: &[u8]) -> Result<bool> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_METADATA_TABLE)?;
        Ok(table.get(key)?.is_some())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REDB_METADATA_TABLE)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn iterate(
        &self,
        prefix: Option<&[u8]>,
        f: &mut dyn FnMut(&[u8], &Metadata) -> bool,
    ) -> Result<()> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_METADATA_TABLE)?;
        for item in table.iter()? {
            let (key, val) = item?;
            let key = key.value();
            if let Some(prefix) = prefix {
                if !key.starts_with(prefix) {
                    continue;
                }
            }
            let meta = decode_meta(val.value())?;
            if !f(key, &meta) {
                break;
            }
        }
        Ok(())
    }

    fn expired(&self, now: i64, f: &mut dyn FnMut(&[u8], &Metadata) -> bool) -> Result<()> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_METADATA_TABLE)?;
        for item in table.iter()? {
            let (key, val) = item?;
            let meta = decode_meta(val.value())?;
            if meta.expires_at > 0 && meta.expires_at <= now {
                if !f(key.value(), &meta) {
                    break;
                }
            }
        }
        Ok(())
    }

    fn gc(&self) -> Result<()> {
        Ok(())
    }

    fn len(&self) -> Result<usize> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_METADATA_TABLE)?;
        Ok(table.len()? as usize)
    }
}

pub struct LmdbIndexDB {
    env: heed::Env,
    db: heed::Database<Bytes, Bytes>,
}

impl LmdbIndexDB {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        std::fs::create_dir_all(path)?;
        let env = unsafe { EnvOpenOptions::new().max_dbs(4).open(path)? };
        let db = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database(&mut wtxn, Some("metadata"))?;
            wtxn.commit()?;
            db
        };
        Ok(Arc::new(Self { env, db }))
    }
}

impl IndexDB for LmdbIndexDB {
    fn get(&self, key: &[u8]) -> Result<Option<Metadata>> {
        let rtxn = self.env.read_txn()?;
        match self.db.get(&rtxn, key)? {
            Some(val) => Ok(Some(decode_meta(val)?)),
            None => Ok(None),
        }
    }

    fn set(&self, key: &[u8], val: &Metadata) -> Result<()> {
        let raw = encode_meta(val)?;
        let mut wtxn = self.env.write_txn()?;
        self.db.put(&mut wtxn, key, raw.as_slice())?;
        wtxn.commit()?;
        Ok(())
    }

    fn exist(&self, key: &[u8]) -> Result<bool> {
        let rtxn = self.env.read_txn()?;
        Ok(self.db.get(&rtxn, key)?.is_some())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        self.db.delete(&mut wtxn, key)?;
        wtxn.commit()?;
        Ok(())
    }

    fn iterate(
        &self,
        prefix: Option<&[u8]>,
        f: &mut dyn FnMut(&[u8], &Metadata) -> bool,
    ) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        let mut iter = self.db.iter(&rtxn)?;
        while let Some(item) = iter.next() {
            let (key, val) = item?;
            if let Some(prefix) = prefix {
                if !key.starts_with(prefix) {
                    continue;
                }
            }
            let meta = decode_meta(val)?;
            if !f(key, &meta) {
                break;
            }
        }
        Ok(())
    }

    fn expired(&self, now: i64, f: &mut dyn FnMut(&[u8], &Metadata) -> bool) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        let mut iter = self.db.iter(&rtxn)?;
        while let Some(item) = iter.next() {
            let (key, val) = item?;
            let meta = decode_meta(val)?;
            if meta.expires_at > 0 && meta.expires_at <= now {
                if !f(key, &meta) {
                    break;
                }
            }
        }
        Ok(())
    }

    fn gc(&self) -> Result<()> {
        self.env.force_sync()?;
        Ok(())
    }

    fn len(&self) -> Result<usize> {
        let rtxn = self.env.read_txn()?;
        Ok(self.db.len(&rtxn)? as usize)
    }
}

fn redb_path(path: &Path) -> PathBuf {
    if path.extension().is_some() {
        path.to_path_buf()
    } else {
        path.join("redb.db")
    }
}
