use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

pub const ID_HASH_SIZE: usize = 20;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct IdHash(pub [u8; ID_HASH_SIZE]);

impl IdHash {
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }

    pub fn wpath(&self, root: &Path) -> PathBuf {
        let hash = hex::encode(self.0);
        root.join(&hash[0..1]).join(&hash[2..4]).join(&hash)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Id {
    path: String,
    ext: String,
    hash: IdHash,
    cache_id: String,
}

impl Id {
    pub fn new(path: &str) -> Self {
        let hash = sha1_hash(path.as_bytes());
        let cache_id = format!("{{{}:{}{}}}", hash.to_hex(), path, "");
        Self {
            path: path.to_string(),
            ext: String::new(),
            hash,
            cache_id,
        }
    }

    pub fn new_virtual(path: &str, virtual_key: &str) -> Self {
        let mut combined = String::with_capacity(path.len() + virtual_key.len());
        combined.push_str(path);
        combined.push_str(virtual_key);
        let hash = sha1_hash(combined.as_bytes());
        let cache_id = format!("{{{}:{}{}}}", hash.to_hex(), path, virtual_key);
        Self {
            path: path.to_string(),
            ext: virtual_key.to_string(),
            hash,
            cache_id,
        }
    }

    pub fn key(&self) -> String {
        format!("{}{}", self.path, self.ext)
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn ext(&self) -> &str {
        &self.ext
    }

    pub fn hash(&self) -> IdHash {
        self.hash
    }

    pub fn hash_str(&self) -> String {
        self.hash.to_hex()
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.hash.0.to_vec()
    }

    pub fn wpath(&self, root: &Path) -> PathBuf {
        self.hash.wpath(root)
    }

    pub fn wpath_slice(&self, root: &Path, slice_index: u32) -> PathBuf {
        let hash = self.hash.to_hex();
        root.join(&hash[0..1])
            .join(&hash[2..4])
            .join(format!("{hash}-{slice_index:06}"))
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.cache_id)
    }
}

fn sha1_hash(input: &[u8]) -> IdHash {
    let mut hasher = Sha1::new();
    hasher.update(input);
    let result = hasher.finalize();
    let mut hash = [0u8; ID_HASH_SIZE];
    hash.copy_from_slice(&result[..]);
    IdHash(hash)
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct CacheFlag(u8);

impl CacheFlag {
    pub const CACHE: CacheFlag = CacheFlag(0);
    pub const VARY_INDEX: CacheFlag = CacheFlag(1 << 0);
    pub const VARY_CACHE: CacheFlag = CacheFlag(1 << 1);
    pub const CHUNKED_CACHE: CacheFlag = CacheFlag(1 << 2);

    pub fn contains(self, other: CacheFlag) -> bool {
        (self.0 & other.0) != 0
    }

    pub fn is_vary(self) -> bool {
        self == CacheFlag::VARY_INDEX
    }

    pub fn is_vary_cache(self) -> bool {
        self.contains(CacheFlag::VARY_CACHE)
    }

    pub fn is_chunked(self) -> bool {
        self.contains(CacheFlag::CHUNKED_CACHE)
    }

    pub fn is_vary_chunked(self) -> bool {
        self.is_vary_cache() && self.is_chunked()
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ChunkSet {
    inner: BTreeSet<u32>,
}

impl ChunkSet {
    pub fn insert(&mut self, value: u32) {
        self.inner.insert(value);
    }

    pub fn contains(&self, value: u32) -> bool {
        self.inner.contains(&value)
    }

    pub fn count(&self) -> usize {
        self.inner.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &u32> {
        self.inner.iter()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub flags: CacheFlag,
    pub id: Id,
    pub block_size: u64,
    pub chunks: ChunkSet,
    pub parts: ChunkSet,
    pub code: i32,
    pub size: u64,
    pub resp_unix: i64,
    pub last_ref_unix: i64,
    pub refs: i64,
    pub expires_at: i64,
    pub headers: Vec<(String, String)>,
    pub virtual_key: Vec<String>,
}

impl Metadata {
    pub fn has_complete(&self) -> bool {
        if self.flags.is_vary() {
            return false;
        }
        if self.size == 0 || self.block_size == 0 {
            return false;
        }
        let mut n = self.size / self.block_size;
        if self.size % self.block_size != 0 {
            n += 1;
        }
        n as usize == self.chunks.count()
    }
}
