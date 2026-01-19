use indexmap::IndexMap;

use crate::storage::object::IdHash;

pub struct LruTracker {
    max: Option<usize>,
    order: IndexMap<IdHash, ()>,
}

impl LruTracker {
    pub fn new(max: Option<usize>) -> Self {
        Self {
            max: max.filter(|v| *v > 0),
            order: IndexMap::new(),
        }
    }

    pub fn touch(&mut self, key: IdHash) -> Vec<IdHash> {
        self.insert_or_bump(key);
        self.evict_overflow()
    }

    pub fn touch_no_evict(&mut self, key: IdHash) {
        self.insert_or_bump(key);
    }

    pub fn evict_overflow(&mut self) -> Vec<IdHash> {
        let Some(max) = self.max else {
            return Vec::new();
        };
        let mut evicted = Vec::new();
        while self.order.len() > max {
            if let Some((key, _)) = self.order.shift_remove_index(0) {
                evicted.push(key);
            } else {
                break;
            }
        }
        evicted
    }

    pub fn remove(&mut self, key: &IdHash) {
        let _ = self.order.shift_remove(key);
    }

    fn insert_or_bump(&mut self, key: IdHash) {
        if self.order.contains_key(&key) {
            let _ = self.order.shift_remove(&key);
        }
        self.order.insert(key, ());
    }
}
