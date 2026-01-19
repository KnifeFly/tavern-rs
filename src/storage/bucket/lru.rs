use std::collections::HashMap;

use indexmap::IndexMap;

use crate::storage::object::IdHash;

#[derive(Clone, Copy, Debug)]
pub enum EvictionPolicy {
    Lru,
    Fifo,
    Lfu,
}

pub struct EvictionTracker {
    policy: EvictionPolicy,
    max: Option<usize>,
    order: IndexMap<IdHash, ()>,
    lfu: HashMap<IdHash, (u64, u64)>,
    counter: u64,
}

impl EvictionTracker {
    pub fn new(policy: EvictionPolicy, max: Option<usize>) -> Self {
        Self {
            policy,
            max: max.filter(|v| *v > 0),
            order: IndexMap::new(),
            lfu: HashMap::new(),
            counter: 0,
        }
    }

    pub fn touch(&mut self, key: IdHash) -> Vec<IdHash> {
        self.touch_inner(key);
        self.evict_overflow()
    }

    pub fn touch_no_evict(&mut self, key: IdHash) {
        self.touch_inner(key);
    }

    pub fn evict_overflow(&mut self) -> Vec<IdHash> {
        let Some(max) = self.max else {
            return Vec::new();
        };
        let mut evicted = Vec::new();
        while self.len() > max {
            if let Some(key) = self.pop_oldest() {
                evicted.push(key);
            } else {
                break;
            }
        }
        evicted
    }

    pub fn remove(&mut self, key: &IdHash) {
        match self.policy {
            EvictionPolicy::Lru | EvictionPolicy::Fifo => {
                let _ = self.order.shift_remove(key);
            }
            EvictionPolicy::Lfu => {
                self.lfu.remove(key);
            }
        }
    }

    fn touch_inner(&mut self, key: IdHash) {
        match self.policy {
            EvictionPolicy::Lru => {
                if self.order.contains_key(&key) {
                    let _ = self.order.shift_remove(&key);
                }
                self.order.insert(key, ());
            }
            EvictionPolicy::Fifo => {
                if !self.order.contains_key(&key) {
                    self.order.insert(key, ());
                }
            }
            EvictionPolicy::Lfu => {
                let entry = self.lfu.entry(key).or_insert_with(|| {
                    let order = self.counter;
                    self.counter = self.counter.saturating_add(1);
                    (0, order)
                });
                entry.0 = entry.0.saturating_add(1);
            }
        }
    }

    fn len(&self) -> usize {
        match self.policy {
            EvictionPolicy::Lru | EvictionPolicy::Fifo => self.order.len(),
            EvictionPolicy::Lfu => self.lfu.len(),
        }
    }

    fn pop_oldest(&mut self) -> Option<IdHash> {
        match self.policy {
            EvictionPolicy::Lru | EvictionPolicy::Fifo => self.order.shift_remove_index(0).map(|v| v.0),
            EvictionPolicy::Lfu => {
                let mut selected: Option<(IdHash, u64, u64)> = None;
                for (key, (count, order)) in self.lfu.iter() {
                    match selected {
                        None => selected = Some((*key, *count, *order)),
                        Some((_, best_count, best_order)) => {
                            if *count < best_count || (*count == best_count && *order < best_order) {
                                selected = Some((*key, *count, *order));
                            }
                        }
                    }
                }
                if let Some((key, _, _)) = selected {
                    self.lfu.remove(&key);
                    return Some(key);
                }
                None
            }
        }
    }
}
