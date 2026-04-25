# 跨进程共享缓存设计边界

本文定义跨进程共享缓存的接口和一致性要求。本轮不实现 mmap/shm 数据结构，也不改变现有 `CacheStore` 行为。

## 目标

- 允许同一机器上的多个 Tavern 进程共享热点对象元数据和小对象 body。
- 保持现有 storage bucket 作为权威落盘数据源。
- 进程崩溃后，其他进程可以检测并回收失效锁和未完成写入。

## 非目标

- 不替代磁盘 bucket 和 IndexDB。
- 不提供跨机器共享缓存。
- 不要求动态扩容共享内存段；容量变化可以通过重启或后续迁移机制处理。

## 接口草案

未来实现应落在独立 trait 后面，避免把 mmap 细节泄漏给缓存中间件：

```rust
pub trait SharedObjectCache: Send + Sync {
    fn get(&self, key: &str) -> anyhow::Result<Option<SharedObject>>;
    fn insert(&self, key: &str, object: SharedObject) -> anyhow::Result<()>;
    fn remove(&self, key: &str) -> anyhow::Result<bool>;
    fn expire(&self, key: &str, expires_at_unix: i64) -> anyhow::Result<bool>;
    fn stats(&self) -> SharedCacheStats;
}
```

`SharedObject` 至少包含：

- cache key
- status code
- response headers
- body offset/length 或 inline body
- created/expires unix time
- chunk size 和完整性标记
- Vary base/variant 关系

## 一致性模型

- 写入采用 two-phase 状态：`Writing` -> `Committed`。
- 读路径只读取 `Committed` 对象。
- 每条记录带 writer pid、generation 和 last_heartbeat。
- 进程启动时扫描 `Writing` 且 heartbeat 过期的记录，标记为 free。
- 删除和过期需要同时处理 base key 与 Vary variant 索引。

## 锁与恢复

- 元数据表使用分段锁，避免全局锁阻塞所有 key。
- 大 body 区域使用 free-list 或 slab class 管理。
- 锁记录必须包含 owner pid；发现 owner 不存在时允许抢占。
- 所有偏移和长度必须校验边界，避免损坏共享段导致越界读取。

## 与现有模块的关系

- `CacheStore` 继续作为进程内 L1。
- `SharedObjectCache` 作为可选 L2，位于 `CacheStore` 和 storage bucket 之间。
- storage bucket 仍是持久化权威；共享缓存可丢失、可重建。
- 后台 GC 需要同时调用共享缓存的 expire/remove 接口。

## 后续实施建议

1. 先实现纯内存 mock `SharedObjectCache`，用同一 trait 验证缓存路径。
2. 再实现 mmap 元数据区，只共享 headers 和小对象 body。
3. 最后引入大对象分片共享、崩溃恢复扫描和指标。
