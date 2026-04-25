# 功能缺口分析

本文基于当前仓库代码、集成测试和配置结构整理。

## 已覆盖能力

| 领域 | 状态 | 说明 |
| --- | --- | --- |
| HTTP 服务 | 已实现 | TCP/Unix socket、HTTP/1.1/HTTP/2、基础超时、优雅升级。 |
| 上游代理 | 已实现 | HTTP/HTTPS 上游、连接池、每上游连接限制、TLS 校验开关。 |
| 负载均衡 | 已实现 | round-robin、random、weighted round-robin；配置支持节点权重。 |
| GET/HEAD 缓存 | 已实现 | TTL、内存缓存、落盘存储、HEAD 空 body 返回。 |
| Range 缓存 | 已实现 | 单 Range 解析、分片、部分命中、溢出处理、回源填充。 |
| 多 Range | 部分实现 | 可合并第一段或拼 multipart 响应；没有和缓存层深度协同。 |
| 重验证 | 已实现 | ETag/Last-Modified/304；本轮补齐 revalidate miss 回写 storage。 |
| Vary | 已实现 | 内存 vary index、vary key、落盘变体索引和 GC 清理。 |
| 缓存管理 | 已实现 | `PURGE`、`/cache/push` prefetch/expire/delete；本轮补齐内存缓存同步。 |
| 存储 | 已实现 | memory/disk/empty bucket，sled/rocksdb/redb/lmdb 索引。 |
| 热冷分层 | 已实现 | 提升、降级、write-through、迁移队列和指标。 |
| 指标 | 已实现 | Prometheus 文本指标。 |
| 访问日志 | 已实现 | 文件/stdout、分钟轮转、可选 AES-GCM 行加密。 |
| 插件 | 已实现 | 编译期注册内置插件；支持 C ABI v1 动态库加载。 |
| 文档 | 本轮补齐 | README、设计文档、使用文档和缺口分析。 |

## 本轮修复的缺口

### 非 GET/HEAD 请求体旁路代理

原行为：

- caching 中间件只允许 GET/HEAD。
- upstream client 构造请求时 body 总是空。

修复：

- 非 GET/HEAD 请求直接绕过缓存读写，调用后续 round tripper。
- 原始 request body 以 stream 形式转发给 upstream。
- 响应标记 `X-Cache: BYPASS`。

### 旁路响应流式转发

原行为：

- 代理回源响应统一通过 `BodyExt::collect()` 读完整 body。

修复：

- 服务响应体统一为 boxed body。
- 旁路响应直接转发 upstream `Incoming` body。
- 可缓存 GET/HEAD miss 仍保留完整 collect，用于维持现有缓存写入逻辑。

### 上游权重配置

修复：

- `upstream.address` 兼容字符串和 `{ address, weight }` 写法。
- `weight` 默认 1，0 会启动失败。
- DNS resolve 后的节点继承配置权重。

### 后台 GC 和 Vary 落盘清理

修复：

- 新增 `storage.gc.enabled`、`interval_secs`、`batch_size`。
- native storage 实现 `gc_expired`。
- Vary 变体写入 `SharedKV` 辅助索引 `vary/{base_key}`。
- purge 和 GC 会同步删除 base key、variant、目录索引和 vary 索引。

### 动态插件 ABI

修复：

- 新增 `plugin[].library`。
- 动态库通过 `tavern_plugin_create_v1` 暴露 C ABI v1。
- 宿主保存 dynamic library handle，使用 JSON 交换配置和请求/响应元数据。

### 共享缓存设计

修复：

- 新增 `docs/shared-cache-design.md`。
- 明确共享缓存的 trait、元数据、一致性、锁和崩溃恢复边界。

### `/cache/push` 未同步内存缓存

原行为：

- `delete` 和 `expire` 只调用 storage purge。
- 进程内 `CacheStore` 仍保留旧对象。
- 删除后下一次请求可能直接 `HIT` 旧对象，无法触发回源。

修复：

- 增加 `CachingState::cache_key_for_url`，管理接口和正常请求共用 cache key 规则。
- 增加 `CachingState::remove_url` 和 `CachingState::expire_url`。
- `delete` 同时删除内存缓存和 storage。
- `expire` 同时标记内存缓存和 storage 过期。
- 新增 `tests/push.rs` 覆盖 delete/expire。

### 单对象 expire 被当成 hard delete

原行为：

- `NativeStorage::purge_single` 不看 `PurgeControl.mark_expired`。
- `/cache/push` 的 `expire` 实际会删除对象。

修复：

- `purge_single` 接收 `PurgeControl`。
- `mark_expired && !hard` 时只更新 metadata 的 `expires_at`。
- hard delete 行为保持不变。

### Revalidate miss 未回写 storage

原行为：

- 过期对象回源得到新 200 响应后，只更新进程内缓存。
- storage 仍可能保留旧对象。

修复：

- `handle_revalidate` 的 cacheable 分支调用 `store_to_storage`。
- cache completed 事件沿用原有发布逻辑。

### 运行时索引被纳入仓库

原行为：

- `target/` 和 `index/` 没有 `.gitignore` 保护。
- 测试会生成或修改 `index/sharedkv/*`。

修复：

- 新增 `.gitignore` 忽略 `target/`、`index/`、`.indexdb/`、`bucket-*`、日志和 pidfile。

## 后续优先级建议

### P0：可缓存响应流式写缓存

现状：

- 非缓存/旁路响应已经流式转发。
- 可缓存 GET/HEAD miss 仍通过 `BodyExt::collect()` 一次性读入内存。

影响：

- 超大可缓存对象和慢连接场景内存压力仍然较高。

建议：

- 可缓存响应采用边读边写 chunk，并向客户端流式输出。

### P1：动态插件能力扩展

现状：

- C ABI 插件可以处理请求元数据并返回响应。
- 当前不读取请求 body，也不注册本地管理路由。

建议：

- 增加异步 body 读取/流式回调模型。
- 设计动态插件路由注册 ABI。

### P1：共享内存缓存实现

现状：

- 已有设计文档和接口边界。
- 尚未实现 mmap/shm 数据结构。

建议：

- 先落地纯内存 mock L2。
- 再实现 mmap metadata、小对象 body 和崩溃恢复。

### P2：配置 schema 和示例

现状：

- serde 结构已经存在，但没有机器可读 schema。

建议：

- 生成 JSON Schema 或维护 `config.example.yaml`。
- 在 CI 中测试 example config 可解析。

## 验证覆盖

现有集成测试覆盖：

- GET/HEAD 缓存方法。
- 单 Range、Range 溢出、Range 重验证。
- 文件大小、ETag、Last-Modified 变化。
- 错误状态码缓存。
- 预取 Range。
- chunked 中断读取。

本轮新增：

- `/cache/push` delete 删除内存缓存后能重新回源。
- `/cache/push` expire 标记内存缓存过期后能触发 `REVALIDATE_MISS`。
- 非 GET/HEAD body 原样转发且不写缓存。
- 旁路响应首段 body 可在 upstream 完成前返回。
- 上游权重配置解析和 weighted round-robin 分布。
- storage GC 删除过期 Vary base 和 variant。
- 动态插件加载成功、缺失 symbol、ABI mismatch。
