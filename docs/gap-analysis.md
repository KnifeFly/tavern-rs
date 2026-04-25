# 功能缺口分析

本文基于当前仓库代码、集成测试和配置结构整理。

## 已覆盖能力

| 领域 | 状态 | 说明 |
| --- | --- | --- |
| HTTP 服务 | 已实现 | TCP/Unix socket、HTTP/1.1/HTTP/2、基础超时、优雅升级。 |
| 上游代理 | 已实现 | HTTP/HTTPS 上游、连接池、每上游连接限制、TLS 校验开关。 |
| 负载均衡 | 部分实现 | round-robin、random、weighted round-robin 逻辑存在，但配置没有解析节点权重。 |
| GET/HEAD 缓存 | 已实现 | TTL、内存缓存、落盘存储、HEAD 空 body 返回。 |
| Range 缓存 | 已实现 | 单 Range 解析、分片、部分命中、溢出处理、回源填充。 |
| 多 Range | 部分实现 | 可合并第一段或拼 multipart 响应；没有和缓存层深度协同。 |
| 重验证 | 已实现 | ETag/Last-Modified/304；本轮补齐 revalidate miss 回写 storage。 |
| Vary | 部分实现 | 内存 vary index 和 vary key 已实现；落盘变体垃圾回收仍不完整。 |
| 缓存管理 | 已实现 | `PURGE`、`/cache/push` prefetch/expire/delete；本轮补齐内存缓存同步。 |
| 存储 | 已实现 | memory/disk/empty bucket，sled/rocksdb/redb/lmdb 索引。 |
| 热冷分层 | 已实现 | 提升、降级、write-through、迁移队列和指标。 |
| 指标 | 已实现 | Prometheus 文本指标。 |
| 访问日志 | 已实现 | 文件/stdout、分钟轮转、可选 AES-GCM 行加密。 |
| 插件 | 部分实现 | 编译期注册内置插件；未实现动态插件加载。 |
| 文档 | 本轮补齐 | README、设计文档、使用文档和缺口分析。 |

## 本轮修复的缺口

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

### P0：请求体代理

现状：

- caching 中间件只允许 GET/HEAD。
- upstream client 构造请求时 body 总是空。

影响：

- 不能作为通用 HTTP 反向代理处理 POST/PUT/PATCH。

建议：

- 在 caching 前增加明确 bypass 逻辑。
- `UpstreamClient::fetch` 支持传入 body stream 或 bytes。
- 对非缓存方法只代理，不进入缓存写路径。

### P0：流式响应

现状：

- 回源响应通过 `BodyExt::collect()` 一次性读入内存。

影响：

- 超大对象和慢连接场景内存压力高。

建议：

- 拆分响应转发和缓存写入。
- 非缓存响应直接流式转发。
- 可缓存响应采用边读边写 chunk，并向客户端流式输出。

### P1：上游权重配置

现状：

- `Node::weight` 支持权重。
- `build_upstream_nodes` 创建节点时权重固定为 1。

建议：

- 支持配置：

```yaml
upstream:
  address:
    - url: "http://origin-a"
      weight: 3
    - url: "http://origin-b"
      weight: 1
```

或在保持兼容的前提下新增 `nodes` 字段。

### P1：Vary 变体落盘 GC

现状：

- 内存删除会删除 `{base_key}` 和 `{base_key}#vary:*`。
- storage 单对象删除只按精确 key 删除。

建议：

- 在 SharedKV 中为 vary base 建立变体索引。
- delete/expire/purge 时遍历变体索引并清理 storage。

### P1：后台过期对象清理

现状：

- 过期对象在访问、purge 或迁移时被动处理。

建议：

- 增加 storage GC loop。
- 使用 `IndexDB::expired` 扫描过期 metadata。
- 控制 batch、间隔和 IO 速率。

### P2：插件动态加载

现状：

- 插件通过 Rust 代码注册到全局 registry。

建议：

- 先稳定插件 trait 和路由能力。
- 再评估动态库 ABI 或 WASM 插件模型。

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
