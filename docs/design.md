# Tavern RS 设计文档

## 目标

Tavern RS 是一个缓存型反向代理/HTTP 代理。核心目标是：

- 以 Rust 实现可维护、可测试的 Tavern 兼容内核。
- 对大文件和 Range 下载友好，支持分片缓存和部分命中。
- 将内存缓存、落盘缓存、热冷分层、管理接口和观测能力组合成可部署服务。

非目标：

- 当前不做通用 HTTP 网关的完整请求体代理。
- 当前不做端到端流式缓存；上游响应会被收集为完整 body 后处理。
- 当前不提供动态插件 ABI，插件通过编译期注册。

## 模块边界

- `src/main.rs`：CLI 入口，负责读取配置、初始化日志、写 pidfile、启动 server，并监听配置文件变更触发 graceful reload。
- `src/server.rs`：监听 TCP/Unix socket、路由本地管理接口和代理请求、组装插件和中间件链。
- `src/middleware/caching.rs`：缓存核心逻辑，负责 cache key、命中判断、回源、Range、重验证和写存储。
- `src/middleware/multirange.rs`：多 Range 请求处理。
- `src/middleware/rewrite.rs`：请求/响应 header 改写。
- `src/cache.rs`：进程内缓存索引和 body 缓存。
- `src/storage/`：落盘/内存 bucket、索引 DB、共享 KV、热冷分层迁移。
- `src/upstream.rs`：上游 HTTP/HTTPS client、连接限制、TLS 配置。
- `src/proxy/`：上游节点选择策略和 singleflight。
- `src/plugin/`：插件注册和内置插件。
- `src/push.rs`：本地 `/cache/push` 管理接口。
- `src/metrics/`、`src/access_log.rs`、`src/logging.rs`：观测和日志。

## 请求路径

代理请求进入 `server::handle`：

1. 根据 Host 判断是本地管理请求还是代理请求。
2. 本地请求进入 `/healthz/*`、`/version`、`/metrics`、`/cache/push`、`/debug/pprof` 或插件路由。
3. 代理请求先交给插件 `handle_request`，插件未处理时进入中间件链。
4. 中间件链按配置顺序执行，最后由 `UpstreamRoundTripper` 回源。

典型缓存链：

```text
client
  -> server::handle_proxy
  -> recovery
  -> rewrite(request)
  -> multirange
  -> caching
  -> upstream client
  -> rewrite(response)
  -> client
```

## Cache Key

缓存 key 默认格式：

```text
{scheme}://{authority}{path_and_query}
```

如果请求头包含 `X-Store-Url`，缓存中间件优先使用该 URL 生成 key。`include_query_in_cache_key: false` 时，query string 不参与 key。

这个设计用于兼容 CDN 代理请求：客户端可以访问代理地址，但通过 `X-Store-Url` 指定真实缓存对象 URL。管理接口也复用同一套 cache key 生成逻辑，避免删除/过期和正常请求使用不同 key。

## 缓存生命周期

### Miss

1. 尝试进程内缓存。
2. 尝试落盘存储完整对象。
3. 回源获取对象。
4. 根据 TTL 和状态码判断是否可缓存。
5. 写入进程内缓存。
6. 写入 storage bucket。
7. 返回响应并写 `X-Cache: MISS`。

TTL 来源：

- `X-CacheTime: <seconds>`
- `Cache-Control: max-age=<seconds>`

4xx/5xx 默认不缓存。只有响应头 `i-x-ct-code: 1` 且存在 TTL 时才缓存错误响应。

### Hit

命中进程内缓存且未过期时直接返回。Range 请求会计算所需分片：

- 所需分片全在缓存中：`HIT`
- 部分存在：`PART_HIT`
- 都不存在：`PART_MISS`

完整对象也可以从 storage 读取并返回 `HIT`。

### Revalidate

进程内缓存过期后，如果有 `ETag` 或 `Last-Modified`：

1. 用 `If-None-Match` / `If-Modified-Since` 回源。
2. 上游返回 `304`：更新本地 TTL，返回 `REVALIDATE_HIT`。
3. 上游返回新对象：更新进程内缓存并回写 storage，返回 `REVALIDATE_MISS`。
4. 上游失败：保守返回旧对象，标记为 `REVALIDATE_HIT`。

### Vary

当响应包含 `Vary`：

1. base key 存储一个 vary index。
2. 实际响应写入 `{base_key}#vary:{header=value|...}`。
3. 后续请求先读取 base key，再根据 vary header 计算变体 key。

当前内存删除会同时删除 base key 和 vary 变体。落盘变体 GC 仍属于后续工作。

## Range 与分片

`slice_size` 是缓存分片大小，默认 1 MiB。Range 请求使用 `src/http_range.rs` 解析单 Range：

- `bytes=start-end`
- `bytes=start-`
- `bytes=-suffix`

缓存中间件会根据 Range 和 `fill_range_percent` 扩展回源范围，以提高后续相邻 Range 命中概率。

响应状态：

- 合法 Range：`206 Partial Content`
- 超出对象大小：`416 Range Not Satisfiable`
- Range 格式错误：`400 Bad Request`

`multirange` 中间件处理 `bytes=0-1,4-5` 这类多 Range：

- `merge: true` 时只保留第一段 Range 并进入后续链。
- `merge: false` 且存在 state 时，会拼出 `multipart/byteranges` 响应。

## Storage 设计

storage 由 `NativeStorage` 管理：

- `SharedKV` 保存辅助索引，例如域名对象数和目录 purge 索引。
- `IndexDB` 保存对象元数据。
- `Bucket` 保存实际 chunk 数据。
- `Selector` 负责根据对象 id 选择 bucket。

对象 id 基于 cache key 的 SHA-1：

```text
cache key -> SHA-1 -> chunk path
```

磁盘 chunk 路径：

```text
{bucket_path}/{hash[0..1]}/{hash[2..4]}/{hash}-{chunk_index}
```

支持的 IndexDB/SharedKV：

- `sled`
- `rocksdb`
- `redb`
- `lmdb`
- `memory` / `mem`

未知 DB 类型会回退到 sled 并记录 warning。

## 热冷分层

`cache_tiers.enabled: true` 后启用热冷 bucket 迁移：

- `type: hot` 或未设置：热 bucket。
- `type: cold`：冷 bucket。
- `max_hot_object_size` 可让大对象直接写入冷 bucket。
- 冷对象命中后可按 `promote_threshold_hits` 提升到热 bucket。
- 热对象可按 `demote_age_seconds` 降到冷 bucket。
- 迁移通过异步队列执行，指标前缀为 `tr_tavern_tiered_*`。

## 管理接口

本地接口由 Host 判断，默认允许：

- `localhost`
- `127.0.0.1`
- `127.1`

可通过 `server.local_api_allow_hosts` 扩展。

`/cache/push` 支持：

- `prefetch`：回源并写缓存。
- `expire`：标记内存缓存和 storage 元数据过期。
- `delete`：删除内存缓存和 storage 对象。

`PURGE` 支持单对象和目录：

- `PURGE <url>`
- `Purge-Type: dir`

## 并发与保护

- `collapsed_request` 使用 singleflight 合并同一 cache key 的并发 Miss。
- `upstream.max_connections_per_server` 对每个 upstream authority 加 semaphore 限流。
- `upstream.features.limit_rate_by_fd` 可按文件描述符预算限制并发。
- `object_pool_enabled` 当前用于限制进程内缓存条目数。

## 观测

本地 `/metrics` 暴露 Prometheus 格式指标：

- `tavern_requests_total`
- `tavern_requests_status_total`
- `tr_tavern_requests_code_total`
- `tr_tavern_requests_unexpected_closed_total`
- `tr_tavern_disk_usage`
- `tr_tavern_disk_io`
- `tr_tavern_tiered_events_total`
- `tr_tavern_tiered_queue_depth`

访问日志可输出到文件或 stdout，支持 AES-GCM 行加密。

## 优雅升级

服务监听 `SIGUSR1` 后会尝试 fork/exec 当前二进制，并通过环境变量传递监听 fd：

- `TAVERN_GRACEFUL_FD`
- `TAVERN_GRACEFUL_TYPE`
- `TAVERN_GRACEFUL_ADDR`
- `TAVERN_GRACEFUL_READY_FD`

新进程 ready 后，旧进程收到 shutdown 信号并退出 accept loop。

在 Unix 平台上，主进程会监听配置文件所在目录。配置文件发生 create/modify/remove 事件时，进程会做 300 ms 防抖，然后向自身发送 `SIGUSR1`，复用同一套 graceful upgrade 流程。

## 当前限制

- 上游响应和缓存对象会被完整收集到内存，超大对象需要谨慎设置缓存策略和内存预算。
- 非 GET/HEAD 请求在 caching 中间件下返回 `405`；即使绕过 caching，当前 upstream client 也不转发请求体。
- 上游节点权重尚未从配置解析。
- Vary 变体的落盘垃圾回收不完整。
- 没有独立后台 expired-object GC。
