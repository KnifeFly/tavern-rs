# Tavern RS

Tavern RS 是 Tavern 的 Rust 重写版本，目标是提供一个面向 CDN/代理缓存场景的高性能 HTTP 缓存代理。当前实现重点覆盖 GET/HEAD 缓存、Range 分片缓存、落盘存储、缓存预热/删除/过期、Prometheus 指标、访问日志和基础插件机制。

## 功能状态

已实现：

- HTTP/1.1、HTTP/2 自动服务端连接，支持 TCP 和 Unix socket 监听。
- 上游代理，支持 round-robin、random、weighted round-robin 策略。
- GET/HEAD 缓存，TTL 来自 `X-CacheTime` 或 `Cache-Control: max-age=...`。
- Range 请求缓存，支持分片落盘、部分命中、部分回源、Range 溢出处理。
- 缓存重验证，支持 `ETag`、`Last-Modified`、`304 Not Modified`。
- `Vary` 响应缓存索引、fuzzy refresh、collapsed request、prefetch。
- 存储 bucket：内存 bucket、磁盘 bucket、empty bucket；索引支持 sled、rocksdb、redb、lmdb。
- 热/冷分层缓存迁移、按命中提升、按年龄降级。
- 管理接口：`/cache/push`、`PURGE`、健康检查、版本、指标、pprof。
- 中间件：`caching`、`multirange`、`rewrite`、`recovery`。
- 插件注册框架和内置 `purge`、`verifier`、`example` 插件。
- 访问日志、加密访问日志、基础日志轮转、优雅升级信号处理。
- 配置文件变更监听，检测到配置文件修改后触发 graceful reload。

本轮补齐：

- `/cache/push` 的 `delete` 和 `expire` 现在会同步处理内存缓存和存储元数据。
- 单对象 `expire` 不再误删落盘对象，而是将元数据标记为过期。
- `REVALIDATE_MISS` 会回写存储，避免内存更新后落盘仍保留旧对象。
- 新增 `.gitignore`，避免 `target/`、运行时索引和缓存数据进入提交。

仍是后续工作：

- 非 GET/HEAD 请求体的完整代理转发和缓存旁路。
- 上游响应流式转发；当前实现会把上游响应收集为完整 body。
- 从配置中解析上游节点权重；目前节点权重固定为 1。
- 动态插件 ABI/动态库加载；当前是编译期注册内置插件。
- 后台过期对象 GC、Vary 变体落盘垃圾回收、跨进程共享内存缓存。

详细分析见 [docs/gap-analysis.md](docs/gap-analysis.md)。

## 快速开始

构建：

```bash
cargo build
```

创建 `config.yaml`：

```yaml
server:
  addr: "127.0.0.1:8080"
  read_timeout: "60s"
  write_timeout: "60s"
  idle_timeout: "90s"
  read_header_timeout: "30s"
  max_header_bytes: 1048576
  middleware:
    - name: recovery
    - name: rewrite
    - name: multirange
      options:
        merge: false
    - name: caching
      options:
        include_query_in_cache_key: true
        slice_size: 1048576
        collapsed_request: true
        collapsed_request_wait_timeout: "3s"

upstream:
  balancing: roundrobin
  address:
    - "https://origin.example.com"
  max_idle_conns: 1024
  max_idle_conns_per_host: 256
  max_connections_per_server: 512

storage:
  db_type: sled
  db_path: ".indexdb"
  slice_size: 1048576
  io_read_limit: 0
  io_write_limit: 0
  io_burst_bytes: 0
  eviction_policy: lru
  selection_policy: hashring
  buckets:
    - driver: disk
      type: hot
      path: "./cache-data"
      db_path: ".indexdb/bucket-0"
      max_object_limit: 1000000

logger:
  level: info
  path: "logs/tavern.log"
  max_size: 256
  max_backups: 7
  compress: true
```

运行：

```bash
cargo run -- -c config.yaml
```

通过代理访问：

```bash
curl -x http://127.0.0.1:8080 http://www.example.com/path/file.bin -I
curl -x http://127.0.0.1:8080 -H "Range: bytes=0-1048575" http://www.example.com/path/file.bin
```

查看缓存状态：

```bash
curl -x http://127.0.0.1:8080 http://www.example.com/path/file.bin -I
# 响应头 X-Cache: MISS / HIT / PART_HIT / PART_MISS / REVALIDATE_HIT / REVALIDATE_MISS
```

## 管理接口

本地接口通过 Host 判断，默认允许 `localhost`、`127.0.0.1`、`127.1`。

健康检查：

```bash
curl -H "Host: localhost" http://127.0.0.1:8080/healthz/startup-probe
curl -H "Host: localhost" http://127.0.0.1:8080/healthz/liveness-probe
curl -H "Host: localhost" http://127.0.0.1:8080/healthz/readiness-probe
```

指标和版本：

```bash
curl -H "Host: localhost" http://127.0.0.1:8080/metrics
curl -H "Host: localhost" http://127.0.0.1:8080/version
```

预热：

```bash
curl -H "Host: localhost" \
  -H "Content-Type: application/json" \
  -X POST http://127.0.0.1:8080/cache/push \
  -d '{"action":"prefetch","url":"http://www.example.com/path/file.bin"}'
```

软过期：

```bash
curl -H "Host: localhost" \
  -H "Content-Type: application/json" \
  -X POST http://127.0.0.1:8080/cache/push \
  -d '{"action":"expire","url":"http://www.example.com/path/file.bin"}'
```

删除：

```bash
curl -H "Host: localhost" \
  -H "Content-Type: application/json" \
  -X POST http://127.0.0.1:8080/cache/push \
  -d '{"action":"delete","url":"http://www.example.com/path/file.bin"}'
```

PURGE：

```bash
curl -x http://127.0.0.1:8080 -X PURGE http://www.example.com/path/file.bin
curl -x http://127.0.0.1:8080 -X PURGE -H "Purge-Type: dir" http://www.example.com/path/
```

## 开发命令

```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test --tests
cargo test --test range
```

更多配置、运维和故障排查见 [docs/usage.md](docs/usage.md)，架构说明见 [docs/design.md](docs/design.md)。
