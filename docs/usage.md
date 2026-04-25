# Tavern RS 使用文档

## 环境要求

- Rust 1.78 或更高版本。
- macOS/Linux。
- 如果启用 rocksdb/lmdb，首次构建会编译对应 native 依赖，耗时较长。

## 构建与运行

```bash
cargo build
cargo run -- -c config.yaml
```

CLI 参数：

```text
tavern -c <config.yaml> [-v]
```

- `-c`：配置文件路径，默认 `config.yaml`。
- `-v` / `--verbose`：将日志级别提升到 debug。

## 最小配置

```yaml
server:
  addr: "127.0.0.1:8080"
  middleware:
    - name: caching

upstream:
  address:
    - "http://127.0.0.1:9000"

storage:
  db_type: sled
  db_path: ".indexdb"
  slice_size: 1048576
  buckets:
    - driver: disk
      path: "./cache-data"
      db_path: ".indexdb/bucket-0"
```

启动后：

```bash
curl -x http://127.0.0.1:8080 http://example.com/file.bin -I
```

## 配置参考

### 顶层字段

```yaml
strict: false
hostname: "node-a"
pidfile: "/var/run/tavern.pid"
logger: {}
server: {}
plugin: []
upstream: {}
storage: {}
cache_tiers: {}
```

- `strict`：为 true 时，配置中出现未知字段会启动失败。
- `hostname`：节点名；未设置时读取环境变量 `HOSTNAME`。
- `pidfile`：启动时写入当前进程 pid。

### logger

```yaml
logger:
  level: info
  path: "logs/tavern.log"
  caller: false
  traceid: true
  max_size: 256
  max_age: 14
  max_backups: 7
  compress: true
  nopid: false
```

- `level`：`trace`、`debug`、`info`、`warn`、`error`。
- `path`：为空时输出到 stdout/stderr。
- `max_size`：日志文件大小上限，单位 MiB。
- `max_age`：后台清理超过 N 天的旧日志。
- `max_backups`：保留轮转文件数。
- `compress`：配合 `max_backups` 使用压缩轮转文件。

### server

```yaml
server:
  addr: "0.0.0.0:8080"
  read_timeout: "60s"
  write_timeout: "60s"
  idle_timeout: "90s"
  read_header_timeout: "30s"
  max_header_bytes: 1048576
  local_api_allow_hosts:
    - "admin.local"
  middleware:
    - name: recovery
    - name: rewrite
    - name: multirange
    - name: caching
  pprof:
    username: "admin"
    password: "change-me"
  access_log:
    enabled: true
    path: "logs/access.log"
    encrypt:
      enabled: false
      secret: ""
```

- `addr` 支持 `127.0.0.1:8080`、`:8080`、`unix:///tmp/tavern.sock`、`/tmp/tavern.sock`。
- `local_api_allow_hosts` 扩展本地管理 Host；默认已有 `localhost`、`127.0.0.1`、`127.1`。
- `middleware` 顺序就是执行顺序，第一项最外层。

### upstream

```yaml
upstream:
  balancing: wrr
  address:
    - "https://origin-a.example.com"
    - address: "https://origin-b.example.com"
      weight: 3
  max_idle_conns: 1024
  max_idle_conns_per_host: 256
  max_connections_per_server: 512
  insecure_skip_verify: false
  resolve_addresses: false
  features:
    limit_rate_by_fd: false
```

- `balancing`：`roundrobin`、`random`、`wrr`。
- `address`：上游地址；可包含 `http://` 或 `https://`。每项可以是字符串，也可以是 `{ address, weight }` 对象；未配置权重默认 `1`，`weight: 0` 会启动失败。
- `resolve_addresses`：为 true 时启动时解析域名并将解析结果作为节点。
- `insecure_skip_verify`：跳过 TLS 证书校验，只建议测试环境使用。

### storage

```yaml
storage:
  db_type: sled
  db_path: ".indexdb"
  async_load: true
  eviction_policy: lru
  selection_policy: hashring
  slice_size: 1048576
  io_read_limit: 0
  io_write_limit: 0
  io_burst_bytes: 0
  gc:
    enabled: true
    interval_secs: 60
    batch_size: 1024
  buckets:
    - driver: disk
      type: hot
      path: "./cache-hot"
      db_type: sled
      db_path: ".indexdb/hot"
      async_load: true
      slice_size: 1048576
      max_object_limit: 1000000
    - driver: disk
      type: cold
      path: "./cache-cold"
      db_path: ".indexdb/cold"
```

- `driver`：`disk`、`memory`、`mem`、`fastmemory`、`empty`。未知 driver 会按磁盘 bucket 处理。
- `db_type`：`sled`、`rocksdb`、`redb`、`lmdb`、`memory`。
- `eviction_policy`：`lru`、`fifo`、`lfu`。
- `selection_policy`：`hashring`、`roundrobin`。
- `slice_size`：分片大小，0 时默认 1 MiB。
- `io_read_limit` / `io_write_limit`：磁盘 bucket 读写限速，单位 bytes/s，0 表示不限速。
- `io_burst_bytes`：读写限速的 burst token 上限，0 时使用读写限速中的较大值。
- `gc.enabled`：是否启动后台过期对象清理任务。
- `gc.interval_secs`：GC 扫描间隔，默认 60 秒。
- `gc.batch_size`：单轮最多删除对象数，默认 1024；0 表示不限制。
- `max_object_limit`：bucket 最大对象数，<= 0 时使用默认上限。

### cache_tiers

```yaml
cache_tiers:
  enabled: true
  promote_on_hit: true
  promote_threshold_hits: 2
  write_through_cold: false
  max_hot_object_size: 1073741824
  demote_age_seconds: 86400
  demote_interval_seconds: 3600
  demote_min_hits: 1
  demote_min_range_ratio: 0.0
  async_workers: 4
  async_queue_size: 10000
  demote_batch: 100
  retry_max: 3
  retry_backoff_ms: 50
  rate_limit_per_sec: 0
```

启用热冷分层前，至少配置一个 hot bucket 和一个 cold bucket。

## 中间件配置

### caching

```yaml
- name: caching
  options:
    fuzzy_refresh: true
    fuzzy_refresh_rate: 0.8
    collapsed_request: true
    collapsed_request_wait_timeout: "3s"
    include_query_in_cache_key: true
    fill_range_percent: 100
    object_pool_enabled: true
    object_pool_size: 100000
    async_flush_chunk: false
    vary_limit: 100
    vary_ignore_key:
      - "User-Agent"
    slice_size: 1048576
```

- `collapsed_request`：合并同一 cache key 的并发 Miss。
- `fill_range_percent`：Range 回源时扩展到分片边界的容忍比例。
- `object_pool_size`：进程内缓存最大条目数。
- `async_flush_chunk`：每写一个 chunk 就更新一次元数据。

### multirange

```yaml
- name: multirange
  options:
    merge: false
```

- `merge: true`：多 Range 请求只保留第一段。
- `merge: false`：尝试返回 `multipart/byteranges`。

### rewrite

```yaml
- name: rewrite
  options:
    request_headers_rewrite:
      set:
        X-From-Proxy: tavern-rs
      remove:
        - X-Debug
    response_headers_rewrite:
      set:
        X-Served-By: tavern-rs
```

### recovery

```yaml
- name: recovery
```

用于捕获中间件链 panic 并返回 500。

## 管理接口

### 健康检查

```bash
curl -H "Host: localhost" http://127.0.0.1:8080/healthz/startup-probe
curl -H "Host: localhost" http://127.0.0.1:8080/healthz/liveness-probe
curl -H "Host: localhost" http://127.0.0.1:8080/healthz/readiness-probe
```

### 指标

```bash
curl -H "Host: localhost" http://127.0.0.1:8080/metrics
```

### 预热

```bash
curl -H "Host: localhost" \
  -H "Content-Type: application/json" \
  -X POST http://127.0.0.1:8080/cache/push \
  -d '{"action":"prefetch","urls":["http://example.com/a.bin","http://example.com/b.bin"]}'
```

### 软过期

```bash
curl -H "Host: localhost" \
  -H "Content-Type: application/json" \
  -X POST http://127.0.0.1:8080/cache/push \
  -d '{"action":"expire","url":"http://example.com/a.bin"}'
```

下一次访问会触发重验证。上游返回 304 时继续使用旧对象，上游返回 200 时更新缓存。

### 删除

```bash
curl -H "Host: localhost" \
  -H "Content-Type: application/json" \
  -X POST http://127.0.0.1:8080/cache/push \
  -d '{"action":"delete","url":"http://example.com/a.bin"}'
```

### PURGE

```bash
curl -x http://127.0.0.1:8080 -X PURGE http://example.com/a.bin
curl -x http://127.0.0.1:8080 -X PURGE -H "Purge-Type: dir" http://example.com/path/
```

## 插件

配置：

```yaml
plugin:
  - name: purge
    options:
      threshold: 10000
      allow_hosts:
        - "127.0.0.1"
      header_name: "Purge-Type"
      log_path: "logs/purge.log"
  - name: verifier
  - name: example
```

当前内置插件：

- `purge`：拦截 `PURGE` 方法，提供 allow list、阈值统计和 purge 日志。
- `verifier`：提供校验相关能力。
- `example`：示例插件。

动态插件：

```yaml
plugin:
  - name: custom-ffi-plugin
    library: "/opt/tavern/plugins/libcustom_plugin.so"
    options:
      tenant: "default"
```

动态库必须导出 `tavern_plugin_create_v1`，宿主通过 C ABI v1 传入 JSON 配置。插件可以实现 `name`、`start`、`stop`、`handle_request` 和资源释放函数；`handle_request` 接收请求元数据 JSON，返回响应 JSON。动态插件不暴露 Rust trait object ABI。

## 部署建议

- 使用磁盘 bucket 时，将 `storage.db_path` 和 bucket `path` 放在持久化目录。
- 将 `.indexdb` 和缓存数据目录排除出 git 和镜像构建上下文。
- 生产环境不要启用 `insecure_skip_verify`。
- 大对象场景下调大 `slice_size` 可以减少元数据和 chunk 文件数量。
- Range 下载多的场景可以启用热冷分层，将热门小对象留在热盘。
- 将 `/metrics` 接入 Prometheus，按 `X-Cache` 和状态码观察命中质量。

## 故障排查

看缓存状态：

```bash
curl -x http://127.0.0.1:8080 -I http://example.com/a.bin
```

常见响应：

- `X-Cache: MISS`：未命中，已回源。
- `X-Cache: HIT`：完整命中。
- `X-Cache: PART_HIT`：Range 请求部分命中。
- `X-Cache: PART_MISS`：Range 请求需要继续回源。
- `X-Cache: REVALIDATE_HIT`：过期后重验证，继续使用旧对象。
- `X-Cache: REVALIDATE_MISS`：过期后上游返回新对象。
- `X-Cache: BYPASS`：请求绕过缓存，通常是非 GET/HEAD 方法。

常见问题：

- 非 GET/HEAD 请求没有命中缓存：这是预期行为，请检查上游是否收到完整请求体。
- `upstream.address is empty`：配置缺少上游地址。
- `upstream.address node ... has invalid weight 0`：上游节点权重必须大于 0。
- `invalid host`：请求没有 authority，也没有 Host header。
- Range 返回 416：请求范围超出对象大小。
- `/cache/push` 404：请求 Host 没有命中本地管理 Host，添加 `-H "Host: localhost"`。

## 测试

```bash
cargo test --tests
cargo test --test push
cargo test --test range
```

格式化和 lint：

```bash
cargo fmt
cargo clippy --all-targets --all-features
```
