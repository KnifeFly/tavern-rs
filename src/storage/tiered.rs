use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::time::{self, Duration};

use crate::config::CacheTiers;
use crate::storage::object::{IdHash, Metadata};
use crate::storage::{self, Bucket};
use crate::metrics;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MigrationMode {
    Promote,
    CopyToCold,
    Demote,
}

#[derive(Clone)]
struct MigrationTask {
    mode: MigrationMode,
    from_bucket: String,
    meta: Metadata,
}

struct Migrator {
    tx: mpsc::Sender<MigrationTask>,
    inflight: Mutex<HashSet<IdHash>>,
}

static SETTINGS: OnceLock<CacheTiers> = OnceLock::new();
static MIGRATOR: OnceLock<Arc<Migrator>> = OnceLock::new();

pub fn init(cfg: &CacheTiers) {
    let _ = SETTINGS.set(cfg.clone());
    if !cfg.enabled {
        return;
    }
    let (tx, rx) = mpsc::channel::<MigrationTask>(cfg.async_queue_size.max(1));
    let rx = Arc::new(AsyncMutex::new(rx));
    let migrator = Arc::new(Migrator {
        tx,
        inflight: Mutex::new(HashSet::new()),
    });
    let _ = MIGRATOR.set(Arc::clone(&migrator));
    for _ in 0..cfg.async_workers.max(1) {
        let migrator = Arc::clone(&migrator);
        let rx = Arc::clone(&rx);
        tokio::spawn(async move {
            loop {
                let task = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                let Some(task) = task else {
                    break;
                };
                let hash = task.meta.id.hash();
                let result = process_task_with_retry(task).await;
                let ok = result.is_ok();
                let event = match result {
                    Ok(mode) => mode,
                    Err(mode) => mode,
                };
                metrics::record_tiered_event(event, ok);
                let mut inflight = migrator.inflight.lock().expect("migrator inflight");
                inflight.remove(&hash);
                metrics::set_tiered_queue_depth(inflight.len() as i64);
            }
        });
    }

    if cfg.demote_interval_seconds > 0 && cfg.demote_age_seconds > 0 {
        tokio::spawn(demote_loop(
            Duration::from_secs(cfg.demote_interval_seconds),
            cfg.demote_age_seconds,
            cfg.demote_batch,
        ));
    }
}

pub fn enabled() -> bool {
    SETTINGS.get().map(|cfg| cfg.enabled).unwrap_or(false)
}

pub fn on_storage_hit(bucket: &dyn Bucket, meta: &Metadata, is_range: bool) {
    let cfg = SETTINGS.get();
    let Some(cfg) = cfg else {
        return;
    };
    if !cfg.enabled {
        return;
    }
    let storage = storage::current();
    let key = format!("if/hit/{}", meta.id.hash_str());
    let hits = storage
        .shared_kv()
        .incr(key.as_bytes(), 1)
        .unwrap_or(0);
    if is_range {
        let rkey = format!("if/range/{}", meta.id.hash_str());
        let _ = storage.shared_kv().incr(rkey.as_bytes(), 1);
    }

    if cfg.promote_on_hit
        && bucket.store_type().eq_ignore_ascii_case("cold")
        && (cfg.promote_threshold_hits == 0 || hits >= cfg.promote_threshold_hits)
    {
        enqueue(MigrationTask {
            mode: MigrationMode::Promote,
            from_bucket: bucket.id().to_string(),
            meta: meta.clone(),
        });
    }
}

pub fn maybe_write_through(bucket: &dyn Bucket, meta: &Metadata) {
    let cfg = SETTINGS.get();
    if cfg
        .map(|c| c.enabled && c.write_through_cold)
        .unwrap_or(false)
        && !bucket.store_type().eq_ignore_ascii_case("cold")
    {
        enqueue(MigrationTask {
            mode: MigrationMode::CopyToCold,
            from_bucket: bucket.id().to_string(),
            meta: meta.clone(),
        });
    }
}

pub fn select_write_bucket(id: &storage::object::Id, size: u64) -> Option<Arc<dyn Bucket>> {
    let cfg = SETTINGS.get();
    let storage = storage::current();
    let Some(cfg) = cfg else {
        return storage.select_hot(id);
    };
    if !cfg.enabled {
        return storage.select_hot(id);
    }
    if cfg.max_hot_object_size > 0 && size > cfg.max_hot_object_size {
        if let Some(bucket) = storage.select_cold(id) {
            return Some(bucket);
        }
    }
    storage.select_hot(id)
}

fn enqueue(task: MigrationTask) {
    let Some(migrator) = MIGRATOR.get() else {
        return;
    };
    let mut inflight = migrator.inflight.lock().expect("migrator inflight");
    let hash = task.meta.id.hash();
    if inflight.contains(&hash) {
        return;
    }
    if migrator.tx.try_send(task).is_ok() {
        inflight.insert(hash);
        metrics::set_tiered_queue_depth(inflight.len() as i64);
    }
}

async fn process_task_with_retry(task: MigrationTask) -> Result<&'static str, &'static str> {
    let mode = match task.mode {
        MigrationMode::Promote => "promote",
        MigrationMode::Demote => "demote",
        MigrationMode::CopyToCold => "copy",
    };
    let cfg = SETTINGS.get().cloned().unwrap_or_default();
    let mut attempt = 0u32;
    loop {
        if let Err(err) = process_task(&task).await {
            attempt = attempt.saturating_add(1);
            if cfg.retry_max == 0 || attempt > cfg.retry_max {
                log::warn!("tiered {mode} failed: {err}");
                return Err(mode);
            }
            let backoff = cfg.retry_backoff_ms.saturating_mul(1u64 << attempt.min(5));
            time::sleep(Duration::from_millis(backoff)).await;
            continue;
        }
        return Ok(mode);
    }
}

async fn process_task(task: &MigrationTask) -> anyhow::Result<()> {
    let cfg = SETTINGS.get().cloned().unwrap_or_default();
    if cfg.rate_limit_per_sec > 0 {
        let per_worker = (cfg.async_workers.max(1) as u64)
            .saturating_mul(1_000)
            / cfg.rate_limit_per_sec as u64;
        if per_worker > 0 {
            time::sleep(Duration::from_millis(per_worker)).await;
        }
    }
    let storage = storage::current();
    let Some(src) = storage.bucket_by_id(&task.from_bucket) else {
        return Ok(());
    };
    let id = task.meta.id.clone();
    let dst = match task.mode {
        MigrationMode::Promote => storage.select_hot(&id),
        MigrationMode::CopyToCold | MigrationMode::Demote => storage.select_cold(&id),
    };
    let Some(dst) = dst else {
        return Ok(());
    };
    if src.id() == dst.id() {
        return Ok(());
    }
    let meta = task.meta.clone();
    let src_clone = Arc::clone(&src);
    let dst_clone = Arc::clone(&dst);
    tokio::task::spawn_blocking(move || copy_object(src_clone.as_ref(), dst_clone.as_ref(), &meta))
        .await??;

    if task.mode != MigrationMode::CopyToCold {
        let _ = src.discard(&id);
        let ix_key = format!("ix/{}/{}", src.id(), id.key());
        let _ = storage.shared_kv().delete(ix_key.as_bytes());
    }
    let ix_key = format!("ix/{}/{}", dst.id(), id.key());
    let _ = storage.shared_kv().set(ix_key.as_bytes(), &id.hash().0);
    Ok(())
}

fn copy_object(src: &dyn Bucket, dst: &dyn Bucket, meta: &Metadata) -> anyhow::Result<()> {
    for idx in meta.chunks.iter() {
        let (mut reader, _) = src.read_chunk_file(&meta.id, *idx)?;
        let (mut writer, _) = dst.write_chunk_file(&meta.id, *idx)?;
        std::io::copy(&mut reader, &mut writer)?;
        let _ = writer.flush();
    }
    dst.store(meta)?;
    Ok(())
}

async fn demote_loop(interval: Duration, age_seconds: u64, max_batch: usize) {
    let mut ticker = time::interval(interval);
    loop {
        ticker.tick().await;
        let cfg = SETTINGS.get().cloned().unwrap_or_default();
        let storage = storage::current();
        let hot_buckets = storage.hot_buckets();
        let cold_buckets = storage.cold_buckets();
        if cold_buckets.is_empty() {
            continue;
        }
        let now = storage::unix_now();
        let mut queued = 0usize;
        for bucket in hot_buckets {
            let _ = bucket.iterate(&mut |meta| {
                if queued >= max_batch && max_batch > 0 {
                    return Ok(());
                }
                let age = now.saturating_sub(meta.resp_unix);
                if age_seconds > 0 && age >= age_seconds as i64 {
                    if cfg.demote_min_hits > 0 || cfg.demote_min_range_ratio > 0.0 {
                        let hits = read_counter("if/hit", meta);
                        let range_hits = read_counter("if/range", meta);
                        let ratio = if hits == 0 {
                            0.0
                        } else {
                            range_hits as f64 / hits as f64
                        };
                        if hits >= cfg.demote_min_hits
                            && cfg.demote_min_range_ratio > 0.0
                            && ratio >= cfg.demote_min_range_ratio
                        {
                            return Ok(());
                        }
                        if hits >= cfg.demote_min_hits && cfg.demote_min_range_ratio <= 0.0 {
                            return Ok(());
                        }
                        if cfg.demote_min_range_ratio > 0.0 && ratio >= cfg.demote_min_range_ratio {
                            return Ok(());
                        }
                    }
                    enqueue(MigrationTask {
                        mode: MigrationMode::Demote,
                        from_bucket: bucket.id().to_string(),
                        meta: meta.clone(),
                    });
                    queued += 1;
                }
                Ok(())
            });
            if max_batch > 0 && queued >= max_batch {
                break;
            }
        }
    }
}

fn read_counter(prefix: &str, meta: &Metadata) -> u32 {
    let storage = storage::current();
    let key = format!("{prefix}/{}", meta.id.hash_str());
    match storage.shared_kv().get(key.as_bytes()) {
        Ok(raw) => {
            if raw.len() >= 4 {
                u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]])
            } else {
                0
            }
        }
        Err(_) => 0,
    }
}
