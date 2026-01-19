use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::time::{self, Duration};

use crate::config::CacheTiers;
use crate::storage::object::{IdHash, Metadata};
use crate::storage::{self, Bucket};

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
                let _ = process_task(task).await;
                let mut inflight = migrator.inflight.lock().expect("migrator inflight");
                inflight.remove(&hash);
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

pub fn maybe_promote(bucket: &dyn Bucket, meta: &Metadata) {
    let cfg = SETTINGS.get();
    if cfg.map(|c| c.enabled && c.promote_on_hit).unwrap_or(false)
        && bucket.store_type().eq_ignore_ascii_case("cold")
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
    }
}

async fn process_task(task: MigrationTask) -> anyhow::Result<()> {
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
