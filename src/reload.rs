//! Change detection for the files the hub reloads without a restart: the ACL
//! (`SNODRA_SERVER_ACL_FILE`) and the TLS certificate and key (ADR 0018).
//!
//! A watcher polls the files' modification time and size every `SNODRA_SERVER_RELOAD_INTERVAL`
//! (5 s); [`request_reload`] (the binary calls it on SIGHUP) makes every watcher reload at once.
//! Symlinks are followed, so the atomic-rename swap Kubernetes does for mounted secrets is seen.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

static REQUESTS: AtomicU64 = AtomicU64::new(0);
static WAKE: Notify = Notify::const_new();

/// Asks every watcher to reload now, whether or not its files changed.
pub fn request_reload() {
    REQUESTS.fetch_add(1, Ordering::AcqRel);
    WAKE.notify_waiters();
}

/// Modification time and size of each file; `None` for a file that cannot be read.
pub type Stamp = Vec<Option<(SystemTime, u64)>>;

pub fn stamp(paths: &[PathBuf]) -> Stamp {
    paths
        .iter()
        .map(|p| {
            std::fs::metadata(p)
                .ok()
                .map(|m| (m.modified().unwrap_or(SystemTime::UNIX_EPOCH), m.len()))
        })
        .collect()
}

/// Calls `on_change` whenever `paths` changed since the last look (every `interval`) and
/// whenever [`request_reload`] was called, until `stopping` is cancelled. A change is reported
/// once even when `on_change` fails: the next edit is a new change.
pub async fn watch(
    paths: Vec<PathBuf>,
    interval: Duration,
    stopping: CancellationToken,
    mut on_change: impl FnMut() + Send,
) {
    let mut seen = stamp(&paths);
    let mut requests = REQUESTS.load(Ordering::Acquire);
    loop {
        tokio::select! {
            _ = stopping.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
            _ = WAKE.notified() => {}
        }
        let now = stamp(&paths);
        let asked = REQUESTS.load(Ordering::Acquire);
        if now != seen || asked != requests {
            seen = now;
            requests = asked;
            on_change();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn a_changed_file_triggers_one_reload() {
        let dir = std::env::temp_dir().join(format!("kraken-reload-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("f");
        std::fs::write(&file, "one").unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let stop = CancellationToken::new();
        let seen = Arc::clone(&count);
        let task = tokio::spawn(watch(
            vec![file.clone()],
            Duration::from_millis(20),
            stop.clone(),
            move || {
                seen.fetch_add(1, Ordering::SeqCst);
            },
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0, "no change, no reload");
        std::fs::write(&file, "two, longer").unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        request_reload();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            count.load(Ordering::SeqCst) >= 2,
            "a reload request reloads"
        );
        stop.cancel();
        task.await.unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
