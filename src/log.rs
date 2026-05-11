use std::fs;
use std::sync::Mutex;

use crate::types::{CopySummary, LogEntry};

static LOG_ENTRIES: once_cell::sync::Lazy<Mutex<Vec<LogEntry>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(Vec::new()));

static LOG_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn set_enabled(enabled: bool) {
    LOG_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

pub fn is_enabled() -> bool {
    LOG_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn log(action: &str, path: &str, size: u64, extra: Vec<(&str, String)>) {
    if !is_enabled() {
        return;
    }
    if let Ok(mut entries) = LOG_ENTRIES.lock() {
        let mut entry = LogEntry {
            action: action.to_string(),
            path: path.to_string(),
            size,
            method: None,
            reason: None,
            link_target: None,
            error: None,
        };
        for (key, val) in extra {
            match key {
                "method" => entry.method = Some(val),
                "reason" => entry.reason = Some(val),
                "link_target" => entry.link_target = Some(val),
                "error" => entry.error = Some(val),
                _ => {}
            }
        }
        entries.push(entry);
    }
}

pub fn write_log_file(path: &str, summary: &CopySummary) {
    let entries = LOG_ENTRIES
        .lock()
        .ok()
        .map(|mut e| e.drain(..).collect::<Vec<_>>())
        .unwrap_or_default();

    let log = serde_json::json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "summary": {
            "source": summary.source,
            "destination": summary.destination,
            "mode": summary.mode,
            "total_files": summary.total_files,
            "copied": summary.copied,
            "linked": summary.linked,
            "skipped": summary.skipped,
            "errors": summary.errors,
            "total_bytes": summary.total_bytes,
            "bytes_written": summary.bytes_written,
            "dedup_saved": summary.dedup_saved,
            "elapsed_sec": summary.elapsed_sec,
            "avg_speed_bps": summary.avg_speed_bps,
            "hash_algo": summary.hash_algo,
        },
        "files": entries,
    });

    if let Ok(json) = serde_json::to_string_pretty(&log) {
        let _ = fs::write(path, json);
        eprintln!("  Log:     {}", path);
    }
}
