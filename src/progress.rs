use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

pub struct Progress {
    total_bytes: u64,
    total_files: usize,
    bytes_done: AtomicU64,
    files_done: AtomicUsize,
    start: Instant,
}

impl Progress {
    pub fn new(total_bytes: u64, total_files: usize) -> Self {
        Progress {
            total_bytes,
            total_files,
            bytes_done: AtomicU64::new(0),
            files_done: AtomicUsize::new(0),
            start: Instant::now(),
        }
    }

    pub fn update(&self, bytes: u64, files: usize) {
        self.bytes_done.fetch_add(bytes, Ordering::Relaxed);
        self.files_done.fetch_add(files, Ordering::Relaxed);
    }

    pub fn display(&self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let bytes_done = self.bytes_done.load(Ordering::Relaxed);
        let files_done = self.files_done.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes;

        if elapsed < 0.01 {
            return;
        }

        let pct = if total_bytes > 0 {
            bytes_done as f64 / total_bytes as f64 * 100.0
        } else {
            100.0
        };

        let speed = if elapsed > 0.0 {
            bytes_done as f64 / elapsed
        } else {
            0.0
        };

        let eta = if speed > 0.0 {
            (total_bytes.saturating_sub(bytes_done)) as f64 / speed
        } else {
            0.0
        };

        let bar_w = 30;
        let filled = (bar_w as f64 * (pct / 100.0).min(1.0)) as usize;
        let bar: String = "█".repeat(filled) + &"░".repeat(bar_w - filled);

        use std::io::{stderr, Write};
        let _ = write!(
            stderr(),
            "\r  {} {:5.1}%  {}/{}  {}/s  {}/{} files  ETA {}   ",
            bar,
            pct,
            fmt_size(bytes_done),
            fmt_size(total_bytes),
            fmt_speed(speed),
            files_done,
            self.total_files,
            fmt_time(eta as u64),
        );
    }

    pub fn finish(&self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let bytes_done = self.bytes_done.load(Ordering::Relaxed);
        let speed = if elapsed > 0.0 {
            bytes_done as f64 / elapsed
        } else {
            0.0
        };
        use std::io::{stderr, Write};
        let _ = writeln!(
            stderr(),
            "\r  {} 100%  {} in {}  avg {}  {} files                ",
            "█".repeat(30),
            fmt_size(bytes_done),
            fmt_time(elapsed as u64),
            fmt_speed(speed),
            self.files_done.load(Ordering::Relaxed),
        );
    }
}

pub fn fmt_size(n: u64) -> String {
    fmt_size_f64(n as f64)
}

pub fn fmt_size_f64(n: f64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut size = n;
    for unit in &units {
        if size < 1024.0 {
            return format!("{:.1} {}", size, unit);
        }
        size /= 1024.0;
    }
    format!("{:.1} PB", size)
}

fn fmt_speed(bps: f64) -> String {
    format!("{}/s", fmt_size_f64(bps))
}

pub fn fmt_time(s: u64) -> String {
    if s < 60 {
        return format!("{}s", s);
    }
    let m = s / 60;
    let s = s % 60;
    if m < 60 {
        return format!("{}m {}s", m, s);
    }
    let h = m / 60;
    let m = m % 60;
    format!("{}h {}m {}s", h, m, s)
}
