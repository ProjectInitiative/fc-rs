use serde::{Deserialize, Serialize};

pub const VERSION: &str = "0.1.0";
pub const DEFAULT_BUFFER_MB: usize = 64;
pub const DEFAULT_THREADS: usize = 4;
pub const HASH_CHUNK: usize = 1_048_571;
pub const SMALL_FILE_THRESHOLD: u64 = 1 * 1024 * 1024;
pub const TAR_BUNDLE_NAME: &str = ".fast_copy_bundle.tar";
pub const DEDUP_DB_NAME: &str = ".fast_copy_dedup.db";
pub const REMOTE_MANIFEST_NAME: &str = ".fast_copy_manifest.json";
pub const TAR_CHUNK_SIZE: u64 = 100 * 1024 * 1024;
pub const TAR_CHUNK_MAX_FILES: usize = 10000;

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub src: String,
    pub rel: String,
    pub size: u64,
    pub physical_offset: u64,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FSCapabilities {
    pub hardlink: bool,
    pub symlink: bool,
    pub reflink: bool,
    pub case_sensitive: bool,
}

#[derive(Debug, Clone)]
pub struct FSInfo {
    pub path: String,
    pub fs_type: String,
    pub capabilities: FSCapabilities,
    pub strategy: DedupStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupStrategy {
    Reflink,
    Hardlink,
    Symlink,
    None,
}

#[derive(Debug, Clone)]
pub struct RemoteSpec {
    pub user: String,
    pub host: String,
    pub port: u16,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub action: String,
    pub path: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopySummary {
    pub source: String,
    pub destination: String,
    pub mode: String,
    pub total_files: usize,
    pub copied: usize,
    pub linked: usize,
    pub skipped: usize,
    pub errors: usize,
    pub total_bytes: u64,
    pub bytes_written: u64,
    pub dedup_saved: u64,
    pub elapsed_sec: f64,
    pub avg_speed_bps: f64,
    pub hash_algo: String,
}

pub struct CopyPlan {
    pub source: String,
    pub destination: String,
    pub mode: CopyMode,
    pub buffer_mb: usize,
    pub threads: usize,
    pub dedup: bool,
    pub hash_algo: String,
    pub fs_strategy: Option<DedupStrategy>,
    pub remote_compress: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyMode {
    LocalToLocal,
    LocalToRemote,
    RemoteToLocal,
    RemoteToRemote,
}
