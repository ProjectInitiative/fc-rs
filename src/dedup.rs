use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::dedup_db::DedupDB;
use crate::hashing;
use crate::types::{DedupStrategy, FileEntry};

pub struct DedupResult {
    pub unique_entries: Vec<FileEntry>,
    pub link_map: HashMap<String, LinkTarget>,
    pub saved_bytes: u64,
    pub cache_hits: usize,
    pub crossrun_count: usize,
    pub crossrun_bytes: u64,
}

#[derive(Debug, Clone)]
pub enum LinkTarget {
    Rel(String),
    Abs(String),
}

pub fn deduplicate(
    entries: &[FileEntry],
    threads: usize,
    dedup_db: Option<&DedupDB>,
    _fs_strategy: Option<DedupStrategy>,
) -> DedupResult {
    let _total = entries.len();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap();

    let hashes: Vec<Option<String>> = pool.install(|| {
        entries
            .par_iter()
            .map(|entry| {
                if entry.content_hash.is_some() {
                    return entry.content_hash.clone();
                }

                if let Some(ref db) = dedup_db {
                    let cache_key = &entry.src;
                    if let Ok(meta) = fs::metadata(cache_key) {
                        use std::time::UNIX_EPOCH;
                        let mtime = meta
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                            .map(|d| d.as_nanos() as i64)
                            .unwrap_or(0);
                        if mtime > 0 {
                            if let Some(cached) = db.lookup(cache_key, entry.size, mtime) {
                                return Some(cached);
                            }
                        }
                    }
                }

                hashing::hash_file(Path::new(&entry.src))
            })
            .collect()
    });

    let mut hashed_entries: Vec<FileEntry> = entries
        .iter()
        .zip(hashes.iter())
        .map(|(e, h)| FileEntry {
            content_hash: h.clone(),
            ..e.clone()
        })
        .collect();

    let mut hash_groups: HashMap<(u64, String), Vec<FileEntry>> = HashMap::new();
    let mut unique_entries = Vec::new();

    for e in hashed_entries.drain(..) {
        if let Some(ref h) = e.content_hash {
            hash_groups.entry((e.size, h.clone())).or_default().push(e);
        } else {
            unique_entries.push(e);
        }
    }

    let mut link_map: HashMap<String, LinkTarget> = HashMap::new();
    let mut saved_bytes: u64 = 0;
    let mut crossrun_count = 0;
    let mut crossrun_bytes: u64 = 0;

    for (_key, group) in hash_groups.iter() {
        let canonical = &group[0];

        let mut skip_canonical = false;

        if let Some(ref db) = dedup_db {
            if let Some(ref h) = canonical.content_hash {
                let dst_matches = db.lookup_by_hash(h);
                for (mount_rel, dst_size) in &dst_matches {
                    if mount_rel.contains("..") {
                        continue;
                    }
                    let full_path = Path::new(&db.mount).join(mount_rel);
                    if let Ok(real_full) = fs::canonicalize(&full_path) {
                        let real_mount = Path::new(&db.mount);
                        if real_full.starts_with(real_mount) && *dst_size == canonical.size {
                            if full_path.is_file() {
                                skip_canonical = true;
                                for e in group {
                                    link_map.insert(
                                        e.rel.clone(),
                                        LinkTarget::Abs(full_path.to_string_lossy().to_string()),
                                    );
                                    saved_bytes += e.size;
                                    crossrun_count += 1;
                                    crossrun_bytes += e.size;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }

        if !skip_canonical {
            unique_entries.push(canonical.clone());
            for dup in group.iter().skip(1) {
                link_map.insert(dup.rel.clone(), LinkTarget::Rel(canonical.rel.clone()));
                saved_bytes += dup.size;
            }
        }
    }

    DedupResult {
        unique_entries,
        link_map,
        saved_bytes,
        cache_hits: 0,
        crossrun_count,
        crossrun_bytes,
    }
}

pub fn deduplicate_source_remote(_entries: &[FileEntry], _threads: usize) -> DedupResult {
    DedupResult {
        unique_entries: _entries.to_vec(),
        link_map: HashMap::new(),
        saved_bytes: 0,
        cache_hits: 0,
        crossrun_count: 0,
        crossrun_bytes: 0,
    }
}
