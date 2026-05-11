use std::fs;
use std::path::Path;
use walkdir::WalkDir;

use crate::exclude::ExcludeList;
use crate::types::{FileEntry, DEDUP_DB_NAME, REMOTE_MANIFEST_NAME, TAR_BUNDLE_NAME};

pub fn scan_source(
    src_root: &Path,
    dst_root: Option<&Path>,
    excludes: &ExcludeList,
) -> (Vec<FileEntry>, Vec<(String, String)>) {
    let mut entries = Vec::new();
    let mut errors = Vec::new();
    let _src_real = fs::canonicalize(src_root).unwrap_or_else(|_| src_root.to_path_buf());
    let dst_real = dst_root.and_then(|d| fs::canonicalize(d).ok());

    let auto_excludes = [TAR_BUNDLE_NAME, DEDUP_DB_NAME, REMOTE_MANIFEST_NAME];

    for entry in WalkDir::new(src_root).follow_links(true) {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                errors.push((
                    e.path()
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_default(),
                    e.to_string(),
                ));
                continue;
            }
        };

        if !entry.file_type().is_file() {
            continue;
        }

        let name = entry.file_name().to_string_lossy();
        if auto_excludes.contains(&name.as_ref()) || excludes.is_excluded(name.as_ref()) {
            continue;
        }

        let src_path = entry.path();
        let rel = src_path
            .strip_prefix(src_root)
            .unwrap_or(src_path)
            .to_string_lossy()
            .to_string()
            .replace('\\', "/");

        // Skip destination if inside source
        if let Some(ref dst) = dst_real {
            if let Ok(real) = fs::canonicalize(src_path) {
                if real == *dst || real.starts_with(dst) {
                    continue;
                }
            }
        }

        match src_path.metadata() {
            Ok(meta) => {
                entries.push(FileEntry {
                    src: src_path.to_string_lossy().to_string(),
                    rel,
                    size: meta.len(),
                    physical_offset: 0,
                    content_hash: None,
                });
            }
            Err(e) => {
                errors.push((src_path.to_string_lossy().to_string(), e.to_string()));
            }
        }
    }

    (entries, errors)
}
