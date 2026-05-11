use std::collections::HashMap;
use std::path::Path;

use crate::dedup::LinkTarget;
use crate::types::FileEntry;

pub fn verify_copy(
    entries: &[FileEntry],
    link_map: &HashMap<String, LinkTarget>,
    dst_root: &Path,
) -> bool {
    let total_to_check = entries.len() + link_map.len();
    eprint!("  Verifying {} files...", total_to_check);

    let mut expected: HashMap<&str, Option<u64>> = HashMap::new();
    for entry in entries {
        expected.insert(&entry.rel, Some(entry.size));
    }
    for dup_rel in link_map.keys() {
        expected.insert(dup_rel.as_str(), None);
    }

    let mut found: HashMap<String, u64> = HashMap::new();
    for entry in walkdir::WalkDir::new(dst_root)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let rel = entry
                .path()
                .strip_prefix(dst_root)
                .unwrap_or(entry.path())
                .to_string_lossy()
                .to_string()
                .replace('\\', "/");
            if let Ok(meta) = entry.metadata() {
                found.insert(rel, meta.len());
            }
        }
    }

    let mut missing = Vec::new();
    let mut mismatches = Vec::new();
    let mut grew = Vec::new();

    for (rel, exp_size) in &expected {
        match found.get(*rel) {
            None => missing.push(rel.to_string()),
            Some(actual) => {
                if let Some(exp) = exp_size {
                    if *actual != *exp {
                        if *actual > *exp {
                            grew.push((rel.to_string(), *exp, *actual));
                        } else {
                            mismatches.push((rel.to_string(), *exp, *actual));
                        }
                    }
                }
            }
        }
    }

    if missing.is_empty() && mismatches.is_empty() {
        if grew.is_empty() {
            eprintln!(
                "\r  Verified: all {} files OK              ",
                total_to_check
            );
        } else {
            eprintln!(
                "\r  Verified: all {} files OK ({} grew during copy)  ",
                total_to_check,
                grew.len()
            );
            for (rel, _exp, act) in grew.iter().take(10) {
                eprintln!("    GREW DURING COPY: {} (+{} bytes)", rel, act);
            }
        }
        true
    } else {
        eprintln!("\r  Verification failed:                     ");
        for m in missing.iter().take(10) {
            eprintln!("    MISSING: {}", m);
        }
        for (rel, exp, act) in mismatches.iter().take(10) {
            eprintln!("    SIZE MISMATCH: {} ({} -> {})", rel, exp, act);
        }
        false
    }
}
