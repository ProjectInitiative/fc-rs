use std::path::Path;

use crate::types::FileEntry;

pub fn get_physical_offset(_filepath: &Path) -> u64 {
    0
}

pub fn resolve_physical_offsets(entries: &[FileEntry], _threads: usize) -> Vec<FileEntry> {
    use rayon::prelude::*;

    let mapped: Vec<_> = entries
        .par_iter()
        .map(|e| {
            let offset = get_physical_offset(Path::new(&e.src));
            FileEntry {
                physical_offset: offset,
                ..e.clone()
            }
        })
        .collect();

    let mut sorted = mapped;
    sorted.sort_by(|a, b| b.size.cmp(&a.size));
    sorted
}
