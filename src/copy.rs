use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use crate::progress::Progress;
use crate::types::{DedupStrategy, FileEntry, SMALL_FILE_THRESHOLD};

pub fn split_by_size(entries: &[FileEntry]) -> (Vec<FileEntry>, Vec<FileEntry>) {
    let small: Vec<FileEntry> = entries
        .iter()
        .filter(|e| e.size < SMALL_FILE_THRESHOLD)
        .cloned()
        .collect();
    let large: Vec<FileEntry> = entries
        .iter()
        .filter(|e| e.size >= SMALL_FILE_THRESHOLD)
        .cloned()
        .collect();
    (small, large)
}

pub fn copy_individual(
    entries: &[FileEntry],
    dst_root: &Path,
    progress: &Progress,
    buf_size: usize,
    fs_strategy: Option<DedupStrategy>,
) {
    let mut buf = vec![0u8; buf_size];
    let path_attr = Path::new("");

    for entry in entries {
        let dst_path = dst_root.join(&entry.rel);
        let dst_dir = dst_path.parent().unwrap_or(path_attr);

        if let Err(e) = fs::create_dir_all(dst_dir) {
            eprintln!("Error creating dir {}: {}", dst_dir.display(), e);
            continue;
        }

        if entry.size == 0 {
            if let Err(e) = File::create(&dst_path) {
                eprintln!("Error creating {}: {}", dst_path.display(), e);
            }
            progress.update(0, 1);
            progress.display();
            continue;
        }

        if let Some(strat) = fs_strategy {
            if strat == DedupStrategy::Reflink {
                if try_reflink(Path::new(&entry.src), &dst_path) {
                    progress.update(entry.size, 1);
                    progress.display();
                    continue;
                }
            }
        }

        let src_path = Path::new(&entry.src);
        match (File::open(src_path), File::create(&dst_path)) {
            (Ok(mut fin), Ok(mut fout)) => loop {
                let n = match fin.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                if let Err(e) = fout.write_all(&buf[..n]) {
                    eprintln!("Error writing {}: {}", dst_path.display(), e);
                    break;
                }
                progress.update(n as u64, 0);
                progress.display();
            },
            (Err(e), _) => eprintln!("Error opening {}: {}", entry.src, e),
            (_, Err(e)) => eprintln!("Error creating {}: {}", dst_path.display(), e),
        }

        progress.update(0, 1);
    }
}

fn try_reflink(src: &Path, dst: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let src_file = match File::open(src) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let dst_file = match File::create(dst) {
            Ok(f) => f,
            Err(_) => return false,
        };

        let rc = unsafe { libc::ioctl(dst_file.as_raw_fd(), 0x40049409, src_file.as_raw_fd()) };
        rc == 0
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (src, dst);
        false
    }
}

pub fn create_links(
    link_map: &HashMap<String, crate::dedup::LinkTarget>,
    dst_root: &Path,
    fs_strategy: Option<DedupStrategy>,
) {
    for (dup_rel, target) in link_map {
        let dst_dup = dst_root.join(dup_rel);
        if let Some(parent) = dst_dup.parent() {
            fs::create_dir_all(parent).ok();
        }

        let canonical_path = match target {
            crate::dedup::LinkTarget::Rel(rel) => dst_root.join(rel),
            crate::dedup::LinkTarget::Abs(abs) => Path::new(abs).to_path_buf(),
        };

        if let Some(strat) = fs_strategy {
            if strat == DedupStrategy::Reflink {
                if try_reflink(&canonical_path, &dst_dup) {
                    continue;
                }
            }
        }

        #[cfg(unix)]
        {
            if fs::hard_link(&canonical_path, &dst_dup).is_ok() {
                continue;
            }
        }

        if let Err(e) = fs::copy(&canonical_path, &dst_dup) {
            eprintln!("Error creating link/copy {}: {}", dup_rel, e);
        }
    }
}

pub fn copy_block_stream(small_entries: &[FileEntry], dst_root: &Path, progress: &Progress) {
    if small_entries.is_empty() {
        return;
    }

    let small_size: u64 = small_entries.iter().map(|e| e.size).sum();
    eprintln!(
        "  Streaming {} small files ({}) via pipe...",
        small_entries.len(),
        crate::progress::fmt_size(small_size),
    );

    fs::create_dir_all(dst_root).ok();

    let entries_owned: Vec<FileEntry> = small_entries.to_vec();

    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done_clone = done.clone();

    let producer = std::thread::spawn(move || {
        for entry in &entries_owned {
            if done_clone.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            let data = match fs::read(&entry.src) {
                Ok(d) => d,
                Err(_) => continue,
            };
            if tx.send(data).is_err() {
                break;
            }
        }
    });

    for entry in small_entries {
        if done.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        let dst_path = dst_root.join(&entry.rel);
        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent).ok();
        }
        match rx.recv() {
            Ok(data) => {
                if let Err(e) = fs::write(&dst_path, &data) {
                    eprintln!("Error writing {}: {}", dst_path.display(), e);
                }
                progress.update(data.len() as u64, 1);
                progress.display();
            }
            Err(_) => break,
        }
    }

    done.store(true, std::sync::atomic::Ordering::Relaxed);
    producer.join().ok();
    eprintln!("  Streamed files to destination");
}

pub fn copy_hybrid(
    entries: &[FileEntry],
    dst_root: &Path,
    progress: &Progress,
    buf_size: usize,
    fs_strategy: Option<DedupStrategy>,
) {
    if let Some(strat) = fs_strategy {
        if strat == DedupStrategy::Reflink {
            let total_size: u64 = entries.iter().map(|e| e.size).sum();
            eprintln!(
                "  Strategy: reflink (CoW) for {} files, {}",
                entries.len(),
                crate::progress::fmt_size(total_size),
            );
            copy_individual(entries, dst_root, progress, buf_size, fs_strategy);
            return;
        }
    }

    let (small, large) = split_by_size(entries);
    let small_size: u64 = small.iter().map(|e| e.size).sum();
    let large_size: u64 = large.iter().map(|e| e.size).sum();

    eprintln!("  Strategy:");
    eprintln!(
        "    Small files (<1MB): {} files, {} -> block stream",
        small.len(),
        crate::progress::fmt_size(small_size),
    );
    eprintln!(
        "    Large files (>=1MB): {} files, {} -> individual copy",
        large.len(),
        crate::progress::fmt_size(large_size),
    );

    if !large.is_empty() {
        eprintln!("  -- Large files --");
        copy_individual(&large, dst_root, progress, buf_size, fs_strategy);
    }

    if !small.is_empty() {
        eprintln!("  -- Small files (block stream) --");
        copy_block_stream(&small, dst_root, progress);
    }
}
