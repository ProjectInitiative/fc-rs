use std::fs;
use std::path::Path;
use std::time::Instant;

use crate::types::{DedupStrategy, FSCapabilities, FSInfo};

pub fn detect_fs_type(path: &Path) -> (String, String) {
    #[cfg(target_os = "linux")]
    {
        fs_type_linux(path)
    }
    #[cfg(not(target_os = "linux"))]
    {
        ("unknown".to_string(), "unsupported_os".to_string())
    }
}

#[cfg(target_os = "linux")]
fn fs_type_linux(path: &Path) -> (String, String) {
    let check = match walk_up_to_existing(path) {
        Some(p) => p,
        None => return ("unknown".to_string(), "linux_mountinfo".to_string()),
    };

    let mountinfo = match fs::read_to_string("/proc/self/mountinfo") {
        Ok(s) => s,
        Err(_) => return ("unknown".to_string(), "linux_mountinfo".to_string()),
    };

    let mut best_mount = String::new();
    let mut best_type = "unknown".to_string();

    for line in mountinfo.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        let sep_idx = match parts.iter().position(|&p| p == "-") {
            Some(i) => i,
            None => continue,
        };
        if parts.len() <= sep_idx + 1 {
            continue;
        }
        let mount_point = parts[4];
        let fs_type = parts[sep_idx + 1];

        if mount_point == check
            || check.starts_with(&format!("{}/", mount_point.trim_end_matches('/')))
        {
            if mount_point.len() > best_mount.len() {
                best_mount = mount_point.to_string();
                best_type = fs_type.to_string();
            }
        }
    }

    (best_type, "linux_mountinfo".to_string())
}

fn walk_up_to_existing(path: &Path) -> Option<String> {
    let path_str = path.to_string_lossy();
    if path_str.contains('\0') {
        return None;
    }

    let mut cur = if path.exists() {
        fs::canonicalize(path).ok()?
    } else {
        let abs = fs::canonicalize(".").ok()?.join(path);
        abs
    };

    for _ in 0..64 {
        if cur.is_dir() {
            return Some(cur.to_string_lossy().to_string());
        }
        match cur.parent() {
            Some(parent) if parent != cur => cur = parent.to_path_buf(),
            _ => return None,
        }
    }
    None
}

const CASE_INSENSITIVE_FS: &[&str] = &[
    "vfat", "fat", "fat32", "msdos", "exfat", "ntfs", "ntfs3", "ntfs-3g", "hfs", "hfsplus", "apfs",
];

fn is_known_case_insensitive(fs_type: &str) -> bool {
    let lower = fs_type.to_lowercase();
    CASE_INSENSITIVE_FS.iter().any(|&x| x == lower)
}

const FS_CAPABILITY_TABLE: &[(&str, bool, bool, bool, bool)] = &[
    ("vfat", false, false, false, false),
    ("fat", false, false, false, false),
    ("fat32", false, false, false, false),
    ("msdos", false, false, false, false),
    ("exfat", false, false, false, false),
    ("btrfs", true, true, true, false),
    ("bcachefs", true, true, true, false),
    ("apfs", true, true, true, false),
    ("refs", true, true, true, false),
    ("xfs", true, true, false, true),
    ("zfs", true, true, false, true),
    ("ext2", true, true, false, false),
    ("ext3", true, true, false, false),
    ("ext4", true, true, false, false),
    ("hfs", true, true, false, false),
    ("hfsplus", true, true, false, false),
    ("f2fs", true, true, false, false),
    ("tmpfs", true, true, false, false),
    ("overlay", true, true, false, false),
    ("ntfs", true, true, false, true),
    ("ntfs3", true, true, false, true),
    ("nfs", true, true, false, true),
    ("nfs4", true, true, false, true),
    ("cifs", true, true, false, true),
    ("smbfs", true, true, false, true),
    ("smb3", true, true, false, true),
    ("fuseblk", false, false, false, true),
    ("fuse", false, false, false, true),
    ("sshfs", false, false, false, true),
];

pub fn detect_capabilities(dst_dir: &Path) -> FSInfo {
    let t0 = Instant::now();
    let (fs_type, _method) = detect_fs_type(dst_dir);
    let _detection_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let fs_lc = fs_type.to_lowercase();
    let table_entry = FS_CAPABILITY_TABLE
        .iter()
        .find(|(name, _, _, _, _)| *name == &fs_lc);

    let (hl, sl, rl, _needs_probe) = match table_entry {
        Some((_, h, s, r, np)) => (*h, *s, *r, *np),
        None => (false, false, false, true),
    };

    let case_sensitive = !is_known_case_insensitive(&fs_type);

    let caps = FSCapabilities {
        hardlink: hl,
        symlink: sl,
        reflink: rl,
        case_sensitive,
    };

    let strategy = select_dedup_strategy(&caps);

    FSInfo {
        path: dst_dir.to_string_lossy().to_string(),
        fs_type,
        capabilities: caps,
        strategy,
    }
}

pub fn select_dedup_strategy(caps: &FSCapabilities) -> DedupStrategy {
    if caps.reflink {
        DedupStrategy::Reflink
    } else if caps.hardlink {
        DedupStrategy::Hardlink
    } else if caps.symlink {
        DedupStrategy::Symlink
    } else {
        DedupStrategy::None
    }
}
