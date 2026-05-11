use std::fs;
use std::path::Path;

pub fn check_destination_space(dst: &Path, required_bytes: u64, force: bool) -> bool {
    fs::create_dir_all(dst).ok();

    let usage = match fs2::statvfs(dst) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("  Warning: Could not check free space: {}", e);
            if force {
                eprintln!("  --force: proceeding anyway");
                return true;
            }
            eprintln!("  Use --force to skip this check.");
            return false;
        }
    };

    let free = usage.free_space();
    let total = usage.total_space();

    let pct_free = if total > 0 {
        free as f64 / total as f64 * 100.0
    } else {
        0.0
    };

    eprintln!("  Destination disk:");
    eprintln!("    Total:     {}", crate::progress::fmt_size(total));
    eprintln!(
        "    Free:      {} ({:.1}% free)",
        crate::progress::fmt_size(free),
        pct_free
    );
    eprintln!(
        "    Required:  {}",
        crate::progress::fmt_size(required_bytes)
    );

    if required_bytes > free {
        let shortfall = required_bytes - free;
        eprintln!(
            "  NOT ENOUGH SPACE - need {} more",
            crate::progress::fmt_size(shortfall)
        );
        if force {
            eprintln!("  --force: proceeding anyway");
            return true;
        }
        eprintln!("  Use --force to attempt anyway, or free up space.");
        return false;
    }

    let headroom = free - required_bytes;
    eprintln!("    Headroom:  {}", crate::progress::fmt_size(headroom));
    eprintln!("  Enough space");
    true
}

mod fs2 {
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    pub struct SpaceInfo {
        total: u64,
        free: u64,
    }

    impl SpaceInfo {
        pub fn total_space(&self) -> u64 {
            self.total
        }
        pub fn free_space(&self) -> u64 {
            self.free
        }
    }

    pub fn statvfs(path: &Path) -> Result<SpaceInfo, String> {
        let meta = fs::metadata(path).map_err(|e| e.to_string())?;
        let _dev = meta.dev();

        #[cfg(target_os = "linux")]
        {
            use std::mem::MaybeUninit;
            let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::uninit();
            let p = path.to_string_lossy();
            let cstr = std::ffi::CString::new(p.as_ref()).map_err(|e| e.to_string())?;
            let rc = unsafe { libc::statvfs(cstr.as_ptr(), stat.as_mut_ptr()) };
            if rc != 0 {
                return Err("statvfs failed".to_string());
            }
            let s = unsafe { stat.assume_init() };
            let total = s.f_blocks * s.f_frsize;
            let free = s.f_bavail * s.f_frsize;
            Ok(SpaceInfo { total, free })
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err("unsupported platform".to_string())
        }
    }
}
