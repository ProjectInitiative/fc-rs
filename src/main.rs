use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::time::Instant;

fn shq(s: &str) -> Cow<'_, str> {
    shlex::try_quote(s).unwrap_or(Cow::Borrowed(s))
}

use clap::Parser;

use fc_rs::cli::CliArgs;
use fc_rs::copy::{copy_hybrid, create_links};
use fc_rs::dedup::{self};
use fc_rs::dedup_db::DedupDB;
use fc_rs::exclude::ExcludeList;
use fc_rs::fs_info;
use fc_rs::hashing::{self, HashAlgo};
use fc_rs::log;
use fc_rs::physical_offset;
use fc_rs::progress::{self, Progress};
use fc_rs::scanner::scan_source;
use fc_rs::ssh::SSHConnection;
use fc_rs::types::{CopyMode, DedupStrategy, FileEntry, RemoteSpec};
use fc_rs::update;
use fc_rs::verify::verify_copy;

fn parse_remote_path(path_str: &str) -> Option<RemoteSpec> {
    let re = regex::Regex::new(r"^(?:([^@]+)@)?([^:\s]+):(.+)$").ok()?;
    let caps = re.captures(path_str)?;
    let user = caps
        .get(1)
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| whoami::username());
    let host = caps.get(2)?.as_str().to_string();
    let path = caps.get(3)?.as_str().to_string();

    if host.len() == 1 && host.chars().all(|c| c.is_ascii_alphabetic()) {
        return None;
    }

    Some(RemoteSpec {
        user,
        host,
        port: 22,
        path,
    })
}

fn setup_hash_algo(choice: &str) {
    match choice {
        "xxh128" | "xxh3" => hashing::set_hash_algo(HashAlgo::Xxh3),
        "sha256" => hashing::set_hash_algo(HashAlgo::Sha256),
        _ => {}
    }
}

fn banner(msg: &str) {
    eprintln!("\n  {} {}", "─".repeat(50), msg);
    eprintln!("  {}", "─".repeat(50));
}

fn main() {
    let args = CliArgs::parse();

    if args.check_update {
        update::check_for_update();
        return;
    }

    if args.update.is_some() {
        update::self_update(args.update.as_deref());
        return;
    }

    let source = match &args.source {
        Some(s) => s.clone(),
        None => {
            eprintln!("Error: source is required");
            std::process::exit(1);
        }
    };
    let destination = match &args.destination {
        Some(d) => d.clone(),
        None => {
            eprintln!("Error: destination is required");
            std::process::exit(1);
        }
    };

    setup_hash_algo(&args.hash);
    if args.log_file.is_some() {
        log::set_enabled(true);
    }

    let buf_size = args.buffer * 1024 * 1024;
    let src_remote = parse_remote_path(&source);
    let dst_remote = parse_remote_path(&destination);

    let (dst_path, mode) = match (&src_remote, &dst_remote) {
        (Some(_), Some(_)) => (
            dst_remote.as_ref().unwrap().path.clone(),
            CopyMode::RemoteToRemote,
        ),
        (Some(_), None) => (destination.clone(), CopyMode::RemoteToLocal),
        (None, Some(_)) => (
            dst_remote.as_ref().unwrap().path.clone(),
            CopyMode::LocalToRemote,
        ),
        (None, None) => (destination.clone(), CopyMode::LocalToLocal),
    };

    let fs_info = if dst_remote.is_none() {
        Some(fs_info::detect_capabilities(Path::new(&dst_path)))
    } else {
        None
    };

    let fs_strategy = fs_info.as_ref().map(|i| i.strategy);

    banner("FAST BLOCK-ORDER COPY");
    eprintln!("  Source:      {}", source);
    eprintln!("  Destination: {}", destination);
    eprintln!("  Mode:        {:?}", mode);
    eprintln!("  Buffer:      {} MB", args.buffer);
    eprintln!(
        "  Dedup:       {}",
        if args.no_dedup {
            "disabled".to_string()
        } else if let Some(ref fi) = fs_info {
            format!("enabled ({:?})", fi.strategy)
        } else {
            "enabled".to_string()
        }
    );
    eprintln!("  Hash:        {}", hashing::hash_name());

    let mut src_ssh: Option<SSHConnection> = None;
    let mut dst_ssh: Option<SSHConnection> = None;

    if let Some(ref spec) = src_remote {
        banner("SSH - Connecting to source");
        let mut ssh = SSHConnection::new(spec.clone(), args.compress);
        if let Err(e) = ssh.connect() {
            eprintln!("  Error connecting to source: {}", e);
            std::process::exit(1);
        }
        eprintln!("  Connected to {}@{}:{}", spec.user, spec.host, spec.port);
        src_ssh = Some(ssh);
    }

    if let Some(ref spec) = dst_remote {
        banner("SSH - Connecting to destination");
        let mut ssh = SSHConnection::new(spec.clone(), args.compress);
        if let Err(e) = ssh.connect() {
            eprintln!("  Error connecting to destination: {}", e);
            std::process::exit(1);
        }
        eprintln!("  Connected to {}@{}:{}", spec.user, spec.host, spec.port);
        dst_ssh = Some(ssh);
    }

    banner("Phase 1 - Scanning source");

    let mut exclude_list = ExcludeList::new();
    for pat in &args.exclude {
        if let Err(e) = exclude_list.add(pat) {
            eprintln!("  Invalid exclude pattern '{}': {}", pat, e);
        }
    }

    let (entries, scan_errors) =
        if matches!(mode, CopyMode::RemoteToLocal | CopyMode::RemoteToRemote) {
            if let Some(ref ssh) = src_ssh {
                (scan_remote(ssh, &source, &exclude_list), Vec::new())
            } else {
                (
                    Vec::new(),
                    vec![("".to_string(), "No SSH connection".to_string())],
                )
            }
        } else {
            scan_source(
                Path::new(&source),
                Some(Path::new(&dst_path)),
                &exclude_list,
            )
        };

    if !scan_errors.is_empty() {
        eprintln!("  {} scan errors", scan_errors.len());
    }

    if entries.is_empty() {
        eprintln!("  No files found.");
        return;
    }

    let total_size: u64 = entries.iter().map(|e| e.size).sum();
    let total_files = entries.len();
    eprintln!(
        "  Total: {} in {} files",
        progress::fmt_size(total_size),
        total_files
    );

    let mut dedup_db: Option<DedupDB> = None;
    let mut link_map: HashMap<String, dedup::LinkTarget> = HashMap::new();
    let mut saved_bytes = 0u64;
    let mut copy_entries = entries;

    if mode == CopyMode::LocalToLocal && !args.no_dedup && !args.no_cache {
        if let Ok(db) = DedupDB::new(Path::new(&dst_path)) {
            dedup_db = Some(db);
        }
    }

    if !args.no_dedup {
        banner("Phase 2 - Deduplication");
        let result =
            dedup::deduplicate(&copy_entries, args.threads, dedup_db.as_ref(), fs_strategy);
        link_map = result.link_map;
        saved_bytes = result.saved_bytes;
        copy_entries = result.unique_entries;

        eprintln!("  Dedup complete:");
        eprintln!("    Unique files:    {}", copy_entries.len());
        eprintln!(
            "    Duplicates:      {} ({:.1}% of files)",
            link_map.len(),
            if total_files > 0 {
                link_map.len() as f64 / total_files as f64 * 100.0
            } else {
                0.0
            }
        );
        if saved_bytes > 0 {
            eprintln!(
                "    Space saved:     {} ({} reduction)",
                progress::fmt_size(saved_bytes),
                if total_size > 0 {
                    format!("{:.1}%", saved_bytes as f64 / total_size as f64 * 100.0)
                } else {
                    "0%".to_string()
                }
            );
        }
    }

    let unique_size: u64 = copy_entries.iter().map(|e| e.size).sum();
    let mut _skipped_count = 0usize;
    let mut _skipped_bytes = 0u64;

    if !args.overwrite && (Path::new(&dst_path).is_dir() || dst_ssh.is_some()) {
        banner("Phase 2b - Incremental check");
        match mode {
            CopyMode::LocalToLocal => {
                let (new_copy, new_link_map, sk_count, sk_bytes) =
                    filter_unchanged(&copy_entries, &link_map, Path::new(&dst_path), args.threads);
                copy_entries = new_copy; link_map = new_link_map;
                _skipped_count = sk_count; _skipped_bytes = sk_bytes;
            }
            CopyMode::LocalToRemote => {
                if let Some(ref ssh) = dst_ssh {
                    let (new_copy, new_link_map, sk_count, sk_bytes) =
                        filter_unchanged_remote(ssh, &copy_entries, &link_map, &dst_path);
                    copy_entries = new_copy; link_map = new_link_map;
                    _skipped_count = sk_count; _skipped_bytes = sk_bytes;
                }
            }
            _ => {}
        }
    }

    if copy_entries.is_empty() && link_map.is_empty() {
        banner("DONE - Nothing to copy");
        eprintln!("  All files are already up to date.");
        return;
    }

    banner("Phase 3 - Space check");
    let required = if fs_strategy == Some(DedupStrategy::None) && saved_bytes > 0 {
        unique_size + saved_bytes
    } else {
        unique_size
    };
    eprintln!("  Data to write: {}", progress::fmt_size(required));

    if mode == CopyMode::LocalToLocal {
        if !fc_rs::space_check::check_destination_space(Path::new(&dst_path), required, args.force)
        {
            std::process::exit(1);
        }
    }

    if args.dry_run {
        eprintln!("\n  DRY RUN - No files were copied.");
        return;
    }

    if matches!(mode, CopyMode::LocalToRemote | CopyMode::LocalToLocal) {
        banner("Phase 4 - Mapping physical disk layout");
        eprintln!("  {} files, sorting by size (no FIEMAP on this platform)",
            copy_entries.len());
        copy_entries = physical_offset::resolve_physical_offsets(&copy_entries, args.threads);
    }

    banner("Phase 5 - Copy");

    match mode {
        CopyMode::LocalToLocal => {
            let progress = Progress::new(unique_size, copy_entries.len());
            let t0 = Instant::now();
            copy_hybrid(
                &copy_entries,
                Path::new(&dst_path),
                &progress,
                buf_size,
                fs_strategy,
            );
            progress.finish();

            if !link_map.is_empty() {
                create_links(&link_map, Path::new(&dst_path), fs_strategy);
            }

            if let Some(ref db) = dedup_db {
                let dst_rows: Vec<_> = copy_entries
                    .iter()
                    .filter_map(|e| {
                        e.content_hash
                            .as_ref()
                            .map(|h| (e.rel.clone(), e.size, h.clone()))
                    })
                    .collect();
                if !dst_rows.is_empty() {
                    db.store_dest_batch(&dst_rows);
                }
            }

            if !args.no_verify {
                verify_copy(&copy_entries, &link_map, Path::new(&dst_path));
            }

            let elapsed = t0.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                unique_size as f64 / elapsed
            } else {
                0.0
            };
            banner("DONE");
            eprintln!(
                "  Files:   {} total ({} copied + {} linked)",
                total_files,
                copy_entries.len(),
                link_map.len()
            );
            eprintln!(
                "  Data:    {} written{}",
                progress::fmt_size(unique_size),
                if saved_bytes > 0 {
                    format!(" ({} saved by dedup)", progress::fmt_size(saved_bytes))
                } else {
                    String::new()
                }
            );
            eprintln!("  Time:    {}", progress::fmt_time(elapsed as u64));
            eprintln!("  Speed:   {}/s", progress::fmt_size_f64(speed));
        }

        CopyMode::LocalToRemote => {
            if let Some(ref mut ssh) = dst_ssh {
                let _ = ssh.mkdir_p(&dst_path);
                let progress = Progress::new(unique_size, copy_entries.len());
                let t0 = Instant::now();

                let cfg = fc_rs::transfer::TransferConfig {
                    workers: args.workers,
                    compress_zstd: args.compress,
                    zstd_level: 3,
                    buf_size,
                };
                fc_rs::transfer::copy_remote_parallel(
                    &copy_entries, &dst_path, &progress, &cfg, &ssh.spec,
                );
                progress.finish();

                if !link_map.is_empty() {
                    create_links_remote(ssh, &link_map, &dst_path);
                }

                let elapsed = t0.elapsed().as_secs_f64();
                let speed = if elapsed > 0.0 {
                    unique_size as f64 / elapsed
                } else {
                    0.0
                };

                fc_rs::manifest::save_remote_manifest(ssh, &dst_path, &copy_entries, &link_map);

                if !args.no_verify {
                    verify_copy_remote(ssh, &copy_entries, &link_map, &dst_path);
                }

                banner("DONE");
                eprintln!(
                    "  Remote:  {}@{}:{}",
                    dst_remote.as_ref().unwrap().user,
                    dst_remote.as_ref().unwrap().host,
                    dst_path
                );
                eprintln!("  Files:   {} total", total_files);
                eprintln!("  Data:    {} sent", progress::fmt_size(unique_size));
                eprintln!("  Time:    {}", progress::fmt_time(elapsed as u64));
                eprintln!("  Speed:   {}/s", progress::fmt_size_f64(speed));
            }
        }

        CopyMode::RemoteToLocal => {
            let progress = Progress::new(unique_size, copy_entries.len());
            let t0 = Instant::now();

            if args.workers > 1 {
                if let Some(ref spec) = src_remote {
                    let cfg = fc_rs::transfer::TransferConfig {
                        workers: args.workers,
                        compress_zstd: args.compress,
                        zstd_level: 3,
                        buf_size,
                    };
                    fc_rs::transfer::copy_remote_pull_parallel(
                        &copy_entries,
                        spec,
                        &source,
                        Path::new(&dst_path),
                        &progress,
                        &cfg,
                    );
                }
            } else if let Some(ref mut ssh) = src_ssh {
                copy_hybrid_remote_to_local(
                    &copy_entries,
                    ssh,
                    &source,
                    Path::new(&dst_path),
                    &progress,
                    buf_size,
                );
            }
            progress.finish();

            if !link_map.is_empty() {
                create_links(&link_map, Path::new(&dst_path), fs_strategy);
            }

            let elapsed = t0.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                unique_size as f64 / elapsed
            } else {
                0.0
            };

            if !args.no_verify {
                verify_copy(&copy_entries, &link_map, Path::new(&dst_path));
            }

            banner("DONE");
            eprintln!(
                "  Source:  {}@{}",
                src_remote.as_ref().unwrap().user,
                src_remote.as_ref().unwrap().host
            );
            eprintln!("  Files:   {} total", total_files);
            eprintln!("  Data:    {} downloaded", progress::fmt_size(unique_size));
            eprintln!("  Time:    {}", progress::fmt_time(elapsed as u64));
            eprintln!("  Speed:   {}/s", progress::fmt_size_f64(speed));
        }

        CopyMode::RemoteToRemote => {
            if let (Some(ref src_spec), Some(ref dst_spec)) =
                (src_remote.as_ref(), dst_remote.as_ref())
            {
                if let Some(ref mut s) = dst_ssh { let _ = s.mkdir_p(&dst_path); }
                let progress = Progress::new(unique_size, copy_entries.len());
                let t0 = Instant::now();

                if args.workers > 1 {
                    let cfg = fc_rs::transfer::TransferConfig {
                        workers: args.workers,
                        compress_zstd: args.compress,
                        zstd_level: 3,
                        buf_size,
                    };
                    fc_rs::transfer::copy_remote_relay_parallel(
                        &copy_entries,
                        src_spec,
                        dst_spec,
                        &source,
                        &dst_path,
                        &progress,
                        &cfg,
                    );
                } else if let (Some(ref mut src_ssh_val), Some(ref mut dst_ssh_val)) =
                    (&mut src_ssh, &mut dst_ssh)
                {
                    let _ = dst_ssh_val.mkdir_p(&dst_path);
                    copy_hybrid_r2r(
                        &copy_entries,
                        src_ssh_val,
                        dst_ssh_val,
                        &source,
                        &dst_path,
                        &progress,
                        buf_size,
                    );
                }
                progress.finish();

                if let Some(ref mut s) = dst_ssh {
                    if !link_map.is_empty() {
                        create_links_remote(s, &link_map, &dst_path);
                    }

                    let elapsed = t0.elapsed().as_secs_f64();
                    let speed = if elapsed > 0.0 {
                        unique_size as f64 / elapsed
                    } else {
                        0.0
                    };

                    fc_rs::manifest::save_remote_manifest(s, &dst_path, &copy_entries, &link_map);

                    banner("DONE");
                    eprintln!("  Files:   {} total", total_files);
                    eprintln!("  Data:    {} relayed", progress::fmt_size(unique_size));
                    eprintln!("  Time:    {}", progress::fmt_time(elapsed as u64));
                    eprintln!("  Speed:   {}/s", progress::fmt_size_f64(speed));
                }
            }
        }
    }
}

fn scan_remote(ssh: &SSHConnection, src_root: &str, _excludes: &ExcludeList) -> Vec<FileEntry> {
    let clean = shq(src_root);
    let cmd = format!("find {} -type f -printf \"%s\\t%p\\n\" 2>/dev/null || find {} -type f -exec stat -c \"%s %n\" {{}} + 2>/dev/null", clean, clean);
    let (stdout, _, rc) = ssh.exec_cmd(&cmd, 60000).unwrap_or_default();
    if rc != 0 {
        return Vec::new();
    }

    let mut entries = Vec::new();
    for line in stdout.lines() {
        if line.is_empty() {
            continue;
        }
        if let Some((size_str, path)) = line.split_once('\t').or_else(|| line.split_once(' ')) {
            if let Ok(size) = size_str.parse::<u64>() {
                let rel = path
                    .strip_prefix(src_root)
                    .unwrap_or(path)
                    .trim_start_matches('/')
                    .to_string();
                entries.push(FileEntry {
                    src: path.to_string(),
                    rel,
                    size,
                    physical_offset: 0,
                    content_hash: None,
                });
            }
        }
    }
    entries
}

fn filter_unchanged(
    entries: &[FileEntry],
    link_map: &HashMap<String, dedup::LinkTarget>,
    dst_root: &Path,
    _threads: usize,
) -> (
    Vec<FileEntry>,
    HashMap<String, dedup::LinkTarget>,
    usize,
    u64,
) {
    let mut need_copy = Vec::new();
    let mut skipped = 0usize;
    let mut skipped_bytes = 0u64;
    let mut new_link_map = link_map.clone();

    for entry in entries {
        let dst_path = dst_root.join(&entry.rel);
        if !dst_path.exists() {
            need_copy.push(entry.clone());
            continue;
        }
        let dst_size = std::fs::metadata(&dst_path).map(|m| m.len()).unwrap_or(0);
        if dst_size != entry.size {
            need_copy.push(entry.clone());
            continue;
        }
        if let Some(ref src_hash) = entry.content_hash {
            if let Some(dst_hash) = hashing::hash_file(&dst_path) {
                if *src_hash == dst_hash {
                    skipped += 1;
                    skipped_bytes += entry.size;
                    continue;
                }
            }
        }
        need_copy.push(entry.clone());
    }

    let mut filtered_links = HashMap::new();
    for (dup_rel, target) in new_link_map.drain() {
        let dst_path = dst_root.join(&dup_rel);
        if dst_path.exists() {
            skipped += 1;
        } else {
            filtered_links.insert(dup_rel, target);
        }
    }

    (need_copy, filtered_links, skipped, skipped_bytes)
}

fn filter_unchanged_remote(
    ssh: &SSHConnection,
    entries: &[FileEntry],
    link_map: &HashMap<String, dedup::LinkTarget>,
    remote_root: &str,
) -> (
    Vec<FileEntry>,
    HashMap<String, dedup::LinkTarget>,
    usize,
    u64,
) {
    eprint!("  Checking remote for existing files...");

    let cmd = format!("find {} -type f -printf \"%s\\t%p\\n\" 2>/dev/null", shq(remote_root));
    let (stdout, _, rc) = ssh.exec_cmd(&cmd, 60000).unwrap_or_default();
    if rc != 0 {
        eprintln!("\r  Could not scan remote — copying all files                  ");
        return (entries.to_vec(), link_map.clone(), 0, 0);
    }

    let mut existing: HashMap<String, u64> = HashMap::new();
    for line in stdout.lines() {
        if line.is_empty() { continue; }
        if let Some((sz, path)) = line.split_once('\t') {
            if let Ok(size) = sz.parse::<u64>() {
                let rel = path.strip_prefix(remote_root).unwrap_or(path).trim_start_matches('/').to_string();
                existing.insert(rel, size);
            }
        }
    }

    eprintln!("\r  Remote has {} files                              ", existing.len());

    let mut need_copy = Vec::new();
    let mut skipped = 0usize;
    let mut skipped_bytes = 0u64;
    let mut new_link_map = link_map.clone();

    for entry in entries {
        match existing.get(&entry.rel) {
            Some(&size) if size == entry.size => {
                skipped += 1;
                skipped_bytes += entry.size;
            }
            _ => need_copy.push(entry.clone()),
        }
    }

    let mut filtered_links = HashMap::new();
    for (dup_rel, target) in new_link_map.drain() {
        if existing.contains_key(&dup_rel) {
            skipped += 1;
        } else {
            filtered_links.insert(dup_rel, target);
        }
    }

    eprintln!("  Incremental check: {} to copy, {} skipped ({} saved)",
        need_copy.len(), skipped, progress::fmt_size(skipped_bytes));
    (need_copy, filtered_links, skipped, skipped_bytes)
}

fn verify_copy_remote(
    ssh: &SSHConnection,
    entries: &[FileEntry],
    link_map: &HashMap<String, dedup::LinkTarget>,
    remote_root: &str,
) -> bool {
    let total = entries.len() + link_map.len();
    eprint!("  Verifying {} files on remote...", total);

    let cmd = format!(
        "find {} -type f -printf \"%s\\t%p\\n\" 2>/dev/null",
        shq(remote_root)
    );
    let (stdout, _, rc) = ssh.exec_cmd(&cmd, 60000).unwrap_or_default();

    if rc != 0 {
        eprintln!("\r  Could not verify remote files                    ");
        return false;
    }

    let mut remote_files: HashMap<String, u64> = HashMap::new();
    for line in stdout.lines() {
        if line.is_empty() {
            continue;
        }
        if let Some((size_str, path)) = line.split_once('\t') {
            if let Ok(size) = size_str.parse::<u64>() {
                let rel = path
                    .strip_prefix(remote_root)
                    .unwrap_or(path)
                    .trim_start_matches('/')
                    .to_string();
                remote_files.insert(rel, size);
            }
        }
    }

    let mut missing = Vec::new();
    let mut mismatches = Vec::new();

    for entry in entries {
        match remote_files.get(&entry.rel) {
            None => missing.push(entry.rel.clone()),
            Some(&size) if size != entry.size => {
                mismatches.push((entry.rel.clone(), entry.size, size))
            }
            _ => {}
        }
    }

    for dup_rel in link_map.keys() {
        if !remote_files.contains_key(dup_rel) {
            missing.push(dup_rel.clone());
        }
    }

    if missing.is_empty() && mismatches.is_empty() {
        eprintln!(
            "\r  Verified: all {} files OK on remote              ",
            total
        );
        true
    } else {
        eprintln!("\r  Verification failed on remote:                   ");
        for m in missing.iter().take(10) {
            eprintln!("    MISSING: {}", m);
        }
        for (rel, exp, act) in mismatches.iter().take(10) {
            eprintln!("    SIZE MISMATCH: {} ({} -> {})", rel, exp, act);
        }
        false
    }
}

fn copy_individual_remote(
    entries: &[FileEntry],
    ssh: &mut SSHConnection,
    remote_root: &str,
    progress: &Progress,
    _buf_size: usize,
) {
    for entry in entries {
        let remote_path = format!("{}/{}", remote_root, entry.rel);
        let src_path = Path::new(&entry.src);

        if entry.size == 0 {
            let _ = ssh.exec_cmd(&format!("touch {}", shq(&remote_path)), 30000);
            progress.update(0, 1);
            progress.display();
            continue;
        }

        if let Ok(sftp) = ssh.open_sftp() {
            if let Ok(mut remote_file) = sftp.create(&std::path::PathBuf::from(&remote_path)) {
                if let Ok(mut local_file) = std::fs::File::open(src_path) {
                    let mut buf = vec![0u8; 1048576];
                    loop {
                        let n = local_file.read(&mut buf).unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        let _ = remote_file.write_all(&buf[..n]);
                        progress.update(n as u64, 0);
                        progress.display();
                    }
                }
            }
        }
        progress.update(0, 1);
    }
}

fn copy_hybrid_remote(
    entries: &[FileEntry],
    ssh: &mut SSHConnection,
    remote_root: &str,
    progress: &Progress,
    buf_size: usize,
) {
    if ssh.caps.contains(&"tar".to_string()) {
        copy_block_stream_remote(entries, ssh, remote_root, progress);
    } else {
        copy_individual_remote(entries, ssh, remote_root, progress, buf_size);
    }
}

fn copy_block_stream_remote(
    entries: &[FileEntry],
    ssh: &mut SSHConnection,
    remote_root: &str,
    progress: &Progress,
) {
    if entries.is_empty() {
        return;
    }
    for entry in entries {
        let remote_parent = std::path::Path::new(&entry.rel)
            .parent()
            .unwrap_or(std::path::Path::new(""))
            .to_string_lossy()
            .to_string();
        if !remote_parent.is_empty() {
            let _ = ssh.exec_cmd(
                &format!(
                    "mkdir -p {}/{}",
                    shq(remote_root),
                    shq(&remote_parent)
                ),
                10000,
            );
        }

        let remote_path = format!("{}/{}", remote_root, entry.rel);
        if let Ok(sftp) = ssh.open_sftp() {
            if let Ok(mut remote_file) = sftp.create(&std::path::PathBuf::from(&remote_path)) {
                if let Ok(data) = std::fs::read(&entry.src) {
                    let _ = remote_file.write_all(&data);
                }
            }
        }
        progress.update(entry.size, 1);
        progress.display();
    }
}

fn create_links_remote(
    ssh: &mut SSHConnection,
    link_map: &HashMap<String, dedup::LinkTarget>,
    remote_root: &str,
) {
    eprint!("  Creating {} links on remote...", link_map.len());
    for (dup_rel, target) in link_map {
        let _dup_path = format!("{}/{}", remote_root, dup_rel);
        let target_path = match target {
            dedup::LinkTarget::Rel(rel) => format!("{}/{}", remote_root, rel),
            dedup::LinkTarget::Abs(abs) => abs.clone(),
        };
        let _ = ssh.exec_cmd(&format!("ln {}", shq(&target_path)), 10000);
    }
    eprintln!(" done");
}

fn copy_hybrid_remote_to_local(
    entries: &[FileEntry],
    ssh: &mut SSHConnection,
    _src_root: &str,
    dst_root: &Path,
    progress: &Progress,
    buf_size: usize,
) {
    if ssh.caps.contains(&"tar".to_string()) {
        copy_block_stream_remote_to_local(entries, ssh, dst_root, progress);
    } else {
        copy_individual_remote_to_local(entries, ssh, dst_root, progress, buf_size);
    }
}

fn copy_individual_remote_to_local(
    entries: &[FileEntry],
    ssh: &mut SSHConnection,
    dst_root: &Path,
    progress: &Progress,
    _buf_size: usize,
) {
    for entry in entries {
        let dst_path = dst_root.join(&entry.rel);
        if let Some(parent) = dst_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        if entry.size == 0 {
            let _ = std::fs::File::create(&dst_path);
            progress.update(0, 1);
            progress.display();
            continue;
        }

        if let Ok(sftp) = ssh.open_sftp() {
            if let Ok(mut remote_file) = sftp.open(&std::path::PathBuf::from(&entry.src)) {
                if let Ok(mut local_file) = std::fs::File::create(&dst_path) {
                    let mut buf = vec![0u8; 1048576];
                    loop {
                        let n = remote_file.read(&mut buf).unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        let _ = local_file.write_all(&buf[..n]);
                        progress.update(n as u64, 0);
                        progress.display();
                    }
                }
            }
        }
        progress.update(0, 1);
    }
}

fn copy_block_stream_remote_to_local(
    entries: &[FileEntry],
    ssh: &mut SSHConnection,
    dst_root: &Path,
    progress: &Progress,
) {
    if entries.is_empty() {
        return;
    }
    for entry in entries {
        let dst_path = dst_root.join(&entry.rel);
        if let Some(parent) = dst_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        if let Ok(sftp) = ssh.open_sftp() {
            if let Ok(mut remote_file) = sftp.open(&std::path::PathBuf::from(&entry.src)) {
                let mut data = Vec::new();
                let _ = remote_file.read_to_end(&mut data);
                let _ = std::fs::write(&dst_path, &data);
            }
        }
        progress.update(entry.size, 1);
        progress.display();
    }
}

fn copy_hybrid_r2r(
    entries: &[FileEntry],
    src_ssh: &mut SSHConnection,
    dst_ssh: &mut SSHConnection,
    _src_root: &str,
    dst_root: &str,
    progress: &Progress,
    buf_size: usize,
) {
    if src_ssh.caps.contains(&"tar".to_string()) && dst_ssh.caps.contains(&"tar".to_string()) {
        copy_block_stream_r2r(entries, src_ssh, dst_ssh, dst_root, progress);
    } else {
        copy_individual_r2r(entries, src_ssh, dst_ssh, dst_root, progress, buf_size);
    }
}

fn copy_individual_r2r(
    entries: &[FileEntry],
    src_ssh: &mut SSHConnection,
    dst_ssh: &mut SSHConnection,
    dst_root: &str,
    progress: &Progress,
    _buf_size: usize,
) {
    for entry in entries {
        let remote_dst = format!("{}/{}", dst_root, entry.rel);
        let dst_dir = std::path::Path::new(&remote_dst)
            .parent()
            .unwrap_or(std::path::Path::new(""))
            .to_string_lossy()
            .to_string();
        if !dst_dir.is_empty() {
            let _ = dst_ssh.mkdir_p(&dst_dir);
        }

        if entry.size == 0 {
            let cmd = format!("touch {}", shq(&remote_dst));
            let _ = dst_ssh.exec_cmd(&cmd, 10000);
            progress.update(0, 1);
            progress.display();
            continue;
        }

        if let (Ok(sftp_src), Ok(sftp_dst)) = (src_ssh.open_sftp(), dst_ssh.open_sftp()) {
            if let Ok(mut src_file) = sftp_src.open(&std::path::PathBuf::from(&entry.src)) {
                if let Ok(mut dst_file) = sftp_dst.create(&std::path::PathBuf::from(&remote_dst)) {
                    let mut buf = vec![0u8; 1048576];
                    loop {
                        let n = src_file.read(&mut buf).unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        let _ = dst_file.write_all(&buf[..n]);
                        progress.update(n as u64, 0);
                        progress.display();
                    }
                }
            }
        }
        progress.update(0, 1);
    }
}

fn copy_block_stream_r2r(
    entries: &[FileEntry],
    src_ssh: &mut SSHConnection,
    dst_ssh: &mut SSHConnection,
    dst_root: &str,
    progress: &Progress,
) {
    copy_individual_r2r(entries, src_ssh, dst_ssh, dst_root, progress, 1048576);
}
