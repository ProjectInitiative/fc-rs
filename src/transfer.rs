use std::borrow::Cow;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use crossbeam_channel::unbounded;

use crate::progress::Progress;
use crate::ssh::SSHConnection;
use crate::types::{FileEntry, RemoteSpec, SMALL_FILE_THRESHOLD};

fn shq(s: &str) -> Cow<'_, str> {
    shlex::try_quote(s).unwrap_or(Cow::Borrowed(s))
}

#[derive(Clone)]
pub struct TransferConfig {
    pub workers: usize,
    pub compress_zstd: bool,
    pub zstd_level: i32,
    pub buf_size: usize,
}

impl Default for TransferConfig {
    fn default() -> Self {
        TransferConfig { workers: 4, compress_zstd: false, zstd_level: 3, buf_size: 4 * 1024 * 1024 }
    }
}

pub enum TransferJob {
    Batch(Vec<FileEntry>),
    Shutdown,
}

pub struct TransferPlanner {
    pub batches: Vec<Vec<FileEntry>>,
}

impl TransferPlanner {
    pub fn new(entries: &[FileEntry], num_workers: usize) -> Self {
        let large_threshold = SMALL_FILE_THRESHOLD;
        let mut large = Vec::new();
        let mut small = Vec::new();
        for e in entries {
            if e.size >= large_threshold { large.push(e.clone()); } else { small.push(e.clone()); }
        }
        large.sort_by(|a, b| b.size.cmp(&a.size));
        let mut batches: Vec<Vec<FileEntry>> = (0..num_workers).map(|_| Vec::new()).collect();
        for (i, entry) in large.iter().enumerate() {
            batches[i % num_workers].push(entry.clone());
        }
        let mut batch: Vec<FileEntry> = Vec::new();
        let mut batch_size = 0u64;
        let limit = 64 * 1024 * 1024;
        let mut wi = 0usize;
        for entry in &small {
            if batch_size + entry.size > limit && !batch.is_empty() {
                batches[wi % num_workers].append(&mut batch);
                batch_size = 0; wi += 1;
            }
            batch_size += entry.size;
            batch.push(entry.clone());
        }
        if !batch.is_empty() { batches[wi % num_workers].append(&mut batch); }
        TransferPlanner { batches }
    }

    pub fn into_jobs(self) -> Vec<TransferJob> {
        self.batches.into_iter().filter(|b| !b.is_empty()).map(TransferJob::Batch).collect()
    }
}

// ── Push mode: local → remote ─────────────────────────────────────────

pub fn copy_remote_parallel(
    entries: &[FileEntry],
    spec: &RemoteSpec,
    remote_root: &str,
    progress: &Progress,
    config: &TransferConfig,
) {
    let planner = TransferPlanner::new(entries, config.workers);
    let jobs = planner.into_jobs();
    if jobs.is_empty() { return; }

    let (job_tx, job_rx) = unbounded::<TransferJob>();
    let (result_tx, result_rx) = unbounded::<(u64, usize)>();

    for job in jobs { job_tx.send(job).ok(); }
    for _ in 0..config.workers { job_tx.send(TransferJob::Shutdown).ok(); }

    let handles: Vec<_> = (0..config.workers).map(|worker_id| {
        let jr = job_rx.clone();
        let rt = result_tx.clone();
        let sp = spec.clone();
        let rr = remote_root.to_string();
        let cfg = config.clone();
        std::thread::spawn(move || {
            let mut ssh = SSHConnection::new(sp, false);
            if let Err(e) = ssh.connect() {
                eprintln!("  Worker {}: SSH connect failed: {}", worker_id, e);
                return;
            }
            let _ = ssh.exec_cmd(&format!("mkdir -p {}", shq(&rr)), 30000);
            let mut total = (0u64, 0usize);
            loop {
                match jr.recv() {
                    Ok(TransferJob::Shutdown) | Err(_) => break,
                    Ok(TransferJob::Batch(batch)) => {
                        let (b, f) = send_tar_batch(&batch, &mut ssh, &rr, &cfg);
                        total.0 += b; total.1 += f;
                    }
                }
            }
            let _ = rt.send(total);
        })
    }).collect();

    for _ in &handles {
        if let Ok((b, f)) = result_rx.recv() { progress.update(b, f); progress.display(); }
    }
    for h in handles { h.join().ok(); }
}

// ── Pull mode: remote → local ─────────────────────────────────────────

pub fn copy_remote_pull_parallel(
    entries: &[FileEntry],
    spec: &RemoteSpec,
    src_root: &str,
    dst_root: &Path,
    progress: &Progress,
    config: &TransferConfig,
) {
    let planner = TransferPlanner::new(entries, config.workers);
    let jobs = planner.into_jobs();
    if jobs.is_empty() { return; }

    let (job_tx, job_rx) = unbounded::<TransferJob>();
    let (result_tx, result_rx) = unbounded::<(u64, usize)>();

    for job in jobs { job_tx.send(job).ok(); }
    for _ in 0..config.workers { job_tx.send(TransferJob::Shutdown).ok(); }

    let handles: Vec<_> = (0..config.workers).map(|worker_id| {
        let jr = job_rx.clone();
        let rt = result_tx.clone();
        let sp = spec.clone();
        let sr = src_root.to_string();
        let dr = dst_root.to_path_buf();
        let cfg = config.clone();
        std::thread::spawn(move || {
            let mut ssh = SSHConnection::new(sp, false);
            if let Err(e) = ssh.connect() {
                eprintln!("  Worker {}: SSH connect failed: {}", worker_id, e);
                return;
            }
            let mut total_bytes = 0u64;
            let mut total_files = 0usize;
            loop {
                let job = match jr.recv() { Ok(j) => j, Err(_) => break };
                match job {
                    TransferJob::Shutdown => break,
                    TransferJob::Batch(batch) => {
                        let (b, f) = recv_tar_batch(&batch, &mut ssh, &sr, &dr, &cfg);
                        total_bytes += b; total_files += f;
                    }
                }
            }
            let _ = rt.send((total_bytes, total_files));
        })
    }).collect();

    for _ in &handles {
        if let Ok((b, f)) = result_rx.recv() { progress.update(b, f); progress.display(); }
    }
    for h in handles { h.join().ok(); }
}

// ── Relay mode: remote → remote ───────────────────────────────────────

pub fn copy_remote_relay_parallel(
    entries: &[FileEntry],
    src_spec: &RemoteSpec,
    dst_spec: &RemoteSpec,
    src_root: &str,
    dst_root: &str,
    progress: &Progress,
    config: &TransferConfig,
) {
    let planner = TransferPlanner::new(entries, config.workers);
    let jobs = planner.into_jobs();
    if jobs.is_empty() { return; }

    let (job_tx, job_rx) = unbounded::<TransferJob>();
    let (result_tx, result_rx) = unbounded::<(u64, usize)>();

    for job in jobs { job_tx.send(job).ok(); }
    for _ in 0..config.workers { job_tx.send(TransferJob::Shutdown).ok(); }

    let handles: Vec<_> = (0..config.workers).map(|worker_id| {
        let jr = job_rx.clone();
        let rt = result_tx.clone();
        let ss = src_spec.clone();
        let ds = dst_spec.clone();
        let sr = src_root.to_string();
        let dr = dst_root.to_string();
        let cfg = config.clone();
        std::thread::spawn(move || {
            let mut src_ssh = SSHConnection::new(ss, false);
            let mut dst_ssh = SSHConnection::new(ds, false);
            if let Err(e) = src_ssh.connect() {
                eprintln!("  Worker {}: src SSH failed: {}", worker_id, e); return;
            }
            if let Err(e) = dst_ssh.connect() {
                eprintln!("  Worker {}: dst SSH failed: {}", worker_id, e); return;
            }
            let _ = dst_ssh.exec_cmd(&format!("mkdir -p {}", shq(&dr)), 30000);
            let mut total_bytes = 0u64;
            let mut total_files = 0usize;
            loop {
                let job = match jr.recv() { Ok(j) => j, Err(_) => break };
                match job {
                    TransferJob::Shutdown => break,
                    TransferJob::Batch(batch) => {
                        let (b, f) = relay_tar_batch(&batch, &mut src_ssh, &mut dst_ssh, &sr, &dr, &cfg);
                        total_bytes += b; total_files += f;
                    }
                }
            }
            let _ = rt.send((total_bytes, total_files));
        })
    }).collect();

    for _ in &handles {
        if let Ok((b, f)) = result_rx.recv() { progress.update(b, f); progress.display(); }
    }
    for h in handles { h.join().ok(); }
}

// ── Push: build tar locally, send over SSH ────────────────────────────

const MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;  // 64 MB per tar batch

fn send_tar_batch(
    batch: &[FileEntry], ssh: &mut SSHConnection, remote_root: &str, config: &TransferConfig,
) -> (u64, usize) {
    if batch.is_empty() { return (0, 0); }
    let mut total_bytes = 0u64;
    let mut total_files = 0usize;
    let mut chunk_start = 0usize;

    while chunk_start < batch.len() {
        let mut chunk_end = chunk_start;
        let mut chunk_size = 0u64;
        while chunk_end < batch.len() && chunk_size < MAX_BATCH_BYTES {
            chunk_size += batch[chunk_end].size;
            chunk_end += 1;
        }
        if chunk_end == chunk_start { chunk_end = chunk_start + 1; }

        let chunk = &batch[chunk_start..chunk_end];
        chunk_start = chunk_end;

        let tar_data = build_tar(chunk, config);
        if tar_data.is_empty() { continue; }

        let cmd = if config.compress_zstd {
            format!("zstd -d 2>/dev/null | tar xf - --no-same-owner --no-same-permissions -C {}", shq(remote_root))
        } else {
            format!("tar xf - --no-same-owner --no-same-permissions -C {}", shq(remote_root))
        };

        match ssh.open_channel() {
            Ok(mut channel) => {
                if channel.exec(&cmd).is_err() { break; }
                let _ = channel.write_all(&tar_data);
                let _ = channel.eof();
                channel.wait_close().ok();
                total_bytes += tar_data.len() as u64;
                total_files += chunk.len();
            }
            Err(e) => { eprintln!("  channel error: {}", e); break; }
        }
    }
    (total_bytes, total_files)
}

// ── Pull: exec tar on remote, read stream, extract locally ────────────

fn recv_tar_batch(
    batch: &[FileEntry], ssh: &mut SSHConnection, src_root: &str, dst_root: &Path, config: &TransferConfig,
) -> (u64, usize) {
    if batch.is_empty() { return (0, 0); }
    let mut names = Vec::new();
    for e in batch { names.extend_from_slice(e.rel.as_bytes()); names.push(b'\0'); }

    let cmd = format!("cd {} && tar cf - --null -T -", shq(src_root));
    let mut channel = match ssh.open_channel() { Ok(c) => c, Err(e) => { eprintln!("  ch err: {}", e); return (0, 0); } };
    if channel.exec(&cmd).is_err() { return (0, 0); }
    let _ = channel.write_all(&names);
    let _ = channel.eof();

    let mut raw = Vec::new();
    if channel.read_to_end(&mut raw).is_err() { return (0, 0); }
    channel.wait_close().ok();

    let data = if config.compress_zstd { zstd::decode_all(&raw[..]).unwrap_or(raw) } else { raw };
    let mut archive = tar::Archive::new(std::io::Cursor::new(&data));
    if archive.unpack(dst_root).is_err() { return (0, 0); }

    (batch.iter().map(|e| e.size).sum(), batch.len())
}

// ── Relay: pipe tar from source SSH → local → dest SSH ───────────────

fn relay_tar_batch(
    batch: &[FileEntry], src_ssh: &mut SSHConnection, dst_ssh: &mut SSHConnection,
    src_root: &str, dst_root: &str, config: &TransferConfig,
) -> (u64, usize) {
    if batch.is_empty() { return (0, 0); }
    let mut names = Vec::new();
    for e in batch { names.extend_from_slice(e.rel.as_bytes()); names.push(b'\0'); }

    let src_cmd = format!("cd {} && tar cf - --null -T -", shq(src_root));
    let dst_cmd = if config.compress_zstd {
        format!("zstd -d 2>/dev/null | tar xf - --no-same-owner --no-same-permissions -C {}", shq(dst_root))
    } else {
        format!("tar xf - --no-same-owner --no-same-permissions -C {}", shq(dst_root))
    };

    let mut sc = match src_ssh.open_channel() { Ok(c) => c, Err(e) => { eprintln!("  src ch: {}", e); return (0, 0); } };
    let mut dc = match dst_ssh.open_channel() { Ok(c) => c, Err(e) => { eprintln!("  dst ch: {}", e); return (0, 0); } };

    if sc.exec(&src_cmd).is_err() || dc.exec(&dst_cmd).is_err() { return (0, 0); }
    let _ = sc.write_all(&names); let _ = sc.eof();

    let mut relayed = 0u64;
    let mut buf = vec![0u8; config.buf_size];
    loop {
        let n = match sc.read(&mut buf) { Ok(0) => break, Ok(n) => n, Err(_) => break };
        if dc.write_all(&buf[..n]).is_err() { break; }
        relayed += n as u64;
    }
    let _ = dc.eof(); sc.wait_close().ok(); dc.wait_close().ok();
    (relayed, batch.len())
}

// ── Shared tar builder ────────────────────────────────────────────────

fn build_tar(batch: &[FileEntry], config: &TransferConfig) -> Vec<u8> {
    let total_size: u64 = batch.iter().map(|e| e.size).sum();
    let cap = (total_size as usize).saturating_add(total_size as usize / 4).min(128 * 1024 * 1024);
    let mut tar_data = Vec::with_capacity(cap);
    {
        let mut tar_builder = tar::Builder::new(std::io::Cursor::new(&mut tar_data));
        for entry in batch {
            let data = match std::fs::read(&entry.src) { Ok(d) => d, Err(_) => continue };
            if let Ok(meta) = std::fs::metadata(&entry.src) {
                let mut header = tar::Header::new_gnu();
                header.set_size(data.len() as u64);
                header.set_mtime(meta.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_secs()).unwrap_or(0));
                header.set_mode(meta.permissions().mode());
                header.set_entry_type(tar::EntryType::Regular);
                let _ = tar_builder.append_data(&mut header, &entry.rel, std::io::Cursor::new(&data));
            }
        }
        tar_builder.finish().ok();
    }
    if config.compress_zstd { tar_data = zstd::encode_all(&tar_data[..], config.zstd_level).unwrap_or(tar_data); }
    tar_data
}
