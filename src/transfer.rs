use std::borrow::Cow;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn shq(s: &str) -> Cow<'_, str> {
    shlex::try_quote(s).unwrap_or(Cow::Borrowed(s))
}

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::progress::Progress;
use crate::ssh::SSHConnection;
use crate::types::{FileEntry, RemoteSpec, SMALL_FILE_THRESHOLD};

#[derive(Clone)]
pub struct TransferConfig {
    pub workers: usize,
    pub compress_zstd: bool,
    pub zstd_level: i32,
    pub buf_size: usize,
}

impl Default for TransferConfig {
    fn default() -> Self {
        TransferConfig {
            workers: 4,
            compress_zstd: false,
            zstd_level: 3,
            buf_size: 4 * 1024 * 1024,
        }
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
        let mut large: Vec<FileEntry> = Vec::new();
        let mut small: Vec<FileEntry> = Vec::new();

        for e in entries {
            if e.size >= large_threshold {
                large.push(e.clone());
            } else {
                small.push(e.clone());
            }
        }

        large.sort_by(|a, b| b.size.cmp(&a.size));

        let mut batches: Vec<Vec<FileEntry>> = (0..num_workers).map(|_| Vec::new()).collect();

        for (i, entry) in large.iter().enumerate() {
            batches[i % num_workers].push(entry.clone());
        }

        let mut small_batch: Vec<FileEntry> = Vec::new();
        let mut small_size: u64 = 0;
        let batch_size_limit: u64 = 64 * 1024 * 1024;

        let mut wi = 0usize;
        for entry in &small {
            if small_size + entry.size > batch_size_limit && !small_batch.is_empty() {
                batches[wi % num_workers].append(&mut small_batch);
                small_size = 0;
                wi += 1;
            }
            small_size += entry.size;
            small_batch.push(entry.clone());
        }
        if !small_batch.is_empty() {
            batches[wi % num_workers].append(&mut small_batch);
        }

        TransferPlanner { batches }
    }

    pub fn into_jobs(self) -> Vec<TransferJob> {
        self.batches
            .into_iter()
            .filter(|b| !b.is_empty())
            .map(|batch| TransferJob::Batch(batch))
            .collect()
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
    if jobs.is_empty() {
        return;
    }

    let (job_tx, job_rx): (Sender<TransferJob>, Receiver<TransferJob>) = bounded(jobs.len());
    let (result_tx, result_rx): (Sender<(u64, u64)>, Receiver<(u64, u64)>) =
        bounded(config.workers);

    for job in jobs {
        job_tx.send(job).ok();
    }
    for _ in 0..config.workers {
        job_tx.send(TransferJob::Shutdown).ok();
    }

    let workers: Vec<_> = (0..config.workers)
        .map(|worker_id| {
            let job_rx = job_rx.clone();
            let result_tx = result_tx.clone();
            let spec = spec.clone();
            let remote_root = remote_root.to_string();
            let cfg = config.clone();

            std::thread::spawn(move || {
                let mut ssh = SSHConnection::new(spec, false);
                if let Err(e) = ssh.connect() {
                    eprintln!("  Worker {}: SSH connect failed: {}", worker_id, e);
                    let _ = result_tx.send((0, 0));
                    return;
                }
                let _ = ssh.exec_cmd(&format!("mkdir -p {}", shq(&remote_root)), 30000);

                loop {
                    let job = match job_rx.recv() {
                        Ok(j) => j,
                        Err(_) => break,
                    };
                    match job {
                        TransferJob::Shutdown => break,
                        TransferJob::Batch(batch) => {
                            let (bytes, files) =
                                send_tar_batch(&batch, &mut ssh, &remote_root, &cfg);
                            let _ = result_tx.send((bytes, files as u64));
                        }
                    }
                }
            })
        })
        .collect();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3600);
    for _ in &workers {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() { break; }
        if let Ok((b, f)) = result_rx.recv_timeout(remaining) {
            progress.update(b, f as usize);
            progress.display();
        } else {
            eprintln!("  Worker timeout — some workers may have failed");
            break;
        }
    }
    for w in workers {
        w.join().ok();
    }
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
    if jobs.is_empty() {
        return;
    }

    let (job_tx, job_rx): (Sender<TransferJob>, Receiver<TransferJob>) = bounded(jobs.len());
    let (result_tx, result_rx): (Sender<(u64, u64)>, Receiver<(u64, u64)>) =
        bounded(config.workers);

    for job in jobs {
        job_tx.send(job).ok();
    }
    for _ in 0..config.workers {
        job_tx.send(TransferJob::Shutdown).ok();
    }

    let workers: Vec<_> = (0..config.workers)
        .map(|worker_id| {
            let job_rx = job_rx.clone();
            let result_tx = result_tx.clone();
            let spec = spec.clone();
            let src_root = src_root.to_string();
            let dst_root = dst_root.to_path_buf();
            let cfg = config.clone();

                std::thread::spawn(move || {
                    let mut ssh = SSHConnection::new(spec, false);
                    if let Err(e) = ssh.connect() {
                        eprintln!("  Worker {}: SSH connect failed: {}", worker_id, e);
                        let _ = result_tx.send((0, 0));
                        return;
                    }
                    loop {
                        let job = match job_rx.recv() {
                            Ok(j) => j,
                            Err(_) => break,
                        };
                        match job {
                            TransferJob::Shutdown => break,
                            TransferJob::Batch(batch) => {
                                let (bytes, files) =
                                    recv_tar_batch(&batch, &mut ssh, &src_root, &dst_root, &cfg);
                                let _ = result_tx.send((bytes, files as u64));
                            }
                        }
                    }
                })
            })
            .collect();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3600);
        for _ in &workers {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() { break; }
            if let Ok((b, f)) = result_rx.recv_timeout(remaining) {
                progress.update(b, f as usize);
                progress.display();
            } else {
                break;
            }
        }
        for w in workers {
            w.join().ok();
        }
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
    if jobs.is_empty() {
        return;
    }

    let (job_tx, job_rx): (Sender<TransferJob>, Receiver<TransferJob>) = bounded(jobs.len());
    let (result_tx, result_rx): (Sender<(u64, u64)>, Receiver<(u64, u64)>) =
        bounded(config.workers);

    for job in jobs {
        job_tx.send(job).ok();
    }
    for _ in 0..config.workers {
        job_tx.send(TransferJob::Shutdown).ok();
    }

    let workers: Vec<_> = (0..config.workers)
        .map(|worker_id| {
            let job_rx = job_rx.clone();
            let result_tx = result_tx.clone();
            let src_spec = src_spec.clone();
            let dst_spec = dst_spec.clone();
            let src_root = src_root.to_string();
            let dst_root = dst_root.to_string();
            let cfg = config.clone();

            std::thread::spawn(move || {
                let mut src_ssh = SSHConnection::new(src_spec, false);
                let mut dst_ssh = SSHConnection::new(dst_spec, false);

                if let Err(e) = src_ssh.connect() {
                    eprintln!("  Worker {}: source SSH connect failed: {}", worker_id, e);
                    let _ = result_tx.send((0, 0));
                    return;
                }
                if let Err(e) = dst_ssh.connect() {
                    eprintln!("  Worker {}: dest SSH connect failed: {}", worker_id, e);
                    let _ = result_tx.send((0, 0));
                    return;
                }
                let _ = dst_ssh.exec_cmd(&format!("mkdir -p {}", shq(&dst_root)), 30000);

                loop {
                    let job = match job_rx.recv() {
                        Ok(j) => j,
                        Err(_) => break,
                    };
                    match job {
                        TransferJob::Shutdown => break,
                        TransferJob::Batch(batch) => {
                            let (bytes, files) = relay_tar_batch(
                                &batch,
                                &mut src_ssh,
                                &mut dst_ssh,
                                &src_root,
                                &dst_root,
                                &cfg,
                            );
                            let _ = result_tx.send((bytes, files as u64));
                        }
                    }
                }
            })
        })
        .collect();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3600);
    for _ in &workers {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() { break; }
        if let Ok((b, f)) = result_rx.recv_timeout(remaining) {
            progress.update(b, f as usize);
            progress.display();
        } else {
            break;
        }
    }
    for w in workers {
        w.join().ok();
    }
}

// ── Push: build tar locally, send over SSH ────────────────────────────

fn send_tar_batch(
    batch: &[FileEntry],
    ssh: &mut SSHConnection,
    remote_root: &str,
    config: &TransferConfig,
) -> (u64, usize) {
    if batch.is_empty() {
        return (0, 0);
    }

    let tar_data = build_tar(batch, config);
    if tar_data.is_empty() {
        return (0, 0);
    }

    let cmd = if config.compress_zstd {
        format!(
            "zstd -d 2>/dev/null | tar xf - --no-same-owner --no-same-permissions -C {}",
            shq(remote_root)
        )
    } else {
        format!(
            "tar xf - --no-same-owner --no-same-permissions -C {}",
            shq(remote_root)
        )
    };

    match ssh.open_channel() {
        Ok(mut channel) => {
            if channel.exec(&cmd).is_err() {
                return (0, 0);
            }
            let _ = channel.write_all(&tar_data);
            let _ = channel.eof();
            channel.wait_close().ok();
            (tar_data.len() as u64, batch.len())
        }
        Err(e) => {
            eprintln!("  channel error: {}", e);
            (0, 0)
        }
    }
}

// ── Pull: exec tar on remote, read stream, extract locally ────────────

fn recv_tar_batch(
    batch: &[FileEntry],
    ssh: &mut SSHConnection,
    src_root: &str,
    dst_root: &Path,
    config: &TransferConfig,
) -> (u64, usize) {
    if batch.is_empty() {
        return (0, 0);
    }

    let mut file_names: Vec<u8> = Vec::new();
    for e in batch {
        file_names.extend_from_slice(e.rel.as_bytes());
        file_names.push(b'\0');
    }

    let src_cmd = format!("cd {} && tar cf - --null -T -", shq(src_root));

    let mut channel = match ssh.open_channel() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  channel error: {}", e);
            return (0, 0);
        }
    };

    if channel.exec(&src_cmd).is_err() {
        return (0, 0);
    }

    let _ = channel.write_all(&file_names);
    let _ = channel.eof();

    let data: Vec<u8> = if config.compress_zstd {
        let mut compressed = Vec::new();
        if channel.read_to_end(&mut compressed).is_err() {
            return (0, 0);
        }
        zstd::decode_all(&compressed[..]).unwrap_or(compressed)
    } else {
        let mut raw = Vec::new();
        if channel.read_to_end(&mut raw).is_err() {
            return (0, 0);
        }
        raw
    };

    channel.wait_close().ok();

    let mut archive = tar::Archive::new(std::io::Cursor::new(&data));
    if let Err(e) = archive.unpack(dst_root) {
        eprintln!("  tar extract error: {}", e);
        return (0, 0);
    }

    let total_size: u64 = batch.iter().map(|e| e.size).sum();
    (total_size, batch.len())
}

// ── Relay: pipe tar from source SSH → local → dest SSH ───────────────

fn relay_tar_batch(
    batch: &[FileEntry],
    src_ssh: &mut SSHConnection,
    dst_ssh: &mut SSHConnection,
    src_root: &str,
    dst_root: &str,
    config: &TransferConfig,
) -> (u64, usize) {
    if batch.is_empty() {
        return (0, 0);
    }

    let mut file_names: Vec<u8> = Vec::new();
    for e in batch {
        file_names.extend_from_slice(e.rel.as_bytes());
        file_names.push(b'\0');
    }

    let src_cmd = format!("cd {} && tar cf - --null -T -", shq(src_root));

    let dst_cmd = if config.compress_zstd {
        format!(
            "zstd -d 2>/dev/null | tar xf - --no-same-owner --no-same-permissions -C {}",
            shq(dst_root)
        )
    } else {
        format!(
            "tar xf - --no-same-owner --no-same-permissions -C {}",
            shq(dst_root)
        )
    };

    let mut src_channel = match src_ssh.open_channel() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  src channel error: {}", e);
            return (0, 0);
        }
    };

    let mut dst_channel = match dst_ssh.open_channel() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  dst channel error: {}", e);
            return (0, 0);
        }
    };

    if src_channel.exec(&src_cmd).is_err() {
        return (0, 0);
    }
    if dst_channel.exec(&dst_cmd).is_err() {
        return (0, 0);
    }

    let _ = src_channel.write_all(&file_names);
    let _ = src_channel.eof();

    let mut relayed = 0u64;
    let mut buf = vec![0u8; config.buf_size];
    loop {
        let n = match src_channel.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        if dst_channel.write_all(&buf[..n]).is_err() {
            break;
        }
        relayed += n as u64;
    }

    let _ = dst_channel.eof();
    src_channel.wait_close().ok();
    dst_channel.wait_close().ok();

    (relayed, batch.len())
}

// ── Shared tar builder ────────────────────────────────────────────────

fn build_tar(batch: &[FileEntry], config: &TransferConfig) -> Vec<u8> {
    let total_size: u64 = batch.iter().map(|e| e.size).sum();
    let mut tar_data = Vec::with_capacity(total_size as usize + total_size as usize / 4);

    {
        let mut tar_builder = tar::Builder::new(std::io::Cursor::new(&mut tar_data));

        for entry in batch {
            let data = match std::fs::read(&entry.src) {
                Ok(d) => d,
                Err(_) => continue,
            };

            if let Ok(meta) = std::fs::metadata(&entry.src) {
                let mut header = tar::Header::new_gnu();
                header.set_size(data.len() as u64);
                header.set_mtime(
                    meta.modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                );
                header.set_mode(meta.permissions().mode());
                header.set_entry_type(tar::EntryType::Regular);
                let _ =
                    tar_builder.append_data(&mut header, &entry.rel, std::io::Cursor::new(&data));
            }
        }

        tar_builder.finish().ok();
    }

    if config.compress_zstd {
        tar_data = zstd::encode_all(&tar_data[..], config.zstd_level).unwrap_or(tar_data);
    }

    tar_data
}
