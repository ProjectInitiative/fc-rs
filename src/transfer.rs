use std::io::Write;
use std::os::unix::fs::PermissionsExt;

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
                    return;
                }

                let _ = ssh.exec_cmd(
                    &format!("mkdir -p {}", shlex::quote(&remote_root)),
                    30000,
                );

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

    for _ in &workers {
        if let Ok((b, f)) = result_rx.recv() {
            progress.update(b, f as usize);
            progress.display();
        }
    }

    for w in workers {
        w.join().ok();
    }
}

fn send_tar_batch(
    batch: &[FileEntry],
    ssh: &mut SSHConnection,
    remote_root: &str,
    config: &TransferConfig,
) -> (u64, usize) {
    if batch.is_empty() {
        return (0, 0);
    }

    let total_size: u64 = batch.iter().map(|e| e.size).sum();
    let total_files = batch.len();
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
                let _ = tar_builder.append_data(&mut header, &entry.rel, std::io::Cursor::new(&data));
            }
        }

        tar_builder.finish().ok();
    }

    if config.compress_zstd {
        tar_data = zstd::encode_all(&tar_data[..], config.zstd_level).unwrap_or(tar_data);
    }

    let cmd = if config.compress_zstd {
        format!(
            "zstd -d 2>/dev/null | tar xf - --no-same-owner --no-same-permissions -C {}",
            shlex::quote(remote_root)
        )
    } else {
        format!(
            "tar xf - --no-same-owner --no-same-permissions -C {}",
            shlex::quote(remote_root)
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
            (tar_data.len() as u64, total_files)
        }
        Err(e) => {
            eprintln!("  channel error: {}", e);
            (0, 0)
        }
    }
}
