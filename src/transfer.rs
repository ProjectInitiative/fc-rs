use std::borrow::Cow;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use crossbeam_channel::unbounded;
use tar::Header;

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
    fn default() -> Self { TransferConfig { workers: 4, compress_zstd: false, zstd_level: 3, buf_size: 4 * 1024 * 1024 } }
}

pub enum TransferJob { Batch(Vec<FileEntry>), Shutdown }

pub struct TransferPlanner {
    pub batches: Vec<Vec<FileEntry>>,
}

impl TransferPlanner {
    pub fn new(entries: &[FileEntry], num_workers: usize) -> Self {
        let limit = SMALL_FILE_THRESHOLD;
        let mut large = Vec::new();
        let mut small = Vec::new();
        for e in entries { if e.size >= limit { large.push(e.clone()); } else { small.push(e.clone()); } }
        large.sort_by(|a, b| b.size.cmp(&a.size));
        let mut b: Vec<Vec<FileEntry>> = (0..num_workers).map(|_| Vec::new()).collect();
        for (i, e) in large.iter().enumerate() { b[i % num_workers].push(e.clone()); }
        let mut cur = Vec::new();
        let mut cur_sz = 0u64;
        let cap = 64u64 << 20;
        let mut wi = 0usize;
        for e in &small {
            if cur_sz + e.size > cap && !cur.is_empty() {
                b[wi % num_workers].append(&mut cur); cur_sz = 0; wi += 1;
            }
            cur_sz += e.size; cur.push(e.clone());
        }
        if !cur.is_empty() { b[wi % num_workers].append(&mut cur); }
        while b.len() < num_workers { b.push(Vec::new()); }
        TransferPlanner { batches: b }
    }
    pub fn into_jobs(self) -> Vec<TransferJob> {
        self.batches.into_iter().filter(|b| !b.is_empty()).map(TransferJob::Batch).collect()
    }
}

const MAX_CHUNK: u64 = 64 << 20;

fn copy_headers(entry: &FileEntry, header: &mut Header) {
    if let Ok(meta) = std::fs::metadata(&entry.src) {
        header.set_size(entry.size);
        header.set_mtime(meta.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_secs()).unwrap_or(0));
        header.set_mode(meta.permissions().mode());
        header.set_entry_type(tar::EntryType::Regular);
    }
}

// ── Push: stream tar → (zstd) → SSH channel ─────────────────────────

fn stream_tar_to(chunk: &[FileEntry], channel: ssh2::Channel, compress: bool, level: i32)
    -> Result<(ssh2::Channel, u64), String>
{
    let total = chunk.iter().map(|e| e.size).sum();
    if compress {
        let enc = zstd::stream::write::Encoder::new(channel, level).map_err(|e| e.to_string())?;
        let mut tar = tar::Builder::new(enc);
        for entry in chunk {
            let mut f = File::open(&entry.src).map_err(|e| e.to_string())?;
            let mut h = Header::new_gnu();
            copy_headers(entry, &mut h);
            tar.append_data(&mut h, &entry.rel, &mut f).map_err(|e| e.to_string())?;
        }
        let enc = tar.into_inner().map_err(|e| e.to_string())?;
        let ch = enc.finish().map_err(|e| e.to_string())?;
        Ok((ch, total))
    } else {
        let mut tar = tar::Builder::new(channel);
        for entry in chunk {
            let mut f = File::open(&entry.src).map_err(|e| e.to_string())?;
            let mut h = Header::new_gnu();
            copy_headers(entry, &mut h);
            tar.append_data(&mut h, &entry.rel, &mut f).map_err(|e| e.to_string())?;
        }
        let ch = tar.into_inner().map_err(|e| e.to_string())?;
        Ok((ch, total))
    }
}

fn send_tar_batch(
    batch: &[FileEntry], ssh: &mut SSHConnection, remote_root: &str, config: &TransferConfig,
) -> (u64, usize) {
    let mut total_bytes = 0u64;
    let mut total_files = 0usize;
    let mut pos = 0usize;

    while pos < batch.len() {
        let mut chunk_sz = 0u64;
        let start = pos;
        while pos < batch.len() && chunk_sz < MAX_CHUNK {
            chunk_sz += batch[pos].size; pos += 1;
        }
        if pos == start { pos = start + 1; }

        let chunk = &batch[start..pos];
        let cmd = if config.compress_zstd {
            format!("zstd -d 2>/dev/null | tar xf - --no-same-owner --no-same-permissions -C {}", shq(remote_root))
        } else {
            format!("tar xf - --no-same-owner --no-same-permissions -C {}", shq(remote_root))
        };

        match ssh.open_channel() {
            Ok(mut ch) => {
                if ch.exec(&cmd).is_err() { break; }
                match stream_tar_to(chunk, ch, config.compress_zstd, config.zstd_level) {
                    Ok((mut ch, b)) => {
                        let _ = ch.eof(); ch.wait_close().ok();
                        total_bytes += b; total_files += chunk.len();
                    }
                    Err(e) => { eprintln!("  stream error: {}", e); break; }
                }
            }
            Err(e) => { eprintln!("  channel error: {}", e); break; }
        }
    }
    (total_bytes, total_files)
}

// ── Pull: read SSH channel → (zstd) → extract tar ───────────────────

fn recv_tar_batch(
    batch: &[FileEntry], ssh: &mut SSHConnection, src_root: &str, dst_root: &Path, config: &TransferConfig,
) -> (u64, usize) {
    let mut total_bytes = 0u64;
    let mut pos = 0usize;

    while pos < batch.len() {
        let mut chunk_sz = 0u64;
        let start = pos;
        while pos < batch.len() && chunk_sz < MAX_CHUNK {
            chunk_sz += batch[pos].size; pos += 1;
        }
        if pos == start { pos = start + 1; }
        let chunk = &batch[start..pos];

        let src_cmd = format!("cd {} && tar cf - --null -T -", shq(src_root));
        let mut names = Vec::new();
        for e in chunk { names.extend_from_slice(e.rel.as_bytes()); names.push(b'\0'); }

        let mut ch = match ssh.open_channel() { Ok(c) => c, Err(e) => { eprintln!("  ch: {}", e); break; } };
        if ch.exec(&src_cmd).is_err() { break; }
        let _ = ch.write_all(&names); let _ = ch.eof();

        if config.compress_zstd {
            let mut dec = zstd::stream::read::Decoder::new(&mut ch).map_err(|e| e.to_string()).unwrap();
            let mut archive = tar::Archive::new(&mut dec);
            if archive.unpack(dst_root).is_err() { break; }
        } else {
            let mut archive = tar::Archive::new(&mut ch);
            if archive.unpack(dst_root).is_err() { break; }
        }
        ch.wait_close().ok();
        total_bytes += chunk_sz;
    }
    (total_bytes, batch.len())
}

// ── Relay: pipe src SSH → dst SSH ───────────────────────────────────

fn relay_tar_batch(
    batch: &[FileEntry], src_ssh: &mut SSHConnection, dst_ssh: &mut SSHConnection,
    src_root: &str, dst_root: &str, config: &TransferConfig,
) -> (u64, usize) {
    let mut total_bytes = 0u64;
    let mut pos = 0usize;

    while pos < batch.len() {
        let mut chunk_sz = 0u64;
        let start = pos;
        while pos < batch.len() && chunk_sz < MAX_CHUNK {
            chunk_sz += batch[pos].size; pos += 1;
        }
        if pos == start { pos = start + 1; }
        let chunk = &batch[start..pos];

        let src_cmd = format!("cd {} && tar cf - --null -T -", shq(src_root));
        let dst_cmd = if config.compress_zstd {
            format!("zstd -d 2>/dev/null | tar xf - --no-same-owner --no-same-permissions -C {}", shq(dst_root))
        } else {
            format!("tar xf - --no-same-owner --no-same-permissions -C {}", shq(dst_root))
        };

        let mut names = Vec::new();
        for e in chunk { names.extend_from_slice(e.rel.as_bytes()); names.push(b'\0'); }

        let mut sc = match src_ssh.open_channel() { Ok(c) => c, Err(e) => { eprintln!("  src ch: {}", e); break; } };
        let mut dc = match dst_ssh.open_channel() { Ok(c) => c, Err(e) => { eprintln!("  dst ch: {}", e); break; } };

        if sc.exec(&src_cmd).is_err() || dc.exec(&dst_cmd).is_err() { break; }
        let _ = sc.write_all(&names); let _ = sc.eof();

        let mut buf = vec![0u8; config.buf_size];
        loop {
            let n = match sc.read(&mut buf) { Ok(0) => break, Ok(n) => n, Err(_) => break };
            if dc.write_all(&buf[..n]).is_err() { break; }
            total_bytes += n as u64;
        }
        let _ = dc.eof(); sc.wait_close().ok(); dc.wait_close().ok();
    }
    (total_bytes, batch.len())
}

pub fn copy_remote_parallel(
    entries: &[FileEntry], spec: &RemoteSpec, remote_root: &str,
    progress: &Progress, config: &TransferConfig,
) {
    let planner = TransferPlanner::new(entries, config.workers);
    let jobs = planner.into_jobs();
    if jobs.is_empty() { return; }
    let (jt, jr) = unbounded::<TransferJob>();
    let (rt, rr) = unbounded::<(u64, usize)>();
    for j in jobs { jt.send(j).ok(); }
    for _ in 0..config.workers { jt.send(TransferJob::Shutdown).ok(); }

    let rpath = remote_root.to_string(); let sp = spec.clone();
    let handles: Vec<_> = (0..config.workers).map(|id| {
        let jr = jr.clone(); let rt = rt.clone(); let sp = sp.clone(); let rpath = rpath.clone(); let cf = config.clone();
        std::thread::spawn(move || {
            let mut ssh = SSHConnection::new(sp, false);
            if let Err(e) = ssh.connect() { eprintln!("  W{} SSH: {}", id, e); return; }
            let _ = ssh.exec_cmd(&format!("mkdir -p {}", shq(&rpath)), 30000);
            let mut total = (0u64, 0usize);
            loop {
                match jr.recv() {
                    Ok(TransferJob::Shutdown) | Err(_) => break,
                    Ok(TransferJob::Batch(b)) => { let r = send_tar_batch(&b, &mut ssh, &rpath, &cf); total.0 += r.0; total.1 += r.1; }
                }
            }
            let _ = rt.send(total);
        })
    }).collect();
    for _ in &handles { if let Ok((b, f)) = rr.recv() { progress.update(b, f); progress.display(); } }
    for h in handles { h.join().ok(); }
}

pub fn copy_remote_pull_parallel(
    entries: &[FileEntry], spec: &RemoteSpec, src_root: &str, dst_root: &Path,
    progress: &Progress, config: &TransferConfig,
) {
    let planner = TransferPlanner::new(entries, config.workers);
    let jobs = planner.into_jobs();
    if jobs.is_empty() { return; }
    let (jt, jr) = unbounded::<TransferJob>();
    let (rt, rr) = unbounded::<(u64, usize)>();
    for j in jobs { jt.send(j).ok(); }
    for _ in 0..config.workers { jt.send(TransferJob::Shutdown).ok(); }

    let src_path = src_root.to_string(); let dst_path = dst_root.to_path_buf(); let sp = spec.clone();
    let handles: Vec<_> = (0..config.workers).map(|id| {
        let jr = jr.clone(); let rt = rt.clone(); let sp = sp.clone(); let src_path = src_path.clone(); let dst_path = dst_path.clone(); let cf = config.clone();
        std::thread::spawn(move || {
            let mut ssh = SSHConnection::new(sp, false);
            if let Err(e) = ssh.connect() { eprintln!("  W{} SSH: {}", id, e); return; }
            let mut total = (0u64, 0usize);
            loop {
                match jr.recv() {
                    Ok(TransferJob::Shutdown) | Err(_) => break,
                    Ok(TransferJob::Batch(b)) => { let r = recv_tar_batch(&b, &mut ssh, &src_path, &dst_path, &cf); total.0 += r.0; total.1 += r.1; }
                }
            }
            let _ = rt.send(total);
        })
    }).collect();
    for _ in &handles { if let Ok((b, f)) = rr.recv() { progress.update(b, f); progress.display(); } }
    for h in handles { h.join().ok(); }
}

pub fn copy_remote_relay_parallel(
    entries: &[FileEntry], src_spec: &RemoteSpec, dst_spec: &RemoteSpec,
    src_root: &str, dst_root: &str, progress: &Progress, config: &TransferConfig,
) {
    let planner = TransferPlanner::new(entries, config.workers);
    let jobs = planner.into_jobs();
    if jobs.is_empty() { return; }
    let (jt, jr) = unbounded::<TransferJob>();
    let (rt, rr) = unbounded::<(u64, usize)>();
    for j in jobs { jt.send(j).ok(); }
    for _ in 0..config.workers { jt.send(TransferJob::Shutdown).ok(); }

    let src_root = src_root.to_string(); let dst_root = dst_root.to_string();
    let ss = src_spec.clone(); let ds = dst_spec.clone();
    let handles: Vec<_> = (0..config.workers).map(|id| {
        let jr = jr.clone(); let rt = rt.clone(); let ss = ss.clone(); let ds = ds.clone();
        let src_root = src_root.clone(); let dst_root = dst_root.clone(); let cf = config.clone();
        std::thread::spawn(move || {
            let mut src = SSHConnection::new(ss, false);
            let mut dst = SSHConnection::new(ds, false);
            if src.connect().is_err() { eprintln!("  W{} src SSH fail", id); return; }
            if dst.connect().is_err() { eprintln!("  W{} dst SSH fail", id); return; }
            let _ = dst.exec_cmd(&format!("mkdir -p {}", shq(&dst_root)), 30000);
            let mut total = (0u64, 0usize);
            loop {
                match jr.recv() {
                    Ok(TransferJob::Shutdown) | Err(_) => break,
                    Ok(TransferJob::Batch(b)) => { let r = relay_tar_batch(&b, &mut src, &mut dst, &src_root, &dst_root, &cf); total.0 += r.0; total.1 += r.1; }
                }
            }
            let _ = rt.send(total);
        })
    }).collect();
    for _ in &handles { if let Ok((b, f)) = rr.recv() { progress.update(b, f); progress.display(); } }
    for h in handles { h.join().ok(); }
}
