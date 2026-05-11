// Stubs for pull and relay
pub fn copy_remote_pull_parallel(_e: &[FileEntry], _s: &RemoteSpec, _sr: &str, _dr: &Path, _p: &Progress, _c: &TransferConfig) {
    eprintln!("  Pull parallel not implemented, using single-stream");
}
pub fn copy_remote_relay_parallel(_e: &[FileEntry], _ss: &RemoteSpec, _ds: &RemoteSpec, _sr: &str, _dr: &str, _p: &Progress, _c: &TransferConfig) {
    eprintln!("  Relay parallel not implemented, using single-stream");
}

use std::borrow::Cow;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Instant;

use crossbeam_channel::unbounded;
use tar::Header;

use crate::progress::{self, Progress};
use crate::ssh::SSHConnection;
use crate::types::{FileEntry, RemoteSpec, SMALL_FILE_THRESHOLD};

fn shq(s: &str) -> Cow<'_, str> { shlex::try_quote(s).unwrap_or(Cow::Borrowed(s)) }

#[derive(Clone)]
pub struct TransferConfig {
    pub workers: usize,
    pub compress_zstd: bool,
    pub zstd_level: i32,
    pub buf_size: usize,
}

#[derive(Clone, Debug)]
pub enum WorkerMsg {
    Chunk { id: usize, bytes: u64, done: usize, total: usize },
    Done { id: usize, bytes: u64, files: usize },
}

// ── TransferPlanner ──────────────────────────────────────────────────

pub struct TransferPlanner { pub batches: Vec<Vec<FileEntry>> }
impl TransferPlanner {
    pub fn new(entries: &[FileEntry], n: usize) -> Self {
        let limit = SMALL_FILE_THRESHOLD;
        let mut big = Vec::new(); let mut sml = Vec::new();
        for e in entries { if e.size >= limit { big.push(e.clone()); } else { sml.push(e.clone()); } }
        big.sort_by(|a, b| b.size.cmp(&a.size));
        let mut b: Vec<Vec<FileEntry>> = (0..n).map(|_| Vec::new()).collect();
        for (i, e) in big.iter().enumerate() { b[i % n].push(e.clone()); }
        let mut cur = Vec::new(); let mut sz = 0u64; let cap = 64u64 << 20; let mut wi = 0usize;
        for e in &sml {
            if sz + e.size > cap && !cur.is_empty() { b[wi % n].append(&mut cur); sz = 0; wi += 1; }
            sz += e.size; cur.push(e.clone());
        }
        if !cur.is_empty() { b[wi % n].append(&mut cur); }
        TransferPlanner { batches: b }
    }
    pub fn into_jobs(self) -> Vec<Vec<FileEntry>> {
        self.batches.into_iter().filter(|b| !b.is_empty()).collect()
    }
}

// ── Streaming tar writer ─────────────────────────────────────────────

const MAX_CHUNK: u64 = 64 << 20;

fn add_to_tar(entry: &FileEntry, tar: &mut tar::Builder<impl Write>) -> Result<(), String> {
    let mut f = File::open(&entry.src).map_err(|e| e.to_string())?;
    let mut h = Header::new_gnu();
    if let Ok(meta) = std::fs::metadata(&entry.src) {
        h.set_size(entry.size);
        h.set_mtime(meta.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_secs()).unwrap_or(0));
        h.set_mode(meta.permissions().mode());
        h.set_entry_type(tar::EntryType::Regular);
    }
    tar.append_data(&mut h, &entry.rel, &mut f).map_err(|e| e.to_string())
}

fn stream_tar(chunk: &[FileEntry], channel: ssh2::Channel, compress: bool, level: i32)
    -> Result<(ssh2::Channel, u64), String>
{
    let total = chunk.iter().map(|e| e.size).sum();
    if compress {
        let enc = zstd::stream::write::Encoder::new(channel, level).map_err(|e| e.to_string())?;
        let mut tar = tar::Builder::new(enc);
        for e in chunk { add_to_tar(e, &mut tar)?; }
        let enc = tar.into_inner().map_err(|e| e.to_string())?;
        let ch = enc.finish().map_err(|e| e.to_string())?;
        Ok((ch, total))
    } else {
        let mut tar = tar::Builder::new(channel);
        for e in chunk { add_to_tar(e, &mut tar)?; }
        let ch = tar.into_inner().map_err(|e| e.to_string())?;
        Ok((ch, total))
    }
}

// ── Send chunks with per-worker progress ─────────────────────────────

fn send_batch(
    batch: &[FileEntry], ssh: &mut SSHConnection, rroot: &str, cfg: &TransferConfig,
    id: usize, tx: &crossbeam_channel::Sender<WorkerMsg>,
) -> (u64, usize) {
    let mut total_b = 0u64; let mut total_f = 0usize;
    let nchunks = estimate_chunks(batch, MAX_CHUNK);
    let mut pos = 0usize; let mut idx = 0usize;
    eprintln!("\n  W{} starting batch of {} files ({}), nchunks={}", id, batch.len(), progress::fmt_size(batch.iter().map(|e| e.size).sum()), nchunks);
    while pos < batch.len() {
        let end = next_pos(batch, pos);
        let chunk = &batch[pos..end]; pos = end; idx += 1;
        let cmd = if cfg.compress_zstd {
            format!("zstd -d 2>/dev/null | tar xf - --no-same-owner --no-same-permissions -C {}", shq(rroot))
        } else {
            format!("tar xf - --no-same-owner --no-same-permissions -C {}", shq(rroot))
        };
        match ssh.open_channel() {
            Ok(mut ch) => {
                if ch.exec(&cmd).is_err() { eprintln!("\n  W{} exec failed", id); break; }
                match stream_tar(chunk, ch, cfg.compress_zstd, cfg.zstd_level) {
                    Ok((mut ch, b)) => {
                        let _ = ch.eof(); ch.wait_close().ok();
                        total_b += b; total_f += chunk.len();
                        eprintln!("\n  W{} chunk {}/{} done ({} bytes)", id, idx, nchunks, b);
                        let _ = tx.send(WorkerMsg::Chunk { id, bytes: b, done: idx, total: nchunks });
                    }
                    Err(e) => { eprintln!("\n  W{} stream_tar error: {}", id, e); break; }
                }
            }
            Err(e) => { eprintln!("\n  W{} open_channel error: {}", id, e); break; }
        }
    }
    (total_b, total_f)
}

fn next_pos(batch: &[FileEntry], start: usize) -> usize {
    let mut sz = 0u64; let mut end = start;
    while end < batch.len() && sz < MAX_CHUNK { sz += batch[end].size; end += 1; }
    if end == start { end = start + 1; }
    end
}

fn estimate_chunks(batch: &[FileEntry], max: u64) -> usize {
    let total: u64 = batch.iter().map(|e| e.size).sum();
    std::cmp::max(1, (total / max) as usize)
}

// ── Main render loop ─────────────────────────────────────────────────

fn render_loop(rr: &crossbeam_channel::Receiver<WorkerMsg>, nworkers: usize) {
    let start = Instant::now();
    render(&vec![0; nworkers], &vec![1; nworkers], 0, &start);
    let mut total_bytes = 0u64;
    let mut w_done = vec![0usize; nworkers];
    let mut w_total = vec![0usize; nworkers];
    let mut finished = 0usize;
    let mut done_flag = vec![false; nworkers];

    while finished < nworkers {
        match rr.recv() {
            Ok(WorkerMsg::Chunk { id, bytes, done, total }) => {
                total_bytes += bytes;
                w_done[id] = done; w_total[id] = total;
                render(w_done.as_slice(), w_total.as_slice(), total_bytes, &start);
            }
            Ok(WorkerMsg::Done { id, bytes, files: _ }) => {
                if !done_flag[id] { finished += 1; done_flag[id] = true; }
                total_bytes += bytes;
                render(w_done.as_slice(), w_total.as_slice(), total_bytes, &start);
            }
            Err(_) => break,
        }
    }
    render(w_done.as_slice(), w_total.as_slice(), total_bytes, &start);
}

fn render(done: &[usize], total: &[usize], bytes: u64, start: &Instant) {
    let elapsed = start.elapsed().as_secs_f64();
    let speed = if elapsed > 0.0 { bytes as f64 / elapsed } else { 0.0 };
    let mut line = String::new();
    for i in 0..done.len() {
        if !line.is_empty() { line.push_str("  "); }
        if total[i] > 0 { line.push_str(&format!("W{}:{}/{}", i, done[i], total[i])); }
    }
    use std::io::{Write as IoWrite, stderr};
    let _ = write!(stderr(), "\r  {}  {}  {}/s  {}", line, progress::fmt_size(bytes), progress::fmt_size_f64(speed), progress::fmt_time(elapsed as u64));
    let _ = stderr().flush();
}

// ── Push mode: local → remote ────────────────────────────────────────

pub fn copy_remote_parallel(
    entries: &[FileEntry], spec: &RemoteSpec, remote_root: &str,
    _progress: &Progress, config: &TransferConfig,
) {
    let planner = TransferPlanner::new(entries, config.workers);
    let jobs = planner.into_jobs();
    if jobs.is_empty() { return; }
    let (jt, jr) = unbounded();
    let (rt, rr) = unbounded::<WorkerMsg>();
    for b in jobs { jt.send(b).ok(); }
    for _ in 0..config.workers { jt.send(Vec::new()).ok(); } // shutdown sentinel

    let rroot = remote_root.to_string(); let spec = spec.clone(); let cfg = config.clone();
    let nw = config.workers;
    let handles: Vec<_> = (0..nw).map(|id| {
        let (jr, rt, spec, rroot, cfg) = (jr.clone(), rt.clone(), spec.clone(), rroot.clone(), cfg.clone());
        std::thread::spawn(move || {
            let mut total = (0u64, 0usize);
            let mut ssh = SSHConnection::new(spec, false);
            if let Err(e) = ssh.connect() {
                eprintln!("  W{} SSH fail: {}", id, e);
                let _ = rt.send(WorkerMsg::Done { id, bytes: 0, files: 0 });
                return;
            }
            ssh.exec_cmd(&format!("mkdir -p {}", shq(&rroot)), 30000).ok();
            loop {
                match jr.recv() {
                    Ok(b) if b.is_empty() => break,
                    Ok(b) => {
                        let r = send_batch(&b, &mut ssh, &rroot, &cfg, id, &rt);
                        total.0 += r.0; total.1 += r.1;
                    }
                    Err(_) => break,
                }
            }
            let _ = rt.send(WorkerMsg::Done { id, bytes: total.0, files: total.1 });
        })
    }).collect();

    render_loop(&rr, nw);
    for h in handles { h.join().ok(); }
    eprintln!();
}
