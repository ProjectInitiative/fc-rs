use std::borrow::Cow;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossbeam_channel::unbounded;
use tar::Header;

use crate::progress::{self, Progress};
use crate::ssh::SSHConnection;
use crate::types::{FileEntry, RemoteSpec, SMALL_FILE_THRESHOLD};
use std::path::Path;

fn shq(s: &str) -> Cow<'_, str> { shlex::try_quote(s).unwrap_or(Cow::Borrowed(s)) }

#[derive(Clone)]
pub struct TransferConfig { pub workers: usize, pub compress_zstd: bool, pub zstd_level: i32, pub buf_size: usize }
#[derive(Clone, Debug)]
pub enum WorkerMsg { Chunk { id: usize, bytes: u64, done: usize, total: usize }, Done { id: usize, bytes: u64, files: usize } }
pub struct TransferPlanner { pub jobs: Vec<Vec<FileEntry>> }

impl TransferPlanner {
    pub fn new(entries: &[FileEntry], _nworkers: usize) -> Self {
        let limit = SMALL_FILE_THRESHOLD;
        let mut big = Vec::new(); let mut sml = Vec::new();
        for e in entries { if e.size >= limit { big.push(e.clone()); } else { sml.push(e.clone()); } }
        big.sort_by(|a, b| b.size.cmp(&a.size));
        let cap = 64u64 << 20; let mut jobs: Vec<Vec<FileEntry>> = Vec::new();
        for e in big { jobs.push(vec![e]); }
        let mut cur: Vec<FileEntry> = Vec::new(); let mut cur_sz = 0u64;
        for e in sml {
            if cur_sz + e.size > cap && !cur.is_empty() { jobs.push(std::mem::take(&mut cur)); cur_sz = 0; }
            cur_sz += e.size; cur.push(e);
        }
        if !cur.is_empty() { jobs.push(cur); }
        TransferPlanner { jobs }
    }
}

const MAX_CHUNK: u64 = 64 << 20;

// ── Streaming tar writer ─────────────────────────────────────────────

fn add_to_tar(entry: &FileEntry, tar: &mut tar::Builder<impl Write>) -> Result<(), String> {
    let mut f = File::open(&entry.src).map_err(|e| e.to_string())?;
    let mut h = Header::new_gnu();
    if let Ok(meta) = std::fs::metadata(&entry.src) {
        h.set_size(entry.size);
        h.set_mtime(meta.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_secs()).unwrap_or(0));
        h.set_mode(meta.permissions().mode()); h.set_entry_type(tar::EntryType::Regular);
    }
    tar.append_data(&mut h, &entry.rel, &mut f).map_err(|e| e.to_string())
}

fn stream_tar(chunk: &[FileEntry], channel: ssh2::Channel, compress: bool, level: i32) -> Result<(ssh2::Channel, u64), String> {
    let total = chunk.iter().map(|e| e.size).sum();
    if compress {
        let enc = zstd::stream::write::Encoder::new(channel, level).map_err(|e| e.to_string())?;
        let mut tar = tar::Builder::new(enc);
        for e in chunk { add_to_tar(e, &mut tar)?; }
        let enc = tar.into_inner().map_err(|e| e.to_string())?;
        Ok((enc.finish().map_err(|e| e.to_string())?, total))
    } else {
        let mut tar = tar::Builder::new(channel);
        for e in chunk { add_to_tar(e, &mut tar)?; }
        Ok((tar.into_inner().map_err(|e| e.to_string())?, total))
    }
}

// ── Main copy function ───────────────────────────────────────────────

pub fn copy_remote_parallel(
    entries: &[FileEntry], remote_root: &str,
    _progress: &Progress, config: &TransferConfig,
    ssh: Arc<Mutex<SSHConnection>>,
) {
    let planner = TransferPlanner::new(entries, config.workers);
    let jobs = planner.jobs; if jobs.is_empty() { return; }

    let (jt, jr) = unbounded();
    let (rt, rr) = unbounded::<WorkerMsg>();
    for job in jobs { jt.send(job).ok(); }
    for _ in 0..config.workers { jt.send(Vec::new()).ok(); }

    let rpath = remote_root.to_string(); let cfg = config.clone(); let nw = config.workers;
    let mut handles = Vec::new();
    for id in 0..nw {
        let (jr, rt, rpath, cfg, ssh) = (jr.clone(), rt.clone(), rpath.clone(), cfg.clone(), ssh.clone());
        handles.push(std::thread::spawn(move || {
            let _ = ssh.lock().unwrap().mkdir_p(&rpath);
            let mut total = (0u64, 0usize);
            loop {
                let b = match jr.recv() { Ok(b) => b, Err(_) => break };
        if b.is_empty() { break; }
        eprintln!("\n  W{} got job: {} files, {}", id, b.len(), progress::fmt_size(b.iter().map(|e| e.size).sum()));
        total.0 += b.iter().map(|e| e.size).sum::<u64>();
        total.1 += b.len();
        let nchunks = std::cmp::max(1, (b.iter().map(|e| e.size).sum::<u64>() / MAX_CHUNK) as usize);
                let mut pos = 0usize; let mut idx = 0usize;
                while pos < b.len() {
                    let mut sz = 0u64; let start = pos;
                    while pos < b.len() && sz < MAX_CHUNK { sz += b[pos].size; pos += 1; }
                    if pos == start { pos += 1; }
                    let chunk = &b[start..pos]; idx += 1;
                    let cmd = if cfg.compress_zstd {
                        format!("zstd -d 2>/dev/null | tar xf - --no-same-owner --no-same-permissions -C {}", shq(&rpath))
                    } else {
                        format!("tar xf - --no-same-owner --no-same-permissions -C {}", shq(&rpath))
                    };
                    let ch_result = ssh.lock().unwrap().open_channel();
                    match ch_result {
                        Ok(mut ch) => {
                            if ch.exec(&cmd).is_ok() {
                                if let Ok((mut ch2, bytes)) = stream_tar(chunk, ch, cfg.compress_zstd, cfg.zstd_level) {
                                    let _ = ch2.eof(); ch2.wait_close().ok();
                                    let _ = rt.send(WorkerMsg::Chunk { id, bytes, done: idx, total: nchunks });
                                }
                            }
                        }
                        Err(e) => eprintln!("\n  W{} open_channel: {}", id, e),
                    }
                }
            }
            let _ = rt.send(WorkerMsg::Done { id, bytes: total.0, files: total.1 });
        }));
    }

    render_loop(&rr, nw);
    for h in handles { h.join().ok(); }
    eprintln!();
}

// ── Render loop ──────────────────────────────────────────────────────

fn render_loop(rr: &crossbeam_channel::Receiver<WorkerMsg>, nworkers: usize) {
    let start = Instant::now();
    render(&vec![0; nworkers], &vec![1; nworkers], 0, &start);
    let mut total_bytes = 0u64; let mut w_done = vec![0usize; nworkers];
    let mut w_total = vec![0usize; nworkers]; let mut finished = 0usize;
    let mut flags = vec![false; nworkers];
    while finished < nworkers {
        match rr.recv() {
            Ok(WorkerMsg::Chunk { id, bytes, done, total }) => {
                total_bytes += bytes; w_done[id] = done; w_total[id] = total;
                render(&w_done, &w_total, total_bytes, &start);
            }
            Ok(WorkerMsg::Done { id, bytes, .. }) => {
                if !flags[id] { finished += 1; flags[id] = true; }
                total_bytes += bytes;
                render(&w_done, &w_total, total_bytes, &start);
            }
            Err(_) => break,
        }
    }
    render(&w_done, &w_total, total_bytes, &start);
}

// Stubs — pull/relay not yet ported to shared-session pattern
pub fn copy_remote_pull_parallel(_e: &[FileEntry], _s: &RemoteSpec, _sr: &str, _dr: &Path, _p: &Progress, _c: &TransferConfig) {
    eprintln!("  Pull parallel not implemented, using single-stream");
}
pub fn copy_remote_relay_parallel(_e: &[FileEntry], _ss: &RemoteSpec, _ds: &RemoteSpec, _sr: &str, _dr: &str, _p: &Progress, _c: &TransferConfig) {
    eprintln!("  Relay parallel not implemented, using single-stream");
}

fn render(done: &[usize], total: &[usize], bytes: u64, start: &Instant) {
    let elapsed = start.elapsed().as_secs_f64();
    let speed = if elapsed > 0.0 { bytes as f64 / elapsed } else { 0.0 };
    let mut line = String::new();
    for i in 0..done.len() {
        if !line.is_empty() { line.push_str("  "); }
        if total[i] > 0 { line.push_str(&format!("W{}:{}/{}", i, done[i], total[i])); }
    }
    use std::io::Write as IoWrite;
    let _ = write!(std::io::stderr(), "\r  {}  {}  {}/s  {}", line, progress::fmt_size(bytes), progress::fmt_size_f64(speed), progress::fmt_time(elapsed as u64));
    let _ = std::io::stderr().flush();
}
