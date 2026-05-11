use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::time::Instant;

use crossbeam_channel::unbounded;

use crate::progress::{self, Progress};
use crate::ssh::SSHConnection;
use crate::types::{FileEntry, RemoteSpec, SMALL_FILE_THRESHOLD};

#[derive(Clone)]
pub struct TransferConfig { pub workers: usize, pub compress_zstd: bool, pub zstd_level: i32, pub buf_size: usize }

#[derive(Clone, Debug)]
pub enum WorkerMsg {
    Chunk { id: usize, bytes: u64, done: usize, total: usize },
    Done { id: usize, bytes: u64, files: usize },
}

const CHUNK: u64 = 64 << 20;

// ── TransferPlanner ──────────────────────────────────────────────────

pub struct TransferPlanner {
    pub large_jobs: Vec<Vec<FileEntry>>,
    pub small_jobs: Vec<Vec<FileEntry>>,
}
impl TransferPlanner {
    pub fn new(entries: &[FileEntry]) -> Self {
        let mut big = Vec::new(); let mut sml = Vec::new();
        for e in entries {
            if e.size >= SMALL_FILE_THRESHOLD { big.push(e.clone()); } else { sml.push(e.clone()); }
        }
        big.sort_by(|a, b| b.size.cmp(&a.size));
        let large_jobs: Vec<Vec<FileEntry>> = big.into_iter().map(|e| vec![e]).collect();
        let mut cur = Vec::new(); let mut sz = 0u64;
        let mut small_jobs = Vec::new();
        for e in sml {
            if sz + e.size > CHUNK && !cur.is_empty() { small_jobs.push(std::mem::take(&mut cur)); sz = 0; }
            sz += e.size; cur.push(e);
        }
        if !cur.is_empty() { small_jobs.push(cur); }
        TransferPlanner { large_jobs, small_jobs }
    }
}

// ── File transfer helpers ───────────────────────────────────────────

fn push_file(
    entry: &FileEntry, ssh: &mut SSHConnection, rpath: &str,
    id: usize, rt: &crossbeam_channel::Sender<WorkerMsg>,
) {
    let remote = format!("{}/{}", rpath, entry.rel);
    if let Some(p) = Path::new(&remote).parent() { let _ = ssh.mkdir_p(&p.to_string_lossy()); }
    let nchunks = ((entry.size + CHUNK - 1) / CHUNK) as usize;
    if let Ok(sftp) = ssh.open_sftp() {
        if let Ok(mut rf) = sftp.create(&Path::new(&remote)) {
            if let Ok(mut lf) = File::open(&entry.src) {
                let mut buf = vec![0u8; CHUNK as usize];
                let mut idx = 0usize;
                loop {
                    let n = match lf.read(&mut buf) { Ok(0) => break, Ok(n) => n, Err(_) => break };
                    if rf.write_all(&buf[..n]).is_err() { break; }
                    idx += 1;
                    let _ = rt.send(WorkerMsg::Chunk { id, bytes: n as u64, done: idx, total: nchunks });
                }
            }
        }
    }
}

fn pull_file(
    entry: &FileEntry, ssh: &mut SSHConnection, dst_root: &str,
    id: usize, rt: &crossbeam_channel::Sender<WorkerMsg>,
) {
    let local = Path::new(dst_root).join(&entry.rel);
    if let Some(p) = local.parent() { let _ = std::fs::create_dir_all(p); }
    let nchunks = ((entry.size + CHUNK - 1) / CHUNK) as usize;
    if let Ok(sftp) = ssh.open_sftp() {
        if let Ok(mut rf) = sftp.open(&Path::new(&entry.src)) {
            if let Ok(mut lf) = File::create(&local) {
                let mut buf = vec![0u8; CHUNK as usize];
                let mut idx = 0usize;
                loop {
                    let n = match rf.read(&mut buf) { Ok(0) => break, Ok(n) => n, Err(_) => break };
                    if lf.write_all(&buf[..n]).is_err() { break; }
                    idx += 1;
                    let _ = rt.send(WorkerMsg::Chunk { id, bytes: n as u64, done: idx, total: nchunks });
                }
            }
        }
    }
}

// ── Orchestrators ───────────────────────────────────────────────────

fn launch(
    entries: &[FileEntry], cfg: &TransferConfig, spec: &RemoteSpec, rpath: String, mkdir: bool,
    process: impl Fn(&[FileEntry], &mut SSHConnection, usize, &crossbeam_channel::Sender<WorkerMsg>, &str) + Clone + Send + 'static,
) {
    let planner = TransferPlanner::new(entries);
    if planner.large_jobs.is_empty() && planner.small_jobs.is_empty() { return; }
    let n_large = planner.large_jobs.len();
    let n_small = planner.small_jobs.len();
    let (ljt, ljr) = unbounded();
    let (sjt, sjr) = unbounded();
    let (rt, rr) = unbounded::<WorkerMsg>();
    for j in planner.large_jobs { ljt.send(j).ok(); }
    for j in planner.small_jobs { sjt.send(j).ok(); }
    // Send sentinels to both queues per worker so they fall through
    for _ in 0..cfg.workers {
        ljt.send(Vec::new()).ok();
        sjt.send(Vec::new()).ok();
    }

    let spec = spec.clone(); let nw = cfg.workers;
    let handles: Vec<_> = (0..nw).map(|id| {
        let (ljr, sjr, rt, spec, rp, p) = (
            ljr.clone(), sjr.clone(), rt.clone(), spec.clone(), rpath.clone(), process.clone(),
        );
        std::thread::spawn(move || {
            let mut ssh = SSHConnection::new(spec, false);
            if let Err(e) = ssh.connect() {
                eprintln!("  W{} SSH: {}", id, e); let _ = rt.send(WorkerMsg::Done { id, bytes: 0, files: 0 }); return;
            }
            if mkdir { let _ = ssh.mkdir_p(&rp); }
            let mut total = (0u64, 0usize);
            for recv in [&ljr, &sjr] {
                loop {
                    let b = match recv.recv() { Ok(b) => b, Err(_) => break };
                    if b.is_empty() { break; }
                    p(&b, &mut ssh, id, &rt, &rp);
                    for e in &b { total.0 += e.size; total.1 += 1; }
                }
            }
            let _ = rt.send(WorkerMsg::Done { id, bytes: total.0, files: total.1 });
        })
    }).collect();

    if n_large > 0 { eprintln!("  {} large files, {} tar batches across {} workers",
        n_large, n_small, nw); }
    render_loop(&rr, nw);
    for h in handles { h.join().ok(); } eprintln!();
}

pub fn copy_remote_parallel(
    entries: &[FileEntry], rpath: &str, _p: &Progress, c: &TransferConfig, spec: &RemoteSpec,
) {
    launch(entries, c, spec, rpath.to_string(), true,
        |batch, ssh, id, rt, rp| { for e in batch { push_file(e, ssh, rp, id, rt); } });
}

pub fn copy_remote_pull_parallel(
    entries: &[FileEntry], spec: &RemoteSpec, _src_root: &str, dst_root: &Path, _p: &Progress, c: &TransferConfig,
) {
    launch(entries, c, spec, dst_root.to_string_lossy().to_string(), false,
        |batch, ssh, id, rt, rp| { for e in batch { pull_file(e, ssh, rp, id, rt); } });
}

pub fn copy_remote_relay_parallel(
    entries: &[FileEntry], _src_spec: &RemoteSpec, _dst_spec: &RemoteSpec, _src_root: &str, _dst_root: &str, _p: &Progress, c: &TransferConfig,
) {
    launch(entries, c, _src_spec, String::new(), false,
        |_, _, _, _, _| eprintln!("relay stub"));
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
