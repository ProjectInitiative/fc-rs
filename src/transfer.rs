use std::borrow::Cow;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossbeam_channel::unbounded;

use crate::progress::{self, Progress};
use crate::ssh::SSHConnection;
use crate::types::{FileEntry, RemoteSpec, SMALL_FILE_THRESHOLD};
use std::path::Path;

fn shq(s: &str) -> Cow<'_, str> { shlex::try_quote(s).unwrap_or(Cow::Borrowed(s)) }

#[derive(Clone)]
pub struct TransferConfig { pub workers: usize, pub compress_zstd: bool, pub zstd_level: i32, pub buf_size: usize }
#[derive(Clone, Debug)]
pub enum WorkerMsg { Chunk { id: usize, bytes: u64, done: usize, total: usize }, Done { id: usize, bytes: u64, files: usize } }

// ── Copy a single file via SFTP with chunked progress ───────────────

fn send_one_via_sftp(
    entry: &FileEntry, ssh: &Arc<Mutex<SSHConnection>>, rpath: &str, id: usize,
    rt: &crossbeam_channel::Sender<WorkerMsg>,
) -> (u64, usize) {
    let remote = format!("{}/{}", rpath, entry.rel);
    if let Some(parent) = Path::new(&remote).parent() {
        let _ = ssh.lock().unwrap().mkdir_p(&parent.to_string_lossy());
    }

    let chunks = ((entry.size + (64 << 20) - 1) / (64 << 20)) as usize;

    match ssh.lock().unwrap().open_sftp() {
        Ok(sftp) => {
            match sftp.create(&Path::new(&remote)) {
                Ok(mut rf) => {
                    match File::open(&entry.src) {
                        Ok(mut lf) => {
                            let mut buf = vec![0u8; (64 << 20) as usize];
                            let mut sent = 0u64;
                            let mut idx = 0usize;
                            loop {
                                let n = match lf.read(&mut buf) { Ok(0) => break, Ok(n) => n, Err(_) => break };
                                if rf.write_all(&buf[..n]).is_err() { break; }
                                sent += n as u64; idx += 1;
                                let _ = rt.send(WorkerMsg::Chunk { id, bytes: n as u64, done: idx, total: chunks });
                            }
                            return (sent, 1);
                        }
                        Err(e) => eprintln!("\n  W{} open local: {}", id, e),
                    }
                }
                Err(e) => eprintln!("\n  W{} create remote: {}", id, e),
            }
        }
        Err(e) => eprintln!("\n  W{} sftp: {}", id, e),
    }
    (0, 0)
}

// ── TransferPlanner ──────────────────────────────────────────────────

pub struct TransferPlanner { pub jobs: Vec<Vec<FileEntry>> }
impl TransferPlanner {
    pub fn new(entries: &[FileEntry], _nworkers: usize) -> Self {
        // Each file becomes its own job — send_one_via_sftp handles size-based chunking
        let mut jobs: Vec<Vec<FileEntry>> = Vec::new();
        for e in entries { jobs.push(vec![e.clone()]); }
        TransferPlanner { jobs }
    }
}

// ── Main copy function ───────────────────────────────────────────────

pub fn copy_remote_parallel(
    entries: &[FileEntry], remote_root: &str,
    _progress: &Progress, config: &TransferConfig,
    ssh: &Arc<Mutex<SSHConnection>>,
) {
    let jobs = TransferPlanner::new(entries, config.workers).jobs;
    if jobs.is_empty() { return; }

    let (jt, jr) = unbounded();
    let (rt, rr) = unbounded::<WorkerMsg>();
    for job in jobs { jt.send(job).ok(); }
    for _ in 0..config.workers { jt.send(Vec::new()).ok(); }

    let rpath = remote_root.to_string(); let cfg = config.clone(); let nw = config.workers;
    let mut handles = Vec::new();
    for id in 0..nw {
        let (jr, rt, rpath, cfg, ssh) = (jr.clone(), rt.clone(), rpath.clone(), cfg.clone(), ssh.clone());
        handles.push(std::thread::spawn(move || {
            let mut total = (0u64, 0usize);
            loop {
                let b = match jr.recv() { Ok(b) => b, Err(_) => break };
                if b.is_empty() { break; }
                for entry in &b {
                    let r = send_one_via_sftp(entry, &ssh, &rpath, id, &rt);
                    total.0 += r.0; total.1 += r.1;
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

// Stubs — pull/relay not ported
pub fn copy_remote_pull_parallel(_e: &[FileEntry], _s: &RemoteSpec, _sr: &str, _dr: &Path, _p: &Progress, _c: &TransferConfig) {
    eprintln!("  Pull parallel not implemented, using single-stream");
}
pub fn copy_remote_relay_parallel(_e: &[FileEntry], _ss: &RemoteSpec, _ds: &RemoteSpec, _sr: &str, _dr: &str, _p: &Progress, _c: &TransferConfig) {
    eprintln!("  Relay parallel not implemented, using single-stream");
}
