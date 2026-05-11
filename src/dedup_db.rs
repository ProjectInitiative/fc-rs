use std::fs;
use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};

use crate::hashing;
use crate::types::DEDUP_DB_NAME;

pub struct DedupDB {
    pub mount: String,
    pub dst_root: String,
    pub db_path: String,
    conn: Mutex<Connection>,
}

impl DedupDB {
    pub fn new(dst_root: &Path) -> Result<Self, String> {
        let dst_root = fs::canonicalize(dst_root).map_err(|e| e.to_string())?;
        let mount = find_mount_point(&dst_root).unwrap_or_else(|| dst_root.clone());

        let db_path = if mount == Path::new("/") || !is_writable(&mount) {
            dst_root.join(DEDUP_DB_NAME)
        } else {
            mount.join(DEDUP_DB_NAME)
        };

        let conn = Connection::open(&db_path).map_err(|e| e.to_string())?;
        conn.execute_batch("PRAGMA journal_mode=WAL").ok();
        conn.execute_batch("PRAGMA synchronous=NORMAL").ok();
        conn.execute_batch("PRAGMA user_version=4718").ok();

        init_schema(&conn)?;

        Ok(DedupDB {
            mount: mount.to_string_lossy().to_string(),
            dst_root: dst_root.to_string_lossy().to_string(),
            db_path: db_path.to_string_lossy().to_string(),
            conn: Mutex::new(conn),
        })
    }

    fn mount_rel(&self, rel_path: &str) -> String {
        let dst = Path::new(&self.dst_root);
        let mount = Path::new(&self.mount);
        let rel = dst
            .strip_prefix(mount)
            .map(|p| p.join(rel_path))
            .unwrap_or_else(|_| Path::new(rel_path).to_path_buf());
        rel.to_string_lossy().to_string().replace('\\', "/")
    }

    pub fn lookup(&self, rel_path: &str, size: u64, mtime_ns: i64) -> Option<String> {
        let conn = self.conn.lock().ok()?;
        let mut stmt = conn
            .prepare(
                "SELECT content_hash FROM source_cache \
                 WHERE rel_path = ? AND size = ? AND mtime_ns = ? AND hash_algo = ?",
            )
            .ok()?;
        stmt.query_row(
            params![rel_path, size, mtime_ns, hashing::hash_name()],
            |row| row.get(0),
        )
        .ok()
    }

    pub fn store_source_batch(&self, rows: &[(String, u64, i64, String)]) {
        if let Ok(conn) = self.conn.lock() {
            let hash_name = hashing::hash_name();
            for (rel_path, size, mtime_ns, hash) in rows {
                conn.execute(
                    "INSERT OR REPLACE INTO source_cache \
                     (rel_path, size, mtime_ns, content_hash, hash_algo) \
                     VALUES (?, ?, ?, ?, ?)",
                    params![rel_path, size, mtime_ns, hash, hash_name],
                )
                .ok();
            }
            conn.execute_batch("COMMIT").ok();
        }
    }

    pub fn store_dest_batch(&self, rows: &[(String, u64, String)]) {
        if let Ok(conn) = self.conn.lock() {
            let hash_name = hashing::hash_name();
            for (rel_path, size, hash) in rows {
                let mount_rel = self.mount_rel(rel_path);
                conn.execute(
                    "INSERT OR REPLACE INTO dest_files \
                     (mount_rel, size, content_hash, hash_algo) \
                     VALUES (?, ?, ?, ?)",
                    params![mount_rel, size, hash, hash_name],
                )
                .ok();
            }
            conn.execute_batch("COMMIT").ok();
        }
    }

    pub fn lookup_by_hash(&self, content_hash: &str) -> Vec<(String, u64)> {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        let mut stmt = match conn.prepare(
            "SELECT mount_rel, size FROM dest_files \
             WHERE content_hash = ? AND hash_algo = ?",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let hash_name = hashing::hash_name();
        stmt.query_map(params![content_hash, hash_name], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
        })
        .ok()
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    }
}

fn init_schema(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS source_cache (
            rel_path    TEXT NOT NULL,
            size        INTEGER NOT NULL,
            mtime_ns    INTEGER NOT NULL,
            content_hash TEXT NOT NULL,
            hash_algo   TEXT NOT NULL,
            PRIMARY KEY (rel_path, hash_algo)
        );
        CREATE TABLE IF NOT EXISTS dest_files (
            mount_rel   TEXT PRIMARY KEY,
            size        INTEGER NOT NULL,
            content_hash TEXT NOT NULL,
            hash_algo   TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_dest_hash ON dest_files (content_hash);",
    )
    .map_err(|e| e.to_string())?;
    // Migrate old schema
    conn.execute("DROP TABLE IF EXISTS file_hashes", []).ok();
    Ok(())
}

fn find_mount_point(path: &Path) -> Option<std::path::PathBuf> {
    use std::os::unix::fs::MetadataExt;
    let target = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };

    if let Ok(meta) = target.metadata() {
        let dev = meta.dev();
        let mut cur = Some(target.clone());
        while let Some(ref p) = cur {
            if let Ok(m) = p.metadata() {
                if m.dev() != dev {
                    return Some(p.clone());
                }
            }
            cur = p.parent().map(|pp| pp.to_path_buf());
        }
    }
    Some(Path::new("/").to_path_buf())
}

fn is_writable(path: &Path) -> bool {
    let test_path = path.join(".write_test");
    match fs::write(&test_path, b"") {
        Ok(_) => {
            fs::remove_file(&test_path).ok();
            true
        }
        Err(_) => false,
    }
}
