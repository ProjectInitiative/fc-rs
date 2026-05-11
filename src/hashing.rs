use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::types::HASH_CHUNK;

const ALGO_XXH3: u8 = 0;
const ALGO_SHA256: u8 = 1;

static HASH_ALGO: AtomicU8 = AtomicU8::new(ALGO_XXH3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgo {
    Xxh3,
    Sha256,
}

pub fn set_hash_algo(algo: HashAlgo) {
    HASH_ALGO.store(
        match algo {
            HashAlgo::Xxh3 => ALGO_XXH3,
            HashAlgo::Sha256 => ALGO_SHA256,
        },
        Ordering::Relaxed,
    );
}

pub fn hash_name() -> &'static str {
    match HASH_ALGO.load(Ordering::Relaxed) {
        ALGO_XXH3 => "xxh3",
        _ => "sha256",
    }
}

pub fn hash_bytes(data: &[u8]) -> String {
    match HASH_ALGO.load(Ordering::Relaxed) {
        ALGO_XXH3 => {
            use xxhash_rust::xxh3::xxh3_64;
            format!("{:016x}", xxh3_64(data))
        }
        _ => {
            let mut hasher = Sha256::new();
            hasher.update(data);
            format!("{:064x}", hasher.finalize())
        }
    }
}

pub fn hash_file(path: &Path) -> Option<String> {
    match HASH_ALGO.load(Ordering::Relaxed) {
        ALGO_XXH3 => hash_file_xxh3(path),
        _ => hash_file_sha256(path),
    }
}

fn hash_file_xxh3(path: &Path) -> Option<String> {
    let mut file = File::open(path).ok()?;
    use xxhash_rust::xxh3::Xxh3;
    let mut hasher = Xxh3::new();
    let mut buf = vec![0u8; HASH_CHUNK];
    loop {
        let n = file.read(&mut buf).ok()?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Some(format!("{:016x}", hasher.digest()))
}

pub fn hash_file_sha256(path: &Path) -> Option<String> {
    let mut file = File::open(path).ok()?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; HASH_CHUNK];
    loop {
        let n = file.read(&mut buf).ok()?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Some(format!("{:064x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_xxh3_consistency() {
        set_hash_algo(HashAlgo::Xxh3);
        let h1 = hash_bytes(b"hello world");
        let h2 = hash_bytes(b"hello world");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16);
    }

    #[test]
    fn test_hash_sha256_consistency() {
        set_hash_algo(HashAlgo::Sha256);
        let h1 = hash_bytes(b"hello world");
        let h2 = hash_bytes(b"hello world");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn test_empty_xxh3() {
        set_hash_algo(HashAlgo::Xxh3);
        let h = hash_bytes(&[]);
        assert_eq!(h.len(), 16);
    }

    #[test]
    fn test_empty_sha256() {
        set_hash_algo(HashAlgo::Sha256);
        let h = hash_bytes(&[]);
        assert_eq!(h.len(), 64);
    }
}
