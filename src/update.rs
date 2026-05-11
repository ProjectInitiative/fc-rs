use crate::types::VERSION;

pub fn check_for_update() {
    eprintln!("  fast-copy v{}", VERSION);
    eprintln!("  Update check not implemented in Rust port yet.");
}

pub fn self_update(_target_version: Option<&str>) {
    eprintln!("  Self-update not implemented in Rust port yet.");
}
