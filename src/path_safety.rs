use std::path::Path;

pub fn validate_rel_path(rel: &str) -> Result<(), String> {
    if rel.is_empty() || rel.starts_with('/') {
        return Err("absolute path".to_string());
    }
    let abs = Path::new(rel).is_absolute();
    if abs {
        return Err("absolute path".to_string());
    }
    for part in rel.replace('\\', "/").split('/') {
        if part == ".." {
            return Err("path traversal (..)".to_string());
        }
    }
    if rel.contains('\0') || rel.contains('\n') {
        return Err("null or newline in path".to_string());
    }
    Ok(())
}

pub fn is_safe_remote_rel(rel: &str) -> bool {
    validate_rel_path(rel).is_ok()
}

pub fn sanitize_tar_name(name: &str) -> String {
    name.replace("..", "_").replace('/', "_").replace('\0', "")
}
