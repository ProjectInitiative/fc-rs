use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use crate::dedup::LinkTarget;
use crate::ssh::SSHConnection;
use crate::types::FileEntry;
use crate::types::REMOTE_MANIFEST_NAME;

pub fn save_remote_manifest(
    ssh: &mut SSHConnection,
    remote_root: &str,
    entries: &[FileEntry],
    link_map: &HashMap<String, LinkTarget>,
) {
    let mut manifest = serde_json::Map::new();

    for e in entries {
        if let Some(ref h) = e.content_hash {
            let mut file_entry = serde_json::Map::new();
            file_entry.insert(
                "size".to_string(),
                serde_json::Value::Number(serde_json::Number::from(e.size)),
            );
            file_entry.insert("hash".to_string(), serde_json::Value::String(h.clone()));
            manifest.insert(e.rel.clone(), serde_json::Value::Object(file_entry));
        }
    }

    for (dup_rel, target) in link_map {
        let target_rel = match target {
            LinkTarget::Rel(r) => r.clone(),
            LinkTarget::Abs(_) => continue,
        };
        if let Some(entry) = entries.iter().find(|e| e.rel == target_rel) {
            if let Some(ref h) = entry.content_hash {
                let mut file_entry = serde_json::Map::new();
                file_entry.insert(
                    "size".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(entry.size)),
                );
                file_entry.insert("hash".to_string(), serde_json::Value::String(h.clone()));
                manifest.insert(dup_rel.clone(), serde_json::Value::Object(file_entry));
            }
        }
    }

    if let Ok(json) = serde_json::to_string(&manifest) {
        let manifest_path = PathBuf::from(format!("{}/{}", remote_root, REMOTE_MANIFEST_NAME));
        if let Ok(sftp) = ssh.open_sftp() {
            if let Ok(mut file) = sftp.create(&manifest_path) {
                let _ = file.write_all(json.as_bytes());
            }
        }
    }
}
