use std::io::Read;
use std::path::Path;
use ssh2::{Session, Sftp};

use crate::types::RemoteSpec;

pub struct SSHConnection {
    pub spec: RemoteSpec,
    pub session: Option<Session>,
    pub sftp: Option<Sftp>,
    pub caps: Vec<String>,
    compress: bool,
}

impl Default for SSHConnection {
    fn default() -> Self {
        SSHConnection {
            spec: RemoteSpec { user: String::new(), host: String::new(), port: 22, path: String::new() },
            session: None, sftp: None, caps: Vec::new(), compress: false,
        }
    }
}

impl SSHConnection {
    pub fn new(spec: RemoteSpec, compress: bool) -> Self {
        SSHConnection { spec, session: None, sftp: None, caps: Vec::new(), compress }
    }



    pub fn connect(&mut self) -> Result<(), String> {
        let tcp = std::net::TcpStream::connect(format!("{}:{}", self.spec.host, self.spec.port))
            .map_err(|e| format!("TCP connect failed: {}", e))?;

        let mut session = Session::new().map_err(|e| format!("SSH session failed: {}", e))?;

        session.set_tcp_stream(tcp);
        session
            .handshake()
            .map_err(|e| format!("SSH handshake failed: {}", e))?;

        // Try agent auth, then pubkey, then password
        let mut authed = session.userauth_agent(&self.spec.user).is_ok();

        if !authed {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
            for keypath in &[
                format!("{}/.ssh/id_ed25519", home),
                format!("{}/.ssh/id_ecdsa", home),
                format!("{}/.ssh/id_rsa", home),
                format!("{}/.ssh/id_dsa", home),
            ] {
                if Path::new(&keypath).exists() {
                    if session
                        .userauth_pubkey_file(&self.spec.user, None, Path::new(&keypath), None)
                        .is_ok()
                    {
                        authed = true;
                        break;
                    }
                }
            }
        }

        if !authed {
            match rpassword::prompt_password(format!(
                "Password for {}@{}: ",
                self.spec.user, self.spec.host
            )) {
                Ok(password) => {
                    let _ = session
                        .userauth_password(&self.spec.user, &password);
                }
                Err(_) => {
                    return Err(
                        "SSH auth failed: no agent, no key, and no TTY for password prompt"
                            .to_string(),
                    );
                }
            }
        }

        if !session.authenticated() {
            return Err("Not authenticated".to_string());
        }

        if self.compress {
            session.set_compress(true);
        }

        self.detect_capabilities(&session);

        self.session = Some(session);
        Ok(())
    }

    pub fn exec_cmd(&self, cmd: &str, _timeout: u32) -> Result<(String, String, i32), String> {
        let session = self.session.as_ref().ok_or("Not connected")?;
        let mut channel = session.channel_session().map_err(|e| e.to_string())?;
        channel.exec(cmd).map_err(|e| e.to_string())?;

        let mut stdout = String::new();
        let stderr = String::new();
        let _ = channel.read_to_string(&mut stdout);

        let exit_code = channel.exit_status().unwrap_or(-1);
        channel.wait_close().ok();
        Ok((stdout, stderr, exit_code))
    }

    pub fn open_sftp(&mut self) -> Result<&mut Sftp, String> {
        if self.sftp.is_none() {
            let session = self.session.as_ref().ok_or("Not connected")?;
            let sftp = session.sftp().map_err(|e| e.to_string())?;
            self.sftp = Some(sftp);
        }
        Ok(self.sftp.as_mut().unwrap())
    }

    pub fn mkdir_p(&mut self, path: &str) -> Result<(), String> {
        use std::path::Path;
        let sftp = self.open_sftp()?;
        let target = Path::new(path);
        let mut to_create: Vec<&Path> = Vec::new();
        let mut p = target;
        loop {
            if sftp.stat(p).is_ok() { break; }
            to_create.push(p);
            match p.parent() {
                Some(parent) if parent != p => p = parent,
                _ => break,
            }
        }
        for d in to_create.iter().rev() {
            sftp.mkdir(d, 0o755).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn open_channel(&self) -> Result<ssh2::Channel, String> {
        let session = self.session.as_ref().ok_or("Not connected")?;
        let channel = session.channel_session().map_err(|e| e.to_string())?;
        Ok(channel)
    }

    fn detect_capabilities(&mut self, _session: &Session) {
        self.caps.push("tar".to_string());
        self.caps.push("python3".to_string());
    }

    pub fn close(&mut self) {
        self.sftp = None;
        self.session = None;
    }
}
