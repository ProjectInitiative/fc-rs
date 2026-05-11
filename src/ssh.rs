use std::io::Read;

use ssh2::{Session, Sftp};

use crate::types::RemoteSpec;

pub struct SSHConnection {
    pub spec: RemoteSpec,
    pub session: Option<Session>,
    pub sftp: Option<Sftp>,
    pub caps: Vec<String>,
    compress: bool,
}

impl SSHConnection {
    pub fn new(spec: RemoteSpec, compress: bool) -> Self {
        SSHConnection {
            spec,
            session: None,
            sftp: None,
            caps: Vec::new(),
            compress,
        }
    }

    pub fn connect(&mut self) -> Result<(), String> {
        let tcp = std::net::TcpStream::connect(format!("{}:{}", self.spec.host, self.spec.port))
            .map_err(|e| format!("TCP connect failed: {}", e))?;

        let mut session = Session::new().map_err(|e| format!("SSH session failed: {}", e))?;

        session.set_tcp_stream(tcp);
        session
            .handshake()
            .map_err(|e| format!("SSH handshake failed: {}", e))?;

        // Try agent auth first, then password
        if session.userauth_agent(&self.spec.user).is_err() {
            let password = rpassword::prompt_password(format!(
                "Password for {}@{}: ",
                self.spec.user, self.spec.host
            ))
            .map_err(|e| e.to_string())?;
            session
                .userauth_password(&self.spec.user, &password)
                .map_err(|e| format!("Password auth failed: {}", e))?;
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
