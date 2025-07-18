use std::io::{self, Read};
use std::process::{Child, Command, Stdio};

use thiserror::Error;
use which::which;

#[derive(Error, Debug)]
pub enum SshError {
    #[error("SSH command not found. Please install OpenSSH client.")]
    SshNotFound,

    #[error("Invalid SSH URL format: {0}")]
    InvalidUrl(String),

    #[error("SSH command failed to start: {0}")]
    SshFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

/// Represents a parsed SSH URL
#[derive(Debug, Clone)]
pub struct SshUrl {
    pub user: Option<String>,
    pub host: String,
    pub port: Option<u16>,
    pub path: String,
}

impl SshUrl {
    /// Parse SSH URL in format: ssh://[user@]host[:port]/path or [user@]host:path
    pub fn parse(url: &str) -> Result<Self, SshError> {
        if let Some(url) = url.strip_prefix("ssh://") {
            Self::parse_ssh_scheme(url)
        } else if url.contains(':') && !url.starts_with('/') {
            Self::parse_scp_format(url)
        } else {
            Err(SshError::InvalidUrl(format!(
                "Invalid SSH URL format: {}",
                url
            )))
        }
    }

    fn parse_ssh_scheme(url: &str) -> Result<Self, SshError> {
        let (user_host, path) = url
            .split_once('/')
            .ok_or_else(|| SshError::InvalidUrl("Missing path in SSH URL".to_string()))?;

        let (user, host_port) = if let Some((user, host_port)) = user_host.split_once('@') {
            (Some(user.to_string()), host_port)
        } else {
            (None, user_host)
        };

        let (host, port) = if let Some((host, port_str)) = host_port.split_once(':') {
            let port = port_str
                .parse::<u16>()
                .map_err(|_| SshError::InvalidUrl(format!("Invalid port: {}", port_str)))?;
            (host.to_string(), Some(port))
        } else {
            (host_port.to_string(), None)
        };

        Ok(SshUrl {
            user,
            host,
            port,
            path: format!("/{}", path),
        })
    }

    fn parse_scp_format(url: &str) -> Result<Self, SshError> {
        let (user_host, path) = url
            .split_once(':')
            .ok_or_else(|| SshError::InvalidUrl("Missing colon in SCP format".to_string()))?;

        let (user, host) = if let Some((user, host)) = user_host.split_once('@') {
            (Some(user.to_string()), host.to_string())
        } else {
            (None, user_host.to_string())
        };

        Ok(SshUrl {
            user,
            host,
            port: None,
            path: path.to_string(),
        })
    }

    /// Get SSH connection string with optional port
    pub fn ssh_host(&self) -> String {
        let user_part = self
            .user
            .as_ref()
            .map(|u| format!("{}@", u))
            .unwrap_or_default();

        format!("{}{}", user_part, self.host)
    }
}

/// Reader that streams file content via SSH cat
pub struct SshReader {
    child: Child,
    stdout: std::process::ChildStdout,
}

impl SshReader {
    /// Create a new SSH reader that streams the file via SSH cat
    pub fn new(ssh_url: &str) -> Result<Self, SshError> {
        // Check if ssh is available
        which("ssh").map_err(|_| SshError::SshNotFound)?;

        let url = SshUrl::parse(ssh_url)?;

        let mut cmd = Command::new("ssh");
        cmd.arg("-q") // Quiet mode
            .arg("-o")
            .arg("BatchMode=yes") // Non-interactive
            .arg("-o")
            .arg("StrictHostKeyChecking=accept-new"); // Accept new host keys

        // Add port if specified
        if let Some(port) = url.port {
            cmd.arg("-p").arg(port.to_string());
        }

        cmd.arg(url.ssh_host())
            .arg("cat")
            .arg(&url.path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Spawn SSH command
        let mut child = cmd
            .spawn()
            .map_err(|e| SshError::SshFailed(format!("Failed to spawn SSH: {}", e)))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SshError::SshFailed("Failed to capture stdout".to_string()))?;

        Ok(SshReader { child, stdout })
    }

    /// Create SSH reader with custom SSH arguments
    pub fn with_ssh_args(ssh_url: &str, extra_args: &[&str]) -> Result<Self, SshError> {
        which("ssh").map_err(|_| SshError::SshNotFound)?;

        let url = SshUrl::parse(ssh_url)?;

        let mut cmd = Command::new("ssh");
        cmd.arg("-q")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("StrictHostKeyChecking=accept-new");

        // Add custom arguments
        for arg in extra_args {
            cmd.arg(arg);
        }

        if let Some(port) = url.port {
            cmd.arg("-p").arg(port.to_string());
        }

        cmd.arg(url.ssh_host())
            .arg("cat")
            .arg(&url.path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| SshError::SshFailed(format!("Failed to spawn SSH: {}", e)))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SshError::SshFailed("Failed to capture stdout".to_string()))?;

        Ok(SshReader { child, stdout })
    }
}

impl Read for SshReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stdout.read(buf)
    }
}

impl Drop for SshReader {
    fn drop(&mut self) {
        // Clean up the SSH process
        let _ = self.child.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssh_url_parsing() {
        // Test SSH scheme
        let url = SshUrl::parse("ssh://user@example.com:2222/path/to/file.fastq").unwrap();
        assert_eq!(url.user, Some("user".to_string()));
        assert_eq!(url.host, "example.com");
        assert_eq!(url.port, Some(2222));
        assert_eq!(url.path, "/path/to/file.fastq");

        // Test SCP format
        let url = SshUrl::parse("user@example.com:path/to/file.fastq").unwrap();
        assert_eq!(url.user, Some("user".to_string()));
        assert_eq!(url.host, "example.com");
        assert_eq!(url.port, None);
        assert_eq!(url.path, "path/to/file.fastq");

        // Test without user
        let url = SshUrl::parse("example.com:path/to/file.fastq").unwrap();
        assert_eq!(url.user, None);
        assert_eq!(url.host, "example.com");
        assert_eq!(url.path, "path/to/file.fastq");
    }

    #[test]
    fn test_ssh_host_format() {
        let url = SshUrl::parse("ssh://user@example.com:2222/path/to/file.fastq").unwrap();
        assert_eq!(url.ssh_host(), "user@example.com");

        let url = SshUrl::parse("example.com:path/to/file.fastq").unwrap();
        assert_eq!(url.ssh_host(), "example.com");
    }
}
