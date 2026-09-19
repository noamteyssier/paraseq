//! Remote transport backends for reading sequence files over a network.

#[cfg(feature = "gcs")]
pub mod gcs;

#[cfg(feature = "ssh")]
pub mod ssh;

/// A spawned subprocess whose stdout is streamed via `Read`; the child is reaped on drop.
#[cfg(any(feature = "gcs", feature = "ssh"))]
struct ProcessReader {
    child: std::process::Child,
    stdout: std::process::ChildStdout,
}

#[cfg(any(feature = "gcs", feature = "ssh"))]
impl ProcessReader {
    /// Spawn `cmd` with piped stdout/stderr.
    fn spawn(mut cmd: std::process::Command) -> std::io::Result<Self> {
        use std::process::Stdio;
        let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("Failed to capture stdout"))?;
        Ok(Self { child, stdout })
    }
}

#[cfg(any(feature = "gcs", feature = "ssh"))]
impl std::io::Read for ProcessReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stdout.read(buf)
    }
}

#[cfg(any(feature = "gcs", feature = "ssh"))]
impl Drop for ProcessReader {
    fn drop(&mut self) {
        let _ = self.child.wait();
    }
}
