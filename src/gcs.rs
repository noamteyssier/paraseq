use std::io::{self, Read};
use std::process::{Child, Command, Stdio};

use thiserror::Error;
use which::which;

#[derive(Error, Debug)]
pub enum GcsError {
    #[error("gcloud CLI not found. Please install Google Cloud SDK: https://cloud.google.com/sdk/docs/install")]
    GcloudNotFound,

    #[error("Invalid GCS URL format: {0}")]
    InvalidUrl(String),

    #[error("gcloud command failed: {0}\n\n>> Try running `gcloud auth application-default login` to authenticate")]
    GcloudFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

/// Represents a parsed GCS URL
#[derive(Debug, Clone)]
pub struct GcsUrl {
    pub bucket: String,
    pub object: String,
}

impl GcsUrl {
    /// Parse GCS URL in format: gs://bucket/object/path
    pub fn parse(url: &str) -> Result<Self, GcsError> {
        if let Some(path) = url.strip_prefix("gs://") {
            let parts: Vec<&str> = path.splitn(2, '/').collect();
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                return Err(GcsError::InvalidUrl(format!(
                    "GCS URL must be in format gs://bucket/object, got: {}",
                    url
                )));
            }
            Ok(GcsUrl {
                bucket: parts[0].to_string(),
                object: parts[1].to_string(),
            })
        } else {
            Err(GcsError::InvalidUrl(format!(
                "GCS URL must start with gs://, got: {}",
                url
            )))
        }
    }

    /// Get the full GCS path for gcloud commands
    pub fn gs_path(&self) -> String {
        format!("gs://{}/{}", self.bucket, self.object)
    }
}

/// Reader that streams GCS object content via gcloud storage cat
///
/// This is much faster than making individual HTTP requests because:
/// - Single connection and stream
/// - gcloud handles buffering and retries internally
/// - No per-chunk network overhead
pub struct GcsReader {
    child: Child,
    stdout: std::process::ChildStdout,
    _url: GcsUrl, // Keep for debugging/logging
}

impl GcsReader {
    /// Create a new GCS reader that streams the object via gcloud storage cat
    pub fn new(gcs_url: &str) -> Result<Self, GcsError> {
        // Check if gcloud is available
        which("gcloud").map_err(|_| GcsError::GcloudNotFound)?;

        let url = GcsUrl::parse(gcs_url)?;

        let mut cmd = Command::new("gcloud");
        cmd.arg("storage")
            .arg("cat")
            .arg(url.gs_path())
            .arg("--quiet") // Suppress progress/info messages
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Spawn gcloud command
        let mut child = cmd
            .spawn()
            .map_err(|e| GcsError::GcloudFailed(format!("Failed to spawn gcloud: {}", e)))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| GcsError::GcloudFailed("Failed to capture stdout".to_string()))?;

        Ok(GcsReader {
            child,
            stdout,
            _url: url,
        })
    }

    /// Create GCS reader with custom gcloud arguments
    ///
    /// Common use cases:
    /// - `--project PROJECT_ID` - specify project
    /// - `--billing-project PROJECT_ID` - for requester pays buckets
    /// - `--impersonate-service-account ACCOUNT` - impersonate service account
    pub fn with_gcloud_args(gcs_url: &str, extra_args: &[&str]) -> Result<Self, GcsError> {
        which("gcloud").map_err(|_| GcsError::GcloudNotFound)?;

        let url = GcsUrl::parse(gcs_url)?;

        let mut cmd = Command::new("gcloud");
        cmd.arg("storage").arg("cat");

        // Add custom arguments before the URL
        for arg in extra_args {
            cmd.arg(arg);
        }

        cmd.arg(url.gs_path())
            .arg("--quiet")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| GcsError::GcloudFailed(format!("Failed to spawn gcloud: {}", e)))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| GcsError::GcloudFailed("Failed to capture stdout".to_string()))?;

        Ok(GcsReader {
            child,
            stdout,
            _url: url,
        })
    }

    /// Create GCS reader with specific project (common use case)
    pub fn with_project(gcs_url: &str, project: &str) -> Result<Self, GcsError> {
        Self::with_gcloud_args(gcs_url, &["--project", project])
    }

    /// Create GCS reader for requester pays buckets
    pub fn with_billing_project(gcs_url: &str, billing_project: &str) -> Result<Self, GcsError> {
        Self::with_gcloud_args(gcs_url, &["--billing-project", billing_project])
    }

    /// Create GCS reader with service account impersonation
    pub fn with_impersonation(gcs_url: &str, service_account: &str) -> Result<Self, GcsError> {
        Self::with_gcloud_args(gcs_url, &["--impersonate-service-account", service_account])
    }
}

impl Read for GcsReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stdout.read(buf)
    }
}

impl Drop for GcsReader {
    fn drop(&mut self) {
        // Clean up the gcloud process
        let _ = self.child.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcs_url_parsing() {
        // Test valid GCS URLs
        let url = GcsUrl::parse("gs://my-bucket/path/to/file.fastq").unwrap();
        assert_eq!(url.bucket, "my-bucket");
        assert_eq!(url.object, "path/to/file.fastq");
        assert_eq!(url.gs_path(), "gs://my-bucket/path/to/file.fastq");

        let url = GcsUrl::parse("gs://bucket-name/deep/nested/path/file.fasta.gz").unwrap();
        assert_eq!(url.bucket, "bucket-name");
        assert_eq!(url.object, "deep/nested/path/file.fasta.gz");

        // Test invalid URLs
        assert!(GcsUrl::parse("s3://bucket/object").is_err());
        assert!(GcsUrl::parse("gs://").is_err());
        assert!(GcsUrl::parse("gs://bucket").is_err());
        assert!(GcsUrl::parse("gs://bucket/").is_err());
        assert!(GcsUrl::parse("bucket/object").is_err());
    }

    #[test]
    fn test_gs_path_generation() {
        let url = GcsUrl::parse("gs://test-bucket/some/file.txt").unwrap();
        assert_eq!(url.gs_path(), "gs://test-bucket/some/file.txt");
    }

    // Integration test - requires gcloud and authentication
    #[test]
    #[ignore]
    fn test_gcs_reader_integration() -> Result<(), Box<dyn std::error::Error>> {
        // This test requires:
        // 1. gcloud CLI installed
        // 2. Authentication set up
        // 3. Access to a test bucket/object

        let test_url = "gs://gcp-public-data-landsat/LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2/LC08_L1GT_044034_20130330_20170310_01_T2_MTL.txt";

        let mut reader = GcsReader::new(test_url)?;
        let mut buffer = vec![0u8; 1024];
        let bytes_read = reader.read(&mut buffer)?;

        assert!(bytes_read > 0);
        println!("Successfully read {} bytes from GCS", bytes_read);

        Ok(())
    }
}
