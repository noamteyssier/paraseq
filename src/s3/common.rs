//! Types shared by the S3 backends.
//!
//! Kept free of any client or runtime dependency so both the `s3` (aws-sdk,
//! async) and `s3-lite` (rusty-s3 + ureq, blocking) backends can use them.

use std::io;

use thiserror::Error;

/// Default size of an individual ranged request.
///
/// Below ~8 MiB per-request latency starts to dominate; above ~32 MiB a single
/// slow part stalls in-order delivery for longer than it saves.
pub const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;

/// Default number of ranged requests in flight for a single reader.
pub const DEFAULT_CONCURRENCY: usize = 8;

/// Default number of completed-but-unread parts held between the fetchers and
/// the reader. This is the backpressure knob: a slow consumer stalls the
/// window instead of buffering the whole object.
pub const DEFAULT_QUEUE_DEPTH: usize = 2;

#[derive(Error, Debug)]
pub enum S3Error {
    #[error("Invalid S3 URL format: {0}")]
    InvalidUrl(String),

    #[error("S3 request failed: {0}")]
    Request(String),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

/// Represents a parsed S3 URL
#[derive(Debug, Clone)]
pub struct S3Url {
    pub bucket: String,
    pub key: String,
}

impl S3Url {
    /// Parse S3 URL in format: s3://bucket/key/path
    ///
    /// The `s3a://` and `s3n://` schemes used by the Hadoop ecosystem are
    /// accepted as aliases.
    pub fn parse(url: &str) -> Result<Self, S3Error> {
        let path = ["s3://", "s3a://", "s3n://"]
            .iter()
            .find_map(|scheme| url.strip_prefix(scheme))
            .ok_or_else(|| {
                S3Error::InvalidUrl(format!("S3 URL must start with s3://, got: {}", url))
            })?;

        let (bucket, key) = path.split_once('/').unwrap_or((path, ""));
        if bucket.is_empty() || key.is_empty() {
            return Err(S3Error::InvalidUrl(format!(
                "S3 URL must be in format s3://bucket/key, got: {}",
                url
            )));
        }

        Ok(S3Url {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    /// Get the full S3 URI
    pub fn s3_uri(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.key)
    }
}

/// Identity and size of the object being read, resolved once up front.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    /// Total size of the object in bytes.
    pub content_length: u64,
    /// An opaque token pinning the version being read (an ETag for S3, a
    /// generation for GCS).
    ///
    /// Backends should pass this back on every ranged request so that an
    /// object overwritten mid-read fails loudly rather than silently splicing
    /// two different files together.
    pub version_token: Option<String>,
}

/// Tuning for the fetch window.
#[derive(Debug, Clone, Copy)]
pub struct RangeConfig {
    /// Size of an individual ranged request.
    pub part_size: usize,
    /// Number of requests in flight for this reader.
    pub concurrency: usize,
    /// Completed parts buffered ahead of the reader.
    pub queue_depth: usize,
}

impl Default for RangeConfig {
    fn default() -> Self {
        Self {
            part_size: DEFAULT_PART_SIZE,
            concurrency: DEFAULT_CONCURRENCY,
            queue_depth: DEFAULT_QUEUE_DEPTH,
        }
    }
}

impl RangeConfig {
    /// Peak bytes this configuration may hold in memory for one reader.
    pub fn max_buffered_bytes(&self) -> usize {
        self.part_size
            .saturating_mul(self.concurrency + self.queue_depth)
    }

    /// Number of parts an object of `total` bytes divides into.
    pub(crate) fn part_count(&self, total: u64) -> u64 {
        total.div_ceil(self.part_size.max(1) as u64)
    }

    /// Byte range of part `index`, as `(start, len)`.
    pub(crate) fn part_bounds(&self, index: u64, total: u64) -> (u64, usize) {
        let part_size = self.part_size.max(1) as u64;
        let start = index * part_size;
        (start, part_size.min(total - start) as usize)
    }
}
