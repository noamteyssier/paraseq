//! Blocking S3 backend built on sans-IO signing.
//!
//! Where the [`S3Reader`](super::S3Reader) backend uses `aws-sdk-s3` (and with it
//! tokio, hyper, and h2), this one pairs `rusty-s3` for SigV4 signing with
//! `ureq` for blocking HTTP and `aws-creds` for the credential chain. Nothing
//! here is async, which fits the rest of paraseq: the concurrent window is a
//! pool of `std::thread` workers feeding a bounded `crossbeam` channel, the
//! same shape the parser already uses.
//!
//! Trade-offs against the SDK backend:
//!
//! - No AWS SSO. `aws-creds` resolves environment, profile, STS web identity
//!   (IRSA / EKS), ECS container credentials, and IMDS v1/v2, so deployment
//!   works; SSO users must materialize credentials first (`granted`/`assume`,
//!   or `aws configure export-credentials`).
//! - Retries are hand-rolled here rather than inherited from the SDK.
//! - An in-flight request cannot be cancelled, only abandoned, because a
//!   blocking `ureq` call has no equivalent of dropping a future.

use std::io::{self, BufRead, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use bytes::{Buf, Bytes};
use crossbeam_channel::{bounded, Receiver, Sender};
use rusty_s3::actions::{GetObject, HeadObject};
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

use super::common::{ObjectMeta, RangeConfig, S3Error, S3Url};

/// How long a signed URL stays valid. Generous enough to cover a slow part
/// fetch, short enough that a leaked URL is not useful for long.
const SIGNATURE_TTL: Duration = Duration::from_secs(15 * 60);

/// Attempts per part before giving up.
const MAX_ATTEMPTS: usize = 3;

/// A source of byte ranges that blocks rather than returning futures.
pub trait BlockingRangeFetcher: Send + Sync + 'static {
    /// Resolves the object's size and version token.
    fn open(&self) -> io::Result<ObjectMeta>;

    /// Fetches exactly `len` bytes starting at `start`.
    fn fetch(&self, meta: &ObjectMeta, start: u64, len: usize) -> io::Result<Bytes>;
}

/// A unit of work handed to a worker thread.
struct Job {
    start: u64,
    len: usize,
    reply: Sender<io::Result<Bytes>>,
}

/// An [`io::Read`] over an object fetched by a pool of worker threads.
///
/// Ordering comes from the slot queue rather than from a reorder buffer: the
/// scheduler pushes one single-use receiver per part, in part order, and the
/// reader drains them in that order. Because the slot queue is bounded to
/// `concurrency`, at most that many parts can be outstanding, which is what
/// caps memory.
pub struct ThreadedRangedReader {
    slots: Receiver<Receiver<io::Result<Bytes>>>,
    current: Bytes,
    stop: Arc<AtomicBool>,
    scheduler: Option<JoinHandle<()>>,
    meta: ObjectMeta,
}

impl ThreadedRangedReader {
    /// Opens `fetcher`'s object and starts prefetching.
    ///
    /// Blocks until the object's metadata has been resolved, so authentication
    /// and not-found errors are reported here rather than on first read.
    pub fn new<F: BlockingRangeFetcher>(fetcher: F, config: RangeConfig) -> io::Result<Self> {
        let fetcher = Arc::new(fetcher);
        let meta = fetcher.open()?;

        let concurrency = config.concurrency.max(1);
        let stop = Arc::new(AtomicBool::new(false));

        let (slot_tx, slot_rx) = bounded::<Receiver<io::Result<Bytes>>>(concurrency);
        let (work_tx, work_rx) = bounded::<Job>(concurrency);

        for _ in 0..concurrency {
            let work_rx = work_rx.clone();
            let fetcher = fetcher.clone();
            let meta = meta.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                while let Ok(job) = work_rx.recv() {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let result = fetcher.fetch(&meta, job.start, job.len);
                    // A send error means the reader is gone.
                    if job.reply.send(result).is_err() {
                        break;
                    }
                }
            });
        }
        drop(work_rx);

        let total = meta.content_length;
        let n_parts = config.part_count(total);
        let scheduler_stop = stop.clone();
        let scheduler = thread::spawn(move || {
            for index in 0..n_parts {
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                let (start, len) = config.part_bounds(index, total);
                let (reply_tx, reply_rx) = bounded(1);

                // Bounded: blocks once `concurrency` parts are outstanding.
                if slot_tx.send(reply_rx).is_err() {
                    break;
                }
                if work_tx
                    .send(Job {
                        start,
                        len,
                        reply: reply_tx,
                    })
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Self {
            slots: slot_rx,
            current: Bytes::new(),
            stop,
            scheduler: Some(scheduler),
            meta,
        })
    }

    /// Total size of the object in bytes.
    pub fn content_length(&self) -> u64 {
        self.meta.content_length
    }
}

impl std::fmt::Debug for ThreadedRangedReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadedRangedReader")
            .field("content_length", &self.meta.content_length)
            .field("version_token", &self.meta.version_token)
            .finish()
    }
}

impl Read for ThreadedRangedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for ThreadedRangedReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        // Loop rather than branch so a zero-length part cannot be mistaken
        // for EOF.
        while self.current.is_empty() {
            let Ok(slot) = self.slots.recv() else {
                break; // scheduler finished and every part was consumed
            };
            match slot.recv() {
                Ok(Ok(bytes)) => self.current = bytes,
                Ok(Err(e)) => return Err(e),
                // The worker died without replying.
                Err(_) => return Err(io::Error::other("range fetch worker stopped unexpectedly")),
            }
        }
        Ok(&self.current)
    }

    fn consume(&mut self, amt: usize) {
        self.current.advance(amt.min(self.current.len()));
    }
}

impl Drop for ThreadedRangedReader {
    fn drop(&mut self) {
        // Blocking requests cannot be cancelled, so signal and detach rather
        // than join: an early stop must not wait on in-flight HTTP.
        self.stop.store(true, Ordering::Relaxed);
        drop(std::mem::replace(&mut self.slots, bounded(0).1));
        self.scheduler.take();
    }
}

/// A [`BlockingRangeFetcher`] that signs with `rusty-s3` and transports with `ureq`.
pub struct LiteS3Fetcher {
    bucket: Bucket,
    credentials: Option<Credentials>,
    key: String,
    agent: ureq::Agent,
}

/// Builds an agent that surfaces redirects instead of chasing them.
///
/// S3 answers a request aimed at the wrong regional endpoint with `301` and no
/// `Location` header -- the real region arrives in `x-amz-bucket-region`. A
/// redirect-following client fails that with a confusing protocol error, so we
/// keep the response and read the header ourselves.
fn s3_agent() -> ureq::Agent {
    ureq::Agent::new_with_config(
        ureq::Agent::config_builder()
            .max_redirects(0)
            .max_redirects_will_error(false)
            .build(),
    )
}

/// Asks S3 which region a bucket lives in.
///
/// The global endpoint answers with `x-amz-bucket-region` even when it rejects
/// the request, so this works unsigned and against private buckets.
fn probe_bucket_region(bucket: &str) -> Option<String> {
    let response = s3_agent()
        .head(&format!("https://s3.amazonaws.com/{bucket}"))
        .call()
        .ok()?;
    header_str(response.headers(), "x-amz-bucket-region")
}

/// Reads `region` for `profile` out of the shared AWS config file.
pub(crate) fn region_from_config_file(profile: Option<&str>) -> Option<String> {
    let path = std::env::var("AWS_CONFIG_FILE")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| dirs_home().map(|home| home.join(".aws").join("config")))?;
    let contents = std::fs::read_to_string(path).ok()?;

    let wanted = match profile {
        Some("default") | None => "[default]".to_string(),
        Some(name) => format!("[profile {name}]"),
    };

    let mut in_section = false;
    for line in contents.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_section = line == wanted;
        } else if in_section {
            if let Some(value) = line.strip_prefix("region") {
                if let Some(value) = value.trim_start().strip_prefix('=') {
                    return Some(value.trim().to_string());
                }
            }
        }
    }
    None
}

fn dirs_home() -> Option<std::path::PathBuf> {
    std::env::var("HOME").ok().map(std::path::PathBuf::from)
}

impl LiteS3Fetcher {
    pub fn new(bucket: Bucket, credentials: Option<Credentials>, key: String) -> Self {
        Self {
            bucket,
            credentials,
            key,
            agent: s3_agent(),
        }
    }

    /// Runs `attempt` up to [`MAX_ATTEMPTS`] times with linear backoff.
    ///
    /// The SDK backend inherits this from the AWS runtime; here it is ours to
    /// provide. Retrying per part rather than per stream is the point: a
    /// dropped connection costs one part, not the whole object.
    fn with_retries<T>(context: &str, mut attempt: impl FnMut() -> io::Result<T>) -> io::Result<T> {
        let mut last = None;
        for tries in 0..MAX_ATTEMPTS {
            if tries > 0 {
                thread::sleep(Duration::from_millis(100 * tries as u64));
            }
            match attempt() {
                Ok(value) => return Ok(value),
                Err(e) => last = Some(e),
            }
        }
        Err(io::Error::other(format!(
            "{context} failed after {MAX_ATTEMPTS} attempts: {}",
            last.map(|e| e.to_string()).unwrap_or_default()
        )))
    }
}

/// Strips the quotes S3 wraps around ETag values, leaving the raw token.
fn header_str(headers: &ureq::http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

impl BlockingRangeFetcher for LiteS3Fetcher {
    fn open(&self) -> io::Result<ObjectMeta> {
        let action = HeadObject::new(&self.bucket, self.credentials.as_ref(), &self.key);
        let url = action.sign(SIGNATURE_TTL);

        let response = Self::with_retries("HeadObject", || {
            self.agent
                .head(url.as_str())
                .call()
                .map_err(|e| io::Error::other(format!("HeadObject {}: {e}", self.key)))
        })?;

        if !response.status().is_success() {
            if let Some(region) = header_str(response.headers(), "x-amz-bucket-region") {
                return Err(io::Error::other(format!(
                    "HeadObject {} returned HTTP {}: bucket is in region {region}, \
                     but the request went to a different endpoint",
                    self.key,
                    response.status()
                )));
            }
            return Err(io::Error::other(format!(
                "HeadObject {} returned HTTP {}",
                self.key,
                response.status()
            )));
        }

        let headers = response.headers();
        let content_length = header_str(headers, "content-length")
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| io::Error::other("HeadObject response had no Content-Length"))?;

        Ok(ObjectMeta {
            content_length,
            version_token: header_str(headers, "etag"),
        })
    }

    fn fetch(&self, meta: &ObjectMeta, start: u64, len: usize) -> io::Result<Bytes> {
        debug_assert!(len > 0);
        let end = start + len as u64 - 1;
        let range = format!("bytes={start}-{end}");

        let action = GetObject::new(&self.bucket, self.credentials.as_ref(), &self.key);
        let url = action.sign(SIGNATURE_TTL);

        let body = Self::with_retries(&format!("GetObject {range}"), || {
            // `Range` and `If-Match` ride along unsigned: SigV4 query-string
            // auth only covers the headers named in the signature, and S3
            // honours these regardless.
            let mut request = self.agent.get(url.as_str()).header("range", &range);
            if let Some(etag) = &meta.version_token {
                request = request.header("if-match", etag);
            }

            let mut response = request
                .call()
                .map_err(|e| io::Error::other(format!("GetObject {range}: {e}")))?;

            if !response.status().is_success() {
                return Err(io::Error::other(format!(
                    "GetObject {range} returned HTTP {}",
                    response.status()
                )));
            }

            response
                .body_mut()
                .with_config()
                .limit(u64::MAX)
                .read_to_vec()
                .map_err(|e| io::Error::other(format!("reading GetObject {range}: {e}")))
        })?;

        if body.len() != len {
            return Err(io::Error::other(format!(
                "short range read: requested {} bytes at offset {}, got {}",
                len,
                start,
                body.len()
            )));
        }

        Ok(Bytes::from(body))
    }
}

/// Builder for a blocking S3 reader.
///
/// ```no_run
/// # use paraseq::s3::LiteS3Reader;
/// let reader = LiteS3Reader::builder()
///     .region("us-west-2")
///     .concurrency(8)
///     .build("s3://my-bucket/reads.fastq.gz")?;
/// # Ok::<(), paraseq::s3::S3Error>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct LiteS3ReaderBuilder {
    region: Option<String>,
    endpoint_url: Option<String>,
    profile: Option<String>,
    force_path_style: bool,
    anonymous: bool,
    config: RangeConfig,
}

impl LiteS3ReaderBuilder {
    /// Override the region rather than resolving it from the environment.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Point at an S3-compatible endpoint (MinIO, Ceph, Cloudflare R2).
    ///
    /// Most such endpoints also want [`Self::force_path_style`].
    pub fn endpoint_url(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint.into());
        self
    }

    /// Use a named profile from the shared AWS credentials file.
    pub fn profile(mut self, profile: impl Into<String>) -> Self {
        self.profile = Some(profile.into());
        self
    }

    /// Address buckets as `endpoint/bucket/key` rather than as a subdomain.
    pub fn force_path_style(mut self, enabled: bool) -> Self {
        self.force_path_style = enabled;
        self
    }

    /// Send unsigned requests, for public buckets.
    pub fn anonymous(mut self, enabled: bool) -> Self {
        self.anonymous = enabled;
        self
    }

    /// Size of an individual ranged request.
    pub fn part_size(mut self, bytes: usize) -> Self {
        self.config.part_size = bytes;
        self
    }

    /// Number of ranged requests in flight for this reader.
    pub fn concurrency(mut self, requests: usize) -> Self {
        self.config.concurrency = requests;
        self
    }

    /// Completed parts buffered ahead of the reader.
    pub fn queue_depth(mut self, parts: usize) -> Self {
        self.config.queue_depth = parts;
        self
    }

    /// Resolves the region to sign and address requests with.
    ///
    /// The bucket's region is what matters, and it need not match the caller's
    /// environment, so an explicit setting and the environment are consulted
    /// first and S3 itself is asked as the authority before falling back.
    fn resolve_region(&self, bucket: &str) -> String {
        self.region
            .clone()
            .or_else(|| std::env::var("AWS_REGION").ok())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .or_else(|| region_from_config_file(self.profile.as_deref()))
            .or_else(|| {
                // Only worth a round-trip against real S3; a custom endpoint
                // (MinIO, R2) has no global endpoint to ask.
                if self.endpoint_url.is_none() {
                    probe_bucket_region(bucket)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "us-east-1".to_string())
    }

    fn resolve_credentials(&self) -> Result<Option<Credentials>, S3Error> {
        if self.anonymous {
            return Ok(None);
        }

        let resolved = match &self.profile {
            Some(profile) => awscreds::Credentials::from_profile(Some(profile)),
            // Chains environment, profile, STS web identity (IRSA),
            // container credentials, and IMDS v2/v1.
            None => awscreds::Credentials::default(),
        }
        .map_err(|e| S3Error::Request(format!("could not resolve AWS credentials: {e}")))?;

        let (Some(key), Some(secret)) = (resolved.access_key, resolved.secret_key) else {
            return Err(S3Error::Request(
                "resolved AWS credentials were incomplete".to_string(),
            ));
        };

        Ok(Some(
            match resolved.session_token.or(resolved.security_token) {
                Some(token) => Credentials::new_with_token(key, secret, token),
                None => Credentials::new(key, secret),
            },
        ))
    }

    /// Open `url` for reading with a thread-backed ranged window.
    pub fn build(&self, url: &str) -> Result<ThreadedRangedReader, S3Error> {
        let parsed = S3Url::parse(url)?;
        let region = self.resolve_region(&parsed.bucket);

        let endpoint = self
            .endpoint_url
            .clone()
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let endpoint = endpoint
            .parse()
            .map_err(|e| S3Error::InvalidUrl(format!("invalid endpoint {endpoint}: {e}")))?;

        let style = if self.force_path_style {
            UrlStyle::Path
        } else {
            UrlStyle::VirtualHost
        };

        let bucket = Bucket::new(endpoint, style, parsed.bucket.clone(), region)
            .map_err(|e| S3Error::InvalidUrl(format!("invalid bucket {}: {e}", parsed.bucket)))?;

        let credentials = self.resolve_credentials()?;
        let fetcher = LiteS3Fetcher::new(bucket, credentials, parsed.key);

        ThreadedRangedReader::new(fetcher, self.config).map_err(S3Error::Io)
    }
}

/// Entry point for the blocking S3 backend.
pub struct LiteS3Reader;

impl LiteS3Reader {
    /// Open an S3 object using the default credential chain and tuning.
    pub fn open(url: &str) -> Result<ThreadedRangedReader, S3Error> {
        Self::builder().build(url)
    }

    /// Configure a reader.
    pub fn builder() -> LiteS3ReaderBuilder {
        LiteS3ReaderBuilder::default()
    }
}
