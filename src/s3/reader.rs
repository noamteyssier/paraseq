//! S3 backend for the concurrent ranged reader.
//!
//! Unlike the [`gcs`](crate::gcs) and [`ssh`](crate::ssh) backends, which shell
//! out to a CLI and read one sequential stream, this uses `aws-sdk-s3`
//! directly so that many byte ranges can be fetched concurrently. Using the
//! SDK also means the standard credential chain (environment, profiles, SSO,
//! IMDS, and IRSA / EKS Pod Identity) works without reimplementation.

use std::io;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{config::Builder as S3ConfigBuilder, types::RequestPayer, Client};
use bytes::Bytes;
use thiserror::Error;

use super::range::{shared_runtime, ObjectMeta, RangeConfig, RangeFetcher, RangedObjectReader};

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

/// A [`RangeFetcher`] backed by S3 `GetObject` requests.
pub struct S3Fetcher {
    client: Client,
    url: S3Url,
    request_payer: bool,
}

impl S3Fetcher {
    pub fn new(client: Client, url: S3Url, request_payer: bool) -> Self {
        Self {
            client,
            url,
            request_payer,
        }
    }
}

/// Flattens an SDK error into something with the service message attached.
///
/// The `Display` impl on SDK errors alone is famously unhelpful ("service
/// error"); the source chain carries the actual cause.
fn sdk_error<E: std::error::Error + 'static>(context: &str, err: E) -> io::Error {
    let mut message = format!("{context}: {err}");
    let mut source = err.source();
    while let Some(cause) = source {
        message.push_str(&format!(" -> {cause}"));
        source = cause.source();
    }
    io::Error::other(message)
}

impl RangeFetcher for S3Fetcher {
    async fn open(&self) -> io::Result<ObjectMeta> {
        let mut request = self
            .client
            .head_object()
            .bucket(&self.url.bucket)
            .key(&self.url.key);

        if self.request_payer {
            request = request.request_payer(RequestPayer::Requester);
        }

        let output = request
            .send()
            .await
            .map_err(|e| sdk_error(&format!("HeadObject {}", self.url.s3_uri()), e))?;

        Ok(ObjectMeta {
            content_length: output.content_length().unwrap_or(0).max(0) as u64,
            version_token: output.e_tag().map(str::to_string),
        })
    }

    async fn fetch(&self, meta: ObjectMeta, start: u64, len: usize) -> io::Result<Bytes> {
        debug_assert!(len > 0);
        let end = start + len as u64 - 1;

        let mut request = self
            .client
            .get_object()
            .bucket(&self.url.bucket)
            .key(&self.url.key)
            .range(format!("bytes={start}-{end}"));

        // Pin the version so an object overwritten mid-read fails loudly
        // instead of splicing two files together.
        if let Some(etag) = &meta.version_token {
            request = request.if_match(etag);
        }
        if self.request_payer {
            request = request.request_payer(RequestPayer::Requester);
        }

        let output = request
            .send()
            .await
            .map_err(|e| sdk_error(&format!("GetObject bytes={start}-{end}"), e))?;

        let body = output
            .body
            .collect()
            .await
            .map_err(|e| sdk_error("reading GetObject body", e))?
            .into_bytes();

        if body.len() != len {
            return Err(io::Error::other(format!(
                "short range read: requested {} bytes at offset {}, got {}",
                len,
                start,
                body.len()
            )));
        }

        Ok(body)
    }
}

/// Builder for an S3-backed reader.
///
/// ```no_run
/// # use paraseq::s3::S3Reader;
/// let reader = S3Reader::builder()
///     .region("us-west-2")
///     .part_size(16 * 1024 * 1024)
///     .concurrency(16)
///     .build("s3://my-bucket/reads.fastq.gz")?;
/// # Ok::<(), paraseq::s3::S3Error>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct S3ReaderBuilder {
    region: Option<String>,
    endpoint_url: Option<String>,
    profile: Option<String>,
    force_path_style: bool,
    anonymous: bool,
    request_payer: bool,
    config: RangeConfig,
}

impl S3ReaderBuilder {
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

    /// Use a named profile from the shared AWS config.
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

    /// Bill requests to the requester, for requester-pays buckets.
    pub fn request_payer(mut self, enabled: bool) -> Self {
        self.request_payer = enabled;
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

    /// Build the S3 client this builder describes.
    pub fn build_client(&self) -> Result<Client, S3Error> {
        let rt = shared_runtime().map_err(S3Error::Io)?;

        // The SDK's default HTTPS client pulls in aws-lc-rs, which dominates
        // build time. Wiring the ring-backed client explicitly avoids it.
        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
            ))
            .build_https();

        let client = rt.block_on(async {
            let mut loader =
                aws_config::defaults(BehaviorVersion::latest()).http_client(http_client.clone());

            if let Some(region) = &self.region {
                loader = loader.region(Region::new(region.clone()));
            }
            if let Some(profile) = &self.profile {
                loader = loader.profile_name(profile);
            }
            if self.anonymous {
                loader = loader.no_credentials();
            }

            let shared = loader.load().await;
            let mut builder = S3ConfigBuilder::from(&shared).http_client(http_client);

            // Signing requires *some* region even when the endpoint is not AWS.
            if shared.region().is_none() {
                builder = builder.region(Region::new("us-east-1"));
            }
            if let Some(endpoint) = &self.endpoint_url {
                builder = builder.endpoint_url(endpoint);
            }
            if self.force_path_style {
                builder = builder.force_path_style(true);
            }

            Client::from_conf(builder.build())
        });

        Ok(client)
    }

    /// Open `url` for reading.
    pub fn build(&self, url: &str) -> Result<RangedObjectReader, S3Error> {
        let parsed = S3Url::parse(url)?;
        let client = self.build_client()?;
        let fetcher = S3Fetcher::new(client, parsed, self.request_payer);
        RangedObjectReader::new(fetcher, self.config).map_err(S3Error::Io)
    }
}

/// Entry points for reading S3 objects with concurrent ranged requests.
pub struct S3Reader;

impl S3Reader {
    /// Open an S3 object using the default credential chain and tuning.
    pub fn open(url: &str) -> Result<RangedObjectReader, S3Error> {
        Self::builder().build(url)
    }

    /// Configure a reader.
    pub fn builder() -> S3ReaderBuilder {
        S3ReaderBuilder::default()
    }
}
