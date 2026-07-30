//! Generic concurrent ranged-object reader.
//!
//! A single sequential `GET` against an object store is capped by the
//! throughput of one connection (typically ~80-100 MB/s against S3), which
//! becomes the bottleneck long before the parser does. This module turns one
//! logical stream into a sliding window of concurrent ranged requests that are
//! reassembled in order, so the object store can be driven at many times the
//! single-connection rate while still presenting a plain [`io::Read`].
//!
//! The store-specific part is confined to the [`RangeFetcher`] trait: resolve
//! the object's size once, then fetch arbitrary byte ranges. Everything else
//! (windowing, ordering, backpressure, cancellation) lives here and is shared
//! across backends.

use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, BufRead, Read};
use std::sync::{Arc, OnceLock};

use bytes::{Buf, Bytes};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;

/// Default size of an individual ranged request.
///
/// Below ~8 MiB per-request latency starts to dominate; above ~32 MiB a single
/// slow part stalls in-order delivery for longer than it saves.
pub const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;

/// Default number of ranged requests in flight for a single reader.
pub const DEFAULT_CONCURRENCY: usize = 8;

/// Default number of completed-but-unread parts held between the fetch tasks
/// and the reader. This is the backpressure knob: a slow consumer stalls the
/// window instead of buffering the whole object.
pub const DEFAULT_QUEUE_DEPTH: usize = 2;

/// Runtime shared by every reader in the process.
///
/// A `Collection` of twenty remote files should not spin up twenty runtimes.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Process-wide cap on concurrently in-flight requests across *all* readers.
static GLOBAL_PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// Worker threads for the shared runtime, if overridden.
static RUNTIME_THREADS: OnceLock<usize> = OnceLock::new();

/// Sets the worker-thread count for the shared runtime.
///
/// These threads do TLS decryption as well as IO, so on a fast link (inside a
/// cloud region) the default can become the ceiling rather than the network.
/// Only takes effect if called before the first reader is constructed;
/// returns `false` if the runtime was already built.
pub fn set_runtime_threads(threads: usize) -> bool {
    RUNTIME_THREADS.set(threads.max(1)).is_ok() && RUNTIME.get().is_none()
}

/// Returns the shared tokio runtime, building it on first use.
pub(crate) fn shared_runtime() -> io::Result<&'static Runtime> {
    if let Some(rt) = RUNTIME.get() {
        return Ok(rt);
    }
    let threads = RUNTIME_THREADS
        .get()
        .copied()
        .unwrap_or_else(|| num_cpus::get().clamp(2, 8));
    let rt = Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name("paraseq-range")
        .enable_all()
        .build()?;
    // A losing racer's runtime is simply dropped here, which is harmless
    // because nothing has been spawned onto it yet.
    Ok(RUNTIME.get_or_init(|| rt))
}

/// Sets the process-wide limit on concurrent range requests.
///
/// Only takes effect if called before the first reader is constructed;
/// returns `false` if the limit was already established.
pub fn set_global_request_limit(limit: usize) -> bool {
    GLOBAL_PERMITS
        .set(Arc::new(Semaphore::new(limit.max(1))))
        .is_ok()
}

fn global_permits() -> Arc<Semaphore> {
    GLOBAL_PERMITS
        .get_or_init(|| Arc::new(Semaphore::new((num_cpus::get() * 4).max(16))))
        .clone()
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

/// A backend capable of serving byte ranges of a single object.
pub trait RangeFetcher: Send + Sync + 'static {
    /// Resolves the object's size and version token.
    ///
    /// Called once, before any ranges are requested. This is also where
    /// authentication and existence errors surface.
    fn open(&self) -> impl Future<Output = io::Result<ObjectMeta>> + Send;

    /// Fetches exactly `len` bytes starting at `start`.
    ///
    /// Implementations should pass `meta.version_token` through to the store
    /// so a concurrent overwrite is detected rather than tolerated.
    fn fetch(
        &self,
        meta: ObjectMeta,
        start: u64,
        len: usize,
    ) -> impl Future<Output = io::Result<Bytes>> + Send;
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
}

/// An [`io::Read`] over an object fetched via concurrent ranged requests.
///
/// Parts complete out of order but are delivered strictly in order, so this is
/// a drop-in replacement for a sequential `GET` stream — including in front of
/// a decompressor.
pub struct RangedObjectReader {
    rx: mpsc::Receiver<io::Result<Bytes>>,
    current: Bytes,
    driver: JoinHandle<()>,
    meta: ObjectMeta,
}

impl RangedObjectReader {
    /// Opens `fetcher`'s object and starts prefetching.
    ///
    /// Blocks until the object's metadata has been resolved, so authentication
    /// and not-found errors are reported here rather than on first read.
    pub fn new<F: RangeFetcher>(fetcher: F, config: RangeConfig) -> io::Result<Self> {
        let rt = shared_runtime()?;
        let fetcher = Arc::new(fetcher);
        let meta = rt.block_on(fetcher.open())?;

        let (tx, rx) = mpsc::channel(config.queue_depth.max(1));
        let driver = rt.spawn(drive(fetcher, meta.clone(), config, tx));

        Ok(Self {
            rx,
            current: Bytes::new(),
            driver,
            meta,
        })
    }

    /// Total size of the object in bytes.
    pub fn content_length(&self) -> u64 {
        self.meta.content_length
    }
}

/// Keeps `config.concurrency` parts in flight and forwards them in order.
async fn drive<F: RangeFetcher>(
    fetcher: Arc<F>,
    meta: ObjectMeta,
    config: RangeConfig,
    tx: mpsc::Sender<io::Result<Bytes>>,
) {
    let total = meta.content_length;
    let part_size = config.part_size.max(1) as u64;
    let n_parts = total.div_ceil(part_size);
    let concurrency = config.concurrency.max(1);

    let permits = global_permits();
    let handle = Handle::current();

    // Completed parts are held in their JoinHandles until the reader is ready
    // for them, which is what bounds memory to `concurrency + queue_depth`.
    let mut in_flight: VecDeque<JoinHandle<io::Result<Bytes>>> = VecDeque::new();
    let mut next_part = 0u64;

    loop {
        while in_flight.len() < concurrency && next_part < n_parts {
            let start = next_part * part_size;
            let len = part_size.min(total - start) as usize;

            let fetcher = fetcher.clone();
            let meta = meta.clone();
            let permits = permits.clone();

            in_flight.push_back(handle.spawn(async move {
                let _permit = permits
                    .acquire()
                    .await
                    .map_err(|_| io::Error::other("range request semaphore closed"))?;
                fetcher.fetch(meta, start, len).await
            }));

            next_part += 1;
        }

        let Some(next) = in_flight.pop_front() else {
            break;
        };

        let result = match next.await {
            Ok(result) => result,
            // The task was cancelled (reader dropped) or panicked.
            Err(e) if e.is_cancelled() => break,
            Err(e) => Err(io::Error::other(format!("range fetch task failed: {e}"))),
        };

        let failed = result.is_err();
        // A send error means the reader was dropped; stop fetching.
        if tx.send(result).await.is_err() || failed {
            break;
        }
    }

    for handle in in_flight {
        handle.abort();
    }
}

impl std::fmt::Debug for RangedObjectReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangedObjectReader")
            .field("content_length", &self.meta.content_length)
            .field("version_token", &self.meta.version_token)
            .field("buffered", &self.current.len())
            .finish()
    }
}

impl Read for RangedObjectReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for RangedObjectReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        // Loop rather than branch so a zero-length part cannot be mistaken
        // for EOF.
        while self.current.is_empty() {
            match self.rx.blocking_recv() {
                Some(Ok(bytes)) => self.current = bytes,
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        Ok(&self.current)
    }

    fn consume(&mut self, amt: usize) {
        self.current.advance(amt.min(self.current.len()));
    }
}

impl Drop for RangedObjectReader {
    fn drop(&mut self) {
        // Cancel rather than join: an early stop (record limit, range, error)
        // must not block on in-flight requests draining.
        self.driver.abort();
        self.rx.close();
    }
}
