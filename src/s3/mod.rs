//! Reading FASTX from S3 with concurrent ranged requests.
//!
//! A single sequential `GET` is capped by one connection's throughput, which
//! becomes the bottleneck long before the parser does. This module fetches an
//! object as a sliding window of concurrent byte ranges, reassembled in order,
//! and presents the result as a plain [`std::io::Read`].
//!
//! [`range`] holds the store-agnostic machinery; the S3 specifics live behind
//! [`S3Reader`] and its builder.

pub mod range;
mod reader;

pub use range::{
    set_global_request_limit, set_runtime_threads, ObjectMeta, RangeConfig, RangeFetcher,
    RangedObjectReader, DEFAULT_CONCURRENCY, DEFAULT_PART_SIZE, DEFAULT_QUEUE_DEPTH,
};
pub use reader::{S3Error, S3Fetcher, S3Reader, S3ReaderBuilder, S3Url};

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{self, BufRead, BufReader, Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    use bytes::Bytes;
    use parking_lot::Mutex;

    use super::*;
    use crate::fastx::Reader;
    use crate::prelude::*;
    use crate::ProcessError;

    // ---- generic ranged reader ----

    /// In-memory backend that records how many ranges were requested.
    struct VecFetcher {
        data: Arc<Vec<u8>>,
        requests: Arc<AtomicUsize>,
    }

    impl RangeFetcher for VecFetcher {
        async fn open(&self) -> io::Result<ObjectMeta> {
            Ok(ObjectMeta {
                content_length: self.data.len() as u64,
                version_token: Some("etag".to_string()),
            })
        }

        async fn fetch(&self, _meta: ObjectMeta, start: u64, len: usize) -> io::Result<Bytes> {
            self.requests.fetch_add(1, Ordering::Relaxed);
            let start = start as usize;
            Ok(Bytes::copy_from_slice(&self.data[start..start + len]))
        }
    }

    fn read_all(data: Vec<u8>, config: RangeConfig) -> (Vec<u8>, usize) {
        let requests = Arc::new(AtomicUsize::new(0));
        let fetcher = VecFetcher {
            data: Arc::new(data),
            requests: requests.clone(),
        };
        let mut reader = RangedObjectReader::new(fetcher, config).unwrap();
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        (out, requests.load(Ordering::Relaxed))
    }

    #[test]
    fn test_reassembles_in_order() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let config = RangeConfig {
            part_size: 1024,
            concurrency: 8,
            queue_depth: 2,
        };
        let (out, requests) = read_all(data.clone(), config);
        assert_eq!(out, data);
        assert_eq!(requests, data.len().div_ceil(1024));
    }

    #[test]
    fn test_part_size_larger_than_object() {
        let data: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
        let (out, requests) = read_all(data.clone(), RangeConfig::default());
        assert_eq!(out, data);
        assert_eq!(requests, 1);
    }

    #[test]
    fn test_exact_part_multiple() {
        let data = vec![7u8; 4096];
        let config = RangeConfig {
            part_size: 1024,
            concurrency: 4,
            queue_depth: 1,
        };
        let (out, requests) = read_all(data.clone(), config);
        assert_eq!(out, data);
        assert_eq!(requests, 4);
    }

    #[test]
    fn test_empty_object() {
        let (out, requests) = read_all(Vec::new(), RangeConfig::default());
        assert!(out.is_empty());
        assert_eq!(requests, 0);
    }

    #[test]
    fn test_single_concurrency() {
        let data: Vec<u8> = (0..50_000u32).map(|i| (i % 97) as u8).collect();
        let config = RangeConfig {
            part_size: 512,
            concurrency: 1,
            queue_depth: 1,
        };
        let (out, _) = read_all(data.clone(), config);
        assert_eq!(out, data);
    }

    /// A backend that fails partway through, to check the error surfaces
    /// through `Read` rather than being swallowed as a short read.
    struct FailingFetcher {
        fail_at: u64,
    }

    impl RangeFetcher for FailingFetcher {
        async fn open(&self) -> io::Result<ObjectMeta> {
            Ok(ObjectMeta {
                content_length: 10_000,
                version_token: None,
            })
        }

        async fn fetch(&self, _meta: ObjectMeta, start: u64, len: usize) -> io::Result<Bytes> {
            if start >= self.fail_at {
                return Err(io::Error::other("synthetic fetch failure"));
            }
            Ok(Bytes::from(vec![0u8; len]))
        }
    }

    #[test]
    fn test_fetch_error_propagates() {
        let config = RangeConfig {
            part_size: 1000,
            concurrency: 2,
            queue_depth: 1,
        };
        let mut reader = RangedObjectReader::new(FailingFetcher { fail_at: 5000 }, config).unwrap();
        let mut out = Vec::new();
        let err = reader.read_to_end(&mut out).unwrap_err();
        assert!(err.to_string().contains("synthetic fetch failure"));
    }

    #[test]
    fn test_early_drop_does_not_block() {
        let data = vec![1u8; 10 * 1024 * 1024];
        let config = RangeConfig {
            part_size: 64 * 1024,
            concurrency: 8,
            queue_depth: 2,
        };
        let fetcher = VecFetcher {
            data: Arc::new(data),
            requests: Arc::new(AtomicUsize::new(0)),
        };
        let mut reader = RangedObjectReader::new(fetcher, config).unwrap();
        let mut buf = [0u8; 128];
        reader.read_exact(&mut buf).unwrap();
        // Dropping with parts still in flight must return promptly.
        drop(reader);
    }

    // ---- S3 backend ----

    #[test]
    fn test_s3_url_parsing() {
        let url = S3Url::parse("s3://my-bucket/path/to/file.fastq").unwrap();
        assert_eq!(url.bucket, "my-bucket");
        assert_eq!(url.key, "path/to/file.fastq");
        assert_eq!(url.s3_uri(), "s3://my-bucket/path/to/file.fastq");

        let url = S3Url::parse("s3://bucket-name/deep/nested/file.fasta.gz").unwrap();
        assert_eq!(url.bucket, "bucket-name");
        assert_eq!(url.key, "deep/nested/file.fasta.gz");
    }

    #[test]
    fn test_s3_url_scheme_aliases() {
        for url in ["s3a://bucket/key.fastq", "s3n://bucket/key.fastq"] {
            let parsed = S3Url::parse(url).unwrap();
            assert_eq!(parsed.bucket, "bucket");
            assert_eq!(parsed.key, "key.fastq");
        }
    }

    #[test]
    fn test_s3_url_rejects_invalid() {
        assert!(S3Url::parse("gs://bucket/object").is_err());
        assert!(S3Url::parse("s3://").is_err());
        assert!(S3Url::parse("s3://bucket").is_err());
        assert!(S3Url::parse("s3://bucket/").is_err());
        assert!(S3Url::parse("bucket/object").is_err());
    }

    #[test]
    fn test_key_with_special_characters() {
        let url = S3Url::parse("s3://bucket/sample 1/read+2.fastq").unwrap();
        assert_eq!(url.key, "sample 1/read+2.fastq");
    }

    /// A minimal S3-compatible server backed by in-memory objects.
    struct TestServer {
        addr: SocketAddr,
        /// Number of ranged GETs served, to verify the window actually fans out.
        range_gets: Arc<AtomicUsize>,
        /// Number of ranged GETs that carried an `If-Match` version pin.
        if_match_gets: Arc<AtomicUsize>,
    }

    impl TestServer {
        fn endpoint(&self) -> String {
            format!("http://{}", self.addr)
        }
    }

    fn etag_for(data: &[u8]) -> String {
        // Any stable token works; the reader only round-trips it.
        let mut hash = 0xcbf29ce484222325u64;
        for &byte in data {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        format!("\"{hash:016x}\"")
    }

    fn spawn_server(objects: HashMap<String, Vec<u8>>) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let objects = Arc::new(objects);
        let range_gets = Arc::new(AtomicUsize::new(0));
        let if_match_gets = Arc::new(AtomicUsize::new(0));

        let thread_objects = objects.clone();
        let thread_counter = range_gets.clone();
        let thread_if_match = if_match_gets.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let objects = thread_objects.clone();
                let counter = thread_counter.clone();
                let if_match = thread_if_match.clone();
                thread::spawn(move || {
                    let _ = handle_connection(stream, &objects, &counter, &if_match);
                });
            }
        });

        TestServer {
            addr,
            range_gets,
            if_match_gets,
        }
    }

    fn handle_connection(
        stream: TcpStream,
        objects: &HashMap<String, Vec<u8>>,
        range_gets: &AtomicUsize,
        if_match_gets: &AtomicUsize,
    ) -> std::io::Result<()> {
        let mut writer = stream.try_clone()?;
        let mut reader = BufReader::new(stream);

        // The SDK reuses connections, so serve requests until the peer hangs up.
        loop {
            let mut request_line = String::new();
            if reader.read_line(&mut request_line)? == 0 {
                return Ok(());
            }
            if request_line.trim().is_empty() {
                continue;
            }

            let mut parts = request_line.split_whitespace();
            let method = parts.next().unwrap_or("").to_string();
            let target = parts.next().unwrap_or("/").to_string();

            let mut headers = HashMap::new();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line)? == 0 {
                    return Ok(());
                }
                let line = line.trim_end();
                if line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
                }
            }

            // Strip the query string; path-style addressing puts bucket/key in the path.
            let path = target
                .split('?')
                .next()
                .unwrap_or("/")
                .trim_start_matches('/');
            let key = percent_decode(path);

            let Some(data) = objects.get(&key) else {
                write_response(
                    &mut writer,
                    404,
                    &[],
                    b"<Error><Code>NoSuchKey</Code></Error>",
                )?;
                continue;
            };

            let etag = etag_for(data);
            let total = data.len();

            // Version pinning: a mismatched If-Match must fail the request.
            if let Some(if_match) = headers.get("if-match") {
                if if_match != &etag {
                    write_response(&mut writer, 412, &[], b"")?;
                    continue;
                }
            }

            if method == "HEAD" {
                let extra = [
                    ("Content-Length".to_string(), total.to_string()),
                    ("ETag".to_string(), etag.clone()),
                    ("Accept-Ranges".to_string(), "bytes".to_string()),
                ];
                write_head_response(&mut writer, 200, &extra)?;
                continue;
            }

            match headers.get("range").and_then(|r| parse_range(r, total)) {
                Some((start, end)) => {
                    range_gets.fetch_add(1, Ordering::Relaxed);
                    if headers.contains_key("if-match") {
                        if_match_gets.fetch_add(1, Ordering::Relaxed);
                    }
                    let body = &data[start..=end];
                    let extra = [
                        (
                            "Content-Range".to_string(),
                            format!("bytes {start}-{end}/{total}"),
                        ),
                        ("ETag".to_string(), etag.clone()),
                        ("Accept-Ranges".to_string(), "bytes".to_string()),
                    ];
                    write_response(&mut writer, 206, &extra, body)?;
                }
                None => {
                    let extra = [
                        ("ETag".to_string(), etag.clone()),
                        ("Accept-Ranges".to_string(), "bytes".to_string()),
                    ];
                    write_response(&mut writer, 200, &extra, data)?;
                }
            }
        }
    }

    fn percent_decode(input: &str) -> String {
        let bytes = input.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                if let Ok(byte) = u8::from_str_radix(&input[i + 1..i + 3], 16) {
                    out.push(byte);
                    i += 3;
                    continue;
                }
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    /// Parses `bytes=start-end` into inclusive bounds clamped to the object.
    fn parse_range(header: &str, total: usize) -> Option<(usize, usize)> {
        let spec = header.trim().strip_prefix("bytes=")?;
        let (start, end) = spec.split_once('-')?;
        let start: usize = start.trim().parse().ok()?;
        let end: usize = match end.trim() {
            "" => total.saturating_sub(1),
            value => value.parse().ok()?,
        };
        if start >= total {
            return None;
        }
        Some((start, end.min(total.saturating_sub(1))))
    }

    fn status_text(status: u16) -> &'static str {
        match status {
            200 => "OK",
            206 => "Partial Content",
            404 => "Not Found",
            412 => "Precondition Failed",
            _ => "Unknown",
        }
    }

    fn write_head_response(
        writer: &mut impl Write,
        status: u16,
        extra: &[(String, String)],
    ) -> std::io::Result<()> {
        let mut response = format!("HTTP/1.1 {status} {}\r\n", status_text(status));
        for (name, value) in extra {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        writer.write_all(response.as_bytes())?;
        writer.flush()
    }

    fn write_response(
        writer: &mut impl Write,
        status: u16,
        extra: &[(String, String)],
        body: &[u8],
    ) -> std::io::Result<()> {
        let mut response = format!("HTTP/1.1 {status} {}\r\n", status_text(status));
        response.push_str(&format!("Content-Length: {}\r\n", body.len()));
        for (name, value) in extra {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        writer.write_all(response.as_bytes())?;
        writer.write_all(body)?;
        writer.flush()
    }

    /// Counts records and total sequence bytes across threads.
    #[derive(Clone, Default)]
    struct Counter {
        local_records: usize,
        local_bases: usize,
        total_records: Arc<Mutex<usize>>,
        total_bases: Arc<Mutex<usize>>,
    }

    impl<Rf: Record> ParallelProcessor<Rf> for Counter {
        fn process_record(&mut self, record: Rf) -> Result<(), ProcessError> {
            self.local_records += 1;
            self.local_bases += record.seq().len();
            Ok(())
        }
        fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
            *self.total_records.lock() += self.local_records;
            *self.total_bases.lock() += self.local_bases;
            self.local_records = 0;
            self.local_bases = 0;
            Ok(())
        }
    }

    fn make_fastq(n_records: usize, read_len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        for i in 0..n_records {
            let seq: String = (0..read_len)
                .map(|j| match (i + j) % 4 {
                    0 => 'A',
                    1 => 'C',
                    2 => 'G',
                    _ => 'T',
                })
                .collect();
            out.extend_from_slice(format!("@read_{i} description here\n").as_bytes());
            out.extend_from_slice(seq.as_bytes());
            out.extend_from_slice(b"\n+\n");
            out.extend_from_slice("I".repeat(read_len).as_bytes());
            out.push(b'\n');
        }
        out
    }

    fn builder_for(server: &TestServer) -> S3ReaderBuilder {
        S3Reader::builder()
            .endpoint_url(server.endpoint())
            .force_path_style(true)
            .anonymous(true)
            .region("us-east-1")
    }

    #[test]
    fn test_ranged_read_matches_source_bytes() {
        let data = make_fastq(5_000, 150);
        let objects = HashMap::from([("bucket/reads.fastq".to_string(), data.clone())]);
        let server = spawn_server(objects);

        let part_size = 64 * 1024;
        let mut reader = builder_for(&server)
            .part_size(part_size)
            .concurrency(8)
            .build("s3://bucket/reads.fastq")
            .expect("build reader");

        assert_eq!(reader.content_length(), data.len() as u64);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).expect("read");
        assert_eq!(out, data, "reassembled bytes must match the source exactly");

        let expected_parts = data.len().div_ceil(part_size);
        assert_eq!(
            server.range_gets.load(Ordering::Relaxed),
            expected_parts,
            "each part should be fetched exactly once"
        );

        // Every part must be pinned to the version resolved at open, so an object
        // overwritten mid-read fails instead of silently splicing two files.
        assert_eq!(
            server.if_match_gets.load(Ordering::Relaxed),
            expected_parts,
            "every ranged GET should carry an If-Match version pin"
        );
    }

    #[test]
    fn test_parses_fastq_end_to_end() {
        let n_records = 5_000;
        let read_len = 150;
        let data = make_fastq(n_records, read_len);
        let objects = HashMap::from([("bucket/reads.fastq".to_string(), data)]);
        let server = spawn_server(objects);

        let builder = builder_for(&server).part_size(32 * 1024).concurrency(4);
        let reader = Reader::from_s3_builder(&builder, "s3://bucket/reads.fastq").expect("open");

        let mut counter = Counter::default();
        reader.process_parallel(&mut counter, 4).expect("process");

        assert_eq!(*counter.total_records.lock(), n_records);
        assert_eq!(*counter.total_bases.lock(), n_records * read_len);
    }

    #[test]
    fn test_concurrency_does_not_change_result() {
        let data = make_fastq(2_000, 100);
        let objects = HashMap::from([("bucket/reads.fastq".to_string(), data.clone())]);
        let server = spawn_server(objects);

        // Part boundaries must not affect the parse, regardless of window size.
        for (concurrency, part_size) in [(1, 1024), (2, 4096), (8, 997), (16, 65_536)] {
            let mut reader = builder_for(&server)
                .part_size(part_size)
                .concurrency(concurrency)
                .build("s3://bucket/reads.fastq")
                .expect("build reader");

            let mut out = Vec::new();
            reader.read_to_end(&mut out).expect("read");
            assert_eq!(
                out, data,
                "mismatch at concurrency={concurrency} part_size={part_size}"
            );
        }
    }

    #[test]
    fn test_missing_object_errors_on_open() {
        let server = spawn_server(HashMap::new());
        let err = builder_for(&server)
            .build("s3://bucket/absent.fastq")
            .expect_err("missing object must fail");
        // The failure must surface at open, not as a silent empty stream.
        assert!(err.to_string().contains("HeadObject"), "got: {err}");
    }

    #[test]
    fn test_early_drop_is_prompt() {
        let data = make_fastq(50_000, 150);
        let objects = HashMap::from([("bucket/reads.fastq".to_string(), data)]);
        let server = spawn_server(objects);

        let mut reader = builder_for(&server)
            .part_size(16 * 1024)
            .concurrency(8)
            .build("s3://bucket/reads.fastq")
            .expect("build reader");

        let mut buf = [0u8; 64];
        reader.read_exact(&mut buf).expect("read");

        let start = std::time::Instant::now();
        drop(reader);
        assert!(
            start.elapsed().as_secs() < 5,
            "dropping with parts in flight must not block"
        );
    }

    /// Throughput of the SDK request path over loopback, with no TLS and no disk.
    ///
    /// Used to check that lowering the AWS crates' `opt-level` in the release
    /// profile does not cost runtime performance. Ignored by default because it
    /// allocates a large in-memory object.
    #[test]
    #[ignore]
    fn bench_loopback_throughput() {
        let size = 256 * 1024 * 1024;
        let data = vec![b'A'; size];
        let objects = HashMap::from([("bucket/big.bin".to_string(), data)]);
        let server = spawn_server(objects);

        let mut best = f64::MAX;
        for _ in 0..3 {
            let mut reader = builder_for(&server)
                .part_size(8 * 1024 * 1024)
                .concurrency(8)
                .build("s3://bucket/big.bin")
                .expect("build reader");

            let start = std::time::Instant::now();
            let mut buf = vec![0u8; 1024 * 1024];
            let mut total = 0usize;
            loop {
                let n = reader.read(&mut buf).expect("read");
                if n == 0 {
                    break;
                }
                total += n;
            }
            assert_eq!(total, size);
            best = best.min(start.elapsed().as_secs_f64());
        }
        println!(
            "loopback throughput: {:.0} MiB/s (best of 3)",
            size as f64 / best / (1024.0 * 1024.0)
        );
    }
}
