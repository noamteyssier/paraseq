//! Reading FASTX from S3 with concurrent ranged requests.
//!
//! Includes a benchmark mode that separates raw fetch throughput from
//! end-to-end parse throughput, so you can see where the ceiling actually is
//! (for gzipped input, it is usually the single-threaded decompressor rather
//! than the network).
//!
//! ```sh
//! # parse an object from S3
//! cargo run --release --features s3 --example s3 -- s3://bucket/reads.fastq.gz
//!
//! # compare a single sequential GET against the concurrent ranged reader
//! cargo run --release --features s3 --example s3 -- s3://bucket/reads.fastq.gz --bench
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use std::io::Read;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use common::SeqSum;
use paraseq::fastx::Reader;
use paraseq::prelude::*;
use paraseq::s3::{S3Reader, S3Url};

#[derive(Parser)]
struct Cli {
    /// S3 URL of the input (s3://bucket/key)
    url: String,

    /// Number of processing threads (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,

    /// Ranged requests in flight
    #[clap(short = 'c', long, default_value_t = 8)]
    concurrency: usize,

    /// Size of each ranged request, in MiB
    #[clap(short = 'p', long, default_value_t = 8)]
    part_size_mib: usize,

    /// AWS region (defaults to the environment's resolved region)
    #[clap(long)]
    region: Option<String>,

    /// Named AWS profile to use
    #[clap(long)]
    profile: Option<String>,

    /// Send unsigned requests, for public buckets
    #[clap(long)]
    anonymous: bool,

    /// Fetch only the first N bytes and exit (connectivity/TLS check)
    #[clap(long)]
    probe: Option<usize>,

    /// Use a single streaming GetObject instead of concurrent ranged requests
    #[clap(long)]
    streaming: bool,

    /// Benchmark fetch and parse throughput instead of just parsing
    #[clap(long)]
    bench: bool,

    /// Concurrency levels to sweep in benchmark mode
    #[clap(long, value_delimiter = ',', default_values_t = [1usize, 2, 4, 8, 16, 32, 64])]
    sweep: Vec<usize>,

    /// Repetitions per sweep point; the best time is reported
    #[clap(long, default_value_t = 2)]
    repeat: usize,
}

fn mib_per_sec(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let parsed = S3Url::parse(&args.url)?;
    println!("bucket: {}\nkey:    {}", parsed.bucket, parsed.key);

    // The process-wide default would clamp the high end of the sweep below the
    // requested per-reader concurrency, quietly flattening those data points.
    let peak = args
        .sweep
        .iter()
        .copied()
        .chain([args.concurrency])
        .max()
        .unwrap_or(args.concurrency);
    paraseq::s3::set_global_request_limit(peak * 2);

    let builder = {
        let mut b = S3Reader::builder()
            .concurrency(args.concurrency)
            .part_size(args.part_size_mib * 1024 * 1024);
        if let Some(region) = &args.region {
            b = b.region(region.clone());
        }
        if let Some(profile) = &args.profile {
            b = b.profile(profile.clone());
        }
        if args.anonymous {
            b = b.anonymous(true);
        }
        b
    };

    if let Some(n) = args.probe {
        let mut reader = builder.build(&args.url)?;
        let mut buf = vec![0u8; n];
        let read = reader.read(&mut buf)?;
        println!("content_length: {}", reader.content_length());
        println!("read {read} bytes over TLS");
        println!("{}", String::from_utf8_lossy(&buf[..read.min(200)]));
        return Ok(());
    }

    if args.bench {
        return bench(&args, &builder);
    }

    let start = Instant::now();
    let reader = if args.streaming {
        Reader::from_s3_builder_streaming(&builder, &args.url)?
    } else {
        Reader::from_s3_builder(&builder, &args.url)?
    };
    let mut processor = SeqSum::default();
    reader.process_parallel(&mut processor, args.threads)?;

    let elapsed = start.elapsed();
    processor.report();
    println!(
        "mode: {}",
        if args.streaming {
            "streaming (1 connection)".to_string()
        } else {
            format!(
                "ranged (c={}, {} MiB parts)",
                args.concurrency, args.part_size_mib
            )
        }
    );
    println!("elapsed: {:.2}s", elapsed.as_secs_f64());
    Ok(())
}

/// Reads an object to EOF, discarding the bytes, and reports the rate.
fn drain(mut reader: impl Read) -> Result<(u64, Duration)> {
    let mut buf = vec![0u8; 1024 * 1024];
    let mut total = 0u64;
    let start = Instant::now();
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total += n as u64;
    }
    Ok((total, start.elapsed()))
}

/// Best of `repeat` runs, to blunt run-to-run network variance.
fn best_of<F>(repeat: usize, mut run: F) -> Result<(Duration, Duration, u64)>
where
    F: FnMut() -> Result<(u64, Duration)>,
{
    let mut best = Duration::MAX;
    let mut worst = Duration::ZERO;
    let mut bytes = 0;
    for _ in 0..repeat.max(1) {
        let (n, elapsed) = run()?;
        best = best.min(elapsed);
        worst = worst.max(elapsed);
        bytes = n;
    }
    Ok((best, worst, bytes))
}

/// Three-stage breakdown: each stage adds one layer of work, so the stage
/// where throughput stops improving is the actual bottleneck.
fn bench(args: &Cli, builder: &paraseq::s3::S3ReaderBuilder) -> Result<()> {
    let compressed = builder.build(&args.url)?.content_length();
    println!(
        "\nobject: {:.1} MiB compressed, {} repeat(s) per point (best shown)",
        compressed as f64 / (1024.0 * 1024.0),
        args.repeat
    );

    // Stage 1: network only.
    println!("\n== stage 1: fetch only (no decompress, no parse) ==");
    println!(
        "{:>12}  {:>10}  {:>10}  {:>12}",
        "concurrency", "best s", "worst s", "MiB/s"
    );
    for &concurrency in &args.sweep {
        let b = builder.clone().concurrency(concurrency);
        let (best, worst, bytes) = best_of(args.repeat, || {
            let reader = b.build(&args.url)?;
            drain(reader)
        })?;
        println!(
            "{:>12}  {:>10.2}  {:>10.2}  {:>12.1}",
            concurrency,
            best.as_secs_f64(),
            worst.as_secs_f64(),
            mib_per_sec(bytes, best)
        );
    }

    // Stage 2: network + decompression, no parsing. The gap between this and
    // stage 1 is what the decompressor costs.
    println!("\n== stage 2: fetch + decompress (no parse) ==");
    println!(
        "{:>12}  {:>10}  {:>12}  {:>14}",
        "concurrency", "best s", "MiB/s (comp)", "MiB/s (uncomp)"
    );
    let mut uncompressed_size = 0u64;
    for &concurrency in &args.sweep {
        let b = builder.clone().concurrency(concurrency);
        let (best, _, uncompressed) = best_of(args.repeat, || {
            let s3 = b.build(&args.url)?;
            let (reader, _) = niffler::send::get_reader(Box::new(s3))?;
            drain(reader)
        })?;
        uncompressed_size = uncompressed;
        println!(
            "{:>12}  {:>10.2}  {:>12.1}  {:>14.1}",
            concurrency,
            best.as_secs_f64(),
            mib_per_sec(compressed, best),
            mib_per_sec(uncompressed, best)
        );
    }
    println!(
        "  (uncompressed: {:.1} MiB, ratio {:.2}x)",
        uncompressed_size as f64 / (1024.0 * 1024.0),
        uncompressed_size as f64 / compressed as f64
    );

    // Stage 3: the full pipeline. The gap from stage 2 is what parsing costs.
    println!("\n== stage 3: fetch + decompress + parse ==");
    println!(
        "{:>12}  {:>10}  {:>12}  {:>12}  {:>14}",
        "concurrency", "best s", "records", "MiB/s (comp)", "MiB/s (uncomp)"
    );
    for &concurrency in &args.sweep {
        let b = builder.clone().concurrency(concurrency);
        let mut records = 0;
        let (best, _, _) = best_of(args.repeat, || {
            let start = Instant::now();
            let reader = Reader::from_s3_builder(&b, &args.url)?;
            let mut processor = SeqSum::default();
            reader.process_parallel(&mut processor, args.threads)?;
            records = processor.num_records();
            Ok((compressed, start.elapsed()))
        })?;
        println!(
            "{:>12}  {:>10.2}  {:>12}  {:>12.1}  {:>14.1}",
            concurrency,
            best.as_secs_f64(),
            records,
            mib_per_sec(compressed, best),
            mib_per_sec(uncompressed_size, best)
        );
    }

    Ok(())
}
