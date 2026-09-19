//! Repeatedly parses a FASTQ file to keep the `u8x64` SIMD newline scan
//! (`RecordSet::scan_for_records_simd`) hot, for profiling with `samply`.
//!
//! ```sh
//! cargo build --release --example simd_scan
//! samply record ./target/release/examples/simd_scan data/sample.fastq
//! samply record ./target/release/examples/simd_scan data/sample.fastq --seconds 20
//! ```

use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use paraseq::fastq::RecordSet;
use paraseq::Record;

#[derive(Parser)]
struct Cli {
    /// Input FASTQ file path
    input: String,

    /// Keep re-parsing the file for at least this many seconds
    #[clap(short, long, default_value_t = 10.0)]
    seconds: f64,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let budget = Duration::from_secs_f64(args.seconds);

    let mut total_records = 0u64;
    let mut total_bases = 0u64;
    let mut passes = 0u64;

    let start = Instant::now();
    while start.elapsed() < budget {
        let mut reader = paraseq::ReaderBuilder::path(&args.input).build_fastq()?;
        let mut rset = RecordSet::default();
        while rset.fill(&mut reader)? {
            for record in rset.iter() {
                let record = record?;
                total_bases += record.seq_raw().len() as u64;
                total_records += 1;
            }
        }
        passes += 1;
    }
    let elapsed = start.elapsed();

    let mb_per_pass = std::fs::metadata(&args.input)?.len() as f64 / (1024.0 * 1024.0);
    let gb_per_s = (mb_per_pass * passes as f64 / 1024.0) / elapsed.as_secs_f64();
    eprintln!(
        "{passes} passes, {total_records} records, {total_bases} bases in {elapsed:?} ({gb_per_s:.2} GB/s)"
    );

    Ok(())
}
