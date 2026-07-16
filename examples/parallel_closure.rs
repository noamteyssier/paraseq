//! The same thing as `examples/parallel.rs`, but implemented with a closure
//! over an iterator instead of a `ParallelProcessor` struct. Useful when you
//! want to avoid defining a struct and accumulate results in plain atomics
//! rather than `Arc<Mutex<_>>`.
//!
//! ```sh
//! cargo run --release --example parallel_closure -- data/sample.fastq
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use clap::Parser;
use common::input_handle;
use paraseq::fastx::RefRecord;
use paraseq::{fastx, prelude::*};

#[derive(Parser)]
struct Cli {
    /// Input file path (reads stdin if omitted)
    input: Option<String>,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,

    /// Number of records to process in each batch
    #[clap(short, long, default_value_t = 1024)]
    batch_size: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    let byte_sum = AtomicU64::new(0);
    let num_records = AtomicU64::new(0);
    let byte_sum_ref = &byte_sum;
    let num_records_ref = &num_records;

    // NOTE: if you hit lifetime issues, make sure the reader and the closure
    // agree on the same `RefRecord` type (don't mix fasta and fastq variants).
    let mut processor = |batch: &mut dyn Iterator<Item = RefRecord>| {
        let mut local_bytes = 0u64;
        let mut local_records = 0u64;
        for record in batch {
            local_bytes += record.seq().iter().map(|&b| u64::from(b)).sum::<u64>();
            local_records += 1;
        }
        byte_sum_ref.fetch_add(local_bytes, Ordering::Relaxed);
        num_records_ref.fetch_add(local_records, Ordering::Relaxed);
        Ok(())
    };

    let reader = fastx::Reader::new_with_batch_size(input_handle(&args.input)?, args.batch_size)?;
    reader.process_parallel(&mut processor, args.threads)?;

    println!("num_records: {}", num_records.into_inner());
    println!("byte_sum: {}", byte_sum.into_inner());

    Ok(())
}
