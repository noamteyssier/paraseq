//! Parallel processing of a bounded range of records `[start, end)`.
//!
//! ```sh
//! cargo run --release --example range_parallel -- data/sample.fastq --start 10 --end 30
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::Result;
use clap::Parser;
use common::SeqSum;
use paraseq::{fastx, prelude::*};

#[derive(Parser)]
struct Cli {
    /// Input file path (reads stdin if omitted)
    input: Option<String>,

    /// Starting record index (inclusive)
    #[clap(short, long, default_value_t = 0)]
    start: usize,

    /// Ending record index (exclusive); processes to EOF if omitted
    #[clap(short, long)]
    end: Option<usize>,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let reader = fastx::Reader::from_optional_path(args.input)?;
    let mut processor = SeqSum::default();

    match args.end {
        Some(end) => {
            reader.process_parallel_range(&mut processor, args.threads, args.start..end)?
        }
        None => reader.process_parallel_range(&mut processor, args.threads, args.start..)?,
    }
    processor.report();

    Ok(())
}
