//! Peek at the first few records of a file, then reload the reader so a
//! subsequent parallel pass sees every record (including the peeked ones).
//!
//! ```sh
//! cargo run --release --example reloading_parallel -- data/sample.fastq --prefill 3
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::{bail, Result};
use clap::Parser;
use common::SeqSum;
use paraseq::{prelude::*, ReaderBuilder};

#[derive(Parser)]
struct Cli {
    /// Input file path (reads stdin if omitted)
    input: Option<String>,

    /// Number of records to prefill (peek at) before reloading
    #[clap(short, long, default_value_t = 3)]
    prefill: usize,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let mut reader = ReaderBuilder::optional_path(args.input).build()?;
    let mut rset = reader.new_record_set_with_size(args.prefill);
    if !rset.fill(&mut reader)? {
        bail!("No records in input")
    }
    eprintln!("prefilled {} records", rset.iter().count());
    reader.reload(&mut rset)?;

    let mut processor = SeqSum::default();
    reader.process_parallel(&mut processor, args.threads)?;
    processor.report();
    Ok(())
}
