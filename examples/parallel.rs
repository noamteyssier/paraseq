//! Single-end parallel processing of a FASTA/FASTQ file.
//!
//! ```sh
//! cargo run --release --example parallel -- data/sample.fastq
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

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let reader = fastx::Reader::from_optional_path(args.input)?;
    let mut processor = SeqSum::default();

    reader.process_parallel(&mut processor, args.threads)?;
    processor.report();

    Ok(())
}
