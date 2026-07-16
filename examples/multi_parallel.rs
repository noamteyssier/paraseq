//! Synchronized parallel processing of N (2-8) FASTA/FASTQ files.
//!
//! ```sh
//! cargo run --release --example multi_parallel -- data/r1.fastq data/r2.fastq data/r3.fastq
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::Result;
use clap::Parser;
use common::SeqSum;
use paraseq::{fastx, prelude::*, MAX_ARITY};

#[derive(Parser)]
struct Cli {
    /// Input file paths, all the same format (2-8 files)
    #[clap(num_args = 2..MAX_ARITY, required = true)]
    inputs: Vec<String>,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let mut readers = args
        .inputs
        .iter()
        .map(fastx::Reader::from_path)
        .collect::<Result<Vec<_>, _>>()?;
    let first = readers.remove(0);
    let mut processor = SeqSum::default();

    first.process_parallel_multi(readers, &mut processor, args.threads)?;
    processor.report();

    Ok(())
}
