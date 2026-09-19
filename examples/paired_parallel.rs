//! Paired-end parallel processing of two FASTA/FASTQ files.
//!
//! ```sh
//! cargo run --release --example paired_parallel -- data/r1.fastq data/r2.fastq
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::Result;
use clap::Parser;
use common::SeqSum;
use paraseq::{prelude::*, ReaderBuilder};

#[derive(Parser)]
struct Cli {
    /// First input file path (R1)
    input1: String,

    /// Second input file path (R2)
    input2: String,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let r1 = ReaderBuilder::path(&args.input1).build()?;
    let r2 = ReaderBuilder::path(&args.input2).build()?;
    let mut processor = SeqSum::default();

    r1.process_parallel_paired(r2, &mut processor, args.threads)?;
    processor.report();

    Ok(())
}
