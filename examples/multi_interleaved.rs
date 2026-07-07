//! Parallel processing of a single file interleaved with an arbitrary arity
//! (e.g. `--arity 3` for R1,R2,R3,R1,R2,R3,...).
//!
//! ```sh
//! cargo run --release --example multi_interleaved -- data/r123.fastq --arity 3
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

    /// Number of records interleaved together per group
    #[clap(short, long, default_value_t = 2)]
    arity: usize,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let reader = fastx::Reader::from_optional_path(args.input)?;
    let mut processor = SeqSum::default();

    reader.process_parallel_multi_interleaved(args.arity, &mut processor, args.threads)?;
    processor.report();

    Ok(())
}
