//! Format conversion (FASTA <-> FASTQ) using the auto-detecting `fastx::Reader`,
//! constructed directly from a `Read` handle rather than a path.
//!
//! ```sh
//! cargo run --release --example fastx -- data/sample.fastq --format fasta
//! cat data/sample.fastq | cargo run --release --example fastx -- --format fasta
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::Result;
use clap::Parser;
use common::{input_handle, output_handle, OutputFormat, Writer};
use paraseq::{fastx, prelude::*};

#[derive(Parser)]
struct Cli {
    /// Input file path (reads stdin if omitted)
    input: Option<String>,

    /// Output file path (writes stdout if omitted)
    #[clap(short, long)]
    output: Option<String>,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,

    /// Output format
    #[clap(short, long, default_value = "fasta")]
    format: OutputFormat,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let reader = fastx::Reader::new(input_handle(&args.input)?)?;
    let mut processor = Writer::new(output_handle(&args.output)?, args.format);

    reader.process_parallel(&mut processor, args.threads)?;

    Ok(())
}
