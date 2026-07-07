//! Convert a SAM/BAM/CRAM file to FASTA/FASTQ (requires the `htslib` feature).
//!
//! ```sh
//! cargo run --release --example htslib -- data/sample.bam --format fastq
//! cargo run --release --example htslib -- data/paired.bam --format fastq --paired
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::Result;
use clap::Parser;
use common::{output_handle, OutputFormat, Writer};
use paraseq::htslib;
use paraseq::prelude::*;

#[derive(Parser)]
struct Cli {
    /// Input file path (reads stdin if omitted)
    input: Option<String>,

    /// Output file path (writes stdout if omitted)
    #[clap(short, long)]
    output: Option<String>,

    /// Paired-end mode (input must be name-interleaved SAM/BAM/CRAM)
    #[clap(short, long)]
    paired: bool,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,

    /// Output format
    #[clap(short, long, default_value = "fasta")]
    format: OutputFormat,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let reader = htslib::Reader::from_optional_path(args.input.as_ref())?;
    let mut processor = Writer::new(output_handle(&args.output)?, args.format);

    if args.paired {
        reader.process_parallel_interleaved(&mut processor, args.threads)?;
    } else {
        reader.process_parallel(&mut processor, args.threads)?;
    }

    Ok(())
}
