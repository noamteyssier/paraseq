//! Demonstrates opt-in output ordering for parallel processing via
//! `paraseq::parallel::Ordered`.
//!
//! Worker threads normally call `on_batch_complete` (and so flush their
//! output) as soon as their own batch finishes, regardless of where that
//! batch sits in the input stream - so with enough threads and uneven
//! per-batch work, output order can end up scrambled relative to the
//! input. Wrapping a processor in `Ordered` serializes just the
//! `on_batch_complete` step to match input order; `process_record`/
//! `process_record_batch` still run fully in parallel.
//!
//! ```sh
//! cargo run --release --example ordered_parallel -- data/sample.fastq --ordered
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::Result;
use clap::Parser;
use common::{input_handle, output_handle, OutputFormat, Writer};
use paraseq::parallel::Ordered;
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

    /// Preserve input record order in the output. Costs some throughput
    /// under high thread contention combined with uneven batch timing.
    #[clap(long)]
    ordered: bool,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    let writer = Writer::new(output_handle(&args.output)?, args.format);
    if args.ordered {
        let reader = fastx::Reader::new(input_handle(&args.input)?)?;
        let mut processor = Ordered(writer);
        reader.process_parallel(&mut processor, args.threads)?;
    } else {
        let reader = fastx::Reader::new(input_handle(&args.input)?)?;
        let mut processor = writer;
        reader.process_parallel(&mut processor, args.threads)?;
    }

    Ok(())
}
