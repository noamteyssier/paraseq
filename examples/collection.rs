//! Parallel processing over a `Collection` of many readers at once (single or
//! paired), useful when you have more input files than you want threads.
//!
//! ```sh
//! cargo run --release --example collection -- data/sample.fastq data/multiline.fasta
//! cargo run --release --example collection -- data/r1.fastq data/r2.fastq --paired
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::Result;
use clap::Parser;
use common::SeqSum;
use paraseq::fastx::{Collection, CollectionType};

#[derive(Parser)]
struct Cli {
    /// Input file paths
    #[clap(required = true, num_args = 1..)]
    inputs: Vec<String>,

    /// All incoming files are paired-end
    ///
    /// Note: If paired-end, the file pairs are assumed to be interleaved.
    /// Ex: R1, R2, R1, R2, ...
    #[clap(long)]
    paired: bool,

    /// Total number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,

    /// Number of threads per reader (defaults to threads / num_readers)
    #[clap(short = 'R', long)]
    reader_threads: Option<usize>,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let collection_type = if args.paired {
        CollectionType::Paired
    } else {
        CollectionType::Single
    };
    let collection = Collection::from_paths(&args.inputs, collection_type)?;
    let mut processor = SeqSum::default();

    if args.paired {
        collection.process_parallel_paired(&mut processor, args.threads, args.reader_threads)?;
    } else {
        collection.process_parallel(&mut processor, args.threads, args.reader_threads)?;
    }
    processor.report();

    Ok(())
}
