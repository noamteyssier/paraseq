use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use paraseq::{
    fastx::{self, CollectionType},
    prelude::{PairedParallelProcessor, ParallelProcessor},
    Record,
};
use parking_lot::Mutex;

#[derive(Clone, Default)]
struct Processor {
    total_reads: Arc<Mutex<usize>>,
}
impl<R: Record> ParallelProcessor<R> for Processor {
    fn process_record(&mut self, _record: R) -> paraseq::Result<()> {
        *self.total_reads.lock() += 1;
        Ok(())
    }
}
impl<R: Record> PairedParallelProcessor<R> for Processor {
    fn process_record_pair(&mut self, _record1: R, _record2: R) -> paraseq::Result<()> {
        *self.total_reads.lock() += 1;
        Ok(())
    }
}

#[derive(Parser)]
struct Args {
    #[clap(required = true, num_args = 1..)]
    input: Vec<String>,

    /// All incoming files are paired-end
    ///
    /// Note: If paired-end, the file pairs are assumed to be interleaved.
    /// Ex: R1, R2, R1, R2, ...
    #[clap(long)]
    paired: bool,

    /// Number of threads to use; 0 sets to maximum available
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,

    /// Number of threads per reader
    #[clap(short = 'R', long)]
    reader_threads: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let collection_type = if args.paired {
        CollectionType::Paired
    } else {
        CollectionType::Single
    };
    let reader = fastx::Collection::from_paths(&args.input, collection_type)?;
    let mut proc = Processor::default();
    if args.paired {
        eprintln!("Processing paired-end reads...");
        reader.process_parallel_paired(&mut proc, args.threads, args.reader_threads)?;
    } else {
        eprintln!("Processing single-end reads...");
        reader.process_parallel(&mut proc, args.threads, args.reader_threads)?;
    }

    let total_reads = proc.total_reads.lock().clone();
    println!("Total reads: {}", total_reads);

    Ok(())
}
