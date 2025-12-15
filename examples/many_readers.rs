use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use paraseq::{fastx, prelude::ParallelProcessor, Record};
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

#[derive(Parser)]
struct Args {
    #[clap(required = true, num_args = 1..)]
    input: Vec<String>,

    /// Number of threads to use, 0=auto
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let reader = fastx::ManyReader::from_paths(&args.input, fastx::CollectionType::Single)?;
    let mut proc = Processor::default();
    reader.process_parallel(&mut proc, args.threads)?;

    let total_reads = proc.total_reads.lock().clone();
    println!("Total reads: {}", total_reads);

    Ok(())
}
