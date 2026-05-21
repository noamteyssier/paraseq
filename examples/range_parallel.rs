use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use paraseq::{fastx, parallel::ParallelProcessor, prelude::ParallelReader, ProcessError, Record};
use parking_lot::Mutex;

#[derive(Default, Clone)]
pub struct SeqSum {
    /// Thread local sum of bytes in the sequence
    pub byte_sum: u64,
    /// Thread local number of records
    pub num_records: u64,

    /// Global sum of bytes in the sequence
    pub global_byte_sum: Arc<Mutex<u64>>,
    /// Global number of records
    pub global_num_records: Arc<Mutex<u64>>,
}

impl SeqSum {
    #[must_use]
    pub fn get_num_records(&self) -> u64 {
        *self.global_num_records.lock()
    }
    #[must_use]
    pub fn get_byte_sum(&self) -> u64 {
        *self.global_byte_sum.lock()
    }
}

impl<Rf: Record> ParallelProcessor<Rf> for SeqSum {
    fn process_record(&mut self, record: Rf) -> Result<(), ProcessError> {
        record
            .seq()
            .iter()
            .for_each(|b| self.byte_sum += u64::from(*b));
        self.num_records += 1;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
        *self.global_byte_sum.lock() += self.byte_sum;
        *self.global_num_records.lock() += self.num_records;
        self.byte_sum = 0;
        self.num_records = 0;
        Ok(())
    }
}

#[derive(clap::Parser)]
struct Cli {
    /// Input file path
    input_file: Option<String>,
    /// Output file path (stdout if not provided)
    #[clap(short = 'o')]
    output: Option<String>,
    /// Starting record to process
    #[clap(short, long, default_value_t = 0)]
    start: usize,
    /// End record to process
    #[clap(short, long)]
    end: Option<usize>,
    /// Number of threads to use for processing
    #[clap(short = 'T', default_value = "0")]
    num_threads: usize,
}

fn main() -> Result<(), ProcessError> {
    let args = Cli::parse();
    let reader = fastx::Reader::from_optional_path(args.input_file)?;
    let mut processor = SeqSum::default();

    // Process a range of records
    match args.end {
        Some(end) => {
            println!(
                "Processing records {}..{} with {} threads",
                args.start, end, args.num_threads
            );
            reader.process_parallel_range(&mut processor, args.num_threads, args.start..end)?;
        }
        None => {
            println!(
                "Processing records {}.. with {} threads",
                args.start, args.num_threads
            );
            reader.process_parallel_range(&mut processor, args.num_threads, args.start..)?;
        }
    }

    println!("num_records: {}", processor.get_num_records());
    println!("byte_sum: {}", processor.get_byte_sum());

    Ok(())
}
