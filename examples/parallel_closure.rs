use std::{fs::File, io, sync::atomic::AtomicU64};

use anyhow::Result;
use clap::Parser;
use paraseq::fastx::RefRecord;
use paraseq::{fastx, prelude::*, ProcessError};

type BoxedReader = Box<dyn io::Read + Send>;

#[derive(Parser)]
struct Cli {
    /// Input file path
    input_file: Option<String>,

    /// Number of threads to use for processing
    #[clap(short = 'T', default_value = "1")]
    num_threads: usize,

    /// Number of records to process in each batch
    #[clap(short = 'B', default_value = "10")]
    batch_size: usize,
}
impl Cli {
    pub fn input_handle(&self) -> Result<BoxedReader> {
        if let Some(path) = &self.input_file {
            let file = File::open(path)?;
            Ok(Box::new(file))
        } else {
            Ok(Box::new(io::stdin()))
        }
    }
}

fn main() -> Result<(), ProcessError> {
    let args = Cli::parse();
    let input_handle = args.input_handle()?;

    // This does the same as examples/parallel.rs,
    // but is implemented using a closure that takes an iterator over the records in a batch.
    // This way, we don't have to explicitly define a struct and all context.
    // Furthermore, we can clearly store the final values as atomics on the stack,
    // and use references to them inside the closure. (Rather than Arc<Mutex<>>.)

    let byte_sum = AtomicU64::new(0);
    let num_records = AtomicU64::new(0);

    let byte_sum_ref = &byte_sum;
    let num_records_ref = &num_records;
    let mut processor = |batch: &mut dyn Iterator<Item = RefRecord>| {
        let mut local_byte_sum = 0u64;
        let mut local_num_records = 0u64;
        for record in batch {
            // Simulate some work
            for _ in 0..100 {
                record
                    .seq()
                    .iter()
                    .for_each(|b| local_byte_sum += u64::from(*b));
            }
            local_num_records += 1;
        }
        // At the end of the batch, we accumulate things into the global counters.
        byte_sum_ref.fetch_add(local_byte_sum, std::sync::atomic::Ordering::Relaxed);
        num_records_ref.fetch_add(local_num_records, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    };

    let reader = fastx::Reader::new_with_batch_size(input_handle, args.batch_size)?;
    // NOTE: If you get lifetime issues, make sure that the reader and the processor use
    // exactly the same `RefRecord` type (and do not e.g. mix fasta and fastq variants).
    reader.process_parallel(&mut processor, args.num_threads)?;

    println!("num_records: {}", num_records.into_inner());
    println!("byte_sum: {}", byte_sum.into_inner());

    Ok(())
}
