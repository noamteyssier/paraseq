use std::sync::Arc;

use clap::Parser;
use paraseq::{fastx, parallel::{InterleavedMultiReader, MultiParallelProcessor}, prelude::*, ProcessError, Record};
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

impl<Rf: Record> MultiParallelProcessor<Rf> for SeqSum {
    fn process_multi_record(
        &mut self,
        records: &[Rf],
    ) -> Result<(), ProcessError> {
        for _ in 0..100 {
            for rec in records.iter() {
                // Simulate some work
                rec.seq()
                    .iter()
                    .for_each(|b| self.byte_sum += u64::from(*b));
            }
        }
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

#[derive(Parser)]
struct Args {
    /// Input interleaved FASTX to process
    #[clap(required = true)]
    path: String,
    /// Number of records to process in each interleaved batch
    #[clap(short, long, required = true)]
    arity: usize,
    #[clap(short = 'T', long, default_value_t = 4)]
    threads: usize,
}

fn main() -> Result<(), ProcessError> {
    let args = Args::parse();
    let rdr = fastx::Reader::from_path(&args.path)?;
    let processor = SeqSum::default();
    InterleavedMultiReader::new(rdr, args.arity)
    .process_parallel(processor.clone(), args.threads)?;

    println!("num_records: {}", processor.get_num_records());
    println!("byte_sum: {}", processor.get_byte_sum());
    Ok(())
}
