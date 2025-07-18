use std::sync::Arc;

use clap::Parser;
use paraseq::{fastx, parallel::Result, prelude::*, MAX_ARITY};
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
    pub fn pprint(&self) {
        println!("Total records: {}", self.get_num_records());
        println!("Total bytes: {}", self.get_byte_sum());
    }
}
impl MultiParallelProcessor for SeqSum {
    fn process_record_multi<Rf: Record>(&mut self, records: &[Rf]) -> Result<()> {
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
    fn on_batch_complete(&mut self) -> Result<()> {
        *self.global_byte_sum.lock() += self.byte_sum;
        *self.global_num_records.lock() += self.num_records;
        self.byte_sum = 0;
        self.num_records = 0;
        Ok(())
    }
}

#[derive(Parser)]
pub struct Args {
    /// Input files to process (multiple paired fasta/fastq)
    ///
    /// Must provide at least two files.
    ///
    /// *must all be same format*
    #[clap(num_args = 2..MAX_ARITY, required = true)]
    pub input_fastx: Vec<String>,

    #[clap(short = 'T', long, default_value_t = 1)]
    pub threads: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let rdr_r1 = fastx::Reader::from_path(&args.input_fastx[0])?;
    let remainder = args.input_fastx[1..]
        .iter()
        .map(|path| -> Result<_> {
            let reader = fastx::Reader::from_path(path)?;
            Ok(reader)
        })
        .collect::<Result<Vec<_>>>()?;

    let processor = SeqSum::default();
    rdr_r1.process_parallel_multi(remainder, processor.clone(), args.threads)?;
    processor.pprint();
    Ok(())
}
