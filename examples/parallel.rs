use std::fs::File;
use std::sync::Arc;

// use anyhow::Result;
use paraseq::{
    fasta, fastq,
    fastx::Record,
    parallel::{ParallelProcessor, ParallelReader, ProcessError},
};
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
impl ParallelProcessor for SeqSum {
    fn process_record<Rf: Record>(&mut self, record: Rf) -> Result<(), ProcessError> {
        for _ in 0..100 {
            // Simulate some work
            record
                .seq()
                .iter()
                .for_each(|b| self.byte_sum += u64::from(*b));
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

fn main() -> Result<(), ProcessError> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or("./data/example.fastq".to_string());

    let num_threads = std::env::args()
        .nth(2)
        .unwrap_or("1".to_string())
        .parse::<usize>()
        .unwrap_or(1);

    let file = File::open(&path)?;
    let processor = SeqSum::default();

    if path.ends_with(".fastq") {
        let reader = fastq::Reader::new(file);
        reader.process_parallel(processor.clone(), num_threads)?;
    } else if path.ends_with(".fasta") {
        let reader = fasta::Reader::new(file);
        reader.process_parallel(processor.clone(), num_threads)?;
    } else {
        panic!("Unknown file format {path}");
    }

    println!("num_records: {}", processor.get_num_records());
    println!("byte_sum: {}", processor.get_byte_sum());

    Ok(())
}
