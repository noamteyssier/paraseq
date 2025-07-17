use std::sync::Arc;

use paraseq::{fastx, prelude::*, ProcessError};
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
impl MultiParallelProcessor for SeqSum {
    fn process_record_multi<Rf: Record>(&mut self, records: &[Rf]) -> Result<(), ProcessError> {
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

fn main() -> Result<(), ProcessError> {
    let path_r1 = std::env::args()
        .nth(1)
        .unwrap_or("./data/r1.fastq".to_string());
    let path_r2 = std::env::args()
        .nth(2)
        .unwrap_or("./data/r2.fastq".to_string());
    let path_r3 = std::env::args()
        .nth(3)
        .unwrap_or("./data/r3.fastq".to_string());
    let num_threads = std::env::args()
        .nth(4)
        .unwrap_or("1".to_string())
        .parse::<usize>()
        .unwrap_or(1);

    let processor = SeqSum::default();
    let rdr_r1 = fastx::Reader::from_path(path_r1)?;
    let rem = vec![
        fastx::Reader::from_path(path_r2)?,
        fastx::Reader::from_path(path_r3)?,
    ];
    rdr_r1.process_parallel_multi(rem, processor.clone(), num_threads)?;

    println!("num_records: {}", processor.get_num_records());
    println!("byte_sum: {}", processor.get_byte_sum());

    Ok(())
}
