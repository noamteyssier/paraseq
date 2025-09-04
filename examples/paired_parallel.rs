use std::fs::File;
use std::sync::Arc;

use paraseq::{fasta, fastq, prelude::*, ProcessError};
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
impl<Rf: Record> PairedParallelProcessor<Rf> for SeqSum {
    fn process_record_pair(&mut self, record1: Rf, record2: Rf) -> Result<(), ProcessError> {
        for _ in 0..100 {
            // Simulate some work
            record1
                .seq()
                .iter()
                .for_each(|b| self.byte_sum += u64::from(*b));
            record2
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
    let path_r1 = std::env::args()
        .nth(1)
        .unwrap_or("./data/sample_R1.fastq".to_string());
    let path_r2 = std::env::args()
        .nth(2)
        .unwrap_or("./data/sample_R2.fastq".to_string());

    let num_threads = std::env::args()
        .nth(3)
        .unwrap_or("1".to_string())
        .parse::<usize>()
        .unwrap_or(1);

    let file_r1 = File::open(&path_r1)?;
    let file_r2 = File::open(&path_r2)?;
    let mut processor = SeqSum::default();

    if path_r1.ends_with(".fastq") {
        let r1 = fastq::Reader::new(file_r1);
        let r2 = fastq::Reader::new(file_r2);
        r1.process_parallel_paired(r2, &mut processor, num_threads)?;
    } else if path_r1.ends_with(".fasta") {
        let r1 = fasta::Reader::new(file_r1);
        let r2 = fasta::Reader::new(file_r2);
        r1.process_parallel_paired(r2, &mut processor, num_threads)?;
    } else {
        panic!("Unknown file format");
    }

    println!("num_records: {}", processor.get_num_records());
    println!("byte_sum: {}", processor.get_byte_sum());

    Ok(())
}
