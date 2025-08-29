use std::fs::File;
use std::sync::Arc;

// use anyhow::Result;
use paraseq::{
    fasta, fastq, parallel::{InterleavedPairedReader, PairedParallelProcessor, ProcessError}, prelude::ParallelReader, Record
};
use parking_lot::Mutex;

#[derive(Default, Clone)]
pub struct SeqSum {
    /// Thread local sum of bytes in the sequence
    pub byte_sum: u64,
    /// Thread local number of record pairs
    pub num_pairs: u64,

    /// Global sum of bytes in the sequence
    pub global_byte_sum: Arc<Mutex<u64>>,
    /// Global number of record pairs
    pub global_num_pairs: Arc<Mutex<u64>>,
}
impl SeqSum {
    #[must_use]
    pub fn get_num_pairs(&self) -> u64 {
        *self.global_num_pairs.lock()
    }
    #[must_use]
    pub fn get_byte_sum(&self) -> u64 {
        *self.global_byte_sum.lock()
    }
}
impl<Rf: Record> PairedParallelProcessor<Rf> for SeqSum {
    fn process_record_pair(
        &mut self,
        record1: Rf,
        record2: Rf,
    ) -> Result<(), ProcessError> {
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
        self.num_pairs += 1;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
        *self.global_byte_sum.lock() += self.byte_sum;
        *self.global_num_pairs.lock() += self.num_pairs;
        self.byte_sum = 0;
        self.num_pairs = 0;
        Ok(())
    }
}

fn main() -> Result<(), ProcessError> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or("./data/interleaved.fastq".to_string());

    let num_threads = std::env::args()
        .nth(2)
        .unwrap_or("1".to_string())
        .parse::<usize>()
        .unwrap_or(1);

    let file = File::open(&path)?;
    let processor = SeqSum::default();

    if path.ends_with(".fastq") {
        let reader = InterleavedPairedReader::new( fastq::Reader::new(file));
        reader.process_parallel(processor.clone(), num_threads)?;
    } else if path.ends_with(".fasta") {
        let reader = InterleavedPairedReader::new( fasta::Reader::new(file));
        reader.process_parallel(processor.clone(), num_threads)?;
    } else {
        panic!("Unknown file format {path}");
    }

    println!("num_pairs: {}", processor.get_num_pairs());
    println!("byte_sum: {}", processor.get_byte_sum());

    Ok(())
}
