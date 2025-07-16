use std::fs::File;
use std::sync::Arc;

// use anyhow::Result;
use paraseq::{
    fasta, fastq,
    parallel::{MultiParallelProcessor, MultiParallelReader, ProcessError},
    Record,
};
use parking_lot::Mutex;

#[derive(Default, Clone)]
pub struct SeqSum {
    pub num_records: u64,
    /// Global number of records
    pub global_num_records: Arc<Mutex<u64>>,
}
impl SeqSum {
    #[must_use]
    pub fn get_num_records(&self) -> u64 {
        *self.global_num_records.lock()
    }
}

impl MultiParallelProcessor for SeqSum {
    fn process_record_multi<Rf: Record>(&mut self, records: &[Rf]) -> Result<(), ProcessError> {
        for (i, rec) in records.iter().enumerate() {
            // Simulate some work
            print!("rank {}: {} ", i, String::from_utf8_lossy(rec.id()));
        }
        println!();
        self.num_records += 1;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
        let mut gnr = self.global_num_records.lock();
        *gnr += self.num_records;
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
    let path_r4 = std::env::args()
        .nth(4)
        .unwrap_or("./data/r4.fastq".to_string());

    let num_threads = std::env::args()
        .nth(5)
        .unwrap_or("1".to_string())
        .parse::<usize>()
        .unwrap_or(1);

    let file_r1 = File::open(&path_r1)?;
    let file_r2 = File::open(&path_r2)?;
    let file_r3 = File::open(&path_r3)?;
    let file_r4 = File::open(&path_r4)?;
    let processor = SeqSum::default();

    if path_r1.ends_with(".fastq") {
        let rdr_r1 = fastq::Reader::new(file_r1);
        let mut rem = vec![
            fastq::Reader::new(file_r2),
            fastq::Reader::new(file_r3),
            fastq::Reader::new(file_r4),
        ];
        match rdr_r1.process_parallel_multi(rem.as_mut_slice(), processor.clone(), num_threads) {
            Ok(_) => {}
            Err(e) => {
                println!("{e}");
            }
        }
    } else if path_r1.ends_with(".fasta") {
        let rdr_r1 = fasta::Reader::new(file_r1);
        let mut rem = vec![
            fasta::Reader::new(file_r2),
            fasta::Reader::new(file_r3),
            fasta::Reader::new(file_r4),
        ];
        rdr_r1.process_parallel_multi(rem.as_mut_slice(), processor.clone(), num_threads)?;
    } else {
        panic!("Unknown file format");
    }

    println!("num_records: {}", processor.get_num_records());

    Ok(())
}
