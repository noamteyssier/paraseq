use std::fs::File;
use std::sync::Arc;

use anyhow::{bail, Result};
use paraseq::{
    fasta, fastq,
    parallel::{ParallelProcessor, ParallelReader},
};
use parking_lot::Mutex;

#[derive(Default, Clone)]
pub struct SeqSum {
    // thread local sum of nucleotides
    local_sum: u64,

    // thread local number of records
    local_num: u64,

    // global sum of nucleotides
    sum: Arc<Mutex<u64>>,

    // global number of records
    num: Arc<Mutex<u64>>,
}
impl SeqSum {
    #[must_use]
    pub fn get_sum(&self) -> u64 {
        *self.sum.lock()
    }
    #[must_use]
    pub fn get_num(&self) -> u64 {
        *self.num.lock()
    }
}
impl ParallelProcessor for SeqSum {
    fn process_record<Rf: paraseq::Record>(&mut self, record: Rf) -> paraseq::parallel::Result<()> {
        record
            .seq()
            .iter()
            .for_each(|b| self.local_sum += u64::from(*b));
        self.local_num += 1;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> paraseq::parallel::Result<()> {
        *self.sum.lock() += self.local_sum;
        *self.num.lock() += self.local_num;
        self.local_sum = 0;
        self.local_num = 0;
        Ok(())
    }
}

fn reload_fastq(path: &str, n_threads: usize) -> Result<()> {
    let file = File::open(&path)?;
    let mut reader = fastq::Reader::new(file);
    let mut rset = fastq::RecordSet::new(1);

    if !rset.fill(&mut reader)? {
        bail!("No sequences in input")
    }
    let mut num_prefill = 0;
    for record in rset.iter() {
        record?;
        num_prefill += 1;
    }
    eprintln!("read {num_prefill} records in prefill");

    // Reload the reader
    reader.reload(&mut rset);

    // Parallel process the reader
    let proc = SeqSum::default();
    reader.process_parallel(proc.clone(), n_threads)?;

    eprintln!("num_records: {}", proc.get_num());
    eprintln!("sum: {}", proc.get_sum());

    Ok(())
}

fn reload_fasta(path: &str, n_threads: usize) -> Result<()> {
    let file = File::open(&path)?;
    let mut reader = fasta::Reader::new(file);
    let mut rset = fasta::RecordSet::new(1);

    if !rset.fill(&mut reader)? {
        bail!("No sequences in input")
    }
    let mut num_prefill = 0;
    for record in rset.iter() {
        record?;
        num_prefill += 1;
    }
    eprintln!("read {num_prefill} records in prefill");

    // Reload the reader
    reader.reload(&mut rset);

    // Parallel process the reader
    let proc = SeqSum::default();
    reader.process_parallel(proc.clone(), n_threads)?;

    eprintln!("num_records: {}", proc.get_num());
    eprintln!("sum: {}", proc.get_sum());

    Ok(())
}

fn main() -> Result<()> {
    let path = std::env::args().nth(1).unwrap();
    let threads = std::env::args().nth(2).unwrap().parse::<usize>()?;
    if path.ends_with(".fasta") {
        reload_fasta(&path, threads)
    } else {
        reload_fastq(&path, threads)
    }
}
