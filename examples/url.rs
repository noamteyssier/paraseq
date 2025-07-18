use std::sync::Arc;

use anyhow::Result;
use paraseq::{fastx, prelude::*};
use parking_lot::Mutex;

type BoxedWriter = Box<dyn std::io::Write + Send>;

#[derive(Clone)]
pub struct Processor {
    local_buf: Vec<u8>,
    writer: Arc<Mutex<BoxedWriter>>,
}
impl Processor {
    pub fn new() -> Self {
        let writer = Box::new(std::io::stdout());
        Self {
            local_buf: Vec::new(),
            writer: Arc::new(Mutex::new(writer)),
        }
    }
}
impl ParallelProcessor for Processor {
    fn process_record<Rf: Record>(&mut self, record: Rf) -> paraseq::parallel::Result<()> {
        record.write_fastq(&mut self.local_buf)?;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> paraseq::parallel::Result<()> {
        {
            let mut lock = self.writer.lock();
            lock.write_all(&self.local_buf)?;
            lock.flush()?;
        } // drop lock
        self.local_buf.clear();
        Ok(())
    }
}

impl PairedParallelProcessor for Processor {
    fn process_record_pair<Rf: Record>(
        &mut self,
        record1: Rf,
        record2: Rf,
    ) -> paraseq::parallel::Result<()> {
        record1.write_fastq(&mut self.local_buf)?;
        record2.write_fastq(&mut self.local_buf)?;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> paraseq::parallel::Result<()> {
        {
            let mut lock = self.writer.lock();
            lock.write_all(&self.local_buf)?;
            lock.flush()?;
        } // drop lock
        self.local_buf.clear();
        Ok(())
    }
}

fn main() -> Result<()> {
    let num_threads = 4;
    let base_url = "https://github.com/noamteyssier/paraseq/raw/refs/heads/main/data";

    // Single-end examples
    let examples = vec![
        "sample.fasta",
        "sample.fasta.gz",
        "sample.fasta.zst",
        "sample.fastq",
        "sample.fastq.gz",
        "sample.fastq.zst",
    ];

    for example in &examples {
        let url = format!("{}/{}", base_url, example);
        eprintln!("Processing single-end from: {}", url);
        let processor = Processor::new();
        let reader = fastx::Reader::from_url(&url)?;
        reader.process_parallel(processor, num_threads)?;
    }

    // Paired-end example
    let r1_url = format!("{}/r1.fastq", base_url);
    let r2_url = format!("{}/r2.fastq", base_url);
    eprintln!(
        "Processing paired-end example: \n1. {}\n2. {}",
        r1_url, r2_url
    );
    let processor = Processor::new();
    let reader_r1 = fastx::Reader::from_url(&r1_url)?;
    let reader_r2 = fastx::Reader::from_url(&r2_url)?;
    reader_r1.process_parallel_paired(reader_r2, processor, num_threads)?;

    Ok(())
}
