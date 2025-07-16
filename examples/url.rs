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

fn main() -> Result<()> {
    let url = "https://github.com/noamteyssier/paraseq/raw/refs/heads/main/data/sample.fasta";
    let processor = Processor::new();
    let reader = fastx::Reader::from_url(url)?;
    reader.process_parallel(processor, 4)?;
    Ok(())
}
