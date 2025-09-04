use std::sync::Arc;

use anyhow::{bail, Result};
use clap::Parser;
use paraseq::{fastx, prelude::*};
use parking_lot::Mutex;

type BoxedWriter = Box<dyn std::io::Write + Send>;

#[derive(Clone)]
pub struct Processor {
    local_buf: Vec<u8>,
    writer: Arc<Mutex<BoxedWriter>>,
}
impl Default for Processor {
    fn default() -> Self {
        Self::new()
    }
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
impl<Rf: Record> ParallelProcessor<Rf> for Processor {
    fn process_record(&mut self, record: Rf) -> paraseq::parallel::Result<()> {
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

impl<Rf: Record> PairedParallelProcessor<Rf> for Processor {
    fn process_record_pair(&mut self, record1: Rf, record2: Rf) -> paraseq::parallel::Result<()> {
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

#[derive(Parser)]
struct Args {
    /// SSH url to a FASTX file
    #[clap(required = true, num_args=1..=2)]
    url: Vec<String>,

    /// Number of threads to use
    #[clap(short, long, default_value_t = 4)]
    num_threads: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.url.len() {
        1 => {
            let url = &args.url[0];
            let mut processor = Processor::new();
            let reader = fastx::Reader::from_gcs(url)?;
            reader.process_parallel(&mut processor, args.num_threads)?;
            Ok(())
        }
        2 => {
            let url1 = &args.url[0];
            let url2 = &args.url[1];
            let mut processor = Processor::new();
            let r1 = fastx::Reader::from_gcs(url1)?;
            let r2 = fastx::Reader::from_gcs(url2)?;
            r1.process_parallel_paired(r2, &mut processor, args.num_threads)?;
            Ok(())
        }
        _ => {
            bail!("Invalid number of URLs")
        }
    }
}
