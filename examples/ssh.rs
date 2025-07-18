use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
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
    let processor = Processor::new();
    match args.url.len() {
        1 => {
            let reader = fastx::Reader::from_ssh(&args.url[0])?;
            reader.process_parallel(processor, args.num_threads)?;
        }
        2 => {
            let reader_r1 = fastx::Reader::from_ssh(&args.url[0])?;
            let reader_r2 = fastx::Reader::from_ssh(&args.url[1])?;
            reader_r1.process_parallel_paired(reader_r2, processor, args.num_threads)?;
        }
        _ => {
            eprintln!("Invalid number of URLs (expected 1 or 2)");
            std::process::exit(1);
        }
    };

    Ok(())
}
