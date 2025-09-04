use std::fs::File;
use std::io::{stdout, Read, Write};
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use paraseq::htslib;
use paraseq::prelude::*;
use parking_lot::Mutex;

pub type BoxedReader = Box<dyn Read + Send>;
pub type BoxedWriter = Box<dyn Write + Send>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Fasta,
    Fastq,
}

#[derive(Clone)]
pub struct Processor {
    out_format: OutputFormat,
    local_out: Vec<u8>,
    global_out: Arc<Mutex<BoxedWriter>>,
}
impl Processor {
    pub fn new(writer: BoxedWriter, out_format: OutputFormat) -> Self {
        Self {
            out_format,
            local_out: Vec::new(),
            global_out: Arc::new(Mutex::new(writer)),
        }
    }
}
impl<Rf: paraseq::Record> ParallelProcessor<Rf> for Processor {
    fn process_record(&mut self, record: Rf) -> paraseq::parallel::Result<()> {
        match self.out_format {
            OutputFormat::Fasta => {
                record.write_fasta(&mut self.local_out)?;
            }
            OutputFormat::Fastq => {
                record.write_fastq(&mut self.local_out)?;
            }
        }
        Ok(())
    }

    fn on_batch_complete(&mut self) -> paraseq::parallel::Result<()> {
        {
            let mut global_out = self.global_out.lock();
            global_out.write_all(&self.local_out)?;
            global_out.flush()?;
        } // drops global_out
        self.local_out.clear();
        Ok(())
    }
}
impl<Rf: paraseq::Record> PairedParallelProcessor<Rf> for Processor {
    fn process_record_pair(&mut self, record1: Rf, record2: Rf) -> paraseq::parallel::Result<()> {
        match self.out_format {
            OutputFormat::Fasta => {
                record1.write_fasta(&mut self.local_out)?;
                record2.write_fasta(&mut self.local_out)?;
            }
            OutputFormat::Fastq => {
                record1.write_fastq(&mut self.local_out)?;
                record2.write_fastq(&mut self.local_out)?;
            }
        }
        Ok(())
    }

    fn on_batch_complete(&mut self) -> paraseq::parallel::Result<()> {
        {
            let mut global_out = self.global_out.lock();
            global_out.write_all(&self.local_out)?;
            global_out.flush()?;
        } // drops global_out
        self.local_out.clear();
        Ok(())
    }
}

#[derive(Parser)]
struct Cli {
    /// Input file path
    input_file: Option<String>,
    /// Output file path (stdout if not provided)
    #[clap(short = 'o')]
    output: Option<String>,

    /// Paired-end mode (requires sorted SAM/BAM/CRAM)
    #[clap(short = 'p', long)]
    paired: bool,

    /// Number of threads to use for processing
    #[clap(short = 'T', default_value = "1")]
    num_threads: usize,

    /// Output format
    #[clap(short = 'f', default_value = "fasta")]
    out_format: OutputFormat,
}
impl Cli {
    pub fn output_handle(&self) -> Result<BoxedWriter> {
        if let Some(path) = &self.output {
            let file = File::open(path)?;
            Ok(Box::new(file))
        } else {
            Ok(Box::new(stdout()))
        }
    }
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let handle_out = args.output_handle()?;
    let reader = htslib::Reader::from_optional_path(args.input_file.as_ref())?;
    let mut proc = Processor::new(handle_out, args.out_format);
    if args.paired {
        reader.process_parallel_interleaved(&mut proc, args.num_threads)?;
    } else {
        reader.process_parallel(&mut proc, args.num_threads)?;
    }
    Ok(())
}
