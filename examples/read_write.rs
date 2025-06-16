use std::{
    fs::File,
    io::{stdin, stdout, Read, Write},
    sync::Arc,
};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use paraseq::{
    fasta, fastq,
    parallel::{ParallelProcessor, ParallelReader},
};
use parking_lot::Mutex;

pub type BoxedReader = Box<dyn Read + Send>;
pub type BoxedWriter = Box<dyn Write + Send>;

#[derive(Default, Clone, Copy, ValueEnum)]
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
impl ParallelProcessor for Processor {
    fn process_record<Rf: paraseq::Record>(&mut self, record: Rf) -> paraseq::parallel::Result<()> {
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

#[derive(Parser)]
struct Cli {
    /// Input file path
    input_file: Option<String>,

    /// Output file path (stdout if not provided)
    #[clap(short = 'o')]
    output: Option<String>,

    /// Number of threads to use for processing
    #[clap(short = 'T', default_value = "1")]
    num_threads: usize,

    /// Output format
    #[clap(short = 'f', default_value = "fasta")]
    out_format: OutputFormat,

    /// Input format
    #[clap(short = 'F')]
    in_format: Option<OutputFormat>,
}
impl Cli {
    pub fn input_handle(&self) -> Result<BoxedReader> {
        if let Some(path) = &self.input_file {
            let file = File::open(path)?;
            Ok(Box::new(file))
        } else {
            Ok(Box::new(stdin()))
        }
    }
    pub fn input_file_format(&self) -> OutputFormat {
        if let Some(format) = self.in_format {
            format
        } else if let Some(ref path) = self.input_file {
            if path.ends_with(".fq") || path.ends_with(".fastq") {
                OutputFormat::Fastq
            } else {
                OutputFormat::Fasta
            }
        } else {
            OutputFormat::default()
        }
    }
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
    let in_handle = args.input_handle()?;
    let out_handle = args.output_handle()?;

    let proc = Processor::new(out_handle, args.out_format);
    match args.input_file_format() {
        OutputFormat::Fastq => {
            let reader = fastq::Reader::new(in_handle);
            reader.process_parallel(proc, args.num_threads)?;
        }
        OutputFormat::Fasta => {
            let reader = fasta::Reader::new(in_handle);
            reader.process_parallel(proc, args.num_threads)?;
        }
    }

    Ok(())
}
