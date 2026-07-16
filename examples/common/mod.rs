//! Shared helpers for the example programs in this directory.
//!
//! This module is not an example itself -- see the individual `*.rs` files
//! (and `examples/README.md` for an index) for what each one demonstrates.
//! It lives in a subdirectory so `cargo build --examples` does not try to
//! build it as its own binary.

use std::fs::File;
use std::io::{stdin, stdout, Read, Write};
use std::sync::Arc;

use clap::ValueEnum;
use paraseq::prelude::*;
use paraseq::{ProcessError, Record};
use parking_lot::Mutex;

pub type BoxedReader = Box<dyn Read + Send>;
pub type BoxedWriter = Box<dyn Write + Send>;

/// Opens `path` for reading, or stdin if `None`.
pub fn input_handle(path: &Option<String>) -> std::io::Result<BoxedReader> {
    match path {
        Some(path) => Ok(Box::new(File::open(path)?)),
        None => Ok(Box::new(stdin())),
    }
}

/// Opens `path` for writing (truncating/creating it), or stdout if `None`.
pub fn output_handle(path: &Option<String>) -> std::io::Result<BoxedWriter> {
    match path {
        Some(path) => Ok(Box::new(File::create(path)?)),
        None => Ok(Box::new(stdout())),
    }
}

fn seq_byte_sum(record: &impl Record) -> u64 {
    record.seq().iter().map(|&b| u64::from(b)).sum()
}

/// Counts processed records (or record pairs / multi-record groups) and sums
/// their sequence bytes across threads. Used by the examples that demonstrate
/// single/paired/interleaved/multi processing without needing a custom
/// processor of their own.
#[derive(Clone, Default)]
pub struct SeqSum {
    local_records: u64,
    local_bytes: u64,
    total_records: Arc<Mutex<u64>>,
    total_bytes: Arc<Mutex<u64>>,
}
impl SeqSum {
    #[must_use]
    pub fn num_records(&self) -> u64 {
        *self.total_records.lock()
    }
    #[must_use]
    pub fn byte_sum(&self) -> u64 {
        *self.total_bytes.lock()
    }
    pub fn report(&self) {
        println!("num_records: {}", self.num_records());
        println!("byte_sum: {}", self.byte_sum());
    }
}
impl<Rf: Record> ParallelProcessor<Rf> for SeqSum {
    fn process_record(&mut self, record: Rf) -> Result<(), ProcessError> {
        self.local_records += 1;
        self.local_bytes += seq_byte_sum(&record);
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
        *self.total_records.lock() += self.local_records;
        *self.total_bytes.lock() += self.local_bytes;
        self.local_records = 0;
        self.local_bytes = 0;
        Ok(())
    }
}
impl<Rf: Record> PairedParallelProcessor<Rf> for SeqSum {
    fn process_record_pair(&mut self, r1: Rf, r2: Rf) -> Result<(), ProcessError> {
        self.local_records += 1;
        self.local_bytes += seq_byte_sum(&r1) + seq_byte_sum(&r2);
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
        *self.total_records.lock() += self.local_records;
        *self.total_bytes.lock() += self.local_bytes;
        self.local_records = 0;
        self.local_bytes = 0;
        Ok(())
    }
}
impl<Rf: Record> MultiParallelProcessor<Rf> for SeqSum {
    fn process_multi_record(&mut self, records: &[Rf]) -> Result<(), ProcessError> {
        self.local_records += 1;
        self.local_bytes += records.iter().map(seq_byte_sum).sum::<u64>();
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
        *self.total_records.lock() += self.local_records;
        *self.total_bytes.lock() += self.local_bytes;
        self.local_records = 0;
        self.local_bytes = 0;
        Ok(())
    }
}

/// Output format shared by the examples that convert/write records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Fasta,
    Fastq,
}

/// Writes processed records out as FASTA or FASTQ. Used by the examples that
/// demonstrate reading one container format and writing another.
#[derive(Clone)]
pub struct Writer {
    out_format: OutputFormat,
    local_buf: Vec<u8>,
    writer: Arc<Mutex<BoxedWriter>>,
}
impl Writer {
    pub fn new(writer: BoxedWriter, out_format: OutputFormat) -> Self {
        Self {
            out_format,
            local_buf: Vec::new(),
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    fn write_record(&mut self, record: &impl Record) -> std::io::Result<()> {
        match self.out_format {
            OutputFormat::Fasta => record.write_fasta(&mut self.local_buf),
            OutputFormat::Fastq => record.write_fastq(&mut self.local_buf),
        }
    }

    fn flush_batch(&mut self) -> std::io::Result<()> {
        let mut writer = self.writer.lock();
        writer.write_all(&self.local_buf)?;
        writer.flush()?;
        drop(writer);
        self.local_buf.clear();
        Ok(())
    }
}
impl<Rf: Record> ParallelProcessor<Rf> for Writer {
    fn process_record(&mut self, record: Rf) -> Result<(), ProcessError> {
        self.write_record(&record)?;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
        self.flush_batch()?;
        Ok(())
    }
}
impl<Rf: Record> PairedParallelProcessor<Rf> for Writer {
    fn process_record_pair(&mut self, r1: Rf, r2: Rf) -> Result<(), ProcessError> {
        self.write_record(&r1)?;
        self.write_record(&r2)?;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<(), ProcessError> {
        self.flush_batch()?;
        Ok(())
    }
}
