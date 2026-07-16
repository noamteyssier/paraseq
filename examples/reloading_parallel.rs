//! Peek at the first few records of a file, then reload the reader so a
//! subsequent parallel pass sees every record (including the peeked ones).
//!
//! ```sh
//! cargo run --release --example reloading_parallel -- data/sample.fastq --prefill 3
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::{bail, Result};
use clap::Parser;
use common::SeqSum;
use paraseq::{fasta, fastq, fastx, prelude::*};

#[derive(Parser)]
struct Cli {
    /// Input file path
    input: String,

    /// Number of records to prefill (peek at) before reloading
    #[clap(short, long, default_value_t = 3)]
    prefill: usize,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 0)]
    threads: usize,
}

fn reload_fastq(path: &str, prefill: usize, threads: usize) -> Result<()> {
    let mut reader = fastq::Reader::from_path(path)?;
    let mut rset = reader.new_record_set_with_size(prefill);
    if !rset.fill(&mut reader)? {
        bail!("No sequences in input")
    }
    eprintln!(
        "(fastq) prefilled {} records",
        rset.iter().collect::<Result<Vec<_>, _>>()?.len()
    );

    reader.reload(&mut rset);

    let mut processor = SeqSum::default();
    reader.process_parallel(&mut processor, threads)?;
    eprintln!("(fastq) num_records: {}", processor.num_records());
    eprintln!("(fastq) byte_sum: {}", processor.byte_sum());
    Ok(())
}

fn reload_fasta(path: &str, prefill: usize, threads: usize) -> Result<()> {
    let mut reader = fasta::Reader::from_path(path)?;
    let mut rset = reader.new_record_set_with_size(prefill);
    if !rset.fill(&mut reader)? {
        bail!("No sequences in input")
    }
    eprintln!(
        "(fasta) prefilled {} records",
        rset.iter().collect::<Result<Vec<_>, _>>()?.len()
    );

    reader.reload(&mut rset);

    let mut processor = SeqSum::default();
    reader.process_parallel(&mut processor, threads)?;
    eprintln!("(fasta) num_records: {}", processor.num_records());
    eprintln!("(fasta) byte_sum: {}", processor.byte_sum());
    Ok(())
}

fn reload_fastx(path: &str, prefill: usize, threads: usize) -> Result<()> {
    let mut reader = fastx::Reader::from_path(path)?;
    let mut rset = reader.new_record_set_with_size(prefill);
    if !rset.fill(&mut reader)? {
        bail!("No sequences in input")
    }
    eprintln!(
        "(fastx) prefilled {} records",
        rset.iter().collect::<Result<Vec<_>, _>>()?.len()
    );

    reader.reload(&mut rset)?;

    let mut processor = SeqSum::default();
    reader.process_parallel(&mut processor, threads)?;
    eprintln!("(fastx) num_records: {}", processor.num_records());
    eprintln!("(fastx) byte_sum: {}", processor.byte_sum());
    Ok(())
}

fn main() -> Result<()> {
    let args = Cli::parse();

    // Format-specific readers, side by side with the auto-detecting `fastx` reader.
    if args.input.ends_with(".fasta") {
        reload_fasta(&args.input, args.prefill, args.threads)?;
    } else {
        reload_fastq(&args.input, args.prefill, args.threads)?;
    }
    reload_fastx(&args.input, args.prefill, args.threads)?;

    Ok(())
}
