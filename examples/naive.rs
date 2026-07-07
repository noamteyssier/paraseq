//! Naive sequential (single-threaded) record counting, using the
//! format-specific `fasta`/`fastq` readers side by side with the
//! auto-detecting `fastx` reader.
//!
//! ```sh
//! cargo run --release --example naive -- data/sample.fastq
//! ```

use anyhow::Result;
use clap::Parser;
use paraseq::{fasta, fastq, fastx};

#[derive(Parser)]
struct Cli {
    /// Input file path
    input: String,
}

fn naive_fastq(path: &str) -> Result<()> {
    let mut reader = fastq::Reader::from_path(path)?;
    let mut rset = fastq::RecordSet::default();

    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let _record = record?;
            num_records += 1;
        }
    }
    eprintln!("Number of records (fastq): {num_records}");

    Ok(())
}

fn naive_fasta(path: &str) -> Result<()> {
    let mut reader = fasta::Reader::from_path(path)?;
    let mut rset = fasta::RecordSet::default();

    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let _record = record?;
            num_records += 1;
        }
    }
    eprintln!("Number of records (fasta): {num_records}");

    Ok(())
}

fn naive_fastx(path: &str) -> Result<()> {
    let mut reader = fastx::Reader::from_path(path)?;
    let mut rset = reader.new_record_set();

    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let _record = record?;
            num_records += 1;
        }
    }
    eprintln!("Number of records (fastx): {num_records}");

    Ok(())
}

fn main() -> Result<()> {
    let args = Cli::parse();

    if args.input.contains(".fastq") || args.input.contains(".fq") {
        naive_fastq(&args.input)?;
    } else if args.input.contains(".fasta") || args.input.contains(".fa") {
        naive_fasta(&args.input)?;
    } else {
        eprintln!("Unknown file format for {}", args.input);
    }

    // The auto-detecting reader works regardless of extension.
    naive_fastx(&args.input)?;

    Ok(())
}
