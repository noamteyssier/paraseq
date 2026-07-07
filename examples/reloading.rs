//! Peek at the first few records of a file, then reload the reader so a
//! subsequent sequential pass sees every record (including the peeked ones).
//!
//! ```sh
//! cargo run --release --example reloading -- data/sample.fastq --prefill 3
//! ```

use anyhow::{bail, Result};
use clap::Parser;
use paraseq::{fasta, fastq, fastx};

#[derive(Parser)]
struct Cli {
    /// Input file path
    input: String,

    /// Number of records to prefill (peek at) before reloading
    #[clap(short, long, default_value_t = 3)]
    prefill: usize,
}

fn reload_fasta(path: &str, prefill: usize) -> Result<()> {
    let mut reader = fasta::Reader::from_path(path)?;
    let mut rset = reader.new_record_set_with_size(prefill);
    if !rset.fill(&mut reader)? {
        bail!("No records in input file")
    }
    eprintln!(
        "(fasta) prefilled {} records",
        rset.iter().collect::<Result<Vec<_>, _>>()?.len()
    );

    reader.reload(&mut rset);

    let mut rset = reader.new_record_set();
    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        num_records += rset.iter().collect::<Result<Vec<_>, _>>()?.len();
    }
    eprintln!("(fasta) num_records: {num_records}");
    Ok(())
}

fn reload_fastq(path: &str, prefill: usize) -> Result<()> {
    let mut reader = fastq::Reader::from_path(path)?;
    let mut rset = reader.new_record_set_with_size(prefill);
    if !rset.fill(&mut reader)? {
        bail!("No records in input file")
    }
    eprintln!(
        "(fastq) prefilled {} records",
        rset.iter().collect::<Result<Vec<_>, _>>()?.len()
    );

    reader.reload(&mut rset);

    let mut rset = reader.new_record_set_with_size(prefill);
    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        num_records += rset.iter().collect::<Result<Vec<_>, _>>()?.len();
    }
    eprintln!("(fastq) num_records: {num_records}");
    Ok(())
}

fn reload_fastx(path: &str, prefill: usize) -> Result<()> {
    let mut reader = fastx::Reader::from_path(path)?;
    let mut rset = reader.new_record_set_with_size(prefill);
    if !rset.fill(&mut reader)? {
        bail!("No records in input file")
    }
    eprintln!(
        "(fastx) prefilled {} records",
        rset.iter().collect::<Result<Vec<_>, _>>()?.len()
    );

    reader.reload(&mut rset)?;

    let mut rset = reader.new_record_set();
    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        num_records += rset.iter().collect::<Result<Vec<_>, _>>()?.len();
    }
    eprintln!("(fastx) num_records: {num_records}");
    Ok(())
}

fn main() -> Result<()> {
    let args = Cli::parse();

    // Format-specific readers, side by side with the auto-detecting `fastx` reader.
    if args.input.ends_with(".fastq") {
        reload_fastq(&args.input, args.prefill)?;
    } else if args.input.ends_with(".fasta") {
        reload_fasta(&args.input, args.prefill)?;
    } else {
        eprintln!("Unknown file format for {}", args.input);
    }
    reload_fastx(&args.input, args.prefill)?;

    Ok(())
}
