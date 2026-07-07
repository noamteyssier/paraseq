//! Read FASTA/FASTQ files directly from HTTP(S) URLs (requires the `url` feature).
//!
//! ```sh
//! cargo run --release --example url
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::Result;
use clap::Parser;
use common::{OutputFormat, Writer};
use paraseq::{fastx, prelude::*};

#[derive(Parser)]
struct Cli {
    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 4)]
    threads: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let base_url = "https://github.com/noamteyssier/paraseq/raw/refs/heads/main/data";

    // Single-end, one format/compression combination per iteration.
    let single_end = [
        "sample.fasta",
        "sample.fasta.gz",
        "sample.fasta.zst",
        "sample.fastq",
        "sample.fastq.gz",
        "sample.fastq.zst",
    ];
    for name in single_end {
        let url = format!("{base_url}/{name}");
        eprintln!("Processing single-end from: {url}");
        let mut processor = Writer::new(Box::new(std::io::stdout()), OutputFormat::Fastq);
        let reader = fastx::Reader::from_url(&url)?;
        reader.process_parallel(&mut processor, args.threads)?;
    }

    // Paired-end.
    let r1_url = format!("{base_url}/r1.fastq");
    let r2_url = format!("{base_url}/r2.fastq");
    eprintln!("Processing paired-end from:\n1. {r1_url}\n2. {r2_url}");
    let mut processor = Writer::new(Box::new(std::io::stdout()), OutputFormat::Fastq);
    let r1 = fastx::Reader::from_url(&r1_url)?;
    let r2 = fastx::Reader::from_url(&r2_url)?;
    r1.process_parallel_paired(r2, &mut processor, args.threads)?;

    Ok(())
}
