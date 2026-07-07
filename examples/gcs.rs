//! Read one or two FASTA/FASTQ files from Google Cloud Storage (requires the
//! `gcs` feature and an authenticated `gcloud` CLI).
//!
//! ```sh
//! cargo run --release --example gcs -- gs://bucket/sample.fastq
//! cargo run --release --example gcs -- gs://bucket/r1.fastq gs://bucket/r2.fastq
//! ```

#[path = "common/mod.rs"]
#[allow(dead_code)]
mod common;

use anyhow::{bail, Result};
use clap::Parser;
use common::{OutputFormat, Writer};
use paraseq::{fastx, prelude::*};

#[derive(Parser)]
struct Cli {
    /// One GCS url for single-end, or two for paired-end
    #[clap(required = true, num_args = 1..=2)]
    urls: Vec<String>,

    /// Number of threads to use (0 = all available cores)
    #[clap(short = 'T', long, default_value_t = 4)]
    threads: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let mut processor = Writer::new(Box::new(std::io::stdout()), OutputFormat::Fastq);

    match args.urls.as_slice() {
        [url] => {
            let reader = fastx::Reader::from_gcs(url)?;
            reader.process_parallel(&mut processor, args.threads)?;
        }
        [url1, url2] => {
            let r1 = fastx::Reader::from_gcs(url1)?;
            let r2 = fastx::Reader::from_gcs(url2)?;
            r1.process_parallel_paired(r2, &mut processor, args.threads)?;
        }
        _ => bail!("expected 1 or 2 GCS urls"),
    }

    Ok(())
}
