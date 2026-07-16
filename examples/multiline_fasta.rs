//! Demonstrates multiline FASTA parsing: sequences spanning several lines are
//! transparently concatenated by `Record::seq()`.
//!
//! ```sh
//! cargo run --release --example multiline_fasta -- data/multiline.fasta
//! ```

use clap::Parser;
use paraseq::fasta::{Reader, RecordSet};
use paraseq::prelude::*;

#[derive(Parser)]
struct Cli {
    /// Input file path
    #[clap(default_value = "./data/multiline.fasta")]
    input: String,
}

fn main() -> Result<(), paraseq::Error> {
    let args = Cli::parse();

    let mut reader = Reader::from_path(&args.input)?;
    let mut record_set = RecordSet::new(1024);
    let mut record_count = 0;

    while record_set.fill(&mut reader)? {
        for record in record_set.iter() {
            let record = record?;
            record_count += 1;

            println!("Record {}: {}", record_count, record.id_str());
            println!("  Sequence length: {} bp", record.seq().len());

            // `seq()` transparently strips newlines from multiline records,
            // returning a borrowed slice for single-line ones and an owned,
            // concatenated buffer for multiline ones.
            assert!(!record.seq().contains(&b'\n'));
        }
    }

    println!("Successfully processed {record_count} FASTA records");
    Ok(())
}
