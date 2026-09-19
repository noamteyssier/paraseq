//! Naive sequential (single-threaded) record counting with the
//! auto-detecting `fastx` reader.
//!
//! ```sh
//! cargo run --release --example naive -- data/sample.fastq
//! ```

use anyhow::Result;
use clap::Parser;
use paraseq::ReaderBuilder;

#[derive(Parser)]
struct Cli {
    /// Input file path (reads stdin if omitted)
    input: Option<String>,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let mut reader = ReaderBuilder::optional_path(args.input).build()?;
    let mut rset = reader.new_record_set();
    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let _record = record?;
            num_records += 1;
        }
    }
    println!("num_records: {num_records}");
    Ok(())
}
