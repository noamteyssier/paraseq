use std::fs::File;

use anyhow::{bail, Result};
use paraseq::{fasta, fastq};

fn reload_fasta(path: &str, prefill: usize) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = fasta::Reader::new(file);
    let mut rset = fasta::RecordSet::new(prefill);

    // Fill the record set with records from the reader
    if !rset.fill(&mut reader)? {
        bail!("No records in input file")
    };

    let mut num_prefill = 0;
    for record in rset.iter() {
        let _record = record?;
        num_prefill += 1;
    }
    eprintln!("num_prefill: {}", num_prefill);

    // Reload the reader with the record set
    eprintln!("Reloading reader with {} records", num_prefill);
    reader.reload(&mut rset);

    // Process the records in the record set
    let mut rset = fasta::RecordSet::default();
    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let _record = record?;
            num_records += 1;
        }
    }

    eprintln!("num_records: {}", num_records);
    Ok(())
}

fn reload_fastq(path: &str, prefill: usize) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = fastq::Reader::new(file);
    let mut rset = fastq::RecordSet::new(prefill);

    // Fill the record set with records from the reader
    if !rset.fill(&mut reader)? {
        bail!("No records in input file")
    };

    let mut num_prefill = 0;
    for record in rset.iter() {
        let _record = record?;
        num_prefill += 1;
    }
    eprintln!("num_prefill: {}", num_prefill);

    // Reload the reader with the record set
    eprintln!("Reloading reader with {} records", num_prefill);
    reader.reload(&mut rset);

    // Process the records in the record set
    let mut rset = fastq::RecordSet::default();
    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let _record = record?;
            num_records += 1;
        }
    }

    eprintln!("num_records: {}", num_records);
    Ok(())
}

fn main() -> Result<()> {
    let path = std::env::args().nth(1).unwrap();
    let prefill = std::env::args()
        .nth(2)
        .unwrap_or(String::from("3"))
        .parse::<usize>()?;
    if path.ends_with(".fastq") {
        reload_fastq(&path, prefill)?;
    } else if path.ends_with(".fasta") {
        reload_fasta(&path, prefill)?;
    } else {
        eprintln!("Unknown file format");
    }

    Ok(())
}
