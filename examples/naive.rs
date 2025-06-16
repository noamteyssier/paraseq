use anyhow::{bail, Result};
use paraseq::{fasta, fastq};

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
    eprintln!("Number of records: {num_records}");

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
    eprintln!("Number of records: {num_records}");

    Ok(())
}

fn main() -> Result<()> {
    let Some(path) = std::env::args().nth(1) else {
        bail!("Must provide a file path to a FASTA or FASTQ file (compression optional)")
    };
    if path.contains(".fastq") | path.contains(".fq") {
        naive_fastq(&path)?;
    } else if path.contains(".fasta") | path.contains(".fa") {
        naive_fasta(&path)?;
    } else {
        eprintln!("Unknown file format");
    }

    Ok(())
}
