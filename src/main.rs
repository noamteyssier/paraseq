use anyhow::Result;
use std::{fs::File, io::Read};

fn run_paraseq_fastq<R: Read>(rdr: R) -> Result<()> {
    let mut reader = paraseq::fastq::Reader::new(rdr);
    let mut rset = paraseq::fastq::RecordSet::new(1024);

    let mut num_records = 0;
    let mut num_nucleotides = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let record = record?;
            num_records += 1;
            num_nucleotides += record.seq().len();
        }
    }
    eprintln!("num_records: {}", num_records);
    eprintln!("num_nucleotides: {}", num_nucleotides);
    Ok(())
}

fn run_paraseq_fasta<R: Read>(rdr: R) -> Result<()> {
    let mut reader = paraseq::fasta::Reader::new(rdr);
    let mut rset = paraseq::fasta::RecordSet::new(1024);

    let mut num_records = 0;
    let mut num_nucleotides = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let record = record?;
            num_records += 1;
            num_nucleotides += record.seq().len();
        }
    }
    eprintln!("num_records: {}", num_records);
    eprintln!("num_nucleotides: {}", num_nucleotides);
    Ok(())
}

fn main() -> Result<()> {
    let path = std::env::args().nth(1).unwrap();
    let file = File::open(&path)?;

    if path.ends_with(".fastq") {
        run_paraseq_fastq(file)?;
    } else if path.ends_with(".fasta") {
        run_paraseq_fasta(file)?;
    } else {
        eprintln!("Unknown file format");
    }
    Ok(())
}
