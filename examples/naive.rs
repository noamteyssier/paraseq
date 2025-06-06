use std::fs::File;
use std::io::Read;

use anyhow::Result;
use paraseq::fastq;

fn naive_fastq<R: Read>(rdr: R) -> Result<()> {
    let (pass, _comp) = niffler::get_reader(Box::new(rdr))?;

    let mut reader = fastq::Reader::new(pass);
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

fn naive_fasta<R: Read>(rdr: R) -> Result<()> {
    let (pass, _comp) = niffler::get_reader(Box::new(rdr))?;

    let mut reader = paraseq::fasta::Reader::new(pass);
    let mut rset = paraseq::fasta::RecordSet::default();

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
    let path = std::env::args().nth(1).expect("Missing input file");
    let file = File::open(&path)?;
    if path.ends_with(".fastq") {
        naive_fastq(file)?;
    } else if path.ends_with(".fasta") {
        naive_fasta(file)?;
    } else {
        eprintln!("Unknown file format");
    }

    Ok(())
}
