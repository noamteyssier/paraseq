use anyhow::Result;
use std::{fs::File, io::Read};

fn run_paraseq<R: Read>(rdr: R) -> Result<()> {
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

fn main() -> Result<()> {
    let path = "./data/example.fastq";
    let file = File::open(path)?;

    run_paraseq(file)?;
    Ok(())
}
