use anyhow::{bail, Result};
use paraseq::{fasta, fastq, fastx};

fn reload_fasta(path: &str, prefill: usize) -> Result<()> {
    let mut reader = fasta::Reader::from_path(path)?;
    let mut rset = reader.new_record_set_with_size(prefill);

    // Fill the record set with records from the reader
    if !rset.fill(&mut reader)? {
        bail!("No records in input file")
    }

    let mut num_prefill = 0;
    for record in rset.iter() {
        let _record = record?;
        num_prefill += 1;
    }
    eprintln!("(fasta) num_prefill: {num_prefill}");

    // Reload the reader with the record set
    eprintln!("(fasta) Reloading reader with {num_prefill} records");
    reader.reload(&mut rset);

    // Process the records in the record set
    let mut rset = reader.new_record_set();
    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let _record = record?;
            num_records += 1;
        }
    }

    eprintln!("(fasta) num_records: {num_records}");
    Ok(())
}

fn reload_fastq(path: &str, prefill: usize) -> Result<()> {
    let mut reader = fastq::Reader::from_path(path)?;
    let mut rset = reader.new_record_set_with_size(prefill);

    // Fill the record set with records from the reader
    if !rset.fill(&mut reader)? {
        bail!("No records in input file")
    }

    let mut num_prefill = 0;
    for record in rset.iter() {
        let _record = record?;
        num_prefill += 1;
    }
    eprintln!("(fastq) num_prefill: {num_prefill}");

    // Reload the reader with the record set
    eprintln!("(fastq) Reloading reader with {num_prefill} records");
    reader.reload(&mut rset);

    // Process the records in the record set
    let mut rset = reader.new_record_set_with_size(prefill);
    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let _record = record?;
            num_records += 1;
        }
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

    let mut num_prefill = 0;
    for record in rset.iter() {
        let _record = record?;
        num_prefill += 1;
    }
    eprintln!("(fastx) num_prefill: {num_prefill}");

    // Reload the reader with the record set
    eprintln!("(fastx) Reloading reader with {num_prefill} records");
    reader.reload(&mut rset)?;

    // Process the records in the record set
    let mut rset = reader.new_record_set();
    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let _record = record?;
            num_records += 1;
        }
    }

    eprintln!("(fastx) num_records: {num_records}");
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
    reload_fastx(&path, prefill)?;

    Ok(())
}
