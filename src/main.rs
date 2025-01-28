use std::fs::File;

use anyhow::Result;
use paraseq::fastq::{Reader, RecordSet};

fn main() -> Result<()> {
    let path = "./data/example.fastq";
    let file = File::open(path)?;
    let mut reader = Reader::new(file);
    let mut rset = RecordSet::new(1024);

    let mut num_records = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            // println!("{}", num_records);
            num_records += 1;
        }

        // if num_records > 10000 {
        //     break;
        // }
        // break;
    }
    println!("num_records: {}", num_records);
    Ok(())
}
