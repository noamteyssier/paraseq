use std::fs::File;
use paraseq::fasta::{Reader, RecordSet};
use paraseq::fastx::Record;

fn main() -> Result<(), paraseq::fasta::Error> {
    println!("Demonstrating multiline FASTA parsing with paraseq");
    println!("==================================================\n");

    // Open the multiline FASTA file
    let file = File::open("./data/multiline.fasta")?;
    let mut reader = Reader::new(file);
    let mut record_set = RecordSet::new(1024);

    let mut record_count = 0;

    // Process records in batches
    while record_set.fill(&mut reader)? {
        for record in record_set.iter() {
            let record = record?;
            record_count += 1;

            println!("Record {}: {}", record_count, record.id_str());
            
            // Get the sequence using Cow - this will be borrowed for single-line
            // sequences and owned for multiline sequences that need newline filtering
            let sequence = record.seq();
            
            println!("  Sequence length: {} bp", sequence.len());
            
            // Show first 50 characters of sequence
            let seq_str = record.seq_str();
            let preview = if seq_str.len() > 50 {
                format!("{}...", &seq_str[..50])
            } else {
                seq_str.clone()
            };
            println!("  Sequence preview: {}", preview);
            
            // Demonstrate that multiline sequences are properly concatenated
            if sequence.contains(&b'\n') {
                println!("  ERROR: Sequence still contains newlines!");
            } else {
                println!("  ✓ Sequence properly processed (no newlines)");
            }
            
            println!();
        }
    }

    println!("Successfully processed {} FASTA records", record_count);
    println!("\nNote: Both single-line and multiline FASTA sequences are supported.");
    println!("Single-line sequences use zero-copy borrowing for optimal performance,");
    println!("while multiline sequences are concatenated as needed.");

    Ok(())
}