#![no_main]

use std::hint::black_box;
use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use paraseq::fastx::Reader;
use paraseq::Record;

fuzz_target!(|data: &[u8]| {
    // Use the first byte to pick a record-set capacity so the fuzzer explores
    // different buffer-refill/overflow boundaries, not just the parsing logic.
    let Some((&cap_byte, data)) = data.split_first() else {
        return;
    };
    let capacity = (cap_byte as usize % 32) + 1;

    // `Reader::new` peeks the first byte to decide FASTA vs FASTQ; any other
    // byte (or an empty input) is an expected error, not a bug.
    let Ok(mut reader) = Reader::new(Cursor::new(data)) else {
        return;
    };
    let mut rset = reader.new_record_set_with_size(capacity);

    while let Ok(true) = rset.fill(&mut reader) {
        for record in rset.iter() {
            let Ok(record) = record else { continue };
            black_box(record.id());
            black_box(record.seq());
            black_box(record.seq_raw());
            black_box(record.qual());
        }
    }
});
