#![no_main]

use std::hint::black_box;
use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use paraseq::fasta::{Reader, RecordSet};
use paraseq::Record;

fuzz_target!(|data: &[u8]| {
    // Use the first byte to pick a record-set capacity so the fuzzer explores
    // different buffer-refill/overflow boundaries, not just the parsing logic.
    let Some((&cap_byte, data)) = data.split_first() else {
        return;
    };
    let capacity = (cap_byte as usize % 32) + 1;

    let mut reader = Reader::new(Cursor::new(data));
    let mut rset = RecordSet::new(capacity);

    while let Ok(true) = rset.fill(&mut reader) {
        for record in rset.iter() {
            // A per-record parse error (e.g. missing '>') is expected for
            // malformed input and not itself a bug.
            let Ok(record) = record else { continue };
            black_box(record.id());
            black_box(record.seq());
            black_box(record.seq_raw());
        }
    }
});
