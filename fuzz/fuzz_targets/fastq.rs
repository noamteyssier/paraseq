#![no_main]

use std::hint::black_box;
use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use paraseq::fastq::{Reader, RecordSet};
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
            // A per-record parse error (e.g. bad separator, mismatched
            // seq/qual lengths) is expected for malformed input and not
            // itself a bug.
            let Ok(record) = record else { continue };

            // Any record that *does* validate must have equal-length
            // sequence and quality strings -- that's the whole point of the
            // FASTQ format, and the reader's own validation is supposed to
            // guarantee it.
            let qual = record.qual().expect("fastq record must carry quality scores");
            assert_eq!(
                record.seq().len(),
                qual.len(),
                "validated record has mismatched seq/qual lengths"
            );

            black_box(record.id());
        }
    }
});
