use std::borrow::Cow;
use std::io;

use fearless_simd::{dispatch, prelude::*, u8x64, Level};

use crate::{
    base::{BatchSet, ReaderBase},
    fastx::GenericReader,
    Error, Record,
};

pub type Reader<R> = ReaderBase<R, RecordSet>;

/// Find every `>` in `buffer_prefix[search_from..]` with an explicit u8x64 SIMD compare,
/// pushing the absolute offset of each match that starts a line (position 0 of the whole
/// buffer, or immediately preceded by `\n`) into `record_starts`. Mirrors
/// `RecordSet::find_record_starts`.
#[inline(always)]
fn simd_find_record_starts<S: Simd>(
    simd: S,
    buffer_prefix: &[u8],
    search_from: usize,
    record_starts: &mut Vec<usize>,
) {
    let mut push_if_line_start = |abs_pos: usize| {
        if abs_pos == 0 || buffer_prefix[abs_pos - 1] == b'\n' {
            record_starts.push(abs_pos);
        }
    };

    let needle = u8x64::splat(simd, b'>');
    let (chunks, remainder) = buffer_prefix[search_from..].as_chunks::<64>();
    let mut base = search_from;
    for chunk in chunks {
        let v = u8x64::from_slice(simd, chunk);
        let mut bits = v.simd_eq(needle).to_bitmask();
        while bits != 0 {
            let bit = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            push_if_line_start(base + bit);
        }
        base += 64;
    }
    for (i, &b) in remainder.iter().enumerate() {
        if b == b'>' {
            push_if_line_start(base + i);
        }
    }
}

/// Find every `\n` in `haystack` with an explicit u8x64 SIMD compare, pushing each
/// match's offset (relative to `haystack`) into `newlines`. Used to de-wrap multiline
/// FASTA sequences, which can span many megabases in reference genomes.
#[inline(always)]
fn simd_find_newlines<S: Simd>(simd: S, haystack: &[u8], newlines: &mut Vec<usize>) {
    let needle = u8x64::splat(simd, b'\n');
    let (chunks, remainder) = haystack.as_chunks::<64>();
    let mut base = 0usize;
    for chunk in chunks {
        let v = u8x64::from_slice(simd, chunk);
        let mut bits = v.simd_eq(needle).to_bitmask();
        while bits != 0 {
            let bit = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            newlines.push(base + bit);
        }
        base += 64;
    }
    for (i, &b) in remainder.iter().enumerate() {
        if b == b'\n' {
            newlines.push(base + i);
        }
    }
}

#[derive(Debug)]
pub struct RecordSet {
    /// Main buffer for records
    buffer: Vec<u8>,
    /// Store positions of '>' characters (record starts)
    record_starts: Vec<usize>,
    /// Track the last byte position we've searched for record starts
    last_searched_pos: usize,
    /// Position tracking for complete records
    positions: Vec<Positions>,
    /// Maximum number of records to store
    capacity: usize,
    /// Average number of bytes per record
    avg_record_size: usize,
    /// Global index of the first record in this set within the original file
    base_index: u64,
}

impl Default for RecordSet {
    fn default() -> Self {
        Self::new(1024)
    }
}

impl RecordSet {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(256 * 1024), // 256KB default
            record_starts: Vec::new(),
            last_searched_pos: 0,
            positions: Vec::with_capacity(capacity),
            capacity,
            avg_record_size: 1024, // 1KB default
            base_index: 0,
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.record_starts.clear();
        self.positions.clear();
        self.last_searched_pos = 0;
    }

    /// Returns the number of records currently in this set.
    pub fn n_records(&self) -> usize {
        self.positions.len()
    }

    /// Truncate the record set to at most `n` records.
    pub fn truncate(&mut self, n: usize) {
        self.positions.truncate(n);
    }

    /// Find all record starts ('>' characters) currently in the buffer starting from the last searched position
    /// and ending at the effective end of the buffer
    /// Only considers '>' characters that are at the beginning of lines
    fn find_record_starts(&mut self, current_pos: usize) {
        let level = Level::new();
        let buffer_prefix = &self.buffer[..current_pos];
        let search_from = self.last_searched_pos;
        dispatch!(level, simd => simd_find_record_starts(
            simd,
            buffer_prefix,
            search_from,
            &mut self.record_starts,
        ));
        self.last_searched_pos = current_pos;
    }

    /// Update the internal average record size
    fn update_avg_record_size(&mut self, total_bytes: usize) {
        let total_records = self.positions.len();
        if let Some(avg) = total_bytes.checked_div(total_records) {
            self.avg_record_size = avg;
        }
    }

    /// Main function to fill the record set
    pub fn fill<R: io::Read>(
        &mut self,
        reader: &mut Reader<R>,
    ) -> std::result::Result<bool, Error> {
        // Clear previous data
        self.clear();
        self.base_index = reader.total_records;

        // First, copy any overflow from previous read
        if !reader.overflow.is_empty() {
            self.buffer.extend_from_slice(&reader.overflow);
            reader.overflow.clear();
        }
        self.find_record_starts(self.buffer.len()); // Find record starts in overflow

        // Determine the number of putative complete records in the buffer
        // A complete record needs at least 2 record starts (current + next) or 1 start at EOF
        let initial_complete_records = if self.record_starts.len() > 1 {
            self.record_starts.len() - 1
        } else if self.record_starts.len() == 1 && reader.eof {
            1
        } else {
            0
        };

        // If we already have enough records from overflow, process them
        if initial_complete_records >= self.capacity {
            return self.process_records(reader);
        }

        // Calculate how many more records we need
        let records_needed = self.capacity.saturating_sub(initial_complete_records);
        let target_read_size = self
            .avg_record_size
            .saturating_mul(records_needed)
            .saturating_add(self.avg_record_size * 2) // padding
            .min(4096); // read at most 4kB at a time

        // Start with current buffer size
        let mut current_pos = self.buffer.len();
        let mut target_len = current_pos + target_read_size;

        // Calculate the number of record starts we need to have in the buffer
        // We need capacity + 1 starts to have capacity complete records
        let required_record_starts = self.capacity + 1;

        // Read loop - continue until we have enough complete records or reach EOF
        while self.record_starts.len() < required_record_starts && !reader.eof {
            // In case we run out of space, extend the target without zero-initializing it
            if current_pos >= target_len {
                let additional = (target_read_size / 10).max(4096);
                target_len += additional;
            }

            match crate::buffer::read_into_uninit(&mut self.buffer, &mut reader.reader, target_len)
            {
                Ok(0) => {
                    reader.set_eof();
                    break;
                }
                Ok(n) => {
                    current_pos += n;
                    self.find_record_starts(current_pos);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }

        // Process all complete records in the buffer
        self.process_records(reader)
    }

    // Split out record processing to separate function
    fn process_records<R: io::Read>(&mut self, reader: &mut Reader<R>) -> Result<bool, Error> {
        // Calculate how many complete records we can process
        // A record is complete if there's another record start after it, or if we're at EOF
        let available_complete = if reader.eof && !self.record_starts.is_empty() {
            // At EOF, all records with starts are complete
            self.record_starts.len()
        } else if self.record_starts.len() > 1 {
            // Not at EOF, only records with a following start are complete
            self.record_starts.len() - 1
        } else {
            0
        };

        let records_to_process = available_complete.min(self.capacity);

        if records_to_process > 0 {
            // Build position entries for each complete record
            for i in 0..records_to_process {
                let record_start = self.record_starts[i];
                let record_end = if i + 1 < self.record_starts.len() {
                    self.record_starts[i + 1]
                } else {
                    // Last record goes to end of buffer
                    self.buffer.len()
                };

                // Find the end of the header line (first newline after '>')
                let seq_start = memchr::memchr(b'\n', &self.buffer[record_start..record_end])
                    .map_or(record_end, |pos| record_start + pos + 1);

                self.positions.push(Positions {
                    start: record_start,
                    seq_start,
                    end: record_end,
                });
            }

            // Determine where to truncate the buffer
            let truncate_pos = if records_to_process < self.record_starts.len() {
                // Keep the start of the next incomplete record
                self.record_starts[records_to_process]
            } else {
                // Processed all records
                self.buffer.len()
            };

            self.update_avg_record_size(truncate_pos);

            // Move remaining partial data to overflow
            if truncate_pos < self.buffer.len() {
                reader
                    .overflow
                    .extend_from_slice(&self.buffer[truncate_pos..]);
            }
            self.buffer.truncate(truncate_pos);
        } else if !self.buffer.is_empty() {
            // No complete records found, move everything to overflow
            reader.overflow.extend_from_slice(&self.buffer);
            self.buffer.clear();
        }

        reader.total_records += self.positions.len() as u64;
        Ok(!self.positions.is_empty())
    }

    // Iterator over complete records
    pub fn iter(&self) -> impl Iterator<Item = Result<RefRecord<'_>, Error>> {
        let base_index = self.base_index;
        self.positions
            .iter()
            .enumerate()
            .map(move |(i, &pos)| RefRecord::new(&self.buffer, pos, base_index + i as u64))
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Positions {
    start: usize,
    seq_start: usize,
    end: usize,
}

#[derive(Debug, Default, Clone)]
pub struct RefRecord<'a> {
    buffer: &'a [u8],
    positions: Positions,
    index: u64,
}

impl<'a> RefRecord<'a> {
    fn new(buffer: &'a [u8], positions: Positions, index: u64) -> Result<Self, Error> {
        let ref_record = Self {
            buffer,
            positions,
            index,
        };
        ref_record.validate_record()?;
        Ok(ref_record)
    }

    /// Validate the record for correctness
    ///
    /// 1. Check that positions are within bounds
    /// 2. Check that the record starts with '>'
    fn validate_record(&self) -> Result<(), Error> {
        // Check that record boundaries are within buffer
        if self.positions.start >= self.buffer.len() || self.positions.end > self.buffer.len() {
            return Err(Error::UnboundedPositions);
        }

        // Check that record starts with '>'
        if self.buffer[self.positions.start] != b'>' {
            return Err(Error::InvalidHeader(
                self.buffer[self.positions.start].into(),
                '>',
            ));
        }

        Ok(())
    }

    /// Access the ID bytes
    #[inline]
    #[must_use]
    pub fn id(&self) -> &[u8] {
        self.access_buffer(
            self.positions.start + 1, // Skip '>'
            self.positions.seq_start,
        )
    }

    /// Returns the record's 0-based index within the original file.
    #[inline]
    #[must_use]
    pub fn index(&self) -> u64 {
        self.index
    }

    /// Access the sequence bytes (handling multiline sequences)
    #[inline]
    #[must_use]
    pub fn seq(&self) -> Cow<'_, [u8]> {
        let seq_region = self.seq_raw();

        let mut newlines = Vec::new();
        dispatch!(Level::new(), simd => simd_find_newlines(simd, seq_region, &mut newlines));

        if newlines.is_empty() {
            // No newlines - can borrow directly
            Cow::Borrowed(seq_region)
        } else if newlines.len() == 1 && seq_region.ends_with(b"\n") {
            // Single line with only trailing newline - can borrow without the newline
            Cow::Borrowed(&seq_region[..seq_region.len() - 1])
        } else {
            // Multiline sequence - need to filter out all newlines
            let mut filtered = Vec::with_capacity(seq_region.len() - newlines.len());
            let mut start = 0;
            // Line endings are detected once per record from its first line
            let first = newlines[0];
            let cr = usize::from(first > 0 && seq_region[first - 1] == b'\r');
            for &end in &newlines {
                filtered.extend_from_slice(&seq_region[start..(end - cr).max(start)]);
                start = end + 1;
            }
            filtered.extend_from_slice(&seq_region[start..]);
            Cow::Owned(filtered)
        }
    }

    fn seq_raw(&self) -> &[u8] {
        // `end` marks the start of the next record (or EOF), so the region
        // includes the trailing newline that terminates the last sequence
        // line. That newline is a delimiter, not sequence data, so strip it
        // -- unless the last record in the file has none (no trailing '\n').
        let region = &self.buffer[self.positions.seq_start..self.positions.end];
        let mut end = region.len();
        end -= usize::from(end > 0 && region[end - 1] == b'\n');
        end -= usize::from(end > 0 && region[end - 1] == b'\r');
        &region[..end]
    }

    /// Performs the actual buffer access
    ///
    /// `right` normally points one byte past the newline that terminates
    /// this field, so that trailing byte is stripped. If the field instead
    /// runs straight to EOF with no newline (e.g. a header with no id and
    /// no trailing newline), `right` points at the exact end of the field
    /// and there is nothing to strip.
    #[inline(always)]
    fn access_buffer(&self, left: usize, right: usize) -> &[u8] {
        // The byte before `left` is always '>', so no `right > left` / `end > left` guards
        let mut end = right - usize::from(self.buffer[right - 1] == b'\n');
        end -= usize::from(self.buffer[end - 1] == b'\r');
        unsafe {
            // SAFETY: `left <= end <= right <= buffer.len()`, guaranteed by
            // `validate_record` and the check above.
            self.buffer.get_unchecked(left..end)
        }
    }
}

impl Record for RefRecord<'_> {
    fn id(&self) -> &[u8] {
        self.id()
    }

    fn seq(&self) -> Cow<'_, [u8]> {
        self.seq()
    }

    fn seq_raw(&self) -> &[u8] {
        self.seq_raw()
    }

    fn qual(&self) -> Option<&[u8]> {
        None
    }

    fn index(&self) -> u64 {
        self.index()
    }
}

impl BatchSet for RecordSet {
    fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity)
    }
    fn fill<R: io::Read>(&mut self, reader: &mut Reader<R>) -> Result<bool, Error> {
        RecordSet::fill(self, reader)
    }
    fn clear(&mut self) {
        RecordSet::clear(self);
    }
    fn n_records(&self) -> usize {
        RecordSet::n_records(self)
    }
    fn truncate(&mut self, n: usize) {
        RecordSet::truncate(self, n);
    }
    fn buffer(&self) -> &[u8] {
        &self.buffer
    }
    fn first_seq_len(&self) -> Result<Option<usize>, Error> {
        self.iter()
            .next()
            .map(|r| r.map(|r| r.seq_raw().len()))
            .transpose()
    }
}

impl<R> GenericReader for crate::fasta::Reader<R>
where
    R: io::Read + Send,
{
    type RecordSet = crate::fasta::RecordSet;
    type Error = crate::Error;
    type RefRecord<'a> = crate::fasta::RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet {
        ReaderBase::new_record_set(self)
    }

    fn fill(&mut self, record: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        self.fill_limited(record)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, crate::Error>> {
        let base_index = record_set.base_index;
        record_set
            .positions
            .iter()
            .enumerate()
            .map(move |(i, &pos)| {
                Self::RefRecord::new(&record_set.buffer, pos, base_index + i as u64)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Helper function to create a valid FASTA record
    fn create_test_record(id: &str, seq: &str) -> String {
        format!(">{id}\n{seq}\n")
    }

    fn make_fasta(n: usize) -> String {
        (0..n)
            .map(|i| create_test_record(&format!("seq{i}"), "ACTG"))
            .collect()
    }

    #[test]
    fn test_reload() {
        const N_RECORDS: usize = 50;
        const PREFILL: usize = 7;

        let mut reader = Reader::new(Cursor::new(make_fasta(N_RECORDS)));
        let mut rset = reader.new_record_set_with_size(PREFILL);

        assert!(rset.fill(&mut reader).unwrap());
        let num_prefill = rset.iter().map(Result::unwrap).count();
        assert_eq!(num_prefill, PREFILL);

        reader.reload(&mut rset);

        // Reload pushes the prefilled bytes back onto the reader, so a fresh
        // full drain sees the entire file again (including the prefill).
        let mut num_after_reload = 0;
        let mut rset = reader.new_record_set();
        while rset.fill(&mut reader).unwrap() {
            num_after_reload += rset.iter().map(Result::unwrap).count();
        }

        assert_eq!(num_after_reload, N_RECORDS);
    }

    #[test]
    fn test_index_stable_across_batches() {
        const N_RECORDS: usize = 47;
        const BATCH_SIZE: usize = 10;

        let mut reader = Reader::new(Cursor::new(make_fasta(N_RECORDS)));
        let mut indices = Vec::new();
        let mut rset = reader.new_record_set_with_size(BATCH_SIZE);
        while rset.fill(&mut reader).unwrap() {
            for record in rset.iter() {
                indices.push(record.unwrap().index());
            }
        }

        let expected: Vec<u64> = (0..N_RECORDS as u64).collect();
        assert_eq!(indices, expected);
    }

    #[test]
    fn test_index_unaffected_by_reload() {
        const N_RECORDS: usize = 50;
        const PREFILL: usize = 7;

        let mut reader = Reader::new(Cursor::new(make_fasta(N_RECORDS)));
        let mut rset = reader.new_record_set_with_size(PREFILL);

        assert!(rset.fill(&mut reader).unwrap());
        let prefill_indices: Vec<u64> = rset.iter().map(|r| r.unwrap().index()).collect();
        assert_eq!(prefill_indices, (0..PREFILL as u64).collect::<Vec<_>>());

        reader.reload(&mut rset);

        // After reloading, re-parsing from scratch must reassign the exact
        // same indices to the same records rather than continuing to count
        // up from where the undone batch left off.
        let mut indices = Vec::new();
        let mut rset = reader.new_record_set();
        while rset.fill(&mut reader).unwrap() {
            for record in rset.iter() {
                indices.push(record.unwrap().index());
            }
        }
        assert_eq!(indices, (0..N_RECORDS as u64).collect::<Vec<_>>());
    }

    #[test]
    fn test_update_batch_size_in_bp() {
        let mut reader = Reader::new(Cursor::new(make_fasta(50)));
        reader.update_batch_size_in_bp(100).unwrap();

        let mut num_records = 0;
        let mut rset = reader.new_record_set();
        while rset.fill(&mut reader).unwrap() {
            num_records += rset.iter().map(Result::unwrap).count();
        }
        assert_eq!(num_records, 50);
    }

    #[cfg(feature = "niffler")]
    #[test]
    fn test_from_stdin() {
        if crate::test_util::is_stdin_child() {
            let mut reader = crate::ReaderBuilder::optional_path(None::<&str>)
                .build_fasta()
                .unwrap();
            let mut num_records = 0;
            let mut rset = reader.new_record_set();
            while rset.fill(&mut reader).unwrap() {
                num_records += rset.iter().map(Result::unwrap).count();
            }
            eprintln!("STDIN_COUNT={num_records}");
            return;
        }

        let output = crate::test_util::run_with_piped_stdin(
            "fasta::tests::test_from_stdin",
            make_fasta(20).as_bytes(),
        );
        assert!(output.status.success(), "child failed: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("STDIN_COUNT=20"), "stderr: {stderr}");
    }

    #[test]
    fn test_basic_record_parsing() {
        let record = create_test_record("test1", "ACTG");
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed_record = record_set.iter().next().unwrap().unwrap();

        assert_eq!(parsed_record.id_str(), "test1");
        assert_eq!(parsed_record.seq_str(), "ACTG");
    }

    #[test]
    fn test_multiple_records() {
        let records = [
            create_test_record("test1", "ACTG"),
            create_test_record("test2", "TGCA"),
        ]
        .join("");

        let mut reader = Reader::new(Cursor::new(records));
        let mut record_set = RecordSet::new(2);

        assert!(record_set.fill(&mut reader).unwrap());
        let records: Vec<_> = record_set.iter().collect::<Result<_, _>>().unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].id_str(), "test1");
        assert_eq!(records[1].id_str(), "test2");
    }

    #[test]
    fn test_invalid_header() {
        // Test with a record that has no valid '>' at line start
        let record = "XACTG\nTGCA\n";
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        // Should not find any valid records
        assert!(!record_set.fill(&mut reader).unwrap());
    }

    #[test]
    fn test_junk_before_valid_record() {
        // Test with junk before a valid record
        let record = format!("X\n{}", create_test_record("test1", "ACTG"));
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed_record = record_set.iter().next().unwrap().unwrap();
        assert_eq!(parsed_record.id_str(), "test1");
        assert_eq!(parsed_record.seq_str(), "ACTG");
    }

    #[test]
    fn test_performance_single_vs_multiline() {
        // Test that single-line sequences return borrowed data (Cow::Borrowed)
        let single_line = create_test_record("single", "ACTG");
        let mut reader = Reader::new(Cursor::new(single_line));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let record = record_set.iter().next().unwrap().unwrap();

        // For single-line sequences, we should get borrowed data
        let seq = record.seq();
        match seq {
            std::borrow::Cow::Borrowed(_) => {
                // This is the expected case for single-line sequences
                assert_eq!(record.seq_str(), "ACTG");
            }
            std::borrow::Cow::Owned(_) => {
                panic!("Single-line sequence should return borrowed data for optimal performance");
            }
        }

        // Test that multiline sequences return owned data (Cow::Owned)
        let multiline = ">multiline\nAC\nTG\n";
        let mut reader = Reader::new(Cursor::new(multiline));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let record = record_set.iter().next().unwrap().unwrap();

        // For multiline sequences, we should get owned data
        let seq = record.seq();
        match seq {
            std::borrow::Cow::Borrowed(_) => {
                panic!("Multiline sequence should return owned data after newline filtering");
            }
            std::borrow::Cow::Owned(_) => {
                // This is the expected case for multiline sequences
                assert_eq!(record.seq_str(), "ACTG");
            }
        }
    }

    #[test]
    fn test_passthrough_read() {
        let record = create_test_record("test1", "ACTG");
        let rdr = Cursor::new(record);
        let (pass, _comp) = niffler::get_reader(Box::new(rdr)).unwrap();
        let mut reader = Reader::new(pass);
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed_record = record_set.iter().next().unwrap().unwrap();
        assert_eq!(parsed_record.id_str(), "test1");
        assert_eq!(parsed_record.seq_str(), "ACTG");

        assert!(!record_set.fill(&mut reader).unwrap());
    }

    #[test]
    fn test_crlf() {
        let data = ">a\r\nAC\r\nTG\r\n>b\r\nGG\r\n>c\r\nTT\r";
        let mut reader = Reader::new(Cursor::new(data));
        let mut record_set = RecordSet::new(3);
        assert!(record_set.fill(&mut reader).unwrap());
        let records: Vec<_> = record_set.iter().collect::<Result<_, _>>().unwrap();
        let got: Vec<_> = records.iter().map(|r| (r.id_str(), r.seq_str())).collect();
        assert_eq!(
            got,
            [("a", "ACTG".into()), ("b", "GG".into()), ("c", "TT".into())]
        );
    }

    #[test]
    fn test_multiline_fasta() {
        let multiline_record = ">test_multiline\nACTG\nTGCA\nGGCC\n";
        let mut reader = Reader::new(Cursor::new(multiline_record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed_record = record_set.iter().next().unwrap().unwrap();

        assert_eq!(parsed_record.id_str(), "test_multiline");
        assert_eq!(parsed_record.seq_str(), "ACTGTGCAGGCC");
    }

    #[test]
    fn test_seq_raw_excludes_trailing_newline() {
        // `seq_raw()` must not include the newline that terminates the
        // sequence line -- it's a delimiter, not sequence data.
        let records = [
            create_test_record("a", "ACTG"),
            create_test_record("b", "TGCA"),
        ]
        .join("");
        let mut reader = Reader::new(Cursor::new(records));
        let mut record_set = RecordSet::new(2);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed: Vec<_> = record_set.iter().collect::<Result<_, _>>().unwrap();
        assert_eq!(parsed[0].seq_raw(), b"ACTG");
        assert_eq!(parsed[1].seq_raw(), b"TGCA");
    }

    #[test]
    fn test_seq_raw_last_record_without_trailing_newline() {
        // The final record in a file with no trailing newline has no
        // delimiter to strip.
        let record = ">last\nACTG";
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed = record_set.iter().next().unwrap().unwrap();
        assert_eq!(parsed.seq_raw(), b"ACTG");
    }

    #[test]
    fn test_id_header_without_trailing_newline() {
        // A header with content but no trailing newline before EOF: `right`
        // (seq_start) lands past the last id byte rather than past a
        // newline, so `access_buffer` must not strip a real id byte here.
        let record = ">last";
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed = record_set.iter().next().unwrap().unwrap();
        assert_eq!(parsed.id(), b"last");
    }

    #[test]
    fn test_id_empty_header_without_trailing_newline() {
        // Degenerate case found by fuzzing: a bare `>` with no id and no
        // trailing newline. Here `left == right == seq_start`, which used to
        // underflow in `access_buffer`'s unconditional `right - 1`.
        let record = ">";
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed = record_set.iter().next().unwrap().unwrap();
        assert_eq!(parsed.id(), b"");
        assert_eq!(parsed.seq_raw(), b"");
    }

    #[test]
    fn test_seq_raw_multiline_keeps_embedded_newlines() {
        let record = ">multi\nACTG\nTGCA\nGGCC\n";
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed = record_set.iter().next().unwrap().unwrap();
        // Embedded newlines are preserved; only the final delimiter is stripped.
        assert_eq!(parsed.seq_raw(), b"ACTG\nTGCA\nGGCC");
    }

    #[test]
    fn test_multiline_fasta_across_simd_chunk_boundary() {
        // `simd_find_newlines` scans in 64-byte SIMD chunks. A short line
        // length packs several newlines into a single chunk's bitmask, and
        // enough lines push the sequence past multiple chunk boundaries plus
        // a scalar remainder -- none of which the other multiline tests
        // (all well under 64 bytes) actually exercise.
        let line = "ACGTACGTAC"; // 10 bytes
        let lines: Vec<&str> = std::iter::repeat_n(line, 100).collect();
        let record = format!(">long_multiline\n{}\n", lines.join("\n"));
        let expected: String = lines.concat();

        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed = record_set.iter().next().unwrap().unwrap();
        assert_eq!(parsed.seq_str(), expected);
    }

    #[test]
    fn test_mixed_single_and_multiline() {
        let mixed_records = ">single\nACTG\n>multiline\nTGCA\nGGCC\nAAAA\n>another_single\nTTTT\n";
        let mut reader = Reader::new(Cursor::new(mixed_records));
        let mut record_set = RecordSet::new(3);

        assert!(record_set.fill(&mut reader).unwrap());
        let records: Vec<_> = record_set.iter().collect::<Result<_, _>>().unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].id_str(), "single");
        assert_eq!(records[0].seq_str(), "ACTG");

        assert_eq!(records[1].id_str(), "multiline");
        assert_eq!(records[1].seq_str(), "TGCAGGCCAAAA");

        assert_eq!(records[2].id_str(), "another_single");
        assert_eq!(records[2].seq_str(), "TTTT");
    }

    #[cfg(feature = "niffler")]
    #[test]
    fn test_from_path() {
        for ext in ["", ".gz", ".zst"] {
            dbg!(ext);
            let path = if ext.is_empty() {
                String::from("./data/sample.fasta")
            } else {
                format!("./data/sample.fasta{}", ext)
            };
            let mut reader = crate::ReaderBuilder::path(path).build_fasta().unwrap();
            let mut record_set = RecordSet::new(1);

            assert!(record_set.fill(&mut reader).unwrap());
            let parsed_record = record_set.iter().next().unwrap().unwrap();

            println!("{}", parsed_record.id_str());
        }
    }

    #[cfg(feature = "niffler")]
    #[test]
    fn test_from_path_with_batch_size() {
        for ext in ["", ".gz", ".zst"] {
            dbg!(ext);
            let path = if ext.is_empty() {
                String::from("./data/sample.fasta")
            } else {
                format!("./data/sample.fasta{}", ext)
            };
            let mut reader = crate::ReaderBuilder::path(path).build_fasta().unwrap();
            reader.set_batch_size(2).unwrap();
            let mut record_set = RecordSet::new(1);

            assert!(record_set.fill(&mut reader).unwrap());
            let parsed_record = record_set.iter().next().unwrap().unwrap();

            println!("{}", parsed_record.id_str());
        }
    }
}
