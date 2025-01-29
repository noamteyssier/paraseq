use std::io;

use crate::fastx::Record;

pub struct Reader<R: io::Read> {
    /// Handle to the underlying reader (byte stream)
    reader: R,
    /// Small buffer to hold incomplete records between reads
    overflow: Vec<u8>,
    /// Flag to indicate end of file
    eof: bool,
}

impl<R: io::Read> Reader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            overflow: Vec::with_capacity(1024),
            reader,
            eof: false,
        }
    }
    pub fn set_eof(&mut self) {
        self.eof = true;
    }
    pub fn exhausted(&self) -> bool {
        self.eof && self.overflow.is_empty()
    }

    /// Take back all bytes from the record set and prepend them to the overflow buffer
    ///
    /// This is an expensive operation and should be used sparingly.
    pub fn reload(&mut self, rset: &mut RecordSet) {
        // A complete slice of the record sets buffer
        let buffer_slice = &rset.buffer;

        // Get buffer lengths of incoming and existing data
        let num_incoming = buffer_slice.len();
        let num_existing = self.overflow.len();

        // Allocate space in the overflow buffer for incoming bytes
        let required_space = num_existing + num_incoming;
        self.overflow
            .resize(self.overflow.capacity().max(required_space), 0);

        // Move current bytes to end of overflow buffer
        self.overflow.copy_within(..num_existing, num_incoming);

        // Copy incoming bytes to the beginning of the overflow buffer
        self.overflow[..num_incoming].copy_from_slice(buffer_slice);

        // Clear the record set
        rset.clear();
    }
}

#[derive(Debug)]
pub struct RecordSet {
    /// Main buffer for records
    buffer: Vec<u8>,
    /// Store newlines in buffer
    newlines: Vec<usize>,
    /// Track the last byte position we've searched for newlines
    last_searched_pos: usize,
    /// Position tracking for complete records
    positions: Vec<Positions>,
    /// Maximum number of records to store
    capacity: usize,
    /// Average number of bytes per record
    avg_record_size: usize,
}

impl Default for RecordSet {
    fn default() -> Self {
        Self::new(1024)
    }
}

impl RecordSet {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(256 * 1024), // 256KB default
            newlines: Vec::new(),
            last_searched_pos: 0,
            positions: Vec::with_capacity(capacity),
            capacity,
            avg_record_size: 1024, // 1KB default
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.newlines.clear();
        self.positions.clear();
        self.last_searched_pos = 0;
    }

    /// Find all newlines currently in the buffer starting from the last searched position
    /// and ending at the effective end of the buffer
    fn find_newlines(&mut self, current_pos: usize) {
        let search_buffer = &self.buffer[self.last_searched_pos..current_pos];
        memchr::memchr_iter(b'\n', search_buffer).for_each(|i| {
            self.newlines.push(i + self.last_searched_pos + 1);
        });
        self.last_searched_pos = current_pos;
    }

    /// Update the internal average record size
    fn update_avg_record_size(&mut self, total_bytes: usize) {
        let total_records = self.positions.len();
        if total_records > 0 {
            self.avg_record_size = total_bytes / total_records;
        }
    }

    /// Main function to fill the record set
    pub fn fill<R: io::Read>(&mut self, reader: &mut Reader<R>) -> Result<bool, Error> {
        // Clear previous data
        self.clear();

        // First, copy any overflow from previous read
        if !reader.overflow.is_empty() {
            self.buffer.extend_from_slice(&reader.overflow);
            reader.overflow.clear();
        }
        self.find_newlines(self.buffer.len()); // Find newlines in overflow

        // Determine the number of putative complete records in the buffer
        let initial_complete_records = self.newlines.len() / 2; // Changed from 4 to 2 for FASTA

        // If we already have enough records from overflow, don't read more
        if initial_complete_records >= self.capacity {
            return self.process_records(reader);
        }

        // Calculate how many more records we need
        let records_needed = self.capacity.saturating_sub(initial_complete_records);
        let target_read_size = self
            .avg_record_size
            .saturating_mul(records_needed)
            .saturating_add(self.avg_record_size * 2); // padding

        // Start with current buffer size
        let mut current_pos = self.buffer.len();
        self.buffer.resize(current_pos + target_read_size, 0);

        // Calculate the number of newlines we need to have in the buffer
        let required_newlines = self.capacity * 2; // Changed from 4 to 2 for FASTA

        // Read loop
        while self.newlines.len() < required_newlines && !reader.eof {
            let remaining_space = self.buffer.len() - current_pos;

            // In case we run out of space, resize the buffer
            if remaining_space == 0 {
                let additional = (target_read_size / 10).max(4096);
                self.buffer.resize(self.buffer.len() + additional, 0);
            }

            match reader.reader.read(&mut self.buffer[current_pos..]) {
                Ok(0) => {
                    reader.set_eof();
                    break;
                }
                Ok(n) => {
                    current_pos += n;
                    self.find_newlines(current_pos);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }

        // Truncate to what we actually read
        self.buffer.truncate(current_pos);

        // Process all complete records in the buffer
        self.process_records(reader)
    }

    // Split out record processing to separate function
    fn process_records<R: io::Read>(&mut self, reader: &mut Reader<R>) -> Result<bool, Error> {
        let available_complete = self.newlines.len() / 2; // Changed from 4 to 2 for FASTA
        let records_to_process = available_complete.min(self.capacity);

        if records_to_process > 0 {
            let last_complete_newline = self.newlines[2 * records_to_process - 1]; // Changed from 4 to 2

            // Build position entries
            let mut record_start = 0;
            self.newlines
                .chunks_exact(2) // Changed from 4 to 2
                .take(records_to_process)
                .for_each(|chunk| {
                    let (seq_start, end) = (chunk[0], chunk[1]); // Only need 2 positions now

                    self.positions.push(Positions {
                        start: record_start,
                        seq_start,
                        end,
                    });
                    record_start = end;
                });

            self.update_avg_record_size(last_complete_newline);

            // Move remaining partial data to overflow
            reader
                .overflow
                .extend_from_slice(&self.buffer[last_complete_newline..]);
            self.buffer.truncate(last_complete_newline);
        } else if !self.buffer.is_empty() {
            reader.overflow.extend_from_slice(&self.buffer);
            self.buffer.clear();
        }

        Ok(!self.positions.is_empty())
    }

    // Iterator over complete records
    pub fn iter(&self) -> impl Iterator<Item = Result<RefRecord<'_>, Error>> {
        self.positions
            .iter()
            .map(move |&pos| RefRecord::new(&self.buffer, pos))
    }
}

#[derive(Debug, Clone, Copy)]
struct Positions {
    start: usize,
    seq_start: usize,
    end: usize,
}

#[derive(Debug, Clone)]
pub struct RefRecord<'a> {
    buffer: &'a [u8],
    positions: Positions,
}

impl<'a> RefRecord<'a> {
    fn new(buffer: &'a [u8], positions: Positions) -> Result<Self, Error> {
        let ref_record = Self { buffer, positions };
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
            return Err(Error::InvalidHeader);
        }

        Ok(())
    }

    /// Access the ID bytes
    #[inline]
    pub fn id(&self) -> &[u8] {
        self.access_buffer(
            self.positions.start + 1, // Skip '>'
            self.positions.seq_start,
        )
    }

    /// Access the sequence bytes
    #[inline]
    pub fn seq(&self) -> &[u8] {
        self.access_buffer(self.positions.seq_start, self.positions.end)
    }

    /// Performs the actual buffer access
    #[inline(always)]
    fn access_buffer(&self, left: usize, right: usize) -> &[u8] {
        unsafe {
            // SAFETY: We've checked that left and right are within bounds
            self.buffer.get_unchecked(left..right - 1)
        }
    }
}

impl Record for RefRecord<'_> {
    fn id(&self) -> &[u8] {
        self.id()
    }

    fn seq(&self) -> &[u8] {
        self.seq()
    }

    fn qual(&self) -> Option<&[u8]> {
        None
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error reading from buffer: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid header")]
    InvalidHeader,

    #[error("Unbounded positions")]
    UnboundedPositions,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Helper function to create a valid FASTA record
    fn create_test_record(id: &str, seq: &str) -> String {
        format!(">{}\n{}\n", id, seq)
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
        let record = format!("X{}\n", create_test_record("test1", "ACTG"));
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        assert!(matches!(
            record_set.iter().next().unwrap().unwrap_err(),
            Error::InvalidHeader
        ));
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
}
