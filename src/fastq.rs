use std::borrow::Cow;
use std::io;
#[cfg(feature = "niffler")]
use std::path::Path;

use crate::{Error, Record, DEFAULT_MAX_RECORDS};

pub struct Reader<R: io::Read> {
    /// Handle to the underlying reader (byte stream)
    reader: R,
    /// Small buffer to hold incomplete records between reads
    overflow: Vec<u8>,
    /// Flag to indicate end of file
    eof: bool,
    /// Sets the maximum capcity of records in batches for parallel processing
    ///
    /// If not set, the default `RecordSet` capacity is used.
    pub(crate) batch_size: Option<usize>,
}

#[cfg(feature = "niffler")]
impl Reader<Box<dyn io::Read + Send>> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let (reader, _format) = niffler::send::from_path(path)?;
        Ok(Self::new(reader))
    }

    pub fn from_stdin() -> Result<Self, Error> {
        let (reader, _format) = niffler::send::get_reader(Box::new(io::stdin()))?;
        Ok(Self::new(reader))
    }

    pub fn from_optional_path<P: AsRef<Path>>(path: Option<P>) -> Result<Self, Error> {
        match path {
            Some(path) => Self::from_path(path),
            None => Self::from_stdin(),
        }
    }

    pub fn from_path_with_batch_size<P: AsRef<Path>>(
        path: P,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let (reader, _format) = niffler::send::from_path(path)?;
        Self::with_batch_size(reader, batch_size)
    }

    pub fn from_stdin_with_batch_size(batch_size: usize) -> Result<Self, Error> {
        let (reader, _format) = niffler::send::get_reader(Box::new(io::stdin()))?;
        Self::with_batch_size(reader, batch_size)
    }

    pub fn from_optional_path_with_batch_size<P: AsRef<Path>>(
        path: Option<P>,
        batch_size: usize,
    ) -> Result<Self, Error> {
        match path {
            Some(path) => Self::from_path_with_batch_size(path, batch_size),
            None => Self::from_stdin_with_batch_size(batch_size),
        }
    }
}

#[cfg(feature = "url")]
impl Reader<Box<dyn io::Read + Send>> {
    pub fn from_url(url: &str) -> Result<Self, Error> {
        let stream = reqwest::blocking::get(url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(stream))?;
        Ok(Self::new(reader))
    }

    pub fn from_url_with_batch_size(url: &str, batch_size: usize) -> Result<Self, Error> {
        let stream = reqwest::blocking::get(url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(stream))?;
        Self::with_batch_size(reader, batch_size)
    }
}

#[cfg(feature = "ssh")]
impl Reader<Box<dyn io::Read + Send>> {
    pub fn from_ssh(ssh_url: &str) -> Result<Self, Error> {
        let ssh_reader = crate::ssh::SshReader::new(ssh_url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(ssh_reader))?;
        Ok(Self::new(reader))
    }

    pub fn from_ssh_with_batch_size(ssh_url: &str, batch_size: usize) -> Result<Self, Error> {
        let ssh_reader = crate::ssh::SshReader::new(ssh_url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(ssh_reader))?;
        Self::with_batch_size(reader, batch_size)
    }
}

#[cfg(feature = "gcs")]
impl Reader<Box<dyn io::Read + Send>> {
    /// Create a GCS reader using Application Default Credentials
    pub fn from_gcs(gcs_url: &str) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::new(gcs_url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Ok(Self::new(reader))
    }

    /// Create a GCS reader using custom gcloud arguments
    pub fn from_gcs_with_gcloud_args(gcs_url: &str, args: &[&str]) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::with_gcloud_args(gcs_url, args)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Ok(Self::new(reader))
    }

    /// Create a GCS reader using a specific project ID
    pub fn from_gcs_with_project(gcs_url: &str, project_id: &str) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::with_project(gcs_url, project_id)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Ok(Self::new(reader))
    }

    /// Create a GCS reader with custom batch size using Application Default Credentials
    pub fn from_gcs_with_batch_size(gcs_url: &str, batch_size: usize) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::new(gcs_url)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Self::with_batch_size(reader, batch_size)
    }

    /// Create a GCS reader with custom batch size using custom gcloud arguments
    pub fn from_gcs_with_gcloud_args_and_batch_size(
        gcs_url: &str,
        gcloud_args: &[&str],
        batch_size: usize,
    ) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::with_gcloud_args(gcs_url, gcloud_args)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Self::with_batch_size(reader, batch_size)
    }

    /// Create a GCS reader with custom batch size using a specific project ID
    pub fn from_gcs_with_project_and_batch_size(
        gcs_url: &str,
        project_id: &str,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let gcs_reader = crate::gcs::GcsReader::with_project(gcs_url, project_id)?;
        let (reader, _format) = niffler::send::get_reader(Box::new(gcs_reader))?;
        Self::with_batch_size(reader, batch_size)
    }
}

impl<R: io::Read> Reader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            overflow: Vec::with_capacity(1024), // Start small, can tune this
            reader,
            eof: false,
            batch_size: None,
        }
    }
    pub fn with_batch_size(reader: R, batch_size: usize) -> Result<Self, Error> {
        if batch_size == 0 {
            return Err(Error::InvalidBatchSize(batch_size));
        }
        let mut reader = Self::new(reader);
        reader.batch_size = Some(batch_size);
        Ok(reader)
    }
    /// Initialize a new record set with a configured or default batch size
    pub fn new_record_set(&self) -> RecordSet {
        if let Some(batch_size) = self.batch_size {
            RecordSet::new(batch_size)
        } else {
            RecordSet::default()
        }
    }
    /// Initialize a new record set with a specified size
    pub fn new_record_set_with_size(&self, size: usize) -> RecordSet {
        RecordSet::new(size)
    }
    /// Add bytes to the overflow buffer.
    ///
    /// Use this method sparingly, it is mainly for internal use.
    pub fn add_to_overflow(&mut self, buffer: &[u8]) {
        self.overflow.extend_from_slice(buffer);
    }
    pub fn batch_size(&self) -> usize {
        self.batch_size.unwrap_or(DEFAULT_MAX_RECORDS)
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

        // Truncate the overflow buffer at the end of expected bytes (handles cases where unexpected null bytes are introduced)
        self.overflow.truncate(required_space);

        // Clear the record set
        rset.clear();
    }
}

#[derive(Debug)]
pub struct RecordSet {
    /// Main buffer for records
    pub(crate) buffer: Vec<u8>,
    /// Store newlines in buffer
    newlines: Vec<usize>,
    /// Track the last byte position we've searched for newlines
    last_searched_pos: usize,
    /// Position tracking for complete records
    pub(crate) positions: Vec<Positions>,
    /// Maximum number of records to store
    capacity: usize,
    /// Average number of bytes per record
    avg_record_size: usize,
}
impl Default for RecordSet {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_RECORDS)
    }
}

impl RecordSet {
    #[must_use]
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
        let initial_complete_records = self.newlines.len() / 4;

        // If we already have enough records from overflow, don't read more
        if initial_complete_records >= self.capacity {
            return self.process_records(reader);
        }

        // Calculate how many more records we need, being careful about overflows
        let records_needed = self.capacity.saturating_sub(initial_complete_records);
        let target_read_size = self
            .avg_record_size
            .saturating_mul(records_needed)
            .saturating_add(self.avg_record_size * 2); // padding

        // Start with current buffer size
        let mut current_pos = self.buffer.len();
        self.buffer.resize(current_pos + target_read_size, 0);

        // Calculate the number of newlines we need to have in the buffer
        let required_newlines = self.capacity * 4;

        // Read loop
        while self.newlines.len() < required_newlines && !reader.eof {
            let remaining_space = self.buffer.len() - current_pos;

            // In case we run out of space, resize the buffer (this rare for equivalently sized records)
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
        let available_complete = self.newlines.len() / 4;
        let records_to_process = available_complete.min(self.capacity);

        if records_to_process > 0 {
            let last_complete_newline = self.newlines[4 * records_to_process - 1];

            // Build position entries
            let mut record_start = 0;
            self.newlines
                .chunks_exact(4)
                .take(records_to_process)
                .for_each(|chunk| {
                    let (seq_start, sep_start, qual_start, end) =
                        (chunk[0], chunk[1], chunk[2], chunk[3]);

                    self.positions.push(Positions {
                        start: record_start,
                        seq_start,
                        sep_start,
                        qual_start,
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

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Positions {
    start: usize,
    seq_start: usize,
    sep_start: usize,
    qual_start: usize,
    end: usize,
}

#[derive(Debug, Default, Clone)]
pub struct RefRecord<'a> {
    buffer: &'a [u8],
    positions: Positions,
}
impl<'a> RefRecord<'a> {
    pub(crate) fn new(buffer: &'a [u8], positions: Positions) -> Result<Self, Error> {
        let ref_record = Self { buffer, positions };
        ref_record.validate_record()?;
        Ok(ref_record)
    }

    /// Validate the record for correctness
    ///
    /// 1. Check that positions are within bounds
    /// 2. Check that the record starts with '@'
    /// 3. Check that the separator line starts with '+'
    /// 4. Check that sequence and quality lengths match
    fn validate_record(&self) -> Result<(), Error> {
        // Check that record boundaries are within buffer
        if self.positions.start >= self.buffer.len() || self.positions.end > self.buffer.len() {
            return Err(Error::UnboundedPositions);
        }

        // Check that record starts with '@'
        if self.buffer[self.positions.start] != b'@' {
            return Err(Error::InvalidHeader(
                self.buffer[self.positions.start].into(),
                '@',
            ));
        }

        // Check that separator starts with '+'
        if self.buffer[self.positions.sep_start] != b'+' {
            return Err(Error::InvalidSeparator(
                self.buffer[self.positions.sep_start].into(),
            ));
        }

        // Check that sequence and quality lengths match
        if self.positions.sep_start - self.positions.seq_start
            != self.positions.end - self.positions.qual_start
        {
            return Err(Error::UnequalLengths(
                self.positions.sep_start - self.positions.seq_start - 1, // subtract 1 for embedded newline
                self.positions.end - self.positions.qual_start - 1, // subtract 1 for embedded newline
            ));
        }

        Ok(())
    }

    /// Access the ID bytes
    #[inline]
    #[must_use]
    pub fn id(&self) -> &[u8] {
        self.access_buffer(
            self.positions.start + 1, // Skip '@'
            self.positions.seq_start,
        )
    }

    /// Access the separator bytes
    #[inline]
    #[must_use]
    pub fn sep(&self) -> &[u8] {
        self.access_buffer(self.positions.sep_start, self.positions.qual_start)
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

    fn seq(&self) -> std::borrow::Cow<[u8]> {
        Cow::Borrowed(self.seq_raw())
    }

    #[inline]
    fn seq_raw(&self) -> &[u8] {
        self.access_buffer(self.positions.seq_start, self.positions.sep_start)
    }

    fn qual(&self) -> Option<&[u8]> {
        Some(self.access_buffer(self.positions.qual_start, self.positions.end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Helper function to create a valid FASTQ record
    fn create_test_record(id: &str, seq: &str, sep: &str, qual: &str) -> String {
        format!("@{id}\n{seq}\n+{sep}\n{qual}\n")
    }

    #[test]
    fn test_basic_record_parsing() {
        let record = create_test_record("test1", "ACTG", "", "IIII");
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed_record = record_set.iter().next().unwrap().unwrap();

        assert_eq!(parsed_record.id_str(), "test1");
        assert_eq!(parsed_record.seq_str(), "ACTG");
        assert_eq!(parsed_record.qual_str(), "IIII");
    }

    #[test]
    fn test_multiple_records() {
        let records = [
            create_test_record("test1", "ACTG", "", "IIII"),
            create_test_record("test2", "TGCA", "", "HHHH"),
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
        let record = format!("X{}\n", create_test_record("test1", "ACTG", "", "IIII"));
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        assert!(matches!(
            record_set.iter().next().unwrap().unwrap_err(),
            Error::InvalidHeader('X', '@'),
        ));
    }

    #[test]
    fn test_invalid_separator() {
        let record = format!("@{}\n{}\n{}\n{}\n", "test1", "ACTG", "X", "IIII");
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        assert!(matches!(
            record_set.iter().next().unwrap().unwrap_err(),
            Error::InvalidSeparator('X')
        ));
    }

    #[test]
    fn test_unequal_lengths() {
        let record = create_test_record("test1", "ACTG", "", "III");
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());

        let next_record = record_set.iter().next();
        println!("{next_record:?}");

        assert!(matches!(
            record_set.iter().next().unwrap().unwrap_err(),
            Error::UnequalLengths(4, 3)
        ));
    }

    #[test]
    fn test_buffer_overflow() {
        // Create a record that's larger than the default buffer
        let long_seq = "A".repeat(300 * 1024); // 300KB sequence
        let long_qual = "I".repeat(300 * 1024); // 300KB quality scores
        let record = create_test_record("test1", &long_seq, "", &long_qual);

        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed_record = record_set.iter().next().unwrap().unwrap();
        assert_eq!(parsed_record.seq().len(), long_seq.len());
    }

    #[test]
    fn test_partial_record() {
        let partial_record = "@test1\nACTG\n+\nIII"; // Missing final newline
        let mut reader = Reader::new(Cursor::new(partial_record));
        let mut record_set = RecordSet::new(1);

        assert!(!record_set.fill(&mut reader).unwrap());
        assert!(record_set.iter().next().is_none());
    }

    #[test]
    fn test_empty_input() {
        let mut reader = Reader::new(Cursor::new(""));
        let mut record_set = RecordSet::new(1);

        assert!(!record_set.fill(&mut reader).unwrap());
        assert!(record_set.iter().next().is_none());
    }

    #[test]
    fn test_reader_exhausted() {
        let record = create_test_record("test1", "ACTG", "", "IIII");
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        assert!(!record_set.fill(&mut reader).unwrap());
        assert!(reader.exhausted());
    }

    #[test]
    fn test_capacity_limit() {
        let records = (0..10)
            .map(|i| create_test_record(&format!("test{i}"), "ACTG", "", "IIII"))
            .collect::<String>();

        let mut reader = Reader::new(Cursor::new(records));
        let mut record_set = RecordSet::new(5); // Only process 5 records at a time

        assert!(record_set.fill(&mut reader).unwrap());
        assert_eq!(record_set.iter().count(), 5);

        // Should be able to read the next batch
        assert!(record_set.fill(&mut reader).unwrap());
        assert_eq!(record_set.iter().count(), 5);
    }

    #[test]
    fn test_record_spanning_buffers() {
        // Create two records where the second one might span buffer boundaries
        let records = [
            create_test_record("test1", "ACTG", "", "IIII"),
            create_test_record("test2", &"A".repeat(1024), "", &"I".repeat(1024)),
        ]
        .join("");

        let mut reader = Reader::new(Cursor::new(records));
        let mut record_set = RecordSet::new(2);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed_records: Vec<_> = record_set.iter().collect::<Result<_, _>>().unwrap();
        assert_eq!(parsed_records.len(), 2);
        assert_eq!(parsed_records[1].seq().len(), 1024);
    }

    #[test]
    fn test_invalid_utf8() {
        // Create a valid record structure but with invalid UTF-8 in the sequence
        // @test1\nA<invalid-utf8>CTG\n+\nI<invalid-utf8>III\n
        let record = vec![
            b'@', b't', b'e', b's', b't', b'1', b'\n', // header
            b'A', 0xFF, b'C', b'T', b'G', b'\n', // sequence with invalid UTF-8
            b'+', b'\n', // separator
            b'I', 0xFF, b'I', b'I', b'I', b'\n', // quality
        ];

        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed_record = record_set.iter().next().unwrap().unwrap();
        assert!(std::str::from_utf8(&parsed_record.seq()).is_err());
    }
    #[test]
    fn test_clear_record_set() {
        let record = create_test_record("test1", "ACTG", "", "IIII");
        let mut reader = Reader::new(Cursor::new(record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        record_set.clear();
        assert_eq!(record_set.iter().count(), 0);
        assert_eq!(record_set.buffer.len(), 0);
        assert_eq!(record_set.newlines.len(), 0);
    }

    #[test]
    fn test_passthrough_read() {
        let record = create_test_record("test1", "ACTG", "", "IIII");
        let rdr = Cursor::new(record);
        let (pass, _comp) = niffler::get_reader(Box::new(rdr)).unwrap();
        let mut reader = Reader::new(pass);
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        let parsed_record = record_set.iter().next().unwrap().unwrap();
        assert_eq!(parsed_record.id_str(), "test1");
        assert_eq!(parsed_record.seq_str(), "ACTG");
        assert_eq!(parsed_record.qual_str(), "IIII");

        assert!(!record_set.fill(&mut reader).unwrap());
    }

    #[cfg(feature = "niffler")]
    #[test]
    fn test_from_path() {
        for ext in ["", ".gz", ".zst"] {
            dbg!(ext);
            let path = if ext.is_empty() {
                String::from("./data/sample.fastq")
            } else {
                format!("./data/sample.fastq{}", ext)
            };
            let mut reader = Reader::from_path(path).unwrap();
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
                String::from("./data/sample.fastq")
            } else {
                format!("./data/sample.fastq{}", ext)
            };
            let mut reader = Reader::from_path_with_batch_size(path, 2).unwrap();
            let mut record_set = RecordSet::new(1);

            assert!(record_set.fill(&mut reader).unwrap());
            let parsed_record = record_set.iter().next().unwrap().unwrap();

            println!("{}", parsed_record.id_str());
        }
    }
}
