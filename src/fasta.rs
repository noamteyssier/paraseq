use std::borrow::Cow;
use std::io;

#[cfg(feature = "niffler")]
use std::path::Path;

use crate::{fastx::GenericReader, Error, Record, DEFAULT_MAX_RECORDS};

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
    batch_size: Option<usize>,
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

impl<R: io::Read> Reader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            overflow: Vec::with_capacity(1024),
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

    /// Use the first record in the input to set the number of records per batch
    /// so that the expected length per batch is approximately `batch_size_in_bp`.
    pub fn update_batch_size_in_bp(&mut self, batch_size_in_bp: usize) -> Result<(), Error> {
        let mut rset = self.new_record_set_with_size(1);
        rset.fill(self)?;
        let mut batch_size = 1;
        if let Some(record) = rset.iter().next() {
            let len = record?.seq_raw().len();
            if len > 0 {
                batch_size = batch_size_in_bp.div_ceil(len);
            }
        }
        // Push the record back at the front of the reader.
        self.reload(&mut rset);
        // Update the batch size.
        self.batch_size = Some(batch_size);
        Ok(())
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
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.record_starts.clear();
        self.positions.clear();
        self.last_searched_pos = 0;
    }

    /// Find all record starts ('>' characters) currently in the buffer starting from the last searched position
    /// and ending at the effective end of the buffer
    /// Only considers '>' characters that are at the beginning of lines
    fn find_record_starts(&mut self, current_pos: usize) {
        let search_buffer = &self.buffer[self.last_searched_pos..current_pos];
        memchr::memchr_iter(b'>', search_buffer).for_each(|i| {
            let abs_pos = i + self.last_searched_pos;
            // Check if this '>' is at the start of a line (position 0 or after newline)
            if abs_pos == 0 || self.buffer[abs_pos - 1] == b'\n' {
                self.record_starts.push(abs_pos);
            }
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
    pub fn fill<R: io::Read>(
        &mut self,
        reader: &mut Reader<R>,
    ) -> std::result::Result<bool, Error> {
        // Clear previous data
        self.clear();

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
            .saturating_add(self.avg_record_size * 2); // padding

        // Start with current buffer size
        let mut current_pos = self.buffer.len();
        self.buffer.resize(current_pos + target_read_size, 0);

        // Calculate the number of record starts we need to have in the buffer
        // We need capacity + 1 starts to have capacity complete records
        let required_record_starts = self.capacity + 1;

        // Read loop - continue until we have enough complete records or reach EOF
        while self.record_starts.len() < required_record_starts && !reader.eof {
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
                    self.find_record_starts(current_pos);
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
struct Positions {
    start: usize,
    seq_start: usize,
    end: usize,
}

#[derive(Debug, Default, Clone)]
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

    /// Access the sequence bytes (handling multiline sequences)
    #[inline]
    #[must_use]
    pub fn seq(&self) -> Cow<'_, [u8]> {
        let seq_region = self.seq_raw();

        // // Count newlines in the sequence region
        // let newline_count = memchr::memchr_iter(b'\n', seq_region).count();
        let newlines = memchr::memchr_iter(b'\n', seq_region).collect::<Vec<_>>();

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
            for &end in &newlines {
                filtered.extend_from_slice(&seq_region[start..end]);
                start = end + 1;
            }
            if start < seq_region.len() {
                filtered.extend_from_slice(&seq_region[start..]);
            }
            Cow::Owned(filtered)
        }
    }

    fn seq_raw(&self) -> &[u8] {
        &self.buffer[self.positions.seq_start..self.positions.end]
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

    fn seq(&self) -> Cow<'_, [u8]> {
        self.seq()
    }

    fn seq_raw(&self) -> &[u8] {
        self.seq_raw()
    }

    fn qual(&self) -> Option<&[u8]> {
        None
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
        if let Some(batch_size) = self.batch_size {
            Self::RecordSet::new(batch_size)
        } else {
            Self::RecordSet::default()
        }
    }

    fn fill(&mut self, record: &mut Self::RecordSet) -> std::result::Result<bool, crate::Error> {
        record.fill(self)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, crate::Error>> {
        record_set
            .positions
            .iter()
            .map(move |&pos| Self::RefRecord::new(&record_set.buffer, pos))
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
                String::from("./data/sample.fasta")
            } else {
                format!("./data/sample.fasta{}", ext)
            };
            let mut reader = Reader::from_path_with_batch_size(path, 2).unwrap();
            let mut record_set = RecordSet::new(1);

            assert!(record_set.fill(&mut reader).unwrap());
            let parsed_record = record_set.iter().next().unwrap().unwrap();

            println!("{}", parsed_record.id_str());
        }
    }
}
