use std::borrow::Cow;
use std::io;

#[cfg(feature = "niffler")]
use crate::BoxedReader;
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
    /// Maximum number of records to process before stopping
    record_limit: Option<usize>,
    /// Running count of records already yielded by this reader, used to
    /// assign each parsed record its stable, global index in the file.
    total_records: u64,
}

#[cfg(feature = "niffler")]
impl Reader<BoxedReader> {
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
impl Reader<BoxedReader> {
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
impl Reader<BoxedReader> {
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
impl Reader<BoxedReader> {
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
            record_limit: None,
            total_records: 0,
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

    /// Limit processing to the first `n` records.
    ///
    /// When used with parallel processing, `fill()` will truncate batches to
    /// stay within the limit and return `false` once the limit is reached,
    /// stopping all worker threads cleanly.
    pub fn set_record_limit(&mut self, n: usize) {
        self.record_limit = Some(n);
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
        // These records are being unread, so un-count them; they'll be
        // reassigned the same indices when they're re-parsed.
        self.total_records = self
            .total_records
            .saturating_sub(rset.positions.len() as u64);

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
    buffer: Vec<u8>,
    /// Number of newlines seen in the current incomplete record (0..=3)
    pending_nl: u8,
    /// Byte offsets (one past '\n') for the pending record's newlines
    pending_nl_pos: [usize; 3],
    /// Byte offset where the current record started
    record_start: usize,
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
        Self::new(DEFAULT_MAX_RECORDS)
    }
}

impl RecordSet {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(256 * 1024), // 256KB default
            pending_nl: 0,
            pending_nl_pos: [0; 3],
            record_start: 0,
            positions: Vec::with_capacity(capacity),
            capacity,
            avg_record_size: 1024, // 1KB default
            base_index: 0,
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.positions.clear();
        self.pending_nl = 0;
        self.record_start = 0;
    }

    /// Returns the number of records currently in this set.
    pub fn n_records(&self) -> usize {
        self.positions.len()
    }

    /// Truncate the record set to at most `n` records.
    pub fn truncate(&mut self, n: usize) {
        self.positions.truncate(n);
    }

    /// Update the internal average record size
    fn update_avg_record_size(&mut self, total_bytes: usize) {
        let total_records = self.positions.len();
        if let Some(avg) = total_bytes.checked_div(total_records) {
            self.avg_record_size = avg;
        }
    }

    /// Scan bytes `search_from..search_to` in the buffer, building Positions inline.
    /// Returns true if capacity was reached (caller should stop reading).
    fn scan_for_records(&mut self, search_from: usize, search_to: usize) -> bool {
        for nl in memchr::memchr_iter(b'\n', &self.buffer[search_from..search_to]) {
            let abs = nl + search_from + 1; // one past the '\n'
            if self.pending_nl < 3 {
                self.pending_nl_pos[self.pending_nl as usize] = abs;
                self.pending_nl += 1;
            } else {
                self.positions.push(Positions {
                    start: self.record_start,
                    seq_start: self.pending_nl_pos[0],
                    sep_start: self.pending_nl_pos[1],
                    qual_start: self.pending_nl_pos[2],
                    qual_end: abs - 1,
                    end: abs,
                });
                self.record_start = abs;
                self.pending_nl = 0;
                if self.positions.len() >= self.capacity {
                    return true;
                }
            }
        }
        false
    }

    /// Main function to fill the record set
    ///
    /// Returns true if records were added to the set, false if not
    pub fn fill<R: io::Read>(&mut self, reader: &mut Reader<R>) -> Result<bool, Error> {
        self.clear();
        self.base_index = reader.total_records;

        // Copy any overflow from the previous read
        if !reader.overflow.is_empty() {
            self.buffer.extend_from_slice(&reader.overflow);
            reader.overflow.clear();
        }

        // Scan overflow bytes; may already give us a full batch
        let overflow_end = self.buffer.len();
        if overflow_end > 0 && self.scan_for_records(0, overflow_end) {
            return self.finalize(reader);
        }

        if self.positions.len() >= self.capacity {
            return self.finalize(reader);
        }

        let records_needed = self.capacity.saturating_sub(self.positions.len());
        let target_read_size = self
            .avg_record_size
            .saturating_mul(records_needed)
            .saturating_add(self.avg_record_size * 2);

        let mut current_pos = self.buffer.len();
        let mut target_len = current_pos + target_read_size;

        loop {
            if reader.eof {
                break;
            }

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
                    let prev = current_pos;
                    current_pos += n;
                    if self.scan_for_records(prev, current_pos) {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }

        self.finalize(reader)
    }

    fn finalize<R: io::Read>(&mut self, reader: &mut Reader<R>) -> Result<bool, Error> {
        // Handle a final record with no trailing newline at EOF
        if reader.eof && self.pending_nl == 3 {
            self.buffer.push(b'\n');
            let abs = self.buffer.len();
            self.positions.push(Positions {
                start: self.record_start,
                seq_start: self.pending_nl_pos[0],
                sep_start: self.pending_nl_pos[1],
                qual_start: self.pending_nl_pos[2],
                qual_end: abs - 1,
                end: abs,
            });
            self.record_start = abs;
            self.pending_nl = 0;
        }

        if !self.positions.is_empty() {
            let last_end = self.record_start;
            self.update_avg_record_size(last_end);
            reader.overflow.extend_from_slice(&self.buffer[last_end..]);
            self.buffer.truncate(last_end);
        } else if !self.buffer.is_empty() {
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
    sep_start: usize,
    qual_start: usize,
    qual_end: usize,
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
        if self.positions.sep_start - self.positions.seq_start - 1
            != self.positions.qual_end - self.positions.qual_start
        {
            return Err(Error::UnequalLengths(
                self.positions.sep_start - self.positions.seq_start - 1, // subtract 1 for embedded newline
                self.positions.qual_end - self.positions.qual_start,
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

    /// Returns the record's 0-based index within the original file.
    #[inline]
    #[must_use]
    pub fn index(&self) -> u64 {
        self.index
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

    fn seq(&self) -> std::borrow::Cow<'_, [u8]> {
        Cow::Borrowed(self.seq_raw())
    }

    #[inline]
    fn seq_raw(&self) -> &[u8] {
        self.access_buffer(self.positions.seq_start, self.positions.sep_start)
    }

    fn qual(&self) -> Option<&[u8]> {
        Some(self.access_buffer(
            self.positions.qual_start,
            self.positions.qual_end.max(self.positions.end),
        ))
    }

    fn index(&self) -> u64 {
        self.index()
    }
}

impl<R> GenericReader for crate::fastq::Reader<R>
where
    R: io::Read + Send,
{
    type RecordSet = crate::fastq::RecordSet;
    type Error = crate::Error;
    type RefRecord<'a> = crate::fastq::RefRecord<'a>;

    fn new_record_set(&self) -> Self::RecordSet {
        if let Some(batch_size) = self.batch_size {
            Self::RecordSet::new(batch_size)
        } else {
            Self::RecordSet::default()
        }
    }

    fn fill(&mut self, record: &mut Self::RecordSet) -> std::result::Result<bool, Self::Error> {
        if let Some(0) = self.record_limit {
            return Ok(false);
        }
        let filled = record.fill(self)?;
        if filled {
            if let Some(remaining) = &mut self.record_limit {
                let n = record.n_records().min(*remaining);
                record.truncate(n);
                *remaining -= n;
            }
        }
        Ok(filled)
    }

    fn iter(
        record_set: &Self::RecordSet,
    ) -> impl ExactSizeIterator<Item = std::result::Result<Self::RefRecord<'_>, Self::Error>> {
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

    // Helper function to create a valid FASTQ record
    fn create_test_record(id: &str, seq: &str, sep: &str, qual: &str) -> String {
        format!("@{id}\n{seq}\n+{sep}\n{qual}\n")
    }

    fn make_fastq(n: usize) -> String {
        (0..n)
            .map(|i| create_test_record(&format!("seq{i}"), "ACTG", "", "IIII"))
            .collect()
    }

    #[test]
    fn test_reload() {
        const N_RECORDS: usize = 50;
        const PREFILL: usize = 7;

        let mut reader = Reader::new(Cursor::new(make_fastq(N_RECORDS)));
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

        let mut reader = Reader::new(Cursor::new(make_fastq(N_RECORDS)));
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

        let mut reader = Reader::new(Cursor::new(make_fastq(N_RECORDS)));
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
        let mut reader = Reader::new(Cursor::new(make_fastq(50)));
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
            let mut reader = Reader::from_optional_path(None::<&str>).unwrap();
            let mut num_records = 0;
            let mut rset = reader.new_record_set();
            while rset.fill(&mut reader).unwrap() {
                num_records += rset.iter().map(Result::unwrap).count();
            }
            eprintln!("STDIN_COUNT={num_records}");
            return;
        }

        let output = crate::test_util::run_with_piped_stdin(
            "fastq::tests::test_from_stdin",
            make_fastq(20).as_bytes(),
        );
        assert!(output.status.success(), "child failed: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("STDIN_COUNT=20"), "stderr: {stderr}");
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
        let partial_record = "@test1\nACTG\n+\nIIII"; // Missing final newline
        let mut reader = Reader::new(Cursor::new(partial_record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());

        for record in record_set.iter() {
            let record = record.unwrap();
            assert_eq!(record.seq().len(), record.qual().unwrap().len());
        }
        assert!(record_set.iter().next().unwrap().is_ok());
    }

    #[test]
    fn test_partial_record_invalid() {
        let partial_record = "@test1\nACTG\n+\nII"; // Missing final newline with broken quality scores
        let mut reader = Reader::new(Cursor::new(partial_record));
        let mut record_set = RecordSet::new(1);

        assert!(record_set.fill(&mut reader).unwrap());
        assert!(record_set.iter().next().unwrap().is_err());
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
        assert_eq!(record_set.pending_nl, 0);
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
