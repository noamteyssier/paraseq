use std::io;

pub struct Reader<R: io::Read> {
    // Small buffer to hold incomplete records between reads
    overflow: Vec<u8>,
    reader: R,
    eof: bool,
}

impl<R: io::Read> Reader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            overflow: Vec::with_capacity(1024), // Start small, can tune this
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
}

#[derive(Debug)]
pub struct RecordSet {
    // Main buffer for records
    buffer: Vec<u8>,
    // Store newlines in buffer
    newlines: Vec<usize>,
    // Track the last byte position we've searched for newlines
    last_searched_pos: usize,
    // Position tracking for complete records
    positions: Vec<Positions>,
    capacity: usize,
    /// Average number of bytes per record
    avg_record_size: usize,
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
    fn find_newlines(&mut self) {
        let buffer = &self.buffer;
        memchr::memchr_iter(b'\n', &buffer[self.last_searched_pos..]).for_each(|i| {
            self.newlines.push(i + self.last_searched_pos + 1);
        });
        self.last_searched_pos = self.buffer.len();
    }

    /// Update the internal average record size
    fn update_avg_record_size(&mut self, total_bytes: usize) {
        let total_records = self.positions.len();
        if total_records > 0 {
            self.avg_record_size = total_bytes / total_records;
        }
    }

    /// Main function to fill the record set
    pub fn fill<R: io::Read>(&mut self, reader: &mut Reader<R>) -> io::Result<bool> {
        // Clear previous data
        self.clear();

        // First, copy any overflow from previous read
        if !reader.overflow.is_empty() {
            self.buffer.extend_from_slice(&reader.overflow);
            reader.overflow.clear();
        }
        self.find_newlines(); // Find newlines in overflow

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
                    self.find_newlines();
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }

        // Truncate to what we actually read
        self.buffer.truncate(current_pos);

        // Process all complete records in the buffer
        self.process_records(reader)
    }

    // Split out record processing to separate function
    fn process_records<R: io::Read>(&mut self, reader: &mut Reader<R>) -> io::Result<bool> {
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

#[derive(Debug, Clone, Copy)]
struct Positions {
    start: usize,
    seq_start: usize,
    sep_start: usize,
    qual_start: usize,
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
            return Err(Error::InvalidHeader);
        }

        // Check that separator starts with '+'
        if self.buffer[self.positions.sep_start] != b'+' {
            return Err(Error::InvalidSeparator);
        }

        // Check that sequence and quality lengths match
        if self.positions.sep_start - self.positions.seq_start
            != self.positions.end - self.positions.qual_start
        {
            return Err(Error::UnequalLengths);
        }

        Ok(())
    }

    /// Access the ID bytes
    #[inline]
    pub fn id(&self) -> &[u8] {
        self.access_buffer(
            self.positions.start + 1, // Skip '@'
            self.positions.seq_start,
        )
    }

    /// Access the sequence bytes
    #[inline]
    pub fn seq(&self) -> &[u8] {
        self.access_buffer(self.positions.seq_start, self.positions.sep_start)
    }

    /// Access the separator bytes
    #[inline]
    pub fn sep(&self) -> &[u8] {
        self.access_buffer(self.positions.sep_start, self.positions.qual_start)
    }

    /// Access the quality bytes
    #[inline]
    pub fn qual(&self) -> &[u8] {
        self.access_buffer(self.positions.qual_start, self.positions.end)
    }

    /// Performs the actual buffer access
    #[inline(always)]
    fn access_buffer(&self, left: usize, right: usize) -> &[u8] {
        unsafe {
            // SAFETY: We've checked that left and right are within bounds
            self.buffer.get_unchecked(left..right - 1)
        }
    }

    /// Convert ID to string reference (UTF-8)
    ///
    /// # Safety
    /// Will panic if ID is not valid UTF-8
    pub fn id_str(&self) -> &str {
        std::str::from_utf8(self.id()).unwrap()
    }

    /// Convert sequence to string reference (UTF-8)
    ///
    /// # Safety
    /// Will panic if sequence is not valid UTF-8
    pub fn seq_str(&self) -> &str {
        std::str::from_utf8(self.seq()).unwrap()
    }

    /// Convert separator to string reference (UTF-8)
    ///
    /// # Safety
    /// Will panic if separator is not valid UTF-8
    pub fn sep_str(&self) -> &str {
        std::str::from_utf8(self.sep()).unwrap()
    }

    /// Convert quality to string reference (UTF-8)
    ///
    /// # Safety
    /// Will panic if quality is not valid UTF-8
    pub fn qual_str(&self) -> &str {
        std::str::from_utf8(self.qual()).unwrap()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error reading from buffer: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid header")]
    InvalidHeader,

    #[error("Invalid separator")]
    InvalidSeparator,

    #[error("Unbounded positions")]
    UnboundedPositions,

    #[error("Sequence and quality lengths do not match")]
    UnequalLengths,
}
