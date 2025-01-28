pub struct Reader<R: std::io::Read> {
    // Small buffer to hold incomplete records between reads
    overflow: Vec<u8>,
    reader: R,
    eof: bool,
}

impl<R: std::io::Read> Reader<R> {
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

#[derive(Debug, Clone, Copy)]
struct Positions {
    start: usize,
    seq_start: usize,
    sep_start: usize,
    qual_start: usize,
    end: usize,
}

impl RecordSet {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(64 * 1024), // 256KB default
            newlines: Vec::new(),
            last_searched_pos: 0,
            positions: Vec::with_capacity(capacity),
            capacity,
            avg_record_size: 1024, // 1KB default
        }
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
    pub fn fill<R: std::io::Read>(&mut self, reader: &mut Reader<R>) -> std::io::Result<bool> {
        // Clear previous data
        self.buffer.clear();
        self.newlines.clear();
        self.positions.clear();
        self.last_searched_pos = 0;

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
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }

        // Truncate to what we actually read
        self.buffer.truncate(current_pos);

        // Process all complete records in the buffer
        self.process_records(reader)
    }

    // Split out record processing to separate function
    fn process_records<R: std::io::Read>(
        &mut self,
        reader: &mut Reader<R>,
    ) -> std::io::Result<bool> {
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
            // println!(
            //     "COPYING {} bytes to overflow (complete records)",
            //     self.buffer.len() - last_complete_newline
            // );
            reader
                .overflow
                .extend_from_slice(&self.buffer[last_complete_newline..]);
            self.buffer.truncate(last_complete_newline);
        } else if !self.buffer.is_empty() {
            // println!(
            //     "COPYING {} bytes to overflow (no complete records)",
            //     self.buffer.len()
            // );
            reader.overflow.extend_from_slice(&self.buffer);
            self.buffer.clear();
        }

        Ok(!self.positions.is_empty())
    }
    // Iterator over complete records
    pub fn iter(&self) -> impl Iterator<Item = Record<'_>> {
        self.positions.iter().map(move |&pos| Record {
            id: &self.buffer[pos.start + 1..pos.seq_start - 1],
            seq: &self.buffer[pos.seq_start..pos.sep_start - 1],
            qual: &self.buffer[pos.qual_start..pos.end - 1],
        })
    }
}

#[derive(Debug)]
pub struct Record<'a> {
    pub id: &'a [u8],
    pub seq: &'a [u8],
    pub qual: &'a [u8],
}
