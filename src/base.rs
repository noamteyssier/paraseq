//! Reader state and methods shared by `fasta::Reader` and `fastq::Reader`.
//! Only the per-format `RecordSet` (scanning/parsing) differs; it plugs in via [`BatchSet`].

use std::io;

use crate::{Error, DEFAULT_MAX_RECORDS};

/// The per-format record set operations `ReaderBase` needs.
pub trait BatchSet: Default + Sized {
    fn with_capacity(capacity: usize) -> Self;
    fn fill<R: io::Read>(&mut self, reader: &mut ReaderBase<R, Self>) -> Result<bool, Error>;
    fn clear(&mut self);
    fn n_records(&self) -> usize;
    fn truncate(&mut self, n: usize);
    /// Raw buffer holding all bytes of the records in this set.
    fn buffer(&self) -> &[u8];
    /// Raw sequence length of the first record, if any.
    fn first_seq_len(&self) -> Result<Option<usize>, Error>;
}

pub struct ReaderBase<R: io::Read, S> {
    /// Handle to the underlying reader (byte stream)
    pub(crate) reader: R,
    /// Small buffer to hold incomplete records between reads
    pub(crate) overflow: Vec<u8>,
    /// Flag to indicate end of file
    pub(crate) eof: bool,
    /// Sets the maximum capcity of records in batches for parallel processing
    ///
    /// If not set, the default `RecordSet` capacity is used.
    pub(crate) batch_size: Option<usize>,
    /// Maximum number of records to process before stopping
    pub(crate) record_limit: Option<usize>,
    /// Running count of records already yielded by this reader, used to
    /// assign each parsed record its stable, global index in the file.
    pub(crate) total_records: u64,
    _set: std::marker::PhantomData<fn() -> S>,
}

impl<R: io::Read, S: BatchSet> ReaderBase<R, S> {
    pub fn new(reader: R) -> Self {
        Self {
            overflow: Vec::with_capacity(1024),
            reader,
            eof: false,
            batch_size: None,
            record_limit: None,
            total_records: 0,
            _set: std::marker::PhantomData,
        }
    }
    pub fn with_batch_size(reader: R, batch_size: usize) -> Result<Self, Error> {
        let mut reader = Self::new(reader);
        reader.set_batch_size(batch_size)?;
        Ok(reader)
    }

    /// Sets the maximum number of records per batch for parallel processing.
    pub fn set_batch_size(&mut self, batch_size: usize) -> Result<(), Error> {
        if batch_size == 0 {
            return Err(Error::InvalidBatchSize(batch_size));
        }
        self.batch_size = Some(batch_size);
        Ok(())
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
        if let Some(len) = rset.first_seq_len()? {
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
    pub fn new_record_set(&self) -> S {
        if let Some(batch_size) = self.batch_size {
            S::with_capacity(batch_size)
        } else {
            S::default()
        }
    }

    /// Initialize a new record set with a specified size
    pub fn new_record_set_with_size(&self, size: usize) -> S {
        S::with_capacity(size)
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
    pub fn reload(&mut self, rset: &mut S) {
        // These records are being unread, so un-count them; they'll be
        // reassigned the same indices when they're re-parsed.
        self.total_records = self.total_records.saturating_sub(rset.n_records() as u64);

        // A complete slice of the record sets buffer
        let buffer_slice = rset.buffer();

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

    /// Fill `rset`, honoring the record limit.
    pub(crate) fn fill_limited(&mut self, rset: &mut S) -> Result<bool, Error> {
        if let Some(0) = self.record_limit {
            return Ok(false);
        }
        let filled = rset.fill(self)?;
        if filled {
            if let Some(remaining) = &mut self.record_limit {
                let n = rset.n_records().min(*remaining);
                rset.truncate(n);
                *remaining -= n;
            }
        }
        Ok(filled)
    }
}
