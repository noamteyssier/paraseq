use std::borrow::Cow;

pub trait Record {
    /// Returns the Identifier of the record.
    fn id(&self) -> &[u8];

    /// Get the sequence as a borrowed slice.
    ///
    /// If the sequence is multi-line, the newline characters are removed.
    /// This incurs a copy, but will not allocate if the sequence is already a single line.
    fn seq(&self) -> Cow<[u8]>;

    /// Get the sequence as a borrowed slice.
    ///
    /// If the sequence is multi-line, the newline characters are *not* removed.
    /// This will not incur a copy or allocation, but is not guaranteed to be only nucleotides.
    fn seq_raw(&self) -> &[u8];

    /// Returns the quality scores of the record (if available).
    fn qual(&self) -> Option<&[u8]>;

    /// Convert ID to string reference (UTF-8)
    ///
    /// # Safety
    /// Will panic if ID is not valid UTF-8
    fn id_str(&self) -> &str {
        std::str::from_utf8(self.id()).unwrap()
    }

    /// Convert sequence to string (UTF-8).
    ///
    /// Performs a copy if the sequence is multi-line.
    ///
    /// # Safety
    /// Will panic if sequence is not valid UTF-8
    fn seq_str(&self) -> Cow<'_, str> {
        match self.seq() {
            Cow::Borrowed(bytes) => Cow::Borrowed(std::str::from_utf8(bytes).unwrap()),
            Cow::Owned(bytes) => Cow::Owned(String::from_utf8(bytes).unwrap()),
        }
    }

    /// Convert sequence to string reference (UTF-8).
    ///
    /// Will not allocate if the sequence is already a single line.
    /// May include newline characters if the sequence is multi-line.
    ///
    /// # Safety
    /// Will panic if sequence is not valid UTF-8
    fn seq_str_raw(&self) -> &str {
        std::str::from_utf8(self.seq_raw()).unwrap()
    }

    /// Convert quality to string reference (UTF-8)
    ///
    /// # Safety
    /// Will panic if quality is not valid UTF-8
    fn qual_str(&self) -> &str {
        if let Some(qual) = self.qual() {
            std::str::from_utf8(qual).unwrap()
        } else {
            ""
        }
    }
}
