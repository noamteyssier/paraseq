use std::borrow::Cow;
use std::io::Write;

pub const DEFAULT_QUALITY_SCORE: u8 = b'?';
pub const NUM_QUALITY_SCORES: usize = 1024;
pub const COMPTIME_QUALITY_SCORES: &[u8] = &[DEFAULT_QUALITY_SCORE; NUM_QUALITY_SCORES];

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

    /// Writes the record in FASTA format to a Write.
    fn write_fasta<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(b">")?;
        writer.write_all(self.id())?;
        writer.write_all(b"\n")?;
        writer.write_all(&self.seq())?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    /// Writes the record in FASTQ format to a Write.
    ///
    /// If the record does not have quality scores, default scores are used.
    fn write_fastq<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let seq = self.seq();
        writer.write_all(b"@")?;
        writer.write_all(self.id())?;
        writer.write_all(b"\n")?;
        writer.write_all(&seq)?;
        writer.write_all(b"\n+\n")?;
        if let Some(qual) = self.qual() {
            writer.write_all(qual)?;
        } else if seq.len() > NUM_QUALITY_SCORES {
            // Write default quality scores in chunks of NUM_QUALITY_SCORES
            for _ in 0..seq.len() / NUM_QUALITY_SCORES {
                writer.write_all(COMPTIME_QUALITY_SCORES)?;
            }
            // Write remainder
            writer.write_all(&COMPTIME_QUALITY_SCORES[..seq.len() % NUM_QUALITY_SCORES])?;
        } else {
            writer.write_all(&COMPTIME_QUALITY_SCORES[..seq.len()])?;
        }
        writer.write_all(b"\n")?;
        Ok(())
    }
}
