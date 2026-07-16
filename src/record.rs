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
    fn seq(&self) -> Cow<'_, [u8]>;

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

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeRecord {
        id: Vec<u8>,
        seq: Vec<u8>,
        qual: Option<Vec<u8>>,
    }
    impl Record for FakeRecord {
        fn id(&self) -> &[u8] {
            &self.id
        }
        fn seq(&self) -> Cow<'_, [u8]> {
            Cow::Borrowed(&self.seq)
        }
        fn seq_raw(&self) -> &[u8] {
            &self.seq
        }
        fn qual(&self) -> Option<&[u8]> {
            self.qual.as_deref()
        }
    }

    #[test]
    fn test_seq_str_raw() {
        let record = FakeRecord {
            id: b"id1".to_vec(),
            seq: b"ACGT".to_vec(),
            qual: None,
        };
        assert_eq!(record.seq_str_raw(), "ACGT");
    }

    #[test]
    fn test_qual_str_absent() {
        let record = FakeRecord {
            id: b"id1".to_vec(),
            seq: b"ACGT".to_vec(),
            qual: None,
        };
        assert_eq!(record.qual_str(), "");
    }

    #[test]
    fn test_write_fastq_with_qual() {
        let record = FakeRecord {
            id: b"id1".to_vec(),
            seq: b"ACGT".to_vec(),
            qual: Some(b"IIII".to_vec()),
        };
        let mut buf = Vec::new();
        record.write_fastq(&mut buf).unwrap();
        assert_eq!(buf, b"@id1\nACGT\n+\nIIII\n");
    }

    #[test]
    fn test_write_fastq_default_qual_short() {
        let record = FakeRecord {
            id: b"id1".to_vec(),
            seq: b"ACGT".to_vec(),
            qual: None,
        };
        let mut buf = Vec::new();
        record.write_fastq(&mut buf).unwrap();
        assert_eq!(buf, b"@id1\nACGT\n+\n????\n");
    }

    #[test]
    fn test_write_fastq_default_qual_long() {
        // Longer than NUM_QUALITY_SCORES so the chunked-write branch is exercised.
        let seq = vec![b'A'; NUM_QUALITY_SCORES * 2 + 10];
        let record = FakeRecord {
            id: b"id1".to_vec(),
            seq: seq.clone(),
            qual: None,
        };
        let mut buf = Vec::new();
        record.write_fastq(&mut buf).unwrap();

        let expected_qual: Vec<u8> =
            std::iter::repeat_n(DEFAULT_QUALITY_SCORE, seq.len()).collect();
        let mut expected = Vec::new();
        expected.extend_from_slice(b"@id1\n");
        expected.extend_from_slice(&seq);
        expected.extend_from_slice(b"\n+\n");
        expected.extend_from_slice(&expected_qual);
        expected.extend_from_slice(b"\n");

        assert_eq!(buf, expected);
    }
}
