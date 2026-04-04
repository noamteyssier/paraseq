use thiserror::Error;

use crate::Record;

/// Built-in nucleotide alphabets supported by record validation helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NucleotideAlphabet {
    Dna,
    Rna,
    IupacDna,
    IupacRna,
}

/// Errors returned by built-in record validation helpers.
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("record IDs do not represent a valid pair: '{id1}' and '{id2}'")]
    PairNameMismatch { id1: String, id2: String },

    #[error("record IDs appear to have the same mate designation: '{id1}' and '{id2}'")]
    SameMateDesignation { id1: String, id2: String },

    #[error("record '{id}' contains invalid base '{base}' at position {position}")]
    InvalidBase {
        id: String,
        base: char,
        position: usize,
    },

    #[error("record '{id}' contains invalid quality score byte {score} at position {position}")]
    InvalidQualityScore {
        id: String,
        score: u8,
        position: usize,
    },

    #[error("record '{id}' does not contain quality scores")]
    MissingQualityScores { id: String },
}

/// Extension trait for user-defined record validation.
pub trait RecordValidation: Record {
    /// Validate a record with a user-provided callback.
    fn validate_with<E, F>(&self, func: F) -> Result<(), E>
    where
        Self: Sized,
        F: FnOnce(&Self) -> Result<(), E>,
    {
        func(self)
    }

    /// Validate a pair of records with a user-provided callback.
    fn validate_pair_with<R, E, F>(&self, other: &R, func: F) -> Result<(), E>
    where
        Self: Sized,
        R: Record + ?Sized,
        F: FnOnce(&Self, &R) -> Result<(), E>,
    {
        func(self, other)
    }

    /// Validate that the record sequence belongs to a supported nucleotide alphabet.
    fn check_alphabet(&self, alphabet: NucleotideAlphabet) -> Result<(), ValidationError> {
        for (position, base) in self.seq_raw().iter().copied().enumerate() {
            let is_valid = match alphabet {
                NucleotideAlphabet::Dna => is_dna_base(base),
                NucleotideAlphabet::Rna => is_rna_base(base),
                NucleotideAlphabet::IupacDna => is_iupac_dna_base(base),
                NucleotideAlphabet::IupacRna => is_iupac_rna_base(base),
            };

            if !is_valid {
                return Err(ValidationError::InvalidBase {
                    id: record_id(self.id()),
                    base: char::from(base),
                    position,
                });
            }
        }

        Ok(())
    }

    /// Validate that the record sequence contains only conservative DNA bases (`ACGTN`).
    fn check_dna(&self) -> Result<(), ValidationError> {
        self.check_alphabet(NucleotideAlphabet::Dna)
    }

    /// Validate that the record sequence contains only conservative RNA bases (`ACGUN`).
    fn check_rna(&self) -> Result<(), ValidationError> {
        self.check_alphabet(NucleotideAlphabet::Rna)
    }

    /// Validate that the record sequence contains only IUPAC DNA bases.
    fn check_iupac_dna(&self) -> Result<(), ValidationError> {
        self.check_alphabet(NucleotideAlphabet::IupacDna)
    }

    /// Validate that the record sequence contains only IUPAC RNA bases.
    fn check_iupac_rna(&self) -> Result<(), ValidationError> {
        self.check_alphabet(NucleotideAlphabet::IupacRna)
    }

    /// Validate that record qualities are present and fall within the printable PHRED+33 range.
    fn check_phred(&self) -> Result<(), ValidationError> {
        let qualities = self
            .qual()
            .ok_or_else(|| ValidationError::MissingQualityScores {
                id: record_id(self.id()),
            })?;

        for (position, score) in qualities.iter().copied().enumerate() {
            if !(b'!'..=b'~').contains(&score) {
                return Err(ValidationError::InvalidQualityScore {
                    id: record_id(self.id()),
                    score,
                    position,
                });
            }
        }

        Ok(())
    }

    /// Validate that two record IDs represent a paired read.
    ///
    /// This check intentionally implements a conservative set of common paired-read
    /// ID conventions. It accepts:
    ///
    /// - exact ID matches
    /// - `/1` and `/2` suffixes
    /// - CASAVA-style mate fields (`1:` and `2:` after the first ASCII whitespace)
    ///
    /// It does not attempt to infer arbitrary platform- or pipeline-specific naming
    /// conventions. For custom pairing rules, use [`RecordValidation::validate_pair_with`].
    fn check_pair<R>(&self, other: &R) -> Result<(), ValidationError>
    where
        R: Record + ?Sized,
    {
        let id1 = self.id();
        let id2 = other.id();

        if id1 == id2 {
            return Ok(());
        }

        match (id1, id2) {
            // Common FASTQ pairing convention where mates are denoted by `/1` and `/2`.
            (left_id @ [.., b'/', b'1'], right_id @ [.., b'/', b'2'])
                if left_id[..left_id.len() - 2] == right_id[..right_id.len() - 2] =>
            {
                return Ok(());
            }

            // Same as above, but with the mate order reversed.
            (left_id @ [.., b'/', b'2'], right_id @ [.., b'/', b'1'])
                if left_id[..left_id.len() - 2] == right_id[..right_id.len() - 2] =>
            {
                return Ok(());
            }

            // Matching prefixes with `/1` on both records indicates duplicate mate designation.
            (left_id @ [.., b'/', b'1'], right_id @ [.., b'/', b'1'])
                if left_id[..left_id.len() - 2] == right_id[..right_id.len() - 2] =>
            {
                return Err(same_mate_designation(id1, id2));
            }

            // Matching prefixes with `/2` on both records indicates duplicate mate designation.
            (left_id @ [.., b'/', b'2'], right_id @ [.., b'/', b'2'])
                if left_id[..left_id.len() - 2] == right_id[..right_id.len() - 2] =>
            {
                return Err(same_mate_designation(id1, id2));
            }

            // fallthrough for CASAVA-style headers as well as unrecognized or incorrect patterns
            _ => {}
        }

        match (split_casava_id(id1), split_casava_id(id2)) {
            // CASAVA-style headers separate the read name from mate metadata with whitespace.
            (Some((name1, mate1, rest1)), Some((name2, mate2, rest2)))
                if name1 == name2 && rest1 == rest2 =>
            {
                match (mate1, mate2) {
                    // Mate annotations are complementary (`1:` vs `2:`).
                    (b'1', b'2') => Ok(()),
                    (b'2', b'1') => Ok(()),
                    // The records agree on the read name and metadata, but claim the same mate.
                    (b'1', b'1') => Err(same_mate_designation(id1, id2)),
                    (b'2', b'2') => Err(same_mate_designation(id1, id2)),
                    _ => Err(pair_name_mismatch(id1, id2)),
                }
            }
            _ => Err(pair_name_mismatch(id1, id2)),
        }
    }
}

impl<T> RecordValidation for T where T: Record + ?Sized {}

fn pair_name_mismatch(id1: &[u8], id2: &[u8]) -> ValidationError {
    ValidationError::PairNameMismatch {
        id1: record_id(id1),
        id2: record_id(id2),
    }
}

fn same_mate_designation(id1: &[u8], id2: &[u8]) -> ValidationError {
    ValidationError::SameMateDesignation {
        id1: record_id(id1),
        id2: record_id(id2),
    }
}

fn record_id(id: &[u8]) -> String {
    String::from_utf8_lossy(id).into_owned()
}

fn is_dna_base(base: u8) -> bool {
    matches!(
        base,
        b'A' | b'C' | b'G' | b'T' | b'N' | b'a' | b'c' | b'g' | b't' | b'n'
    )
}

fn is_rna_base(base: u8) -> bool {
    matches!(
        base,
        b'A' | b'C' | b'G' | b'U' | b'N' | b'a' | b'c' | b'g' | b'u' | b'n'
    )
}

fn is_iupac_dna_base(base: u8) -> bool {
    matches!(
        base,
        b'A' | b'C'
            | b'G'
            | b'T'
            | b'R'
            | b'Y'
            | b'S'
            | b'W'
            | b'K'
            | b'M'
            | b'B'
            | b'D'
            | b'H'
            | b'V'
            | b'N'
            | b'a'
            | b'c'
            | b'g'
            | b't'
            | b'r'
            | b'y'
            | b's'
            | b'w'
            | b'k'
            | b'm'
            | b'b'
            | b'd'
            | b'h'
            | b'v'
            | b'n'
    )
}

fn is_iupac_rna_base(base: u8) -> bool {
    matches!(
        base,
        b'A' | b'C'
            | b'G'
            | b'U'
            | b'R'
            | b'Y'
            | b'S'
            | b'W'
            | b'K'
            | b'M'
            | b'B'
            | b'D'
            | b'H'
            | b'V'
            | b'N'
            | b'a'
            | b'c'
            | b'g'
            | b'u'
            | b'r'
            | b'y'
            | b's'
            | b'w'
            | b'k'
            | b'm'
            | b'b'
            | b'd'
            | b'h'
            | b'v'
            | b'n'
    )
}

fn split_casava_id(id: &[u8]) -> Option<(&[u8], u8, &[u8])> {
    let split_at = id.iter().position(|b| b.is_ascii_whitespace())?;
    let (name, tail) = id.split_at(split_at);
    let tail = trim_ascii_start(tail);
    let (mate, rest) = tail.split_first()?;

    if !matches!(mate, b'1' | b'2') {
        return None;
    }

    let rest = rest.strip_prefix(b":")?;
    Some((name, *mate, rest))
}

fn trim_ascii_start(bytes: &[u8]) -> &[u8] {
    let offset = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    &bytes[offset..]
}

#[cfg(test)]
mod tests {
    use super::{NucleotideAlphabet, RecordValidation, ValidationError};
    use crate::Record;
    use std::borrow::Cow;

    struct TestRecord {
        id: Vec<u8>,
        seq: Vec<u8>,
        qual: Option<Vec<u8>>,
    }

    impl TestRecord {
        fn new(id: &str) -> Self {
            Self {
                id: id.as_bytes().to_vec(),
                seq: Vec::new(),
                qual: None,
            }
        }

        fn with_seq(mut self, seq: &str) -> Self {
            self.seq = seq.as_bytes().to_vec();
            self
        }

        fn with_qual(mut self, qual: &[u8]) -> Self {
            self.qual = Some(qual.to_vec());
            self
        }
    }

    impl Record for TestRecord {
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
    fn dna_alphabet_accepts_conservative_bases() {
        let record = TestRecord::new("dna").with_seq("ACGTNacgtn");

        assert!(record.check_dna().is_ok());
    }

    #[test]
    fn dna_alphabet_rejects_rna_base() {
        let record = TestRecord::new("dna").with_seq("ACGUN");

        assert!(matches!(
            record.check_dna(),
            Err(ValidationError::InvalidBase {
                base: 'U',
                position: 3,
                ..
            })
        ));
    }

    #[test]
    fn rna_alphabet_accepts_conservative_bases() {
        let record = TestRecord::new("rna").with_seq("ACGUNacgun");

        assert!(record.check_rna().is_ok());
    }

    #[test]
    fn rna_alphabet_rejects_dna_base() {
        let record = TestRecord::new("rna").with_seq("ACGTN");

        assert!(matches!(
            record.check_rna(),
            Err(ValidationError::InvalidBase {
                base: 'T',
                position: 3,
                ..
            })
        ));
    }

    #[test]
    fn iupac_dna_accepts_ambiguity_codes() {
        let record = TestRecord::new("iupac-dna").with_seq("ACGTRYSWKMBDHVN");

        assert!(record.check_iupac_dna().is_ok());
    }

    #[test]
    fn iupac_rna_accepts_u_but_not_t() {
        let record = TestRecord::new("iupac-rna").with_seq("ACGURYSWKMBDHVN");

        assert!(record.check_iupac_rna().is_ok());
        assert!(matches!(
            TestRecord::new("iupac-rna")
                .with_seq("ACGTR")
                .check_iupac_rna(),
            Err(ValidationError::InvalidBase {
                base: 'T',
                position: 3,
                ..
            })
        ));
    }

    #[test]
    fn generic_alphabet_dispatch_matches_specific_helpers() {
        let record = TestRecord::new("dispatch").with_seq("ACGUN");

        assert!(record.check_alphabet(NucleotideAlphabet::Rna).is_ok());
        assert!(matches!(
            record.check_alphabet(NucleotideAlphabet::Dna),
            Err(ValidationError::InvalidBase { .. })
        ));
    }

    #[test]
    fn phred_accepts_printable_fastq_scores() {
        let record = TestRecord::new("qual").with_qual(b"!~ABC");

        assert!(record.check_phred().is_ok());
    }

    #[test]
    fn phred_rejects_missing_quality_scores() {
        let record = TestRecord::new("qual");

        assert!(matches!(
            record.check_phred(),
            Err(ValidationError::MissingQualityScores { .. })
        ));
    }

    #[test]
    fn phred_rejects_out_of_range_scores() {
        let record = TestRecord::new("qual").with_qual(&[b'!', 31, b'~']);

        assert!(matches!(
            record.check_phred(),
            Err(ValidationError::InvalidQualityScore {
                score: 31,
                position: 1,
                ..
            })
        ));
    }

    #[test]
    fn exact_ids_pass_pair_validation() {
        let left = TestRecord::new("read123");
        let right = TestRecord::new("read123");

        assert!(left.check_pair(&right).is_ok());
    }

    #[test]
    fn slash_suffixes_pass_pair_validation() {
        let left = TestRecord::new("read123/1");
        let right = TestRecord::new("read123/2");

        assert!(left.check_pair(&right).is_ok());
    }

    #[test]
    fn reversed_slash_suffixes_pass_pair_validation() {
        let left = TestRecord::new("read123/2");
        let right = TestRecord::new("read123/1");

        assert!(left.check_pair(&right).is_ok());
    }

    #[test]
    fn same_slash_mate_fails_validation() {
        let left = TestRecord::new("read123/1");
        let right = TestRecord::new("read123/1");

        assert!(matches!(
            left.check_pair(&right),
            Err(ValidationError::SameMateDesignation { .. })
        ));
    }

    #[test]
    fn casava_mates_pass_pair_validation() {
        let left = TestRecord::new("read123 1:N:0:ATCG");
        let right = TestRecord::new("read123 2:N:0:ATCG");

        assert!(left.check_pair(&right).is_ok());
    }

    #[test]
    fn reversed_casava_mates_pass_pair_validation() {
        let left = TestRecord::new("read123 2:N:0:ATCG");
        let right = TestRecord::new("read123 1:N:0:ATCG");

        assert!(left.check_pair(&right).is_ok());
    }

    #[test]
    fn same_casava_mate_fails_validation() {
        let left = TestRecord::new("read123 1:N:0:ATCG");
        let right = TestRecord::new("read123 1:N:0:ATCG");

        assert!(matches!(
            left.check_pair(&right),
            Err(ValidationError::SameMateDesignation { .. })
        ));
    }

    #[test]
    fn casava_tail_mismatch_fails_validation() {
        let left = TestRecord::new("read123 1:N:0:ATCG");
        let right = TestRecord::new("read123 2:Y:0:ATCG");

        assert!(matches!(
            left.check_pair(&right),
            Err(ValidationError::PairNameMismatch { .. })
        ));
    }

    #[test]
    fn unsupported_pair_convention_fails_conservatively() {
        let left = TestRecord::new("read123-1");
        let right = TestRecord::new("read123-2");

        assert!(matches!(
            left.check_pair(&right),
            Err(ValidationError::PairNameMismatch { .. })
        ));
    }

    #[test]
    fn unequal_exact_ids_fail_validation() {
        let left = TestRecord::new("read123");
        let right = TestRecord::new("read456");

        assert!(matches!(
            left.check_pair(&right),
            Err(ValidationError::PairNameMismatch { .. })
        ));
    }
}
