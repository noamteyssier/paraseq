use thiserror::Error;

use crate::Record;

/// Errors returned by built-in record validation helpers.
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("record IDs do not represent a valid pair: '{id1}' and '{id2}'")]
    PairNameMismatch { id1: String, id2: String },

    #[error("record IDs appear to have the same mate designation: '{id1}' and '{id2}'")]
    SameMateDesignation { id1: String, id2: String },
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
        id1: String::from_utf8_lossy(id1).into_owned(),
        id2: String::from_utf8_lossy(id2).into_owned(),
    }
}

fn same_mate_designation(id1: &[u8], id2: &[u8]) -> ValidationError {
    ValidationError::SameMateDesignation {
        id1: String::from_utf8_lossy(id1).into_owned(),
        id2: String::from_utf8_lossy(id2).into_owned(),
    }
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
    use super::{RecordValidation, ValidationError};
    use crate::Record;
    use std::borrow::Cow;

    struct TestRecord {
        id: Vec<u8>,
    }

    impl TestRecord {
        fn new(id: &str) -> Self {
            Self {
                id: id.as_bytes().to_vec(),
            }
        }
    }

    impl Record for TestRecord {
        fn id(&self) -> &[u8] {
            &self.id
        }

        fn seq(&self) -> Cow<'_, [u8]> {
            Cow::Borrowed(b"")
        }

        fn seq_raw(&self) -> &[u8] {
            b""
        }

        fn qual(&self) -> Option<&[u8]> {
            None
        }
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
