use std::str::FromStr;

use crate::{validation::error::ValidationError, Record};

#[derive(Debug, Default, Clone, Copy)]
pub enum ValidationMode {
    #[default]
    Skip,
    Normal,
    Strict,
}

impl FromStr for ValidationMode {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_lower = s.to_lowercase();
        match s_lower.as_str() {
            "skip" => Ok(Self::Skip),
            "normal" => Ok(Self::Normal),
            "strict" => Ok(Self::Strict),
            val => Err(ValidationError::InvalidMode(val.to_string())),
        }
    }
}

impl From<bool> for ValidationMode {
    fn from(value: bool) -> Self {
        match value {
            true => ValidationMode::Normal,
            false => ValidationMode::Skip,
        }
    }
}

impl ValidationMode {
    pub fn check_pair_id_suffixes<R: Record>(
        self,
        rec1: &R,
        rec2: &R,
    ) -> Result<(), ValidationError> {
        if !matches!(self, ValidationMode::Strict) {
            return Ok(());
        };

        let id1 = rec1.id();
        let id2 = rec2.id();

        if !id1.ends_with("/1".as_bytes()) {
            let debug_str = str::from_utf8(id1)?.to_owned();
            return Err(ValidationError::MissingForwardMateSuffix(debug_str));
        }

        if !id2.ends_with("/2".as_bytes()) {
            let debug_str = str::from_utf8(id2)?.to_owned();
            return Err(ValidationError::MissingReverseMateSuffix(debug_str));
        }

        Ok(())
    }

    pub fn check_read_ids<R: Record>(self, rec1: &R, rec2: &R) -> Result<(), ValidationError> {
        if matches!(self, ValidationMode::Skip) {
            return Ok(());
        }

        let id1 = rec1.id();
        let id2 = rec2.id();

        let ids_match = match (id1, id2) {
            // Case 1: both have the /1 and /2 suffixes
            (a @ [.., b'/', b'1'], b @ [.., b'/', b'2'])
                if a[..a.len() - 2] == b[..b.len() - 2] =>
            {
                true
            }

            // Case 2: neither has the suffix — just compare the entire slices
            (a, b) if a == b => true,

            // Case 3: any other combination (mismatched suffixes, differing bases, etc.)
            _ => false,
        };

        let id1_debug_str = str::from_utf8(id1)?.to_owned();
        let id2_debug_str = str::from_utf8(id2)?.to_owned();
        if !ids_match {
            return Err(ValidationError::PairedRecordsWithDifferentIds(
                id1_debug_str,
                id2_debug_str,
            ));
        }

        Ok(())
    }

    pub fn validate_with<F>(self, func: F) -> Result<(), ValidationError>
    where
        F: FnOnce(ValidationMode) -> Result<(), ValidationError>,
    {
        func(self)
    }

    pub fn validate_record_with<R, F>(self, func: F, record: &R) -> Result<(), ValidationError>
    where
        R: Record,
        F: FnOnce(ValidationMode, &R) -> Result<(), ValidationError>,
    {
        func(self, record)
    }

    pub fn validate_pair_with<R, F>(
        self,
        func: F,
        rec1: &R,
        rec2: &R,
    ) -> Result<(), ValidationError>
    where
        R: Record,
        F: FnOnce(ValidationMode, &R, &R) -> Result<(), ValidationError>,
    {
        func(self, rec1, rec2)
    }

    pub fn run_all_checks<R: Record>(self, rec1: &R, rec2: &R) -> Result<(), ValidationError> {
        self.check_pair_id_suffixes(rec1, rec2)?;
        self.check_read_ids(rec1, rec2)?;

        Ok(())
    }
}

pub mod error {

    use std::str::Utf8Error;
    use thiserror::Error;

    pub type Result<T> = std::result::Result<T, ValidationError>;

    #[derive(Debug, Error)]
    pub enum ValidationError {
        /// Invalid validation mode value provided
        #[error("Invalid validation mode value provided: '{0}'. The mode must be 'strict', 'normal', or 'skip'.")]
        InvalidMode(String),

        /// Error parsing valid UTF-8, indicating invalid characters in the input data.
        #[error("Invalid UTF-8 error: {0}")]
        Utf8Error(#[from] Utf8Error),

        #[error("FASTQ Sequence length ({0}) and quality length ({1}) do not match")]
        UnequalLengths(usize, usize),

        #[error("Paired records with different identifiers encountered when expected: {0} != {1}")]
        PairedRecordsWithDifferentIds(String, String),

        #[error("Validation in strict mode requires that forward read IDs come with a '/1' suffix, whereas the forward read id was '{0}'.")]
        MissingForwardMateSuffix(String),

        #[error("Validation in strict mode requires that reverse read IDs come with a '/2' suffix, whereas the reverse read id was '{0}'.")]
        MissingReverseMateSuffix(String),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRecord {
        id: Vec<u8>,
    }

    impl Record for TestRecord {
        fn id(&self) -> &[u8] {
            &self.id
        }

        fn seq(&self) -> std::borrow::Cow<'_, [u8]> {
            std::borrow::Cow::Borrowed(b"ACTG")
        }

        fn seq_raw(&self) -> &[u8] {
            b"ACTG"
        }

        fn qual(&self) -> Option<&[u8]> {
            Some(b"IIII")
        }
    }

    #[test]
    fn test_skip_mode_accepts_everything() {
        let rec1 = TestRecord {
            id: b"read1".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"completely_different".to_vec(),
        };

        assert!(ValidationMode::Skip.check_read_ids(&rec1, &rec2).is_ok());
    }

    #[test]
    fn test_normal_mode_matching_ids_with_suffixes() {
        let rec1 = TestRecord {
            id: b"read_name/1".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"read_name/2".to_vec(),
        };

        assert!(ValidationMode::Normal.check_read_ids(&rec1, &rec2).is_ok());
    }

    #[test]
    fn test_normal_mode_identical_ids_no_suffixes() {
        let rec1 = TestRecord {
            id: b"read_name".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"read_name".to_vec(),
        };

        assert!(ValidationMode::Normal.check_read_ids(&rec1, &rec2).is_ok());
    }

    #[test]
    fn test_normal_mode_different_base_ids_with_suffixes() {
        let rec1 = TestRecord {
            id: b"read_A/1".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"read_B/2".to_vec(),
        };

        let result = ValidationMode::Normal.check_read_ids(&rec1, &rec2);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            error::ValidationError::PairedRecordsWithDifferentIds(_, _)
        ));
    }

    #[test]
    fn test_normal_mode_mismatched_suffixes() {
        let rec1 = TestRecord {
            id: b"read_name/2".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"read_name/1".to_vec(),
        };

        let result = ValidationMode::Normal.check_read_ids(&rec1, &rec2);
        assert!(result.is_err());
    }

    #[test]
    fn test_normal_mode_only_one_has_suffix() {
        let rec1 = TestRecord {
            id: b"read_name/1".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"read_name".to_vec(),
        };

        let result = ValidationMode::Normal.check_read_ids(&rec1, &rec2);
        assert!(result.is_err());
    }

    #[test]
    fn test_strict_mode_valid_suffixes() {
        let rec1 = TestRecord {
            id: b"read_name/1".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"read_name/2".to_vec(),
        };

        assert!(ValidationMode::Strict
            .check_pair_id_suffixes(&rec1, &rec2)
            .is_ok());
        assert!(ValidationMode::Strict.check_read_ids(&rec1, &rec2).is_ok());
    }

    #[test]
    fn test_strict_mode_missing_forward_suffix() {
        let rec1 = TestRecord {
            id: b"read_name".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"read_name/2".to_vec(),
        };

        let result = ValidationMode::Strict.check_pair_id_suffixes(&rec1, &rec2);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            error::ValidationError::MissingForwardMateSuffix(_)
        ));
    }

    #[test]
    fn test_strict_mode_missing_reverse_suffix() {
        let rec1 = TestRecord {
            id: b"read_name/1".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"read_name".to_vec(),
        };

        let result = ValidationMode::Strict.check_pair_id_suffixes(&rec1, &rec2);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            error::ValidationError::MissingReverseMateSuffix(_)
        ));
    }

    #[test]
    fn test_validation_mode_from_str() {
        assert!(matches!(
            "skip".parse::<ValidationMode>().unwrap(),
            ValidationMode::Skip
        ));
        assert!(matches!(
            "normal".parse::<ValidationMode>().unwrap(),
            ValidationMode::Normal
        ));
        assert!(matches!(
            "strict".parse::<ValidationMode>().unwrap(),
            ValidationMode::Strict
        ));

        assert!("SKIP".parse::<ValidationMode>().is_ok());
        assert!("Normal".parse::<ValidationMode>().is_ok());

        let result = "invalid".parse::<ValidationMode>();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            error::ValidationError::InvalidMode(_)
        ));
    }

    #[test]
    fn test_validation_mode_from_bool() {
        assert!(matches!(ValidationMode::from(true), ValidationMode::Normal));
        assert!(matches!(ValidationMode::from(false), ValidationMode::Skip));
    }

    #[test]
    fn test_complex_read_id_with_suffixes() {
        let rec1 = TestRecord {
            id: b"@HISEQ:123:ABC:1:1101:1234:5678/1".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"@HISEQ:123:ABC:1:1101:1234:5678/2".to_vec(),
        };

        assert!(ValidationMode::Normal.check_read_ids(&rec1, &rec2).is_ok());
    }

    #[test]
    fn test_empty_ids() {
        let rec1 = TestRecord { id: b"".to_vec() };
        let rec2 = TestRecord { id: b"".to_vec() };

        assert!(ValidationMode::Normal.check_read_ids(&rec1, &rec2).is_ok());
    }

    #[test]
    fn test_run_all_checks() {
        let rec1 = TestRecord {
            id: b"read/1".to_vec(),
        };
        let rec2 = TestRecord {
            id: b"read/2".to_vec(),
        };

        assert!(ValidationMode::Strict.run_all_checks(&rec1, &rec2).is_ok());
        assert!(ValidationMode::Normal.run_all_checks(&rec1, &rec2).is_ok());
        assert!(ValidationMode::Skip.run_all_checks(&rec1, &rec2).is_ok());
    }
}
