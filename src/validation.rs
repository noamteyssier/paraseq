use crate::Record;

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
}

impl<T> RecordValidation for T where T: Record + ?Sized {}
