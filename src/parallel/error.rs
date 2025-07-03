use std::error::Error as StdError;
use std::fmt;

use crossbeam_channel::SendError;
use thiserror::Error;

// Convenience Result type alias
pub type Result<T> = std::result::Result<T, ProcessError>;

/// Error type for parallel processing operations
#[derive(Error, Debug)]
pub enum ProcessError {
    /// Error occurred during parallel processing
    #[error("Processing error: {0}")]
    Process(Box<dyn StdError + Send + Sync>),

    /// Invalid number of threads specified
    #[error("Invalid thread count specified")]
    InvalidThreadCount,

    /// Incompatible readers specified
    #[error("Incompatible readers specified, expected both readers to be the same input format")]
    IncompatibleReaders,

    /// Record synchronization error between paired files
    #[error("Record synchronization error between paired files. {0} has less records.")]
    PairedRecordMismatch(RecordPair),

    /// Error sending data between threads
    #[error("Channel error: {0}")]
    SendError(#[from] SendError<Option<usize>>),

    /// Error joining threads
    #[error("Thread join error.")]
    JoinError,

    /// Error reading from input
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Error from FASTX processing
    #[error("FASTX error: {0}")]
    FastxError(#[from] crate::Error),

    /// Error from HTSlib
    #[error("HTSlib error: {0}")]
    HtslibError(#[from] rust_htslib::errors::Error),
}

/// Enum for identifying record pairs
#[derive(Debug)]
pub enum RecordPair {
    R1,
    R2,
}
impl fmt::Display for RecordPair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RecordPair::R1 => write!(f, "R1"),
            RecordPair::R2 => write!(f, "R2"),
        }
    }
}

/// Trait for converting arbitrary errors into `ProcessError`
pub trait IntoProcessError {
    fn into_process_error(self) -> ProcessError;
}

// Implement conversion for Box<dyn Error>
impl<E> IntoProcessError for E
where
    E: StdError + Send + Sync + 'static,
{
    fn into_process_error(self) -> ProcessError {
        ProcessError::Process(Box::new(self))
    }
}

// Feature-gated anyhow support
#[cfg(feature = "anyhow")]
impl From<anyhow::Error> for ProcessError {
    fn from(err: anyhow::Error) -> Self {
        ProcessError::Process(err.into())
    }
}
