use std::error::Error as StdError;
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

    /// Record synchronization error between paired files
    #[error("Record synchronization error between paired files")]
    PairedRecordMismatch,

    /// Error reading from input
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Error from FASTA processing
    #[error("FASTA error: {0}")]
    FastaError(#[from] crate::fasta::Error),

    /// Error from FASTQ processing
    #[error("FASTQ error: {0}")]
    FastqError(#[from] crate::fastq::Error),
}

/// Trait for converting arbitrary errors into ProcessError
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
