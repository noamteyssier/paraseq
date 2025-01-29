use std::error::Error as StdError;
use std::fmt;

/// Error type for parallel processing operations
#[derive(Debug)]
pub enum ProcessError {
    /// Error occurred during parallel processing
    Process(Box<dyn StdError + Send + Sync>),
    /// Invalid number of threads specified
    InvalidThreadCount,
    /// Error in channel communication
    ChannelError,
    /// Error reading from input
    IoError(std::io::Error),
    /// Error from FASTA processing
    FastaError(crate::fasta::Error),
    /// Error from FASTQ processing
    FastqError(crate::fastq::Error),
}

impl fmt::Display for ProcessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcessError::Process(e) => write!(f, "Processing error: {}", e),
            ProcessError::InvalidThreadCount => write!(f, "Invalid thread count specified"),
            ProcessError::ChannelError => write!(f, "Channel communication error"),
            ProcessError::IoError(e) => write!(f, "I/O error: {}", e),
            ProcessError::FastaError(e) => write!(f, "FASTA error: {}", e),
            ProcessError::FastqError(e) => write!(f, "FASTQ error: {}", e),
        }
    }
}

impl StdError for ProcessError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ProcessError::Process(e) => Some(e.as_ref()),
            ProcessError::IoError(e) => Some(e),
            ProcessError::FastaError(e) => Some(e),
            ProcessError::FastqError(e) => Some(e),
            _ => None,
        }
    }
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

// Implement specific conversions
impl From<std::io::Error> for ProcessError {
    fn from(err: std::io::Error) -> Self {
        ProcessError::IoError(err)
    }
}

impl From<crate::fasta::Error> for ProcessError {
    fn from(err: crate::fasta::Error) -> Self {
        ProcessError::FastaError(err)
    }
}

impl From<crate::fastq::Error> for ProcessError {
    fn from(err: crate::fastq::Error) -> Self {
        ProcessError::FastqError(err)
    }
}

// Feature-gated anyhow support
#[cfg(feature = "anyhow")]
mod anyhow_support {
    use super::*;

    impl From<anyhow::Error> for ProcessError {
        fn from(err: anyhow::Error) -> Self {
            ProcessError::Process(err.into())
        }
    }
}

// Convenience Result type alias
pub type Result<T> = std::result::Result<T, ProcessError>;
