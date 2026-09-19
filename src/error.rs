use std::error::Error as StdError;
use std::io;

use crossbeam_channel::SendError;
use thiserror::Error as ThisError;

#[cfg(feature = "htslib")]
use rust_htslib::errors::Error as HtslibError;

/// The crate-wide `Result` type.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Invalid start character ({0}), expected either '>' or '@'")]
    InvalidStartCharacter(char),

    #[error("There is a format mismatch between the reader and the record set")]
    FormatMismatch,

    #[error("Error reading from buffer: {0}")]
    Io(#[from] io::Error),

    #[cfg(feature = "url")]
    #[error("Networking error: {0}")]
    Network(#[from] reqwest::Error),

    #[cfg(feature = "niffler")]
    #[error("Error reading from file: {0}")]
    Niffler(#[from] niffler::Error),

    #[cfg(feature = "ssh")]
    #[error("SSH error: {0}")]
    Ssh(#[from] crate::remote::ssh::SshError),

    #[cfg(feature = "gcs")]
    #[error("GCS error: {0}")]
    Gcs(#[from] crate::remote::gcs::GcsError),

    #[error("Invalid batch size ({0}), must be greater than zero")]
    InvalidBatchSize(usize),

    #[error("Invalid header: ({0}): expected ({1})")]
    InvalidHeader(char, char),

    #[error("Unbounded positions")]
    UnboundedPositions,

    #[error("Invalid FASTQ separator: {0}, expected '+'")]
    InvalidSeparator(char),

    #[error("FASTQ Sequence length ({0}) and quality length ({1}) do not match")]
    UnequalLengths(usize, usize),

    #[error("Unexpected format request. Found fastx: {0}, requested: {1}")]
    UnexpectedFormatRequest(String, String),

    /// Error occurred during parallel processing, wrapping an arbitrary
    /// caller error that has no dedicated variant. See [`IntoParaseqError`].
    #[error("Processing error: {0}")]
    Process(Box<dyn StdError + Send + Sync>),

    /// Invalid number of threads specified
    #[error("Invalid thread count specified")]
    InvalidThreadCount,

    #[error(
        "Collection size mismatch, expected multiple of {} found {}",
        arity,
        found
    )]
    CollectionSizeMismatch { arity: usize, found: usize },

    /// Incompatible readers specified
    #[error("Incompatible readers specified, expected both readers to be the same input format")]
    IncompatibleReaders,

    #[error("Incompatible record set sizes: {0} != {1}")]
    IncompatibleRecordSetSizes(usize, usize),

    #[error("Incompatible interleaved set size - expected an even number: {0}")]
    IncompatibleInterleavedSetSize(usize),

    /// Record synchronization error between paired files
    #[error("Record synchronization error between paired files. {0} has less records.")]
    PairedRecordMismatch(&'static str),

    /// Record synchronization error between paired files
    #[error(
        "Record synchronization error between multiple files. (at least) File {0} has fewer records."
    )]
    MultiRecordMismatch(usize),

    #[error("Record set length ({0}) must be divisible by {1}")]
    MultiRecordSetSizeMismatch(usize, usize),

    /// Error sending data between threads
    #[error("Channel error: {0}")]
    SendError(#[from] SendError<Option<usize>>),

    /// Error joining threads
    #[error("Thread join error.")]
    JoinError,

    /// Error from HTSlib
    #[cfg(feature = "htslib")]
    #[error("HTSlib error: {0}")]
    HtslibError(#[from] HtslibError),

    /// Error for parallel processing of HTSlib files
    #[cfg(feature = "htslib")]
    #[error("Parallel HTSlib error: {0}")]
    ParallelHtslibError(#[from] crate::htslib::ParallelHtslibError),
}

/// Converts an arbitrary error into [`Error`], for callers whose error type
/// has no dedicated variant.
pub trait IntoParaseqError {
    fn into_paraseq_error(self) -> Error;
}

impl<E> IntoParaseqError for E
where
    E: StdError + Send + Sync + 'static,
{
    fn into_paraseq_error(self) -> Error {
        Error::Process(Box::new(self))
    }
}

#[cfg(feature = "anyhow")]
impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Error::Process(err.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paired_record_mismatch_display() {
        assert_eq!(
            Error::PairedRecordMismatch("R1").to_string(),
            "Record synchronization error between paired files. R1 has less records."
        );
    }

    #[test]
    fn test_into_paraseq_error() {
        let io_err = io::Error::other("boom");
        let err = io_err.into_paraseq_error();
        assert!(err.to_string().contains("boom"));
    }

    #[cfg(feature = "anyhow")]
    #[test]
    fn test_from_anyhow_error() {
        let anyhow_err = anyhow::anyhow!("anyhow boom");
        let err: Error = anyhow_err.into();
        assert!(err.to_string().contains("anyhow boom"));
    }
}
