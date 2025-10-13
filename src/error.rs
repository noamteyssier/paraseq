use std::{io, str::Utf8Error};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid start character ({0}), expected either '>' or '@'")]
    InvalidStartCharacter(char),

    #[error("There is a format mismatch between the reader and the record set")]
    FormatMismatch,

    #[error("Error reading from buffer: {0}")]
    Io(#[from] io::Error),

    #[error("Error encountered while validating input data:\n\n{0}")]
    ValidationError(#[from] crate::validation::error::ValidationError),

    /// Error parsing valid UTF-8, indicating invalid characters in the input data.
    #[error("Invalid UTF-8 error: {0}")]
    Utf8Error(#[from] Utf8Error),

    #[cfg(feature = "url")]
    #[error("Networking error: {0}")]
    Network(#[from] reqwest::Error),

    #[cfg(feature = "niffler")]
    #[error("Error reading from file: {0}")]
    Niffler(#[from] niffler::Error),

    #[cfg(feature = "ssh")]
    #[error("SSH error: {0}")]
    Ssh(#[from] crate::ssh::SshError),

    #[cfg(feature = "gcs")]
    #[error("GCS error: {0}")]
    Gcs(#[from] crate::gcs::GcsError),

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

    #[error("Paired records with different identifiers encountered when expected: {0} != {1}")]
    PairedRecordsWithDifferentIds(String, String),

    #[error("Unexpected format request. Found fastx: {0}, requested: {1}")]
    UnexpectedFormatRequest(String, String),
}
