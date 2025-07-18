use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
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
    Ssh(#[from] crate::ssh::SshError),

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
}
