use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid start character ({0}), expected either '>' or '@'")]
    InvalidStartCharacter(char),

    #[error("Error reading from buffer: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid batch size ({0}), must be greater than zero")]
    InvalidBatchSize(usize),

    #[error("Invalid header: ({0}): expected ({1})")]
    InvalidHeader(char, char),

    #[error("Invalid FASTA record")]
    FastaError(#[from] FastaError),

    #[error("Invalid FASTQ record")]
    FastqError(#[from] FastqError),
}

#[derive(thiserror::Error, Debug)]
pub enum FastaError {
    #[error("Unbounded positions")]
    UnboundedPositions,
}

#[derive(thiserror::Error, Debug)]
pub enum FastqError {
    #[error("Invalid separator")]
    InvalidSeparator,

    #[error("Unbounded positions")]
    UnboundedPositions,

    #[error("Sequence and quality lengths do not match")]
    UnequalLengths,
}
