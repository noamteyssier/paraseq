#![doc = include_str!("../README.md")]

mod error;
pub mod fasta;
pub mod fastq;
pub mod fastx;
pub mod parallel;
mod record;

pub use error::{Error, FastaError, FastqError};
pub use record::Record;

/// Default maximum number of records in a record set.
pub const DEFAULT_MAX_RECORDS: usize = 1024;
