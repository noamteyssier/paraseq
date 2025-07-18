#![doc = include_str!("../README.md")]

mod error;
pub mod fasta;
pub mod fastq;
pub mod fastx;
pub mod parallel;
pub mod prelude;
mod record;

#[cfg(feature = "htslib")]
pub mod htslib;

#[cfg(feature = "ssh")]
pub mod ssh;

pub use error::Error;
pub use parallel::ProcessError;
pub use record::Record;

/// Default maximum number of records in a record set.
pub const DEFAULT_MAX_RECORDS: usize = 1024;
pub const MAX_ARITY: usize = 8;
