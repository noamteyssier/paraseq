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

#[cfg(feature = "htslib")]
pub extern crate rust_htslib;

#[cfg(feature = "ssh")]
pub mod ssh;

#[cfg(feature = "gcs")]
pub mod gcs;

pub use error::Error;
pub use parallel::{ProcessError, Result};
pub use record::Record;

/// Default maximum number of records in a record set.
pub const DEFAULT_MAX_RECORDS: usize = 1024;
/// The maximum number of record segments supported in a single synchronized record group.
/// For example, single-end reads have arity 1, paired-end reads have arity 2, etc.
pub const MAX_ARITY: usize = 8;
