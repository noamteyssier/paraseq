#![doc = include_str!("../README.md")]

pub mod fasta;
pub mod fastq;
pub mod parallel;
pub mod record;

pub use record::Record;

/// Default maximum number of records in a record set.
pub const DEFAULT_MAX_RECORDS: usize = 1024;
