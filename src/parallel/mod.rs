mod error;
mod multi;
mod paired;
mod processor;
mod single;

pub use error::{ProcessError, Result};
pub use processor::{MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor};
pub use single::ParallelReader;
