mod error;
pub(crate) mod multi;
pub(crate) mod paired;
mod processor;
pub(crate) mod single;

pub use error::{IntoProcessError, ProcessError, Result};
pub use processor::{MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor};
pub use single::ParallelReader;
