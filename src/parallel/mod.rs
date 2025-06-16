mod error;
mod fastx;
mod macros;
mod processor;
mod reader;

pub use error::{IntoProcessError, ProcessError, Result};
pub use processor::{InterleavedParallelProcessor, PairedParallelProcessor, ParallelProcessor};
pub use reader::{InterleavedParallelReader, PairedParallelReader, ParallelReader};
