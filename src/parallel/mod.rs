mod error;
mod macros;
mod processor;
mod reader;

pub use error::{IntoProcessError, ProcessError};
pub use processor::{PairedParallelProcessor, ParallelProcessor};
pub use reader::{PairedParallelReader, ParallelReader};
