mod error;
mod fastx;
mod macros;
mod processor;
mod reader;

pub use error::{IntoProcessError, ProcessError, Result};
pub use processor::{
    InterleavedMultiParallelProcessor, InterleavedParallelProcessor, MultiParallelProcessor,
    PairedParallelProcessor, ParallelProcessor,
};
pub use reader::{
    InterleavedMultiParallelReader, InterleavedParallelReader, MultiParallelReader,
    PairedParallelReader, ParallelReader,
};
