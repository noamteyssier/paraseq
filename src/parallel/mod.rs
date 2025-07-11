mod error;
mod fastx;
mod macros;
mod processor;
mod reader;

pub use error::{IntoProcessError, ProcessError, Result};
pub use processor::{
    InterleavedParallelMultiProcessor, InterleavedParallelProcessor, MultiParallelProcessor,
    PairedParallelProcessor, ParallelProcessor,
};
pub use reader::{
    InterleavedParallelMultiReader, InterleavedParallelReader, MultiParallelReader,
    PairedParallelReader, ParallelReader,
};
