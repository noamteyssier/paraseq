mod error;
mod multi;
mod paired;
mod processor;
mod reader;
mod single;

pub use error::{IntoProcessError, ProcessError, Result};
pub use processor::{MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor};
pub use single::ParallelReader;

pub use multi::InterleavedMultiReader;
pub use multi::MultiReader;
pub use paired::InterleavedPairedReader;
pub use paired::PairedReader;
