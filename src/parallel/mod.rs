mod error;
mod multi;
mod paired;
mod single;
mod processor;
mod reader;

pub use error::{IntoProcessError, ProcessError, Result};
pub use processor::{MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor};
pub use reader::ParallelReader;

pub use multi::InterleavedMultiReader;
pub use multi::MultiReader;
pub use paired::InterleavedPairedReader;
pub use paired::PairedReader;
