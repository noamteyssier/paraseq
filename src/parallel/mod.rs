mod error;
pub(crate) mod multi;
mod ordered;
pub(crate) mod paired;
mod processor;
pub(crate) mod reader;
pub(crate) mod single;

pub use error::{IntoProcessError, ProcessError, Result};
pub use ordered::Ordered;
pub use processor::{MultiParallelProcessor, PairedParallelProcessor, ParallelProcessor};
pub use reader::ParallelReader;
