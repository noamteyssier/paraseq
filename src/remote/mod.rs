//! Remote transport backends for reading sequence files over a network.

#[cfg(feature = "gcs")]
pub mod gcs;

#[cfg(feature = "ssh")]
pub mod ssh;
