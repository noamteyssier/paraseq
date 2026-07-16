//! Shared test-only helpers.

use std::io::Write;
use std::process::{Command, Output, Stdio};

/// Environment variable used to signal that this process is the piped-stdin
/// child spawned by [`run_with_piped_stdin`], rather than the top-level test run.
const STDIN_CHILD_ENV: &str = "PARASEQ_STDIN_CHILD";

/// `from_stdin`-style constructors read a real OS stdin handle (`io::stdin()`),
/// which cannot be substituted from within the same test process. To exercise
/// them for real, re-invoke the current test binary filtered down to exactly
/// `test_path` (e.g. `"fastq::tests::test_from_stdin"`), piping `stdin_data`
/// into its stdin, and return the child's output.
///
/// The target test itself must branch on [`is_stdin_child`] to do the real
/// work when re-invoked as a child, rather than recursing.
pub(crate) fn run_with_piped_stdin(test_path: &str, stdin_data: &[u8]) -> Output {
    let exe = std::env::current_exe().expect("failed to resolve current test executable");
    let mut child = Command::new(exe)
        .args([test_path, "--exact", "--nocapture"])
        .env(STDIN_CHILD_ENV, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn piped-stdin child test process");

    child
        .stdin
        .take()
        .expect("child stdin was not piped")
        .write_all(stdin_data)
        .expect("failed to write to child stdin");

    child
        .wait_with_output()
        .expect("failed to wait for piped-stdin child test process")
}

/// Whether the current process is a child spawned by [`run_with_piped_stdin`].
pub(crate) fn is_stdin_child() -> bool {
    std::env::var_os(STDIN_CHILD_ENV).is_some()
}
