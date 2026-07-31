# Examples

Runnable demonstrations of `paraseq`'s API. Every example takes a positional
input path (reading stdin if omitted, where supported) and a consistent set
of flags across the ones that share a concept:

- `-T, --threads <N>` — number of threads to use; `0` (the default) uses all available cores.
- `-o, --output <PATH>` — output path for format-conversion examples; writes stdout if omitted.
- `-f, --format <fasta|fastq>` — output format for format-conversion examples; defaults to `fasta`.

Run any example with `--help` to see its exact flags, or with no arguments
to see what's required. `examples/common/mod.rs` holds the `SeqSum` and
`Writer` processors shared by most of the examples below — it isn't an
example itself.

## Basics

| Example | Demonstrates |
|---|---|
| [`naive.rs`](naive.rs) | Sequential (single-threaded) record counting with the format-specific `fasta`/`fastq` readers and the auto-detecting `fastx` reader. |
| [`multiline_fasta.rs`](multiline_fasta.rs) | Multiline FASTA parsing — `Record::seq()` transparently concatenates sequences split across lines. |
| [`reloading.rs`](reloading.rs) | Peeking at the first few records via `Reader::reload`, then re-reading the whole file from the start. |

## Parallel Processing

| Example | Demonstrates |
|---|---|
| [`parallel.rs`](parallel.rs) | Single-end parallel processing via `ParallelProcessor`. Linked from the main README as the canonical example. |
| [`parallel_closure.rs`](parallel_closure.rs) | The same as `parallel.rs`, but with a closure over a batch iterator instead of a `ParallelProcessor` struct. |
| [`paired_parallel.rs`](paired_parallel.rs) | Paired-end processing of two separate files via `PairedParallelProcessor`. Linked from the main README. |
| [`interleaved.rs`](interleaved.rs) | Paired-end processing of one interleaved file via `PairedParallelProcessor`. Linked from the main README. |
| [`range_parallel.rs`](range_parallel.rs) | Restricting processing to a bounded `[start, end)` range of records. |
| [`reloading_parallel.rs`](reloading_parallel.rs) | Combining `Reader::reload` with parallel processing. |
| [`ordered_parallel.rs`](ordered_parallel.rs) | Preserving input record order in the output via `parallel::Ordered`, opt-in per processor. |

## Multi-File and Collections

| Example | Demonstrates |
|---|---|
| [`multi_parallel.rs`](multi_parallel.rs) | Synchronized parallel processing across N (2-8) separate files via `MultiParallelProcessor`. |
| [`multi_interleaved.rs`](multi_interleaved.rs) | The same, but interleaved within a single file at an arbitrary arity. |
| [`collection.rs`](collection.rs) | Processing a `Collection` of many readers at once (single or paired), for when you have more input files than threads. |

## Format Conversion

| Example | Demonstrates |
|---|---|
| [`fastx.rs`](fastx.rs) | FASTA/FASTQ conversion via `fastx::Reader::new`, constructed directly from a `Read` handle. |
| [`read_write.rs`](read_write.rs) | The same conversion via `fastx::Reader::from_optional_path`, with transparent `.gz`/`.zst` decompression. |
| [`htslib.rs`](htslib.rs) | Converting SAM/BAM/CRAM to FASTA/FASTQ, single-end or paired (requires the `htslib` feature). |

## Remote Sources

These require their respective feature flags, and (for `ssh`/`gcs`) external
tools and credentials, so they aren't exercised in CI.

| Example | Demonstrates |
|---|---|
| [`url.rs`](url.rs) | Reading FASTA/FASTQ directly from HTTP(S) URLs (requires the `url` feature). |
| [`ssh.rs`](ssh.rs) | Reading over SSH, single-end or paired (requires the `ssh` feature and system SSH config). |
| [`gcs.rs`](gcs.rs) | Reading from Google Cloud Storage, single-end or paired (requires the `gcs` feature and an authenticated `gcloud` CLI). |
