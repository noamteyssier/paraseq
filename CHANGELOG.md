# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## 0.6.0

### Added

- `ReaderBuilder`, a single entry point for constructing `fasta`/`fastq`/`fastx` readers from a path, stdin, url, ssh, or gcs source, with chainable `.batch_size()`, `.record_limit()`, `.ssh_args()`, `.gcloud_args()`, and `.project()` config.
- Windows (`\r\n`) line endings in `fasta` and `fastq`: a `\r` before the terminating `\n` is no longer treated as part of the id, sequence, or quality. Previously it was counted as a base, or caused a spurious sequence/quality length mismatch. No measurable throughput change on FASTQ; ~1% on FASTA. For multi-line FASTA, the line ending is detected from each record's first line, and `seq_raw()` still returns the raw bytes including line endings.

### Changed

- The resizable `ThreadPool`/`PoolParallelReader` API is now always available (no longer gated behind the `pool` feature flag), and is the sole implementation behind every parallel entry point: a fixed thread count is now a `ThreadPool` whose target never moves. Benchmarked against the previous fixed-thread implementation on a 50M-record FASTQ across 1/2/4/8/10 threads with no measurable overhead (within ~1% noise).
- `paraseq::gcs` and `paraseq::ssh` moved to `paraseq::remote::gcs` and `paraseq::remote::ssh`, grouping the remote-transport backends under one module instead of the crate root. Most callers go through `ReaderBuilder` and are unaffected.
- `ProcessError` merged into `Error`, and `paraseq::Result<T>` now aliases `Result<T, Error>`, so the crate has one error type instead of two with a wrapping relationship between them. `IntoProcessError`/`into_process_error` renamed to `IntoParaseqError`/`into_paraseq_error` to match.

### Removed

- `from_url`, `from_ssh`, `from_gcs`, `from_gcs_with_gcloud_args`, and `from_gcs_with_project` on `fasta::Reader`, `fastq::Reader`, and `fastx::Reader` — use `ReaderBuilder::url(..)`/`::ssh(..)`/`::gcs(..)` with `.build_fasta()`/`.build_fastq()`/`.build()` instead. Likewise `from_path`, `from_stdin`, and `from_optional_path` on those readers — use `ReaderBuilder::path(..)`/`::stdin()`/`::optional_path(..)` (`Reader::new` is unchanged). The `htslib::Reader` constructors are unaffected.
- `parking_lot` dependency — all internal `Mutex`/`Condvar` usage now uses `std::sync`. Benchmarking on real FASTQ workloads showed no measurable difference, since these locks aren't contended enough to matter.
- `pool` feature flag — see Changed.

### Performance

- Record-boundary scanning (`\n` in `fastq`, `>` in `fasta`) now uses an explicit `u8x64` SIMD compare via `fearless_simd` instead of `memchr::memchr_iter`. On real data this measured ~1.5-2x throughput on FASTQ (newlines every ~220bp) and ~8-20% on FASTA (sparse `>`, once per multi-KB record). (tests are measuring in-memory parsing not I/O.)
- `fasta` and `fastq` field accessors strip the trailing `\r`/`\n` without redundant bounds guards, and multiline `fasta` `seq()` copies its final line unconditionally. ~3-5% on in-memory FASTQ and single-line FASTA parsing; no change on multiline FASTA.
- `fastq` newline scanning writes every newline offset in bulk (four unconditional writes per 64-byte chunk, advanced by popcount) and builds record positions from groups of four, replacing the per-newline state machine, with no `unsafe`. ~35% on variable-length reads (59.7 → 38.7 ms for 2M records) and no change on fixed-length reads, where branches already predict well.
- `fasta` `seq()` detects single-line records with one `memchr` (no SIMD dispatch per record) and de-wraps multiline records straight into the output without building an intermediate newline-offset `Vec`. On in-memory parsing: ~42% on single-line fixed-length reads, ~21% single-line variable-length, ~29% and ~12% on short multiline records.
- `fasta` `seq()` de-wraps fixed-width multiline records by stride, checking that each newline sits where expected instead of searching for it, and falls back to the width-agnostic path for irregular layouts (output is identical). ~40% on 80-column wrapped FASTA with variable-length records (49.6 → 30.2 ms for 400MB), ~7% on short 60-column records.

## 0.5.1

### Added

- Experimental `Pool` feature for dynamically allocating threads during a processing run under a feature flag.

## 0.5.0

### Added

- A global record index to the `Record` trait to keep track of original record positions in the input file
- `parallel::Ordered<P>` processor wrapper for opt-in output ordering: serializes `on_batch_complete` calls to match the original record stream order, at the cost of head-of-line blocking on the slowest outstanding batch. `process_record`/`process_record_batch` remain fully parallel.

### Fixed

- Fixed a race between claiming a batch's position in the stream and the reader's internal lock around `fill`, which could let offset/limit range processing (and ordering) attribute the wrong records to a batch under high thread contention with small batch sizes.
- Paired and multi-file processing (`process_parallel_paired`, `process_parallel_multi`, and their `_range` variants) now return an error instead of silently dropping trailing records when the input files have different lengths and the mismatch isn't caught within a single batch.
- Interleaved paired/multi processing (`process_parallel_interleaved`, `process_parallel_multi_interleaved`) now validates that each batch's record count is an exact multiple of the pair/arity size before processing it, instead of silently truncating a trailing partial record.

### Performance

- Paired and multi-file parallel processing no longer serializes every worker thread's reads behind a single lock; each file's decompression can again fully overlap with other files' and other threads' reads, restoring throughput to the same level as single-end processing.

## 0.4.14

### Fixed

- Fixed a bug in handling malformed data (regression test added) ([#69](https://github.com/noamteyssier/paraseq/pull/69))
- `seq_raw` no longer includes the trailing newline for FASTA records ([#68](https://github.com/noamteyssier/paraseq/pull/68))

### Performance

- Use uninitialized memory when filling buffers to reduce zero-initialization overhead ([#68](https://github.com/noamteyssier/paraseq/pull/68))
- Scan for newlines and build positions in a single pass ([#66](https://github.com/noamteyssier/paraseq/pull/66))

### Testing

- Added fuzz testing for FASTA/FASTQ/FASTX parsing ([#69](https://github.com/noamteyssier/paraseq/pull/69))
- Added a `justfile` with test/fuzzing-specific recipes ([#69](https://github.com/noamteyssier/paraseq/pull/69))

### CI

- Added `fmt` and `clippy` checks to CI ([#67](https://github.com/noamteyssier/paraseq/pull/67))
- Refactored CI to run examples in a single runner with a shared compilation step ([#67](https://github.com/noamteyssier/paraseq/pull/67))
- Examples simplified with a common API and added documentation ([#67](https://github.com/noamteyssier/paraseq/pull/67))

## (start - 0.4.13)

No changelog - see github releases or git history before this version
