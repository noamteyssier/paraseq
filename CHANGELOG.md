# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
