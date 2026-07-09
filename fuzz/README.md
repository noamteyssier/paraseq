# Fuzzing

[cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/tutorial.html) is used
to find panics, out-of-bounds access, and other invariant violations in the
FASTA/FASTQ parsers on arbitrary/malformed input. This is a manual, local
tool -- it is not run in CI.

Each target feeds raw bytes to a reader and just drives it to completion,
asserting that:

- parsing never panics or reads out of bounds, no matter how malformed the
  input is (bad headers, truncated records, missing newlines, embedded
  garbage, etc.), and
- a record the reader considers *valid* actually satisfies the format's
  invariants (e.g. FASTQ sequence and quality strings have equal length).

A per-record `Err` (e.g. a missing `>`/`@`, bad separator) is an expected
outcome for malformed input, not a bug -- targets skip those and keep going.

The first fuzzed byte is used to pick a `RecordSet` capacity (1-32), so the
fuzzer also explores different buffer-refill/overflow boundaries rather than
only exercising one fixed batch size.

Targets:

- `fasta` -- `paraseq::fasta::Reader`
- `fastq` -- `paraseq::fastq::Reader`
- `fastx` -- `paraseq::fastx::Reader`, the format-auto-detecting reader

# Setup

Install cargo-fuzz (requires a nightly toolchain):

```sh
cargo install cargo-fuzz
```

# Running

```sh
cargo +nightly fuzz run fasta
cargo +nightly fuzz run fastq
cargo +nightly fuzz run fastx
```

Add `-- -max_total_time=60` to bound a run to 60 seconds, or `-j4` to run
with 4 parallel workers. Corpus files accumulate under `fuzz/corpus/<target>`
and crashing inputs under `fuzz/artifacts/<target>`.

There's also a `justfile` at the repo root with shortcuts (see `just --list`):

```sh
just fuzz fasta          # run one target for 60s (default)
just fuzz fastq 300      # ...or for a custom number of seconds
just fuzz fastx 0        # 0 = run until interrupted
just fuzz-all            # run fasta, fastq, and fastx in turn
just fuzz-clean          # wipe fuzz/corpus, fuzz/artifacts, fuzz/target
```

# Debugging a crash

Minify a crashing input:

```sh
cargo fuzz tmin fasta fuzz/artifacts/fasta/crash-<hash>
```

Then reproduce it directly against the minimized input:

```sh
cargo fuzz run fasta fuzz/artifacts/fasta/minimized-from-<hash>
```
