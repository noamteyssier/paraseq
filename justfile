# Fuzzing helpers for the FASTA/FASTQ parsers.
# See fuzz/README.md for what each target actually checks.
#
# cargo-fuzz requires a nightly toolchain even if your default is stable,
# so every recipe here pins it explicitly.
nightly := "+nightly"

# List available recipes
default:
    @just --list

# Run one fuzz target: `just fuzz fasta`, `just fuzz fastq 300`, `just fuzz fastx 0` (0 = unbounded)
fuzz target time="20":
    cargo {{nightly}} fuzz run {{target}} -- -max_total_time={{time}}

# Run every fuzz target one after another, `time` seconds each
fuzz-all time="20": (fuzz "fasta" time) (fuzz "fastq" time) (fuzz "fastx" time)

# Remove generated corpus/artifacts/build output under fuzz/
fuzz-clean:
    rm -rf fuzz/corpus fuzz/artifacts fuzz/target
