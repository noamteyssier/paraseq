# paraseq

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE.md)
[![Crates.io](https://img.shields.io/crates/d/paraseq?color=orange&label=crates.io)](https://crates.io/crates/paraseq)
[![docs.rs](https://img.shields.io/docsrs/paraseq?color=green&label=docs.rs)](https://docs.rs/paraseq/latest/paraseq/)

A high-performance Rust library for parallel processing of FASTA/FASTQ (FASTX) sequence files, optimized for modern hardware and large datasets.

`paraseq` provides a simplified interface for parallel processing of FASTX files that can be invariant to the file format or paired status.

## Summary

`paraseq` is built specifically for processing paired-end FASTA/FASTQ sequences in a parallel manner.
It uses `RecordSets` as the primary unit of buffering, with each set _directly reading_ a fixed number of records from the input stream.
To handle incomplete records it uses the shared `Reader` as an overflow buffer.
It adaptively manages buffer sizes based on observed record sizes, estimating the required space for the number of records in the set and minimizing the number of copies between buffers.
`paraseq` is most efficient in cases where the variance in record sizes is low as it can accurately minimize the number of copies between buffers.

It matches performance of non-record-set parsers (like [`seq_io`](https://docs.rs/seq_io) and [`needletail`](https://docs.rs/needletail)), but can get higher-throughput in record-set contexts (i.e. multi-threaded contexts) by skipping a full copy of the input.

![throughput](https://github.com/noamteyssier/paraseq_benchmark/raw/main/notebooks/throughput.svg)
See [benchmarking repo](https://github.com/noamteyssier/paraseq_benchmark) for benchmarking implementation.

If you're interested in reading more about it, I wrote a small [blog post](https://noamteyssier.github.io/2025-02-03/) discussing its design and motivation.

## Features

- High performance parsing with minimal-copy of input data.
- Multi-line parsing support for FASTA
- Parallel processing of single-end, paired-end, interleaved, and multi-FASTX files with a consistent API.
- Simple construction of readers from file paths and handles with optional transparent decompression support with [niffler](https://github.com/luizirber/niffler).
- Generalized Map-Reduce pattern for processing sequencing data (single-end, paired-end, and interleaved)

### Optional Features (Feature Flags)

- Supports parallel processing of SAM/BAM/CRAM files using [`rust_htslib`](https://crates.io/crates/rust_htslib) (with `htslib` feature flag)
- Supports URLs as input for FASTX files over HTTP and HTTPS (with `url` feature flag)
- Supports SSH paths as inputs respecting system configuration (with `ssh` feature flag)
- Supports Google Cloud Storage (GCS) URIs (with `gcs` feature flag). _requires [`gcloud`](https://cloud.google.com/sdk/docs/install) to be installed and authenticated_

## Usage

The benefit of using `paraseq` is that it makes it easy to distribute paired-end records to per-record and per-batch processing functions.

### Code Examples

Check out the [`examples`](https://github.com/noamteyssier/paraseq/tree/main/examples) directory for code examples or the [API documentation](https://docs.rs/paraseq) for more information.

Feel free to also explore [seqpls](https://github.com/noamteyssier/seqpls) a parallel paired-end FASTA/FASTQ sequence grepper to see how `paraseq` can be used in a non-toy example.

### Basic Usage

This is a simple example using `paraseq` to iterate records in a single-threaded context.

It is not recommended to use `paraseq` in this way - it will be more performant to use the parallel processing interface.

```rust
use std::fs::File;
use paraseq::{fastq, Record};

fn main() -> Result<(), paraseq::Error> {
    let path = "./data/sample.fastq";
    let mut reader = fastq::Reader::from_path(path)?;
    let mut record_set = reader.new_record_set();

    while record_set.fill(&mut reader)? {
        for record in record_set.iter() {
            let record = record?;
            // Process record...
            println!("ID: {}", record.id_str());
        }
    }
    Ok(())

}
```

### Parallel Processing

To distribute processing across multiple threads you can implement the `ParallelProcessor` trait on your arbitrary struct.

For an example of a single-end parallel processor see the [parallel example](https://github.com/noamteyssier/paraseq/blob/main/examples/parallel.rs).

```rust
use std::fs::File;
use paraseq::{fastx, ProcessError};
use paraseq::prelude::*;

#[derive(Clone, Default)]
struct MyProcessor {
// Your processing state here
}

impl ParallelProcessor for MyProcessor {
    fn process_record<R: Record>(&mut self, record: R) -> Result<(), ProcessError> {
        // Process record in parallel
        Ok(())
    }
}

fn main() -> Result<(), ProcessError> {
    let path = "./data/sample.fastq";
    let reader = fastx::Reader::from_path(path)?;
    let processor = MyProcessor::default();
    let num_threads = 8;

    reader.process_parallel(processor, num_threads)?;
    Ok(())

}
```

### Paired-End Processing

Paired end processing is as simple as single-end processing, but involves implementing the `PairedParallelProcessor` trait on your struct.

For an example of paired parallel processing see the [paired example](https://github.com/noamteyssier/paraseq/blob/main/examples/paired_parallel.rs).

```rust
use std::fs::File;
use paraseq::{
    fastx,
    ProcessError,
    prelude::*,
};

#[derive(Clone, Default)]
struct MyPairedProcessor {
    // Your processing state here
}

impl PairedParallelProcessor for MyPairedProcessor {
    fn process_record_pair<R: Record>(&mut self, r1: R, r2: R) -> Result<(), ProcessError> {
        // Process paired records in parallel
        Ok(())
    }
}

fn main() -> Result<(), ProcessError> {
    let path1 = "./data/r1.fastq";
    let path2 = "./data/r2.fastq";

    let reader1 = fastx::Reader::from_path(path1)?;
    let reader2 = fastx::Reader::from_path(path2)?;
    let processor = MyPairedProcessor::default();
    let num_threads = 8;

    reader1.process_parallel_paired(reader2, processor, num_threads)?;
    Ok(())
}
```

### Interleaved Processing

Interleaved processing is also supported by implementing the `InterleavedParallelProcessor` trait on your struct.

For an example of interleaved parallel processing see the [interleaved example](https://github.com/noamteyssier/paraseq/blob/main/examples/interleaved.rs).

```rust
use std::fs::File;
use paraseq::{
    fastx,
    ProcessError,
    prelude::*,
};

#[derive(Clone, Default)]
struct MyInterleavedProcessor {
    // Your processing state here
}

impl InterleavedParallelProcessor for MyInterleavedProcessor {
    fn process_interleaved_pair<R: Record>(&mut self, r1: R, r2: R) -> Result<(), ProcessError> {
        // Process interleaved paired records in parallel
        Ok(())
    }
}

fn main() -> Result<(), ProcessError> {
    let path = "./data/interleaved.fastq";
    let reader = fastx::Reader::from_path(path)?;
    let processor = MyInterleavedProcessor::default();
    let num_threads = 8;

    reader.process_parallel_interleaved(processor, num_threads)?;
    Ok(())
}
```

## Limitations

1. In cases where records have high variance in size, the buffer size predictions will become inaccurate and the shared `Reader` overflow buffer will incur more copy operations. This may lead to suboptimal performance.

2. Multiline FASTA is now supported! However, you will incur additional memory usage when calling the `Record::seq()` method with multiline FASTA - as it incurs an allocation to remove newlines from the sequence. You can choose to avoid this by using the newly introduced `Record::seq_raw()` method which will return the sequence buffer without any modifications (though it may have newlines). Here there be dragons!

3. Each `RecordSet` maintains its own buffer, so memory usage practically scales with the number of threads and record capacity. This shouldn't be a concern unless you're running this on a system with very limited memory.

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Similar Projects

This work is inspired by the following projects:

- [seq_io](https://github.com/markschl/seq_io)
- [fastq](https://github.com/aseyboldt/fastq-rs)

This project aims to be directed more specifically at ergonomically processing of paired records in parallel and is optimized mainly for FASTQ files.
It can be faster than `seq_io` for some use cases, but it is not currently as rigorously tested. However, it now supports both single-line and multi-line FASTA files with optimized performance.

If the libraries assumptions do not fit your use case, you may want to consider using `seq_io` or `fastq` instead.
