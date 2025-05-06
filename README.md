# paraseq

A high-performance Rust library for parallel processing of FASTA/FASTQ sequence files, optimized for modern hardware and large datasets.

## Summary

`paraseq` is built specifically for processing paired-end FASTA/FASTQ sequences in a parallel manner.
It uses `RecordSets` as the primary unit of buffering, with each set _directly reading_ a fixed number of records from the input stream.
To handle incomplete records it uses the shared `Reader` as an overflow buffer.
It adaptively manages buffer sizes based on observed record sizes, estimating the required space for the number of records in the set and minimizing the number of copies between buffers.
`paraseq` is most efficient in cases where the variance in record sizes is low as it can accurately minimize the number of copies between buffers.

It matches performance of non-record-set parsers (like [`seq_io`](https://docs.rs/seq_io) and [`needletail`](https://docs.rs/needletail)), but can get higher-throughput in record-set contexts (i.e. multi-threaded contexts) by skipping a full copy of the input.
See [benchmarks](https://github.com/noamteyssier/paraseq_benchmark) for more details.

If you're interested in reading more about it, I wrote a small [blog post](https://noamteyssier.github.io/2025-02-03/) discussing its design and motivation.

## Usage

The benefit of using `paraseq` is that it makes it easy to distribute paired-end records to per-record and per-batch processing functions.

### Code Examples

Check out the [`examples`](https://github.com/noamteyssier/paraseq/tree/main/examples) directory for code examples or the [API documentation](https://docs.rs/paraseq) for more information.

Feel free to also explore [seqpls](https://github.com/noamteyssier/seqpls) a parallel paired-end FASTA/FASTQ sequence grepper to see how `paraseq` can be used in a non-toy example.

### Basic Usage

This is a simple example using `paraseq` in a single-threaded context.

```rust
use std::fs::File;
use paraseq::fastq::{Reader, RecordSet};
use paraseq::fastx::Record;

fn main() -> Result<(), paraseq::fastq::Error> {
    let file = File::open("./data/sample.fastq")?;
    let mut reader = Reader::new(file);
    let mut record_set = RecordSet::new(1024); // Buffer up to 1024 records

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

```rust
use std::fs::File;
use paraseq::{
    fastq,
    fastx::Record,
    parallel::{ParallelProcessor, ParallelReader, ProcessError},
};

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
    let file = File::open("./data/sample.fastq")?;
    let reader = fastq::Reader::new(file);
    let processor = MyProcessor::default();
    let num_threads = 8;

    reader.process_parallel(processor, num_threads)?;
    Ok(())

}
```

### Paired-End Processing

Paired end processing is as simple as single-end processing, but involves implementing the `PairedParallelProcessor` trait on your struct.

```rust
use std::fs::File;
use paraseq::{
    fastq,
    fastx::Record,
    parallel::{PairedParallelProcessor, PairedParallelReader, ProcessError},
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
    let file1 = File::open("./data/r1.fastq")?;
    let file2 = File::open("./data/r2.fastq")?;

    let reader1 = fastq::Reader::new(file1);
    let reader2 = fastq::Reader::new(file2);
    let processor = MyPairedProcessor::default();
    let num_threads = 8;

    reader1.process_parallel_paired(reader2, processor, num_threads)?;
    Ok(())
}
```

### Interleaved Processing

Interleaved processing is also supported by implementing the `InterleavedParallelProcessor` trait on your struct.

```rust
use std::fs::File;
use paraseq::{
    fastq,
    fastx::Record,
    parallel::{InterleavedParallelProcessor, InterleavedParallelReader, ProcessError},
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
    let file = File::open("./data/interleaved.fastq")?;

    let reader = fastq::Reader::new(file);
    let processor = MyInterleavedProcessor::default();
    let num_threads = 8;

    reader.process_parallel_interleaved(processor, num_threads)?;
    Ok(())
}
```

## Limitations

1. In cases where records have high variance in size, the buffer size predictions will become inaccurate and the shared `Reader` overflow buffer will incur more copy operations. This may lead to suboptimal performance.

2. This library currently does not support multiline FASTA.

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
It can be faster than `seq_io` for some use cases, but it is not as feature-rich or rigorously tested, and it does not support multi-line FASTA files.

If the libraries assumptions do not fit your use case, you may want to consider using `seq_io` or `fastq` instead.
