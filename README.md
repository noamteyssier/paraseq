# paraseq

A high-performance Rust library for parallel processing of FASTA/FASTQ sequence files, optimized for modern hardware and large datasets.

## Features

- **Efficient Record Buffering**: Uses RecordSets as the primary unit of buffering, with each set managing its own memory and dynamically adapting to record sizes
- **Zero-Copy Records**: Records are reference-based and avoid unnecessary allocations.
- **Minimal-Copy Processing**: Minimizes copies between buffers by accurately estimating required space
- **Parallel Processing**: Built-in support for both single-file, paired-end, and interleaved parallel processing
- **Adaptive Buffer Management**: Automatically adjusts buffer sizes based on observed record sizes
- **SIMD-Accelerated Parsing**: Uses `memchr` for optimized newline scanning
- **Error Handling**: Comprehensive error types for robust error handling and recovery
- **Flexible Processing**: Supports both FASTA and FASTQ formats with the same interface
- **Thread Safety**: Thread-safe design for parallel processing with minimal synchronization

## Design

paraseq takes a unique approach to sequence file parsing:

1. **RecordSet-Centric Design**: Unlike traditional parsers that work on individual records, paraseq operates on sets of records. Each RecordSet:

- Maintains its own buffer
- First fills from overflow bytes (incomplete records from previous reads)
- Dynamically expands to accommodate its target capacity
- Uses runtime statistics to optimize buffer sizes

2. **Optimized Memory Management**:

- Tracks average record sizes to predict optimal buffer allocations
- Minimizes copies between buffers by accurately estimating required space
- Uses a smart overflow system for handling records that span buffer boundaries

3. **Parallel Processing Architecture**:

- Double-buffering design for optimal throughput
- Lock-free communication between reader and worker threads
- Support for both single-end and paired-end processing

## Usage

Check out the `examples` directory for more detailed examples or the [API documentation](https://docs.rs/paraseq) for more information.

### Basic Usage

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

- **Record Size Variance**: This library is optimized for sequence files where records have similar sizes. It may not perform well with files that have large discrepancies in record sizes, as the buffer size predictions become less accurate.
- **Multiline FASTA**: The library does not support multiline FASTA format. All sequences must be on a single line.
- **Memory Usage**: Since each RecordSet maintains its own buffer, memory usage scales with the number of threads and record capacity.

## Performance Considerations

- **Buffer Sizes**: Default RecordSet buffer is 256KB, which works well for most use cases. Adjust based on your specific needs.
- **Record Capacity**: Choose RecordSet capacity based on your processing patterns. Higher capacities reduce system calls but increase memory usage.
- **Thread Count**: For optimal performance, use thread count equal to or slightly less than available CPU cores.
- **Memory Usage**: Memory usage scales with thread count × record capacity × average record size.

For optimal performance, the project uses native CPU optimizations. You can customize this in `.cargo/config.toml`.

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Similar Projects

This work is inspired by the following projects:

- [seq_io](https://github.com/markschl/seq_io)
- [fastq](https://github.com/aseyboldt/fastq-rs)

This project aims to be directed more specifically at ergonomically processing of paired records in parallel and is optimized mainly for FASTQ files.
It can be faster than seq_io for some use cases, but it is not as feature-rich or rigorously tested, and it does not support multi-line FASTA files.

If the libraries assumptions do not fit your use case, you may want to consider using seq_io or fastq instead.

## Benchmarks

For performance benchmarks, see the following repository: [paraseq-benchmarks](https://github.com/noamteyssier/paraseq_benchmark).
