# emstar

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE) [![Rust](https://img.shields.io/badge/rust-1.85+-orange.svg)](https://www.rust-lang.org) [![Cargo](https://img.shields.io/crates/v/emstar.svg)](https://crates.io/crates/emstar) [![Documentation](https://docs.rs/emstar/badge.svg)](https://docs.rs/emstar)

**emstar** is a high-performance Rust library for reading and writing [STAR files](https://en.wikipedia.org/wiki/Self-defining_Text_Archive_and_Retrieval).

Emstar provides fast, memory-efficient parsing, reading  and writing of STAR file formats commonly used in cryo-EM workflows.

## Features

- ⚡ **High Performance**: Written in Rust with zero-copy parsing where possible
- 🎯 **Type-Safe**: Strongly typed API with comprehensive error handling
- 📊 **Flexible Data**: Support for both simple blocks (key-value) and loop blocks (tabular data)
- 🗂️ **CRUD Operations**: Create, Read, Update, Delete APIs for easy data manipulation
- 📈 **Statistics API**: Analyze STAR file contents with detailed statistics
- 🧪 **Well-Tested**: Comprehensive test suite with integration tests
- 📊 **Benchmarks**: Performance benchmarks for large-scale data processing
- 🔧 **Easy to Use**: Simple, intuitive API similar to the Python starfile package

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
emstar = "0.1"
```

### Basic Usage

```rust
use emstar::{read, write, DataBlock, DataValue, LoopBlock};
use std::collections::HashMap;

// Read a STAR file
let data_blocks = read("particles.star")?;

// Access a data block
if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
    println!("Found {} particles", particles.row_count());
    println!("Columns: {:?}", particles.columns());
}

// Write modified data
write(&data_blocks, "modified_particles.star")?;
# Ok::<(), emstar::Error>(())
```

### Creating STAR Files

```rust
use emstar::{write, DataBlock, DataValue, LoopBlock, SimpleBlock};
use std::collections::HashMap;

// Create a simple block using array initialization
let simple: SimpleBlock = [
    ("rlnImageSize", DataValue::Integer(256)),
    ("rlnPixelSize", DataValue::Float(1.23)),
].into();

// Create a loop block using the builder pattern
let particles = LoopBlock::builder()
    .columns(&["rlnCoordinateX", "rlnCoordinateY"])
    .row(vec![
        DataValue::Float(91.7987),
        DataValue::Float(83.6226),
    ])
    .row(vec![
        DataValue::Float(97.6358),
        DataValue::Float(80.4370),
    ])
    .build()?;

// Combine into data blocks
let mut data = HashMap::new();
data.insert("general".to_string(), DataBlock::Simple(simple));
data.insert("particles".to_string(), DataBlock::Loop(particles));

// Write to file
write(&data, "output.star")?;
# Ok::<(), emstar::Error>(())
```

## File Operations

emstar provides simple read/write operations. For file management, use the Rust standard library:

```rust
use emstar::{read, write, stats};
use std::path::Path;
use std::fs;

// Read a file
let data = read("particles.star")?;

// Write a file (creates new or overwrites existing)
write(&data_blocks, "output.star")?;

// Check if file exists
if Path::new("particles.star").exists() {
    println!("File exists!");
}

// Delete a file
fs::remove_file("old.star")?;

// Get file statistics
let file_stats = stats("particles.star")?;
println!("Total blocks: {}", file_stats.n_blocks);
println!("Total particles: {}", file_stats.total_loop_rows);
```

### SimpleBlock CRUD

```rust
use emstar::{SimpleBlock, DataValue};

let mut block = SimpleBlock::new();

// Create
block.set("rlnImageSize", DataValue::Integer(256));
block.set("rlnPixelSize", DataValue::Float(1.06));

// Read
if let Some(value) = block.get("rlnImageSize") {
    println!("Image size: {:?}", value);
}

// Update
block.set("rlnPixelSize", DataValue::Float(1.12));

// Delete
block.remove("rlnPixelSize");
block.clear(); // Remove all

// Utilities
let has_key = block.contains_key("rlnImageSize");
let count = block.len();
```

### LoopBlock CRUD

```rust
use emstar::{LoopBlock, DataValue};

let mut block = LoopBlock::new();

// Create - add columns and rows
block.add_column("rlnCoordinateX");
block.add_column("rlnCoordinateY");
block.add_row(vec![
    DataValue::Float(100.0),
    DataValue::Float(200.0),
])?;

// Read
let value = block.get(0, 0); // row 0, col 0
let value = block.get_by_name(0, "rlnCoordinateX");
let column = block.get_column("rlnCoordinateX");

// Update
block.set_by_name(0, "rlnCoordinateX", DataValue::Float(150.0))?;
block.update_row(0, vec![DataValue::Float(150.0), DataValue::Float(250.0)])?;

// Delete
block.remove_row(0)?;
block.remove_column("rlnCoordinateY")?;
block.clear_rows(); // Keep columns, remove all rows
block.clear(); // Remove everything

// Utilities
let has_col = block.has_column("rlnCoordinateX");
let n_rows = block.row_count();
let n_cols = block.column_count();

// Iterate
for row in block.iter_rows() {
    println!("{:?}", row);
}
```

#### LoopBlock Builder Pattern

For more ergonomic LoopBlock creation, use the builder pattern:

```rust
use emstar::{LoopBlock, DataValue};

// Create a LoopBlock using the builder
let particles = LoopBlock::builder()
    .columns(&["rlnCoordinateX", "rlnCoordinateY", "rlnAnglePsi"])
    .row(vec![
        DataValue::Float(100.0),
        DataValue::Float(200.0),
        DataValue::Float(45.0),
    ])
    .row(vec![
        DataValue::Float(150.0),
        DataValue::Float(250.0),
        DataValue::Float(90.0),
    ])
    .build()?;

assert_eq!(particles.row_count(), 2);
assert_eq!(particles.column_count(), 3);
```

Builder methods:

- `.columns(&["col1", "col2"])` - Set all column names at once
- `.column("col_name")` - Add a single column
- `.rows(&[vec![...], vec![...]])` - Add multiple rows at once
- `.row(vec![...])` - Add a single row
- `.build()` - Construct the LoopBlock

### DataBlock Convenience Methods

Access blocks without verbose pattern matching:

```rust
use emstar::{DataBlock, SimpleBlock, LoopBlock};

let data_blocks = emstar::read("particles.star")?;

// Using expect_simple/expect_loop (panics with message if wrong type)
let simple_block = data_blocks.get("general")
    .expect_simple("general should be a SimpleBlock");
let loop_block = data_blocks.get("particles")
    .expect_loop("particles should be a LoopBlock");

// Using as_simple/as_loop (returns Option)
if let Some(simple) = data_blocks.get("general").as_simple() {
    // Work with SimpleBlock
}

// Check block type
if data_blocks.get("particles").is_loop() {
    // It's a LoopBlock
}
```

### SimpleBlock Array Initialization

Create a SimpleBlock from an array of key-value pairs:

```rust
use emstar::{SimpleBlock, DataValue};

let block: SimpleBlock = [
    ("rlnImageSize", DataValue::Integer(256)),
    ("rlnPixelSize", DataValue::Float(1.06)),
    ("rlnVoltage", DataValue::Float(300.0)),
].into();

assert_eq!(block.len(), 3);
```

## Statistics API

Analyze STAR file contents:

```rust
use emstar::{stats, block_stats, DataBlockStats};
use std::collections::HashMap;

// Get statistics from file (loads entire file into memory)
let file_stats = stats("particles.star")?;

println!("Total blocks: {}", file_stats.n_blocks);
println!("SimpleBlocks: {}", file_stats.n_simple_blocks);
println!("LoopBlocks: {}", file_stats.n_loop_blocks);
println!("Total particles: {}", file_stats.total_loop_rows);
println!("Avg rows per LoopBlock: {:.1}", file_stats.avg_rows_per_loop());

// Get specific block stats
if let Some(DataBlockStats::Loop(l)) = file_stats.get_block_stats("particles") {
    println!("Particles: {} rows x {} cols", l.n_rows, l.n_cols);
}

// Get stats from in-memory data
let blocks: HashMap<String, DataBlock> = read("particles.star")?;
let mem_stats = block_stats(&blocks);
```

## Data Types

emstar provides strongly typed data representations:

### DataValue

Represents a single value in a STAR file:

- `DataValue::String(String)` - String values
- `DataValue::Integer(i64)` - Integer values
- `DataValue::Float(f64)` - Floating-point values
- `DataValue::Null` - Null/NA values

### DataBlock

Represents a data block in a STAR file:

- `DataBlock::Simple(SimpleBlock)` - Key-value pairs
- `DataBlock::Loop(LoopBlock)` - Tabular data with columns and rows

### SimpleBlock

Key-value pairs for simple data blocks:

```rust
let mut block = SimpleBlock::new();
block.set("key", DataValue::String("value".into()));
let value = block.get("key");

// Statistics
let stats = block.stats();
println!("Entries: {}", stats.n_entries);
```

### LoopBlock

Tabular data for loop blocks:

```rust
// Using the builder pattern (recommended)
let block = LoopBlock::builder()
    .columns(&["col1", "col2"])
    .row(vec![DataValue::Integer(1), DataValue::Integer(2)])
    .build()?;

// Or using traditional methods
let mut block = LoopBlock::new();
block.add_column("col1");
block.add_column("col2");
block.add_row(vec![DataValue::Integer(1), DataValue::Integer(2)])?;

let n_rows = block.row_count();
let n_cols = block.column_count();
let value = block.get(0, 0); // Get value at row 0, col 0
let value = block.get_by_name(0, "col2"); // Get value by column name (row first!)

// Statistics
let stats = block.stats();
println!("Rows: {}, Cols: {}, Cells: {}", stats.n_rows, stats.n_cols, stats.n_cells);
```

## API Reference

### I/O Functions

| Function | Description |
|----------|-------------|
| `read(path)` | Read a STAR file from disk |
| `write(&data, path)` | Write data to a STAR file (creates or overwrites) |
| `to_string(&data)` | Convert data to STAR format string |
| `list_blocks(&blocks)` | List all blocks with their names and types |

For file management (delete, exists), use `std::fs` and `std::path::Path`.

### Statistics Functions

| Function | Description |
|----------|-------------|
| `stats(path)` | Calculate statistics for a STAR file |
| `block_stats(blocks)` | Calculate statistics from in-memory data |

### SimpleBlock Methods

| Method | Description |
|--------|-------------|
| `get(key)` | Read value by key |
| `set(key, value)` | Create/Update value |
| `remove(key)` | Delete key-value pair |
| `contains_key(key)` | Check if key exists |
| `keys()` | Get all keys |
| `len()` | Get number of entries |
| `clear()` | Remove all entries |
| `stats()` | Get block statistics |

### LoopBlock Methods

| Method | Description |
|--------|-------------|
| `get(row, col)` | Read value at position |
| `get_by_name(row, col_name)` | Read value by column name |
| `get_column(name)` | Get entire column |
| `iter_rows()` | Iterate over all rows |
| `add_column(name)` | Add a column |
| `add_row(values)` | Add a row |
| `set_by_name(row, col_name, value)` | Update a cell |
| `update_row(idx, values)` | Update entire row |
| `remove_row(idx)` | Delete a row |
| `remove_column(name)` | Delete a column |
| `clear_rows()` | Remove all rows |
| `clear()` | Remove all data |
| `has_column(name)` | Check if column exists |
| `row_count()` | Get number of rows |
| `column_count()` | Get number of columns |
| `stats()` | Get block statistics |
| `builder()` | Create a LoopBlockBuilder (fluent API) |

### Statistics Types

| Type | Description |
|------|-------------|
| `StarStats` | Comprehensive STAR file statistics |
| `DataBlockStats` | Block-level statistics (enum) |
| `LoopBlockStats` | LoopBlock statistics (rows, cols, cells) |
| `SimpleBlockStats` | SimpleBlock statistics (entries) |

## Examples

See the `examples/` directory for comprehensive examples:

- `basic_usage.rs` - Basic read/write operations with Polars integration
- `crud_operations.rs` - Complete CRUD operations demonstration
- `statistics.rs` - Statistics API usage

Run examples:

```bash
cargo run --example basic_usage
cargo run --example crud_operations
cargo run --example statistics
```

## Performance

emstar is designed for high performance:

- **Zero-copy parsing** where possible using `SmartString`
- **Efficient numeric parsing** using `lexical`
- **Optimized memory layout** for loop blocks using Polars DataFrames
- **Streaming I/O** for large files

### Benchmarks

Run benchmarks:

```bash
cargo bench
```

Benchmark results (on typical hardware):

- Parse 10,000 rows: ~5-10ms
- Write 10,000 rows: ~2-5ms
- Parse 100,000 rows: ~50-100ms
- Write 100,000 rows: ~20-50ms

## Testing

Run tests:

```bash
cargo test
```

Run tests with coverage:

```bash
cargo tarpaulin --out Html
```

## Error Handling

emstar uses `thiserror` for comprehensive error types:

```rust
use emstar::Error;

match read("file.star") {
    Ok(data) => { /* process data */ }
    Err(Error::FileNotFound(path)) => eprintln!("File not found: {:?}", path),
    Err(Error::Parse { line, message }) => eprintln!("Parse error at line {}: {}", line, message),
    Err(e) => eprintln!("Error: {}", e),
}
```

## Features

- `default` - Core functionality
- `serde` - Optional serde support for (de)serialization (planned)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by the Python [starfile](https://github.com/teamtomo/starfile) package
- Designed for compatibility with [RELION](https://www3.mrc-lmb.cam.ac.uk/relion/index.php/Main_Page)

## Links

- [Documentation](https://docs.rs/emstar)
- [Repository](https://github.com/elemeng/emstar)
- [Issues](https://github.com/elemeng/emstar/issues)
- [Python starfile](https://github.com/teamtomo/starfile)
