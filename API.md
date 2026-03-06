# emstar API Documentation

High-performance STAR file I/O library for Rust, designed for cryo-EM software like RELION.

---

## Table of Contents

- [Quick Start](#quick-start)
- [File-Level Operations](#file-level-operations)
- [Data Block Types](#data-block-types)
- [SimpleBlock API](#simpleblock-api)
- [LoopBlock API](#loopblock-api)
- [Statistics API](#statistics-api)
- [DataValue API](#datavalue-api)
- [Error Handling](#error-handling)
- [Examples](#examples)

---

## Quick Start

```rust
use emstar::{read, write, DataBlock};

// Read a STAR file
let data_blocks = read("particles.star")?;

// Access a data block
if let Some(DataBlock::Loop(df)) = data_blocks.get("particles") {
    println!("Found {} particles", df.row_count());
}

// Write modified data
write(&data_blocks, "output.star")?;
```

---

## File-Level Operations

### `read`

Read a STAR file from disk.

```rust
pub fn read<P: AsRef<Path>>(path: P) -> Result<HashMap<String, DataBlock>>
```

**Arguments:**

- `path` - Path to the STAR file to read

**Returns:**

- `Result<HashMap<String, DataBlock>>` - Map of block names to data blocks

**Example:**

```rust
use emstar::read;

let data = read("particles.star")?;
```

---

### `write`

Write data blocks to a STAR file. Creates a new file or overwrites an existing file.

```rust
pub fn write<P: AsRef<Path>>(
    data_blocks: &HashMap<String, DataBlock>,
    path: P,
) -> Result<()>
```

**Arguments:**

- `data_blocks` - HashMap of data blocks to write
- `path` - Path where the STAR file will be written

**Example:**

```rust
use emstar::write;

// Create new file or overwrite existing
write(&data_blocks, "output.star")?;
```

---

### `to_string`

Convert data blocks to a STAR format string.

```rust
pub fn to_string(data_blocks: &HashMap<String, DataBlock>) -> Result<String>
```

**Arguments:**

- `data_blocks` - HashMap of data blocks to convert

**Returns:**

- `Result<String>` - STAR format string

**Example:**

```rust
use emstar::to_string;

let star_string = to_string(&data_blocks)?;
```

---

### `list_blocks`

List all data blocks with their names and types.

```rust
pub fn list_blocks(blocks: &HashMap<String, DataBlock>) -> Vec<(String, &'static str)>
```

**Arguments:**

- `blocks` - HashMap of data blocks

**Returns:**

- `Vec<(String, &str)>` - Vector of (block_name, block_type) tuples where block_type is "SimpleBlock" or "LoopBlock"

**Example:**

```rust
use emstar::{list_blocks, read};

let data_blocks = read("particles.star")?;
let blocks = list_blocks(&data_blocks);

for (name, block_type) in blocks {
    println!("Block '{}' is a {}", name, block_type);
}
```

---

### File Operations with Standard Library

For file management operations like checking existence or deleting files, use the Rust standard library:

```rust
use std::path::Path;
use std::fs;

// Check if file exists
if Path::new("particles.star").exists() {
    println!("File exists!");
}

// Delete a file
fs::remove_file("old_file.star")?;
```

---

## Data Block Types

### `DataBlock`

Enum representing either a SimpleBlock or LoopBlock.

```rust
pub enum DataBlock {
    Simple(SimpleBlock),
    Loop(LoopBlock),
}
```

**Methods:**

- `is_simple() -> bool` - Check if block is a SimpleBlock
- `is_loop() -> bool` - Check if block is a LoopBlock
- `as_simple() -> Option<&SimpleBlock>` - Get immutable SimpleBlock reference
- `as_loop() -> Option<&LoopBlock>` - Get immutable LoopBlock reference
- `as_simple_mut() -> Option<&mut SimpleBlock>` - Get mutable SimpleBlock reference
- `as_loop_mut() -> Option<&mut LoopBlock>` - Get mutable LoopBlock reference
- `expect_simple(msg: &str) -> &SimpleBlock` - Get SimpleBlock or panic with message
- `expect_loop(msg: &str) -> &LoopBlock` - Get LoopBlock or panic with message
- `block_type() -> &'static str` - Get block type as string
- `count() -> usize` - Count entries (SimpleBlock) or rows (LoopBlock)
- `stats() -> DataBlockStats` - Get block statistics

---

### DataBlock Convenience Methods

Access blocks without verbose pattern matching:

```rust
use emstar::{read, DataBlock, SimpleBlock, LoopBlock};

let data_blocks = read("particles.star")?;

// Using expect methods (panics with message if wrong type)
if let Some(block) = data_blocks.get("general") {
    let general: &SimpleBlock = block.expect_simple("general should be a SimpleBlock");
}
if let Some(block) = data_blocks.get("particles") {
    let particles: &LoopBlock = block.expect_loop("particles should be a LoopBlock");
}

// Using as methods (returns Option)
if let Some(block) = data_blocks.get("general") {
    if let Some(simple) = block.as_simple() {
        // Work with SimpleBlock
    }
}

// Check block type
if let Some(block) = data_blocks.get("particles") {
    if block.is_loop() {
        // It's a LoopBlock
    }
}
```

---

### SimpleBlock Array Initialization

Create a SimpleBlock from an array of key-value pairs:

```rust
use emstar::{SimpleBlock, DataValue};

let general: SimpleBlock = [
    ("rlnImageSize", DataValue::Integer(256)),
    ("rlnPixelSize", DataValue::Float(1.06)),
    ("rlnVoltage", DataValue::Float(300.0)),
].into();

assert_eq!(general.len(), 3);
```

---

## SimpleBlock API

A simple block contains key-value pairs.

### Creation

```rust
let mut block = SimpleBlock::new();
```

### CRUD Operations

#### `get`

Get a value by key.

```rust
pub fn get(&self, key: &str) -> Option<&DataValue>
```

**Example:**

```rust
if let Some(value) = block.get("rlnImageSize") {
    println!("Image size: {:?}", value);
}
```

---

#### `set`

Set or update a value.

```rust
pub fn set(&mut self, key: &str, value: DataValue)
```

**Example:**

```rust
block.set("rlnImageSize", DataValue::Integer(256));
```

---

#### `remove`

Remove a key-value pair.

```rust
pub fn remove(&mut self, key: &str) -> Option<DataValue>
```

**Example:**

```rust
block.remove("old_key");
```

---

#### `contains_key`

Check if a key exists.

```rust
pub fn contains_key(&self, key: &str) -> bool
```

**Example:**

```rust
if block.contains_key("rlnImageSize") {
    // Key exists
}
```

---

### Metadata

#### `keys`

Get all keys in the block.

```rust
pub fn keys(&self) -> impl Iterator<Item = &str>
```

**Example:**

```rust
for key in block.keys() {
    println!("Key: {}", key);
}
```

---

#### `len`

Get the number of key-value pairs.

```rust
pub fn len(&self) -> usize
```

---

#### `is_empty`

Check if the block is empty.

```rust
pub fn is_empty(&self) -> bool
```

---

#### `stats`

Get block statistics.

```rust
pub fn stats(&self) -> SimpleBlockStats
```

---

#### `clear`

Remove all key-value pairs.

```rust
pub fn clear(&mut self)
```

---

## LoopBlock API

A loop block contains tabular data with columns and rows.

### Creation

```rust
let mut block = LoopBlock::new();
```

---

### Column Operations

#### `add_column`

Add a new column to the block.

```rust
pub fn add_column(&mut self, name: &str)
```

**Example:**

```rust
block.add_column("rlnCoordinateX");
block.add_column("rlnCoordinateY");
```

---

#### `remove_column`

Remove a column from the block.

```rust
pub fn remove_column(&mut self, name: &str) -> Result<()>
```

**Example:**

```rust
block.remove_column("old_column")?;
```

---

#### `columns`

Get all column names.

```rust
pub fn columns(&self) -> Vec<String>
```

**Example:**

```rust
let cols = block.columns();
println!("Columns: {:?}", cols);
```

---

#### `has_column`

Check if a column exists.

```rust
pub fn has_column(&self, name: &str) -> bool
```

**Example:**

```rust
if block.has_column("rlnCoordinateX") {
    // Column exists
}
```

---

#### `get_column`

Get all values in a column.

```rust
pub fn get_column(&self, name: &str) -> Option<Vec<DataValue>>
```

**Example:**

```rust
if let Some(values) = block.get_column("rlnCoordinateX") {
    println!("X coordinates: {:?}", values);
}
```

---

### Row Operations

#### `add_row`

Add a new row to the block.

```rust
pub fn add_row(&mut self, row: Vec<DataValue>) -> Result<()>
```

**Example:**

```rust
block.add_row(vec![
    DataValue::Float(100.0),
    DataValue::Float(200.0),
])?;
```

---

#### `remove_row`

Remove a row by index.

```rust
pub fn remove_row(&mut self, index: usize) -> Result<()>
```

**Example:**

```rust
block.remove_row(0)?;
```

---

#### `iter_rows`

Iterate over all rows.

```rust
pub fn iter_rows(&self) -> impl Iterator<Item = Vec<DataValue>> + '_
```

**Example:**

```rust
for (i, row) in block.iter_rows().enumerate() {
    println!("Row {}: {:?}", i, row);
}
```

---

### Cell Operations

#### `get`

Get a cell value by row and column index.

```rust
pub fn get(&self, row: usize, col: usize) -> Option<DataValue>
```

**Example:**

```rust
if let Some(value) = block.get(0, 0) {
    println!("Cell value: {:?}", value);
}
```

---

#### `get_by_name`

Get a cell value by row index and column name.

```rust
pub fn get_by_name(&self, row: usize, col_name: &str) -> Option<DataValue>
```

**Example:**

```rust
if let Some(value) = block.get_by_name(0, "rlnCoordinateX") {
    println!("X coordinate: {:?}", value);
}
```

---

#### `set`

Set a cell value by row and column index.

```rust
pub fn set(&mut self, row: usize, col: usize, value: DataValue) -> Result<()>
```

**Example:**

```rust
block.set(0, 0, DataValue::Float(150.0))?;
```

---

#### `set_by_name`

Set a cell value by row index and column name.

```rust
pub fn set_by_name(
    &mut self,
    row: usize,
    col_name: &str,
    value: DataValue
) -> Result<()>
```

**Example:**

```rust
block.set_by_name(0, "rlnCoordinateX", DataValue::Float(150.0))?;
```

---

### Metadata

#### `row_count`

Get the number of rows.

```rust
pub fn row_count(&self) -> usize
```

---

#### `column_count`

Get the number of columns.

```rust
pub fn column_count(&self) -> usize
```

---

#### `is_empty`

Check if the block is empty.

```rust
pub fn is_empty(&self) -> bool
```

---

#### `stats`

Get block statistics.

```rust
pub fn stats(&self) -> LoopBlockStats
```

---

#### `from_dataframe`

Create a LoopBlock from a Polars DataFrame.

```rust
pub fn from_dataframe(df: DataFrame) -> Self
```

---

#### `builder`

Create a builder for constructing a LoopBlock with a fluent API.

```rust
pub fn builder() -> LoopBlockBuilder
```

**Example:**

```rust
use emstar::{LoopBlock, DataValue};

let block = LoopBlock::builder()
    .columns(&["rlnCoordinateX", "rlnCoordinateY"])
    .rows(&[
        vec![DataValue::Float(100.0), DataValue::Float(200.0)],
        vec![DataValue::Float(150.0), DataValue::Float(250.0)],
    ])
    .build()?;
```

### LoopBlockBuilder

A builder for constructing LoopBlocks with a fluent API.

#### `columns`

Set the column names.

```rust
pub fn columns(self, columns: &[&str]) -> Self
```

#### `column`

Add a single column.

```rust
pub fn column(self, name: &str) -> Self
```

#### `rows`

Set all rows at once.

```rust
pub fn rows(self, rows: &[Vec<DataValue>]) -> Self
```

#### `row`

Add a single row.

```rust
pub fn row(self, row: Vec<DataValue>) -> Self
```

#### `build`

Build the LoopBlock.

```rust
pub fn build(self) -> Result<LoopBlock>
```

---

## Statistics API

### `stats`

Calculate statistics for a STAR file.

**Note:** This function loads the entire file into memory.

```rust
pub fn stats<P: AsRef<Path>>(path: P) -> Result<StarStats>
```

**Example:**

```rust
use emstar::stats;

let stats = stats("particles.star")?;
println!("Total blocks: {}", stats.n_blocks);
println!("Loop blocks: {}", stats.n_loop_blocks);
println!("Total particles: {}", stats.total_loop_rows);
```

---

### `block_stats`

Get statistics for data blocks in memory.

```rust
pub fn block_stats(blocks: &HashMap<String, DataBlock>) -> StarStats
```

**Example:**

```rust
use emstar::block_stats;

let stats = block_stats(&data_blocks);
println!("Total blocks: {}", stats.n_blocks);
```

---

### `StarStats`

Statistics about a STAR file.

**Fields:**

- `n_blocks: usize` - Total number of blocks
- `n_simple_blocks: usize` - Number of SimpleBlocks
- `n_loop_blocks: usize` - Number of LoopBlocks
- `total_loop_rows: usize` - Total rows across all LoopBlocks
- `total_loop_cols: usize` - Total columns across all LoopBlocks
- `total_simple_entries: usize` - Total entries across all SimpleBlocks
- `block_stats: HashMap<String, DataBlockStats>` - Per-block statistics

**Methods:**

- `has_loop_blocks() -> bool` - Check if file has any LoopBlocks
- `has_simple_blocks() -> bool` - Check if file has any SimpleBlocks
- `avg_rows_per_loop() -> f64` - Average rows per LoopBlock
- `get_block_stats(&str) -> Option<&DataBlockStats>` - Get stats for specific block

---

## DataValue API

Represents a value in a STAR file.

```rust
pub enum DataValue {
    String(SmartString),
    Integer(i64),
    Float(f64),
    Null,
}
```

### Conversion Methods

#### `as_integer`

Try to convert to an integer.

```rust
pub fn as_integer(&self) -> Option<i64>
```

**Example:**

```rust
if let Some(i) = value.as_integer() {
    println!("Integer value: {}", i);
}
```

---

#### `as_float`

Try to convert to a float.

```rust
pub fn as_float(&self) -> Option<f64>
```

**Example:**

```rust
if let Some(f) = value.as_float() {
    println!("Float value: {}", f);
}
```

---

#### `as_string`

Try to convert to a string.

```rust
pub fn as_string(&self) -> Option<&str>
```

**Example:**

```rust
if let Some(s) = value.as_string() {
    println!("String value: {}", s);
}
```

---

#### `is_null`

Check if value is null.

```rust
pub fn is_null(&self) -> bool
```

---

## Error Handling

### `Error`

Error types that can occur.

```rust
pub enum Error {
    FileNotFound(PathBuf),
    Io(io::Error),
    Parse { line: usize, message: String },
}
```

### `Result`

Type alias for Result with emstar Error.

```rust
pub type Result<T> = std::result::Result<T, Error>;
```

---

## Examples

### Reading a STAR File

```rust
use emstar::{read, DataBlock};

let data_blocks = read("particles.star")?;

// Access a specific block
if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
    println!("Found {} particles", particles.row_count());
    println!("Columns: {:?}", particles.columns());
}
```

---

### Creating a New STAR File

```rust
use emstar::{write, SimpleBlock, LoopBlock, DataBlock, DataValue};
use std::collections::HashMap;

let mut data = HashMap::new();

// Create a simple block
let mut general = SimpleBlock::new();
general.set("rlnImageSize", DataValue::Integer(256));
general.set("rlnPixelSize", DataValue::Float(1.2));
data.insert("general".to_string(), DataBlock::Simple(general));

// Create a loop block
let mut particles = LoopBlock::new();
particles.add_column("rlnCoordinateX");
particles.add_column("rlnCoordinateY");
particles.add_column("rlnCoordinateZ");

particles.add_row(vec![
    DataValue::Float(100.0),
    DataValue::Float(200.0),
    DataValue::Float(50.0),
])?;

data.insert("particles".to_string(), DataBlock::Loop(particles));

// Write to file (creates or overwrites)
write(&data, "output.star")?;
```

---

### Modifying an Existing STAR File

```rust
use emstar::{read, write, DataBlock, DataValue};

let mut data_blocks = read("particles.star")?;

// Modify a simple block
if let Some(DataBlock::Simple(general)) = data_blocks.get_mut("general") {
    general.set("rlnImageSize", DataValue::Integer(512));
}

// Modify a loop block
if let Some(DataBlock::Loop(particles)) = data_blocks.get_mut("particles") {
    // Add a new particle
    particles.add_row(vec![
        DataValue::Float(150.0),
        DataValue::Float(250.0),
        DataValue::Float(75.0),
    ])?;
}

write(&data_blocks, "modified_particles.star")?;
```

---

### Querying Particle Data

```rust
use emstar::{read, DataBlock};

let data_blocks = read("particles.star")?;

if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
    let x_coords = particles.get_column("rlnCoordinateX").unwrap();
    let y_coords = particles.get_column("rlnCoordinateY").unwrap();
    
    for (x, y) in x_coords.iter().zip(y_coords.iter()) {
        if let (Some(DataValue::Float(x_val)), Some(DataValue::Float(y_val))) = (x, y) {
            println!("Particle at ({}, {})", x_val, y_val);
        }
    }
}
```

---

### Computing Statistics

```rust
use emstar::{stats, block_stats};

// Get statistics from file
let file_stats = stats("particles.star")?;
println!("Total particles: {}", file_stats.total_loop_rows);
println!("Average rows per block: {:.1}", file_stats.avg_rows_per_loop());

// Get statistics from loaded data
let data_blocks = emstar::read("particles.star")?;
let mem_stats = block_stats(&data_blocks);
println!("Total blocks: {}", mem_stats.n_blocks);
```

---

### Filtering Data

```rust
use emstar::{read, write, DataBlock, DataValue};
use std::collections::HashMap;

let data_blocks = read("particles.star")?;

if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
    let mut filtered = LoopBlock::new();
    
    // Copy column structure
    for col in particles.columns() {
        filtered.add_column(col);
    }
    
    // Filter rows
    for row in particles.iter_rows() {
        if let Some(DataValue::Float(x)) = row.get(0) {
            if *x > 100.0 {  // Keep particles with x > 100
                filtered.add_row(row).unwrap();
            }
        }
    }
    
    let mut output = HashMap::new();
    output.insert("particles".to_string(), DataBlock::Loop(filtered));
    write(&output, "filtered_particles.star")?;
}
```

---

## Performance Considerations

- **Parsing:** Uses efficient parsing with lexical crate for fast number parsing
- **Memory:** LoopBlocks use Polars DataFrames for efficient columnar storage
- **String Storage:** Uses `SmartString` for small string optimization
- **Builder Pattern:** Use `LoopBlock::builder()` for ergonomic block construction

---

## License

See LICENSE file for details.
