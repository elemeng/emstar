//! # emstar
//!
//! High-performance STAR file I/O library for Rust, optimized for cryo-EM workflows.
//!
//! ## Overview
//!
//! emstar provides a fast, type-safe, and ergonomic API for reading, writing, and manipulating
//! STAR (Self-defining Text Archival and Retrieval) files. These files are widely used in
//! cryo-electron microscopy software like RELION for storing particle data, optimization parameters,
//! and processing metadata.
//!
//! ## Features
//!
//! - **Fast Parsing**: Efficient parsing with lexical number parsing and smart string optimization
//! - **Type-Safe API**: Strong Rust typing ensures correctness at compile time
//! - **Comprehensive API**: Full CRUD operations for files, data blocks, and individual values
//! - **STAR Format Support**: Handles quoted strings, empty values, multi-block files, and edge cases
//! - **Statistics**: Compute file statistics without loading entire dataset into memory
//! - **Zero-Copy Operations**: Efficient columnar storage using Polars DataFrames
//!
//! ## Data Structures
//!
//! STAR files contain one or more data blocks, each of which can be:
//!
//! - **SimpleBlock**: Key-value pairs (e.g., global parameters)
//! - **LoopBlock**: Tabular data with columns and rows (e.g., particle coordinates)
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use emstar::{read, write, DataBlock};
//!
//! // Read a STAR file
//! let data_blocks = read("particles.star")?;
//!
//! // Access a data block
//! if let Some(DataBlock::Loop(df)) = data_blocks.get("particles") {
//!     println!("Found {} particles", df.row_count());
//!     println!("Columns: {:?}", df.columns());
//! }
//!
//! // Write modified data
//! write(&data_blocks, "output.star")?;
//! # Ok::<(), emstar::Error>(())
//! ```
//!
//! ## Creating a New STAR File
//!
//! ```rust
//! use emstar::{write, SimpleBlock, LoopBlock, DataBlock, DataValue};
//! use std::collections::HashMap;
//!
//! let mut data = HashMap::new();
//!
//! // Create a simple block using array initialization
//! let general: SimpleBlock = [
//!     ("rlnImageSize", DataValue::Integer(256)),
//!     ("rlnPixelSize", DataValue::Float(1.2)),
//! ].into();
//! data.insert("general".to_string(), DataBlock::Simple(general));
//!
//! // Create a loop block using the builder pattern
//! let particles = LoopBlock::builder()
//!     .columns(&["rlnCoordinateX", "rlnCoordinateY", "rlnCoordinateZ"])
//!     .row(vec![
//!         DataValue::Float(100.0),
//!         DataValue::Float(200.0),
//!         DataValue::Float(50.0),
//!     ])
//!     .build()?;
//!
//! data.insert("particles".to_string(), DataBlock::Loop(particles));
//!
//! write(&data, "output.star")?;
//! # Ok::<(), emstar::Error>(())
//! ```
//!
//! ## Querying Data
//!
//! ```rust,no_run
//! use emstar::{read, DataBlock, DataValue};
//!
//! let data_blocks = read("particles.star")?;
//!
//! if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
//!     // Get column data
//!     let x_coords = particles.get_column("rlnCoordinateX").unwrap();
//!     let y_coords = particles.get_column("rlnCoordinateY").unwrap();
//!
//!     // Iterate over coordinates
//!     for (x, y) in x_coords.iter().zip(y_coords.iter()) {
//!         if let (DataValue::Float(x_val), DataValue::Float(y_val)) = (x, y) {
//!             println!("Particle at ({}, {})", x_val, y_val);
//!         }
//!     }
//! }
//! # Ok::<(), emstar::Error>(())
//! ```
//!
//! ## Computing Statistics
//!
//! ```rust,no_run
//! use emstar::stats;
//!
//! // Get statistics from file (loads entire file into memory)
//! let file_stats = stats("particles.star")?;
//! println!("Total blocks: {}", file_stats.n_blocks);
//! println!("Loop blocks: {}", file_stats.n_loop_blocks);
//! println!("Total particles: {}", file_stats.total_loop_rows);
//! println!("Average rows per block: {:.1}", file_stats.avg_rows_per_loop());
//! # Ok::<(), emstar::Error>(())
//! ```
//!
//! ## Data Block Operations
//!
//! ### SimpleBlock (Key-Value Pairs)
//!
//! ```rust,no_run
//! use emstar::{read, DataBlock};
//!
//! let data_blocks = read("parameters.star")?;
//!
//! if let Some(DataBlock::Simple(params)) = data_blocks.get("general") {
//!     // Get a value
//!     if let Some(value) = params.get("rlnImageSize") {
//!         println!("Image size: {:?}", value);
//!     }
//!
//!     // Set a value
//!     // params.set("new_key", DataValue::Integer(42));
//!
//!     // Check if key exists
//!     if params.contains_key("rlnImageSize") {
//!         println!("Key exists");
//!     }
//!
//!     // Get all keys
//!     for key in params.keys() {
//!         println!("Key: {}", key);
//!     }
//! }
//! # Ok::<(), emstar::Error>(())
//! ```
//!
//! ### LoopBlock (Tabular Data)
//!
//! ```rust,no_run
//! use emstar::{read, DataBlock};
//!
//! let data_blocks = read("particles.star")?;
//!
//! if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
//!     // Get dimensions
//!     println!("{} particles with {} columns", particles.row_count(), particles.column_count());
//!
//!     // Get column names
//!     let columns = particles.columns();
//!     println!("Columns: {:?}", columns);
//!
//!     // Get cell value by index
//!     if let Some(value) = particles.get(0, 0) {
//!         println!("First cell: {:?}", value);
//!     }
//!
//!     // Get cell value by column name
//!     if let Some(value) = particles.get_by_name(0, "rlnCoordinateX") {
//!         println!("First X coordinate: {:?}", value);
//!     }
//!
//!     // Iterate over rows
//!     for (i, row) in particles.iter_rows().enumerate() {
//!         println!("Row {}: {:?}", i, row);
//!     }
//! }
//! # Ok::<(), emstar::Error>(())
//! ```
//!
//! ### LoopBlock Builder Pattern
//!
//! Use the builder pattern for more ergonomic LoopBlock creation:
//!
//! ```rust
//! use emstar::{LoopBlock, DataValue};
//!
//! let particles = LoopBlock::builder()
//!     .columns(&["rlnCoordinateX", "rlnCoordinateY", "rlnAnglePsi"])
//!     .row(vec![
//!         DataValue::Float(100.0),
//!         DataValue::Float(200.0),
//!         DataValue::Float(45.0),
//!     ])
//!     .row(vec![
//!         DataValue::Float(150.0),
//!         DataValue::Float(250.0),
//!         DataValue::Float(90.0),
//!     ])
//!     .build()?;
//!
//! assert_eq!(particles.row_count(), 2);
//! assert_eq!(particles.column_count(), 3);
//! # Ok::<(), emstar::Error>(())
//! ```
//!
//! ### SimpleBlock Array Initialization
//!
//! Create a SimpleBlock from an array of key-value pairs:
//!
//! ```rust
//! use emstar::{SimpleBlock, DataValue};
//!
//! let general: SimpleBlock = [
//!     ("rlnImageSize", DataValue::Integer(256)),
//!     ("rlnPixelSize", DataValue::Float(1.06)),
//!     ("rlnVoltage", DataValue::Float(300.0)),
//! ].into();
//!
//! assert_eq!(general.len(), 3);
//! ```
//!
//! ### DataBlock Convenience Methods
//!
//! Access blocks without verbose pattern matching:
//!
//! ```rust,no_run
//! use emstar::{read, DataBlock, SimpleBlock, LoopBlock};
//!
//! let data_blocks = read("particles.star")?;
//!
//! // Using expect methods (panics with message if wrong type)
//! if let Some(block) = data_blocks.get("general") {
//!     let general: &SimpleBlock = block.expect_simple("general should be a SimpleBlock");
//! }
//! if let Some(block) = data_blocks.get("particles") {
//!     let particles: &LoopBlock = block.expect_loop("particles should be a LoopBlock");
//! }
//!
//! // Using as methods (returns Option)
//! if let Some(block) = data_blocks.get("general") {
//!     if let Some(simple) = block.as_simple() {
//!         // Work with SimpleBlock
//!     }
//! }
//!
//! // Check block type
//! if let Some(block) = data_blocks.get("particles") {
//!     if block.is_loop() {
//!         // It's a LoopBlock
//!     }
//! }
//! # Ok::<(), emstar::Error>(())
//! ```
//!
//! ## Error Handling
//!
//! All functions return `Result<T, Error>`. Common error types:
//!
//! - `Error::FileNotFound` - The specified file does not exist
//! - `Error::Io` - I/O error occurred
//! - `Error::Parse` - Failed to parse the STAR file
//!
//! ```rust,no_run
//! use emstar::{read, Error};
//!
//! match read("particles.star") {
//!     Ok(data) => println!("Successfully read {} blocks", data.len()),
//!     Err(Error::FileNotFound(path)) => println!("File not found: {:?}", path),
//!     Err(Error::Parse { line, message }) => {
//!         println!("Parse error at line {}: {}", line, message);
//!     }
//!     Err(e) => println!("Error: {:?}", e),
//! }
//! ```
//!
//! ## Performance Considerations
//!
//! - **Parsing**: Uses the `lexical` crate for fast number parsing
//! - **Memory**: LoopBlocks use Polars DataFrames for efficient columnar storage
//! - **String Storage**: Uses `SmartString` for small string optimization
//! - **Statistics**: Can compute statistics without loading full file into memory
//!
//! ## STAR File Format
//!
//! STAR files have the following structure:
//!
//! ```text
//! data_block_name
//! _key1 value1
//! _key2 value2
//!
//! loop_
//! _column1 _column2 _column3
//! value1   value2   value3
//! value4   value5   value6
//! ```
//!
//! - Data blocks start with `data_` followed by a name
//! - Simple blocks contain key-value pairs starting with `_`
//! - Loop blocks start with `loop_` and contain tabular data
//! - Values can be unquoted, single-quoted, or double-quoted
//! - Empty values are represented as `""` or `''`
//!
//! ## See Also
//!
//! - [API Documentation](https://docs.rs/emstar) - Detailed API reference
//! - [Examples](https://github.com/yourusername/emstar/tree/main/examples) - Example code
//! - [RELION Documentation](https://relion.readthedocs.io/) - Information about STAR files in cryo-EM

mod error;
mod parser;
mod writer;
mod types;

pub use error::{Error, Result};
pub use types::{DataBlock, DataValue, LoopBlock, LoopBlockBuilder, SimpleBlock};
pub use types::{DataBlockStats, LoopBlockStats, SimpleBlockStats, StarStats};

use std::collections::HashMap;
use std::path::Path;

/// Read a STAR file from disk.
///
/// Returns a hashmap of data blocks, where the key is the block name.
/// Each block can be either a [`SimpleBlock`] (key-value pairs) or a [`LoopBlock`] (tabular data).
///
/// See also: [`stats()`]
///
/// # Arguments
///
/// * `path` - Path to the STAR file to read
///
/// # Returns
///
/// A `Result` containing a `HashMap<String, DataBlock>` or an [`Error`]
///
/// # Errors
///
/// Returns [`Error::FileNotFound`] if the file doesn't exist.
/// Returns [`Error::Parse`] if the file contains invalid STAR format.
/// Returns [`Error::Io`] for other I/O errors.
///
/// # Example
///
/// ```rust,no_run
/// use emstar::{read, DataBlock};
///
/// let data_blocks = read("particles.star")?;
/// println!("Read {} data blocks", data_blocks.len());
///
/// // Access a specific block
/// if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
///     println!("Found {} particles", particles.row_count());
/// }
/// # Ok::<(), emstar::Error>(())
/// ```
pub fn read<P: AsRef<Path>>(path: P) -> Result<HashMap<String, DataBlock>> {
    parser::parse_file(path.as_ref())
}

/// Write data blocks to a STAR file.
///
/// Creates or overwrites a STAR file with the provided data blocks.
/// The output format will be standard STAR format compatible with RELION and other cryo-EM software.
///
/// See also: [`to_string()`]
///
/// # Arguments
///
/// * `data_blocks` - HashMap of data blocks to write
/// * `path` - Path where the STAR file will be written
///
/// # Errors
///
/// Returns [`Error::Io`] if the file cannot be written.
///
/// # Example
///
/// ```rust
/// use emstar::{write, LoopBlock, DataBlock};
/// use std::collections::HashMap;
///
/// let mut data = HashMap::new();
/// let mut particles = LoopBlock::new();
/// particles.add_column("rlnCoordinateX");
/// particles.add_row(vec![emstar::DataValue::Float(100.0)]).unwrap();
/// data.insert("particles".to_string(), DataBlock::Loop(particles));
///
/// write(&data, "output.star")?;
/// # Ok::<(), emstar::Error>(())
/// ```
pub fn write<P: AsRef<Path>>(
    data_blocks: &HashMap<String, DataBlock>,
    path: P,
) -> Result<()> {
    writer::write_file(data_blocks, path.as_ref())
}

/// Convert data blocks to a STAR format string.
///
/// Returns the data blocks as a formatted STAR string without writing to disk.
/// Useful for debugging, logging, or string manipulation.
///
/// See also: [`write()`], [`read()`]
///
/// # Arguments
///
/// * `data_blocks` - HashMap of data blocks to convert
///
/// # Returns
///
/// A `Result` containing the STAR format string or an [`Error`]
///
/// # Example
///
/// ```rust
/// use emstar::{to_string, SimpleBlock, DataBlock, DataValue};
/// use std::collections::HashMap;
///
/// let mut data = HashMap::new();
/// let mut simple = SimpleBlock::new();
/// simple.set("key", DataValue::String("value".into()));
/// data.insert("general".to_string(), DataBlock::Simple(simple));
///
/// let star_string = to_string(&data)?;
/// println!("{}", star_string);
/// # Ok::<(), emstar::Error>(())
/// ```
pub fn to_string(data_blocks: &HashMap<String, DataBlock>) -> Result<String> {
    writer::data_blocks_to_string(data_blocks)
}



// ============================================================================
// Statistics Functions
// ============================================================================

/// Calculate statistics for a STAR file.
///
/// This function reads the file and computes comprehensive statistics about
/// all data blocks, including counts of simple blocks, loop blocks, rows, and columns.
///
/// **Note:** This function loads the entire file into memory. For very large files,
/// consider using chunked reading or memory-mapped files (future feature).
///
/// See also: [`block_stats()`], [`read()`]
///
/// # Arguments
///
/// * `path` - Path to the STAR file
///
/// # Returns
///
/// A `Result` containing `StarStats` or an `Error`
///
/// # Example
///
/// ```rust,no_run
/// use emstar::{stats, StarStats};
///
/// let stats = stats("particles.star")?;
/// println!("Total blocks: {}", stats.n_blocks);
/// println!("Loop blocks: {}", stats.n_loop_blocks);
/// println!("Total particles (rows): {}", stats.total_loop_rows);
/// # Ok::<(), emstar::Error>(())
/// ```
pub fn stats<P: AsRef<Path>>(path: P) -> Result<StarStats> {
    let blocks = read(path)?;
    Ok(StarStats::from_blocks(&blocks))
}

/// Get statistics for data blocks in memory.
///
/// This function computes statistics from already-loaded data blocks.
///
/// # Arguments
///
/// * `blocks` - HashMap of data blocks
///
/// # Example
///
/// ```rust
/// use emstar::{block_stats, LoopBlock, SimpleBlock, DataBlock, DataValue};
/// use std::collections::HashMap;
///
/// let mut blocks = HashMap::new();
/// blocks.insert("general".to_string(), DataBlock::Simple(SimpleBlock::new()));
/// blocks.insert("particles".to_string(), DataBlock::Loop(LoopBlock::new()));
///
/// let stats = block_stats(&blocks);
/// assert_eq!(stats.n_blocks, 2);
/// # ```
pub fn block_stats(blocks: &HashMap<String, DataBlock>) -> StarStats {
    StarStats::from_blocks(blocks)
}

/// List all data blocks with their names and types.
///
/// Returns a vector of tuples containing (block_name, block_type) where
/// block_type is either "SimpleBlock" or "LoopBlock".
///
/// # Arguments
///
/// * `blocks` - HashMap of data blocks
///
/// # Returns
///
/// A `Vec<(String, &str)>` containing block names and their types
///
/// # Example
///
/// ```rust
/// use emstar::{list_blocks, SimpleBlock, LoopBlock, DataBlock};
/// use std::collections::HashMap;
///
/// let mut blocks = HashMap::new();
/// blocks.insert("general".to_string(), DataBlock::Simple(SimpleBlock::new()));
/// blocks.insert("particles".to_string(), DataBlock::Loop(LoopBlock::new()));
///
/// let names = list_blocks(&blocks);
/// assert_eq!(names.len(), 2);
/// // names will contain: [("general", "SimpleBlock"), ("particles", "LoopBlock")]
/// // (order may vary since HashMap is unordered)
/// ```
pub fn list_blocks(blocks: &HashMap<String, DataBlock>) -> Vec<(String, &'static str)> {
    blocks
        .iter()
        .map(|(name, block)| (name.clone(), block.block_type()))
        .collect()
}