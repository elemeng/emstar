//! Error types for emstar

use std::io;
use std::path::PathBuf;
use thiserror::Error;

/// Error type for emstar operations
#[derive(Error, Debug)]
pub enum Error {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// File not found
    #[error("File not found: {0}")]
    FileNotFound(PathBuf),

    /// Parse error
    #[error("Parse error at line {line}: {message}")]
    Parse { line: usize, message: String },

    /// Invalid STAR file format
    #[error("Invalid STAR file format: {0}")]
    InvalidFormat(String),

    /// Invalid data value
    #[error("Invalid data value: {0}")]
    InvalidDataValue(String),

    /// Missing required field
    #[error("Missing required field: {0}")]
    MissingField(String),

    /// Duplicate data block
    #[error("Duplicate data block: {0}")]
    DuplicateBlock(String),

    /// Invalid column definition
    #[error("Invalid column definition: {0}")]
    InvalidColumn(String),

    /// Type conversion error
    #[error("Type conversion error: {0}")]
    TypeConversion(String),
}

/// Result type alias for emstar operations
pub type Result<T> = std::result::Result<T, Error>;