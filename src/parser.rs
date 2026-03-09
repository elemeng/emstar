//! STAR file parser
//!
//! This module provides functionality for parsing STAR (Self-defining Text Archival and Retrieval) files.
//!
//! # Parsing Strategy
//!
//! The parser uses a streaming approach to handle large files efficiently:
//! - Reads files line-by-line using buffered I/O
//! - Identifies data blocks by `data_` prefix
//! - Distinguishes between SimpleBlock (key-value) and LoopBlock (tabular) patterns
//! - Uses the `lexical` crate for fast numeric parsing
//!
//! # Functions
//!
//! - [`parse_file()`] - Parse a STAR file from disk
//! - [`parse_reader()`] - Parse STAR content from any reader
//!
//! # Example
//!
//! ```ignore
//! use emstar::parser::parse_file;
//! use std::path::Path;
//!
//! let blocks = parse_file(Path::new("particles.star"))?;
//! ```

use crate::error::{Error, Result};
use crate::types::{DataBlock, DataValue, LoopBlock, SimpleBlock};
use polars::frame::DataFrame;
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Null value representations in STAR files (case-insensitive)
const NULL_REPRESENTATIONS: &[&str] = &["<NA>", "<na>", "nan", "NaN", "NAN"];

/// Maximum number of columns allowed in a loop block (prevents memory exhaustion)
const MAX_COLUMNS: usize = 10_000;
/// Maximum line length allowed (prevents memory exhaustion from malformed input)
const MAX_LINE_LENGTH: usize = 10_000_000; // 10MB

/// Check if a string represents a null value (case-insensitive)
fn is_null_value(s: &str) -> bool {
    let trimmed = s.trim();
    NULL_REPRESENTATIONS.iter().any(|&rep| trimmed.eq_ignore_ascii_case(rep))
}

/// Parse a STAR file from disk
pub fn parse_file(path: &Path) -> Result<HashMap<String, DataBlock>> {
    if !path.exists() {
        return Err(Error::FileNotFound(path.to_path_buf()));
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    parse_reader(reader)
}

/// Parse STAR file content from a reader
pub(crate) fn parse_reader<R: BufRead>(reader: R) -> Result<HashMap<String, DataBlock>> {
    let mut data_blocks = HashMap::new();
    let mut lines = reader.lines().enumerate();

    while let Some((line_num, line_result)) = lines.next() {
        let line = line_result.map_err(|e| Error::Parse {
            line: line_num + 1,
            message: format!("Failed to read line: {}", e),
        })?;

        // Validate line length to prevent memory exhaustion
        if line.len() > MAX_LINE_LENGTH {
            return Err(Error::Parse {
                line: line_num + 1,
                message: format!(
                    "Line exceeds maximum length of {} bytes (got {} bytes). \
                     This may indicate a malformed file.",
                    MAX_LINE_LENGTH,
                    line.len()
                ),
            });
        }

        let trimmed = line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // Check for data block
        if let Some(name) = trimmed.strip_prefix("data_") {
            let block_name = name.trim().to_string();
            let block = parse_data_block(&mut lines, line_num + 1)?;
            data_blocks.insert(block_name, block);
        }
    }

    Ok(data_blocks)
}

/// Parse a single data block
fn parse_data_block<R: BufRead>(
    lines: &mut std::iter::Enumerate<std::io::Lines<R>>,
    _start_line: usize,
) -> Result<DataBlock> {
    // Look for loop_ or simple block indicators
    for (line_num, line_result) in lines.by_ref() {
        let line = line_result.map_err(|e| Error::Parse {
            line: line_num + 1,
            message: format!("Failed to read line: {}", e),
        })?;

        let trimmed = line.trim();

        // Skip empty lines
        if trimmed.is_empty() {
            continue;
        }

        // Check if we hit another data block
        if trimmed.starts_with("data_") {
            // Return empty simple block if no content found
            return Ok(DataBlock::Simple(SimpleBlock::new()));
        }

        // Check for loop block
        if trimmed == "loop_" {
            return parse_loop_block(lines, line_num + 1);
        }

        // Check for simple block (starts with underscore)
        if trimmed.starts_with('_') {
            return parse_simple_block(lines, line_num + 1, trimmed.to_string());
        }
    }

    // Empty block
    Ok(DataBlock::Simple(SimpleBlock::new()))
}

/// Parse a simple block (key-value pairs)
fn parse_simple_block<R: BufRead>(
    lines: &mut std::iter::Enumerate<std::io::Lines<R>>,
    _start_line: usize,
    first_line: String,
) -> Result<DataBlock> {
    let mut block = SimpleBlock::new();

    // Process first line (line number passed as _start_line)
    parse_simple_line(&first_line, &mut block, _start_line)?;

    // Process remaining lines
    for (line_num, line_result) in lines.by_ref() {
        let line = line_result.map_err(|e| Error::Parse {
            line: line_num + 1,
            message: format!("Failed to read line: {}", e),
        })?;

        let trimmed = line.trim();

        // End of block
        if trimmed.is_empty() || trimmed.starts_with("data_") {
            break;
        }

        // Parse line
        if trimmed.starts_with('_') {
            parse_simple_line(trimmed, &mut block, line_num + 1)?;
        }
    }

    Ok(DataBlock::Simple(block))
}

/// Parse a single simple block line
fn parse_simple_line(line: &str, block: &mut SimpleBlock, line_num: usize) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(Error::Parse {
            line: line_num,
            message: format!("Invalid simple block line: '{}'", line),
        });
    }

    let key = parts[0][1..].trim(); // Remove leading underscore
    let value_str = parts[1..].join(" ");
    let value = parse_value(&value_str)?;

    block.set(key, value);
    Ok(())
}

/// Parse a loop block (table)
/// Collects all values first, then determines the best column type based on all values
fn parse_loop_block<R: BufRead>(
    lines: &mut std::iter::Enumerate<std::io::Lines<R>>,
    start_line: usize,
) -> Result<DataBlock> {
    let mut column_names: Vec<String> = Vec::new();
    let mut column_data: Vec<Vec<DataValue>> = Vec::new();
    let mut first_data_line: Option<(usize, String)> = None;

    // Parse column headers
    for (line_num, line_result) in lines.by_ref() {
        let line = line_result.map_err(|e| Error::Parse {
            line: line_num + 1,
            message: format!("Failed to read line: {}", e),
        })?;

        let trimmed = line.trim();

        // End of headers
        if trimmed.is_empty() || !trimmed.starts_with('_') {
            // Save the first data line for processing
            if !trimmed.is_empty() && !trimmed.starts_with("data_") {
                first_data_line = Some((line_num, line));
            }
            break;
        }

        // Parse column name
        let column_name = trimmed.split_whitespace().next().ok_or_else(|| Error::Parse {
            line: line_num + 1,
            message: format!("Invalid column definition: '{}'", trimmed),
        })?;

        // Remove leading underscore
        let name = column_name[1..].trim();
        column_names.push(name.to_string());
        column_data.push(Vec::new());

        // Validate column count to prevent memory exhaustion
        if column_names.len() > MAX_COLUMNS {
            return Err(Error::Parse {
                line: line_num + 1,
                message: format!(
                    "Loop block has too many columns ({}). Maximum allowed is {}.",
                    column_names.len(),
                    MAX_COLUMNS
                ),
            });
        }
    }

    if column_names.is_empty() {
        return Err(Error::Parse {
            line: start_line,
            message: "Loop block has no columns".into(),
        });
    }

    let ncols = column_names.len();

    // Process the first data line if we saved it
    if let Some((line_num, line)) = first_data_line {
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("data_") {
            let row = parse_row(trimmed, ncols, line_num + 1)?;
            for (col_idx, value) in row.into_iter().enumerate() {
                column_data[col_idx].push(value);
            }
        }
    }

    // Parse remaining data rows
    for (line_num, line_result) in lines.by_ref() {
        let line = line_result.map_err(|e| Error::Parse {
            line: line_num + 1,
            message: format!("Failed to read line: {}", e),
        })?;

        let trimmed = line.trim();

        // End of block
        if trimmed.is_empty() || trimmed.starts_with("data_") {
            break;
        }

        // Parse row
        let row = parse_row(trimmed, ncols, line_num + 1)?;
        for (col_idx, value) in row.into_iter().enumerate() {
            column_data[col_idx].push(value);
        }
    }

    // Build Polars DataFrame from column data
    let mut columns: Vec<Column> = Vec::with_capacity(ncols);
    for (col_name, col_values) in column_names.iter().zip(column_data.iter()) {
        let series = data_values_to_series(col_name, col_values)?;
        columns.push(series.into());
    }

    let df = DataFrame::new(columns)
        .map_err(|e| Error::Parse {
            line: start_line,
            message: format!("Failed to create DataFrame: {}", e),
        })?;

    Ok(DataBlock::Loop(LoopBlock::from_dataframe(df)))
}

/// Convert a vector of DataValue to a Polars Series
fn data_values_to_series(name: &str, values: &[DataValue]) -> Result<Series> {
    if values.is_empty() {
        // Create empty series with string dtype as default
        return Ok(Series::new(name.into(), Vec::<String>::new()));
    }

    // Determine the best data type for this column
    let has_int = values.iter().any(|v| matches!(v, DataValue::Integer(_)));
    let has_float = values.iter().any(|v| matches!(v, DataValue::Float(_)));
    let has_string = values.iter().any(|v| matches!(v, DataValue::String(_)));
    let has_bool = values.iter().any(|v| matches!(v, DataValue::Bool(_)));

    // If we have any strings, use string type
    if has_string {
        let string_values: Vec<Option<String>> = values
            .iter()
            .map(|v| match v {
                DataValue::String(s) => Some(s.as_str().to_string()),
                DataValue::Integer(i) => Some(i.to_string()),
                DataValue::Float(f) => Some(f.to_string()),
                DataValue::Bool(b) => Some(b.to_string()),
                DataValue::Null => None,
            })
            .collect();
        Ok(Series::new(name.into(), string_values))
    } else if has_float {
        // If we have any floats, use float type (this will convert integers and bools to floats)
        let float_values: Vec<Option<f64>> = values
            .iter()
            .map(|v| match v {
                DataValue::Float(f) => Some(*f),
                DataValue::Integer(i) => Some(*i as f64),
                DataValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
                DataValue::Null => None,
                DataValue::String(_) => None,
            })
            .collect();
        Ok(Series::new(name.into(), float_values))
    } else if has_int {
        // If we only have integers (and possibly nulls/bools), use integer type
        let int_values: Vec<Option<i64>> = values
            .iter()
            .map(|v| match v {
                DataValue::Integer(i) => Some(*i),
                DataValue::Bool(b) => Some(if *b { 1 } else { 0 }),
                DataValue::Null => None,
                _ => None,
            })
            .collect();
        Ok(Series::new(name.into(), int_values))
    } else if has_bool {
        // If we only have bools (and possibly nulls), use bool type as integers (0/1)
        let int_values: Vec<Option<i64>> = values
            .iter()
            .map(|v| match v {
                DataValue::Bool(b) => Some(if *b { 1 } else { 0 }),
                DataValue::Null => None,
                _ => None,
            })
            .collect();
        Ok(Series::new(name.into(), int_values))
    } else {
        // All nulls or unknown - use string type
        let string_values: Vec<Option<String>> = values
            .iter()
            .map(|v| match v {
                DataValue::Null => None,
                _ => Some(v.as_string().unwrap_or("").to_string()),
            })
            .collect();
        Ok(Series::new(name.into(), string_values))
    }
}

/// Parse a single data row with proper handling of quoted strings
fn parse_row(line: &str, expected_cols: usize, line_num: usize) -> Result<Vec<DataValue>> {
    let values = parse_star_values(line, line_num)?;

    if values.len() != expected_cols {
        return Err(Error::Parse {
            line: line_num,
            message: format!(
                "Expected {} columns, found {} in row: '{}'",
                expected_cols,
                values.len(),
                line
            ),
        });
    }

    let mut row = Vec::with_capacity(expected_cols);
    for value_str in values {
        row.push(parse_value(&value_str)?);
    }

    Ok(row)
}

/// Parse STAR file values respecting quoted strings
/// STAR files can have values enclosed in single or double quotes
/// Values are separated by whitespace, but whitespace inside quotes is preserved
fn parse_star_values(line: &str, line_num: usize) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quote = None::<char>; // None, Some('"'), or Some('\'')
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match in_quote {
            None => {
                // Not in quote - check for quote start or whitespace
                if c == '"' || c == '\'' {
                    in_quote = Some(c);
                } else if c.is_whitespace() {
                    // Whitespace ends the current value (only if we have accumulated content)
                    if !current.is_empty() {
                        values.push(current.clone());
                        current.clear();
                    }
                } else {
                    current.push(c);
                }
            }
            Some(quote_char) => {
                // Inside a quoted string
                if c == quote_char {
                    // Check if this is an escaped quote (double quote)
                    if let Some(&next_c) = chars.peek() {
                        if next_c == quote_char {
                            // Escaped quote - consume both and add one
                            current.push(c);
                            chars.next();
                        } else {
                            // End of quote - save the quoted value
                            values.push(current.clone());
                            current.clear();
                            in_quote = None;
                        }
                    } else {
                        // End of quote at end of line
                        values.push(current.clone());
                        current.clear();
                        in_quote = None;
                    }
                } else {
                    current.push(c);
                }
            }
        }
    }

    // Don't forget the last value
    if !current.is_empty() {
        values.push(current);
    }
    
    // Check for unclosed quote
    if let Some(quote_char) = in_quote {
        return Err(Error::Parse {
            line: line_num,
            message: format!("Unclosed quote (looking for '{}') in line: '{}'", quote_char, line),
        });
    }

    Ok(values)
}

/// Parse a single value
fn parse_value(s: &str) -> Result<DataValue> {
    let trimmed = s.trim();

    // Check for null/NA values
    if is_null_value(trimmed) {
        return Ok(DataValue::Null);
    }

    // Check for boolean values
    match trimmed.to_lowercase().as_str() {
        "true" | "yes" => return Ok(DataValue::Bool(true)),
        "false" | "no" => return Ok(DataValue::Bool(false)),
        _ => {}
    }

    // Try to parse as integer
    if let Ok(i) = lexical::parse::<i64, _>(trimmed) {
        return Ok(DataValue::Integer(i));
    }

    // Try to parse as float
    if let Ok(f) = lexical::parse::<f64, _>(trimmed) {
        // Reject infinity values
        if f.is_infinite() {
            return Err(Error::InvalidDataValue(
                format!("Infinity values are not supported: '{}'", trimmed)
            ));
        }
        return Ok(DataValue::Float(f));
    }

    // String value - only remove surrounding quotes if they match
    let unquoted = remove_matching_quotes(trimmed);
    Ok(DataValue::String(unquoted.into()))
}

/// Remove matching surrounding quotes from a string
/// Only removes quotes if they form a valid pair at the start and end
fn remove_matching_quotes(s: &str) -> &str {
    if s.len() < 2 {
        return s;
    }
    
    let first = s.chars().next().unwrap();
    let last = s.chars().last().unwrap();
    
    // Only remove if both ends have the same quote type
    if (first == '"' || first == '\'') && first == last {
        // Check if the quotes are actually surrounding (not escaped/internal)
        let inner = &s[1..s.len()-1];
        // Only remove if there are no unescaped quotes inside
        if !inner.chars().any(|c| c == first) {
            return inner;
        }
    }
    
    s
}

/// Validate a STAR file without loading all data into memory
pub fn validate_file(path: &Path) -> Result<crate::ValidationDetails> {
    if !path.exists() {
        return Err(Error::FileNotFound(path.to_path_buf()));
    }

    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let reader = BufReader::new(file);
    let lines = reader.lines().enumerate();

    let mut block_names = Vec::new();
    let mut n_blocks = 0;

    for (line_num, line_result) in lines {
        let line = line_result.map_err(|e| Error::Parse {
            line: line_num + 1,
            message: format!("Failed to read line: {}", e),
        })?;

        let trimmed = line.trim();

        // Check for data block
        if let Some(name) = trimmed.strip_prefix("data_") {
            let block_name = name.trim().to_string();
            block_names.push(block_name);
            n_blocks += 1;
        }
    }

    Ok(crate::ValidationDetails {
        n_blocks,
        estimated_size_bytes: file_size,
        block_names,
        is_empty: n_blocks == 0,
    })
}

/// Calculate streaming statistics for a STAR file
pub fn parse_stats_streaming(path: &Path) -> Result<crate::StarStats> {
    if !path.exists() {
        return Err(Error::FileNotFound(path.to_path_buf()));
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let lines = reader.lines().enumerate();

    let mut n_simple_blocks = 0;
    let mut n_loop_blocks = 0;
    let mut loop_row_count = 0;
    let mut total_loop_cols = 0;
    let mut total_simple_entries = 0;
    let mut block_names = Vec::new();

    let mut in_loop = false;

    for (line_num, line_result) in lines {
        let line = line_result.map_err(|e| Error::Parse {
            line: line_num + 1,
            message: format!("Failed to read line: {}", e),
        })?;

        let trimmed = line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // Check for data block
        if let Some(name) = trimmed.strip_prefix("data_") {
            block_names.push(name.trim().to_string());
            in_loop = false;
            continue;
        }

        // Check for loop block
        if trimmed == "loop_" {
            in_loop = true;
            n_loop_blocks += 1;
            continue;
        }

        // Count entries in simple blocks (starts with _)
        if !in_loop && trimmed.starts_with('_') {
            total_simple_entries += 1;
            // Mark as simple block if we haven't already
            if !block_names.is_empty() && n_simple_blocks + n_loop_blocks < block_names.len() {
                n_simple_blocks += 1;
            }
        }

        // Count rows in loop blocks
        if in_loop && !trimmed.starts_with('_') {
            loop_row_count += 1;
        }

        // Count columns in loop blocks
        if in_loop && trimmed.starts_with('_') {
            total_loop_cols += 1;
        }
    }

    Ok(crate::StarStats {
        n_blocks: block_names.len(),
        n_simple_blocks,
        n_loop_blocks,
        total_loop_rows: loop_row_count,
        total_loop_cols,
        total_simple_entries,
        block_stats: Vec::new(), // Streaming mode doesn't collect per-block stats
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_infinity_rejected() {
        // Infinity should be rejected
        assert!(parse_value("inf").is_err());
        assert!(parse_value("-inf").is_err());
        assert!(parse_value("infinity").is_err());
        assert!(parse_value("Infinity").is_err());
        assert!(parse_value("-Infinity").is_err());
    }

    #[test]
    fn test_unclosed_quote_error() {
        let input = r#"data_test
loop_
_col1
"unclosed value
"#;
        let reader = Cursor::new(input);
        let result = parse_reader(reader);
        assert!(result.is_err());
        // Check that error mentions unclosed quote
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("Unclosed quote"), "Error should mention unclosed quote: {}", err_msg);
    }

    #[test]
    fn test_quote_parsing() {
        // Test proper quote removal
        assert_eq!(remove_matching_quotes("\"hello\""), "hello");
        assert_eq!(remove_matching_quotes("'hello'"), "hello");
        // Test quotes not removed when mismatched
        assert_eq!(remove_matching_quotes("\"hello'"), "\"hello'");
        assert_eq!(remove_matching_quotes("'hello\""), "'hello\"");
        // Test quotes not removed when empty
        assert_eq!(remove_matching_quotes("\"\""), "");
        // Test quotes not removed when internal quotes exist
        assert_eq!(remove_matching_quotes("\"he\"llo\""), "\"he\"llo\"");
    }

    #[test]
    fn test_parse_simple_block() {
        let input = r#"data_test
_key1 value1
_key2 42
_key3 3.14
"#;
        let reader = Cursor::new(input);
        let result = parse_reader(reader).unwrap();
        assert_eq!(result.len(), 1);

        let block = result.get("test").unwrap();
        assert!(block.is_simple());

        if let DataBlock::Simple(simple) = block {
            assert_eq!(simple.get("key1"), Some(&DataValue::String("value1".into())));
            assert_eq!(simple.get("key2"), Some(&DataValue::Integer(42)));
            assert_eq!(simple.get("key3"), Some(&DataValue::Float(3.14)));
        }
    }

    #[test]
    fn test_parse_loop_block() {
        let input = "data_particles\n\nloop_\n_rlnCoordinateX #1\n_rlnCoordinateY #2\n_rlnAngleRot #3\n91.7987\t83.6226\t-51.74\n97.6358\t80.4370\t141.5\n";
        let reader = Cursor::new(input);
        let result = parse_reader(reader).unwrap();
        assert_eq!(result.len(), 1);

        let block = result.get("particles").unwrap();
        assert!(block.is_loop());

        if let DataBlock::Loop(loop_block) = block {
            assert_eq!(loop_block.column_count(), 3);
            assert_eq!(loop_block.row_count(), 2);
            assert_eq!(
                loop_block.get_by_name(0, "rlnCoordinateX"),
                Some(DataValue::Float(91.7987))
            );
        }
    }

    #[test]
    fn test_value_parsing() {
        assert_eq!(parse_value("42").unwrap(), DataValue::Integer(42));
        assert_eq!(parse_value("3.14").unwrap(), DataValue::Float(3.14));
        assert_eq!(
            parse_value("hello").unwrap(),
            DataValue::String("hello".into())
        );
        assert_eq!(parse_value("<NA>").unwrap(), DataValue::Null);
        assert_eq!(parse_value("\"quoted\"").unwrap(), DataValue::String("quoted".into()));
    }
}
