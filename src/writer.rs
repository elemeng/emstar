//! STAR file writer

use crate::error::Result;
use crate::types::{DataBlock, DataValue, LoopBlock, SimpleBlock};
use std::collections::HashMap;
use std::fmt::Write;
use std::fs::File;
use std::io::Write as IoWrite;
use std::path::Path;

/// Initial buffer capacity for formatting a simple block (tuned for typical metadata blocks)
const SIMPLE_BLOCK_BUF_CAPACITY: usize = 1024;
/// Initial buffer capacity for formatting a loop block (tuned for small tables)
const LOOP_BLOCK_BUF_CAPACITY: usize = 4096;
/// Output representation for null values
const NULL_OUTPUT: &str = "<NA>";

/// Write data blocks to a STAR file
pub fn write_file(data_blocks: &HashMap<String, DataBlock>, path: &Path) -> Result<()> {
    let content = data_blocks_to_string(data_blocks)?;
    let mut file = File::create(path)?;
    file.write_all(content.as_bytes())?;
    Ok(())
}

/// Convert data blocks to STAR format string
pub fn data_blocks_to_string(data_blocks: &HashMap<String, DataBlock>) -> Result<String> {
    let mut output = String::new();

    // Add package info comment
    output.push_str("# Created by emstar\n");
    output.push('\n');
    output.push('\n');

    for (block_name, block) in data_blocks {
        match block {
            DataBlock::Simple(simple) => {
                output.push_str(&format_simple_block(block_name, simple));
            }
            DataBlock::Loop(loop_block) => {
                output.push_str(&format_loop_block(block_name, loop_block));
            }
        }
    }

    Ok(output)
}

/// Format a simple block with optimized single-allocation strategy
fn format_simple_block(name: &str, block: &SimpleBlock) -> String {
    // Pre-allocate with estimated capacity to minimize reallocations
    let mut output = String::with_capacity(SIMPLE_BLOCK_BUF_CAPACITY);
    
    writeln!(output, "data_{}", name).unwrap();
    output.push('\n');

    for (key, value) in block.iter() {
        // Use single write! call to minimize formatting overhead
        writeln!(output, "_{}\t\t\t{}", key, format_value(value)).unwrap();
    }

    output.push_str("\n\n");
    output
}

/// Format a loop block with optimized allocation strategy
fn format_loop_block(name: &str, block: &LoopBlock) -> String {
    let df = block.as_dataframe();
    let col_names = df.get_column_names();
    let nrows = df.height();
    let ncols = col_names.len();
    
    // Estimate capacity: headers + rows (avg 50 bytes per cell for numeric data)
    let estimated_capacity = LOOP_BLOCK_BUF_CAPACITY + nrows * ncols * 50;
    let mut output = String::with_capacity(estimated_capacity);
    
    writeln!(output, "data_{}", name).unwrap();
    output.push('\n');
    output.push_str("loop_\n");

    // Write column headers
    for (idx, column) in col_names.iter().enumerate() {
        writeln!(output, "_{} #{}", column, idx + 1).unwrap();
    }

    // Write data rows from DataFrame
    if nrows > 0 {
        for row_idx in 0..nrows {
            for (i, col_name) in col_names.iter().enumerate() {
                if i > 0 {
                    output.push('\t');
                }
                match block.get_by_name(row_idx, col_name) {
                    Some(value) => output.push_str(&format_value(&value)),
                    None => output.push_str("<NA>"),
                }
            }
            output.push('\n');
        }
    }

    output.push_str("\n\n");
    output
}

/// Format a single value
fn format_value(value: &DataValue) -> String {
    match value {
        DataValue::String(s) => {
            // Quote strings with spaces or empty strings
            if s.contains(' ') || s.is_empty() {
                format!("\"{}\"", s)
            } else {
                s.to_string()
            }
        }
        DataValue::Integer(i) => itoa::Buffer::new().format(*i).to_string(),
        DataValue::Float(f) => {
            // Format floats using ryu for consistent output
            ryu::Buffer::new().format(*f).to_string()
        }
        DataValue::Null => NULL_OUTPUT.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataValue;

    #[test]
    fn test_format_simple_block() {
        let mut block = SimpleBlock::new();
        block.set("key1", DataValue::String("value1".into()));
        block.set("key2", DataValue::Integer(42));
        block.set("key3", DataValue::Float(3.14));

        let output = format_simple_block("test", &block);

        assert!(output.contains("data_test"));
        assert!(output.contains("_key1"));
        assert!(output.contains("value1"));
        assert!(output.contains("42"));
        assert!(output.contains("3.14"));
    }

    #[test]
    fn test_format_loop_block() {
        use polars::prelude::*;

        // Create DataFrame with test data
        let s1 = Series::new("col1".into(), &[1i64, 1]);
        let s2 = Series::new("col2".into(), &[2i64, 2]);
        let s3 = Series::new("col3".into(), &[3i64, 3]);
        let df = DataFrame::new(vec![s1.into(), s2.into(), s3.into()]).unwrap();
        let block = LoopBlock::from_dataframe(df);

        let output = format_loop_block("particles", &block);

        assert!(output.contains("data_particles"));
        assert!(output.contains("loop_"));
        assert!(output.contains("_col1 #1"));
        assert!(output.contains("_col2 #2"));
        assert!(output.contains("_col3 #3"));
        assert!(output.contains("1\t2\t3"));
    }

    #[test]
    fn test_format_value() {
        assert_eq!(format_value(&DataValue::Integer(42)), "42");
        assert_eq!(format_value(&DataValue::Float(3.14)), "3.14");
        assert_eq!(format_value(&DataValue::String("hello".into())), "hello");
        assert_eq!(
            format_value(&DataValue::String("hello world".into())),
            "\"hello world\""
        );
        assert_eq!(format_value(&DataValue::Null), "<NA>");
    }

    #[test]
    fn test_round_trip_simple() {
        let mut block = SimpleBlock::new();
        block.set("key1", DataValue::String("value1".into()));
        block.set("key2", DataValue::Integer(42));

        let mut data_blocks = HashMap::new();
        data_blocks.insert("test".to_string(), DataBlock::Simple(block));

        let output = data_blocks_to_string(&data_blocks).unwrap();
        let parsed = crate::parser::parse_reader(output.as_bytes()).unwrap();

        assert_eq!(parsed.len(), 1);
        let parsed_block = parsed.get("test").unwrap();
        assert!(parsed_block.is_simple());
    }

    #[test]
    fn test_round_trip_loop() {
        use polars::prelude::*;

        // Create DataFrame with test data
        let s1 = Series::new("col1".into(), &[1i64]);
        let s2 = Series::new("col2".into(), &[2i64]);
        let df = DataFrame::new(vec![s1.into(), s2.into()]).unwrap();
        let block = LoopBlock::from_dataframe(df);

        let mut data_blocks = HashMap::new();
        data_blocks.insert("test".to_string(), DataBlock::Loop(block));

        let output = data_blocks_to_string(&data_blocks).unwrap();
        let parsed = crate::parser::parse_reader(output.as_bytes()).unwrap();

        assert_eq!(parsed.len(), 1);
        let parsed_block = parsed.get("test").unwrap();
        assert!(parsed_block.is_loop());

        if let DataBlock::Loop(parsed_loop) = parsed_block {
            assert_eq!(parsed_loop.row_count(), 1);
            assert_eq!(parsed_loop.column_count(), 2);
        }
    }
}