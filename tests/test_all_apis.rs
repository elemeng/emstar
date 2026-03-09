//! Comprehensive API tests on real STAR files from tests/data

use emstar::{
    block_stats, list_blocks, read, write, stats,
    DataBlock, DataBlockStats, DataValue, LoopBlock, SimpleBlock, StarStats,
};
use std::path::Path;

const TEST_DATA_DIR: &str = "tests/data";

/// Test reading all STAR files in tests/data
#[test]
fn test_read_all_data_files() {
    let files = vec![
        "basic_double_quote.star",
        "basic_single_quote.star",
        "default_pipeline.star",
        "empty_loop.star",
        "loop_double_quote.star",
        "loop_single_quote.star",
        "one_loop.star",
        "postprocess.star",
        "rln3.1_data_style.star",
        "single_line_end_of_multiblock.star",
        "single_line_middle_of_multiblock.star",
        "two_basic_blocks.star",
        "two_single_line_loop_blocks.star",
        "relion_tutorial/run_it025_optimiser_2D.star",
        "relion_tutorial/run_it025_optimiser_3D.star",
        "relion_tutorial/run_it025_sampling_2D.star",
        "relion_tutorial/run_it025_sampling_3D.star",
    ];

    for file in &files {
        let path = format!("{}/{}", TEST_DATA_DIR, file);
        println!("Testing read: {}", path);
        
        let result = read(&path, None);
        assert!(result.is_ok(), "Failed to read {}: {:?}", file, result.err());
        
        let data = result.unwrap();
        println!("  ✓ Read {} data blocks", data.len());
        
        // Verify each block
        for (name, block) in &data {
            println!("    Block '{}': {}", name, block.block_type());
            
            // Test block stats
            let block_stat = block.stats();
            match &block_stat {
                DataBlockStats::Simple(s) => {
                    println!("      - {} entries", s.n_entries);
                }
                DataBlockStats::Loop(l) => {
                    println!("      - {} rows x {} cols", l.n_rows, l.n_cols);
                }
            }
            
            // Test count()
            let count = block.count();
            println!("      - count(): {}", count);
        }
    }
}

/// Test statistics API on all data files
#[test]
fn test_stats_api_on_all_files() {
    let files = vec![
        "basic_double_quote.star",
        "basic_single_quote.star",
        "default_pipeline.star",
        "empty_loop.star",
        "loop_double_quote.star",
        "loop_single_quote.star",
        "one_loop.star",
        "postprocess.star",
        "rln3.1_data_style.star",
        "single_line_end_of_multiblock.star",
        "single_line_middle_of_multiblock.star",
        "two_basic_blocks.star",
        "two_single_line_loop_blocks.star",
        "relion_tutorial/run_it025_optimiser_2D.star",
        "relion_tutorial/run_it025_optimiser_3D.star",
        "relion_tutorial/run_it025_sampling_2D.star",
        "relion_tutorial/run_it025_sampling_3D.star",
    ];

    for file in &files {
        let path = format!("{}/{}", TEST_DATA_DIR, file);
        println!("Testing stats API: {}", path);
        
        // Test stats() function
        let file_stats = stats(&path);
        assert!(file_stats.is_ok(), "stats() failed for {}: {:?}", file, file_stats.err());
        
        let file_stats = file_stats.unwrap();
        println!("  StarStats:");
        println!("    - Total blocks: {}", file_stats.n_blocks);
        println!("    - SimpleBlocks: {}", file_stats.n_simple_blocks);
        println!("    - LoopBlocks: {}", file_stats.n_loop_blocks);
        println!("    - Total rows: {}", file_stats.total_loop_rows);
        println!("    - Total cols: {}", file_stats.total_loop_cols);
        println!("    - Avg rows/loop: {:.1}", file_stats.avg_rows_per_loop());
        println!("    - Has loop blocks: {}", file_stats.has_loop_blocks());
        println!("    - Has simple blocks: {}", file_stats.has_simple_blocks());
        
        // Test get_block_stats for each block
        for (name, _) in &file_stats.block_stats {
            let block_stat = file_stats.get_block_stats(name);
            assert!(block_stat.is_some());
        }
        
        // Compare with block_stats() on loaded data
        let data = read(&path, None).unwrap();
        let mem_stats = block_stats(&data);
        assert_eq!(file_stats.n_blocks, mem_stats.n_blocks);
        assert_eq!(file_stats.total_loop_rows, mem_stats.total_loop_rows);
    }
}

/// Test SimpleBlock CRUD operations
#[test]
fn test_simple_block_crud() {
    let path = format!("{}/two_basic_blocks.star", TEST_DATA_DIR);
    println!("Testing SimpleBlock CRUD on: {}", path);
    
    let mut data = read(&path, None).unwrap();
    
    // Find a SimpleBlock
    let simple_block_name = data.iter()
        .find(|(_, b)| b.is_simple())
        .map(|(n, _)| n.clone())
        .expect("No SimpleBlock found");
    
    println!("  Testing on block: {}", simple_block_name);
    
    if let Some(DataBlock::Simple(block)) = data.get_mut(&simple_block_name) {
        // Test get()
        let keys: Vec<String> = block.keys().map(|k| k.to_string()).collect();
        println!("    Keys: {:?}", keys);
        
        if let Some(first_key) = keys.first() {
            let value = block.get(first_key);
            assert!(value.is_some());
            println!("    get('{}'): {:?}", first_key, value);
        }
        
        // Test set() - Create/Update
        block.set("test_new_key".into(), DataValue::Integer(42));
        println!("    set('test_new_key', 42)");
        
        block.set("test_float".into(), DataValue::Float(3.14));
        println!("    set('test_float', 3.14)");
        
        // Test contains_key()
        assert!(block.contains_key("test_new_key"));
        println!("    contains_key('test_new_key'): true");
        
        // Test len()
        let len_before = block.len();
        println!("    len(): {}", len_before);
        
        // Test remove()
        let removed = block.remove("test_new_key");
        assert!(removed.is_some());
        println!("    remove('test_new_key'): {:?}", removed);
        
        assert_eq!(block.len(), len_before - 1);
        
        // Test stats()
        let stats = block.stats();
        println!("    stats().n_entries: {}", stats.n_entries);
        
        // Test is_empty() and clear()
        assert!(!block.is_empty());
    }
}

/// Test LoopBlock CRUD operations
#[test]
fn test_loop_block_crud() {
    let path = format!("{}/one_loop.star", TEST_DATA_DIR);
    println!("Testing LoopBlock CRUD on: {}", path);
    
    let mut data = read(&path, None).unwrap();
    
    // Find a LoopBlock
    let loop_block_name = data.iter()
        .find(|(_, b)| b.is_loop())
        .map(|(n, _)| n.clone())
        .expect("No LoopBlock found");
    
    println!("  Testing on block: {}", loop_block_name);
    
    if let Some(DataBlock::Loop(block)) = data.get_mut(&loop_block_name) {
        let original_rows = block.row_count();
        let original_cols = block.column_count();
        
        println!("    Original: {} rows x {} cols", original_rows, original_cols);
        
        // Test columns()
        let cols = block.columns();
        println!("    columns(): {:?}", cols);
        
        // Test has_column()
        if let Some(first_col) = cols.first() {
            assert!(block.has_column(first_col));
            println!("    has_column('{}'): true", first_col);
            
            // Test get_column()
            let col_data = block.get_column(first_col);
            assert!(col_data.is_some());
            let col_data = col_data.unwrap();
            println!("    get_column('{}'): {} values", first_col, col_data.len());
            
            // Test get_by_name()
            if original_rows > 0 {
                let value = block.get_by_name(0, first_col);
                assert!(value.is_some());
                println!("    get_by_name(0, '{}'): {:?}", first_col, value);
            }
        }
        
        // Test get()
        if original_rows > 0 && original_cols > 0 {
            let value = block.get(0, 0);
            assert!(value.is_some());
            println!("    get(0, 0): {:?}", value);
        }
        
        // Test stats()
        let stats = block.stats();
        println!("    stats(): {} rows, {} cols, {} cells", 
                 stats.n_rows, stats.n_cols, stats.n_cells);
        
        // Test is_empty()
        println!("    is_empty(): {}", block.is_empty());
        
        // Test iter_rows()
        let row_count = block.iter_rows().count();
        println!("    iter_rows().count(): {}", row_count);
        assert_eq!(row_count, original_rows);
        
        // Test add_column()
        let new_col_name = "test_new_column";
        block.add_column(new_col_name.into());
        assert!(block.has_column(new_col_name));
        assert_eq!(block.column_count(), original_cols + 1);
        println!("    add_column('{}'): now {} cols", new_col_name, block.column_count());
        
        // Test add_row()
        let mut new_row = vec![];
        for _ in 0..block.column_count() {
            new_row.push(DataValue::Float(999.9));
        }
        block.add_row(new_row).expect("Failed to add row");
        assert_eq!(block.row_count(), original_rows + 1);
        println!("    add_row(): now {} rows", block.row_count());
        
        // Test set_by_name() on the new column
        if original_rows > 0 {
            block.set_by_name(0, new_col_name, DataValue::Float(123.456))
                .expect("Failed to set value");
            println!("    set_by_name(0, '{}', 123.456)", new_col_name);
            
            let updated = block.get_by_name(0, new_col_name);
            println!("    get_by_name(0, '{}'): {:?}", new_col_name, updated);
        }
        
        // Test remove_column()
        block.remove_column(new_col_name).expect("Failed to remove column");
        assert!(!block.has_column(new_col_name));
        assert_eq!(block.column_count(), original_cols);
        println!("    remove_column('{}'): back to {} cols", new_col_name, block.column_count());
        
        // Test remove_row() - remove the last row we added
        block.remove_row(original_rows).expect("Failed to remove row");
        assert_eq!(block.row_count(), original_rows);
        println!("    remove_row({}): back to {} rows", original_rows, block.row_count());
    }
}

/// Test file-level CRUD operations
#[test]
fn test_file_level_crud() {
    let test_file = "/tmp/test_api_crud.star";
    let source = format!("{}/one_loop.star", TEST_DATA_DIR);
    
    // Test file existence using standard library
    assert!(Path::new(&source).exists());
    assert!(!Path::new(test_file).exists());
    
    // Copy source to test file using read/write
    let data = read(&source, None).unwrap();
    
    // Test write() creates file
    write(&data, test_file, None).expect("Failed to create file");
    assert!(Path::new(test_file).exists());
    
    // Test read()
    let loaded = read(test_file, None).expect("Failed to read file");
    assert_eq!(loaded.len(), data.len());
    
    // Test write() can update file
    let mut updated_data = data.clone();
    let mut new_block = SimpleBlock::new();
    new_block.set("test_key", DataValue::String("test_value".into()));
    updated_data.insert("test_block".to_string(), DataBlock::Simple(new_block));
    
    write(&updated_data, test_file, None).expect("Failed to update file");
    
    let reloaded = read(test_file, None).unwrap();
    assert!(reloaded.contains_key("test_block"));
    
    // Test stats() on the file
    let file_stats = stats(test_file).unwrap();
    assert_eq!(file_stats.n_blocks, updated_data.len());
    
    // Test file deletion using standard library
    std::fs::remove_file(test_file).expect("Failed to delete file");
    assert!(!Path::new(test_file).exists());
}

/// Test write/read round-trip on all files
#[test]
fn test_write_read_roundtrip() {
    let files = vec![
        "basic_double_quote.star",
        "basic_single_quote.star",
        "default_pipeline.star",
        "empty_loop.star",
        "loop_double_quote.star",
        "loop_single_quote.star",
        "one_loop.star",
        "postprocess.star",
        "rln3.1_data_style.star",
        "single_line_end_of_multiblock.star",
        "single_line_middle_of_multiblock.star",
        "two_basic_blocks.star",
        "two_single_line_loop_blocks.star",
        "relion_tutorial/run_it025_optimiser_2D.star",
        "relion_tutorial/run_it025_optimiser_3D.star",
        "relion_tutorial/run_it025_sampling_2D.star",
        "relion_tutorial/run_it025_sampling_3D.star",
    ];

    for file in &files {
        let source = format!("{}/{}", TEST_DATA_DIR, file);
        // Replace slashes with underscores for temp filename to avoid directory creation issues
        let temp_filename = file.replace('/', "_");
        let temp = format!("/tmp/roundtrip_{}", temp_filename);
        
        println!("Testing roundtrip: {} -> {}", source, temp);
        
        // Read original
        let original = read(&source, None).unwrap();
        let original_stats = block_stats(&original);
        
        // Write to temp
        write(&original, &temp, None).expect("Failed to write");
        
        // Read back
        let reloaded = read(&temp, None).unwrap();
        let reloaded_stats = block_stats(&reloaded);
        
        // Compare stats
        assert_eq!(original_stats.n_blocks, reloaded_stats.n_blocks, 
                   "Block count mismatch for {}", file);
        assert_eq!(original_stats.total_loop_rows, reloaded_stats.total_loop_rows,
                   "Row count mismatch for {}", file);
        assert_eq!(original_stats.total_simple_entries, reloaded_stats.total_simple_entries,
                   "Entry count mismatch for {}", file);
        
        // Cleanup
        std::fs::remove_file(&temp).ok();
        
        println!("  ✓ Roundtrip successful for {}", file);
    }
}

/// Test DataValue conversions
#[test]
fn test_data_value_conversions() {
    // Integer
    let int_val = DataValue::Integer(42);
    assert_eq!(int_val.as_integer(), Some(42));
    assert_eq!(int_val.as_float(), Some(42.0));
    assert_eq!(int_val.as_string(), None);
    assert!(!int_val.is_null());
    
    // Float
    let float_val = DataValue::Float(3.14);
    assert_eq!(float_val.as_float(), Some(3.14));
    assert_eq!(float_val.as_integer(), None); // 3.14 is not a whole number
    assert!(!float_val.is_null());
    
    // String
    let str_val = DataValue::String("hello".into());
    assert_eq!(str_val.as_string(), Some("hello"));
    assert_eq!(str_val.as_integer(), None);
    assert!(!str_val.is_null());
    
    // Null
    let null_val = DataValue::Null;
    assert!(null_val.is_null());
    assert_eq!(null_val.as_integer(), None);
    assert_eq!(null_val.as_float(), None);
    assert_eq!(null_val.as_string(), None);
    
    // Integer from float
    let int_float = DataValue::Float(42.0);
    assert_eq!(int_float.as_integer(), Some(42));
}

/// Test error handling
#[test]
fn test_error_handling() {
    use emstar::Error;
    
    // Test FileNotFound
    let result = read("/nonexistent/path/file.star", None);
    assert!(result.is_err());
    match result {
        Err(Error::FileNotFound(_)) => println!("✓ FileNotFound error caught"),
        Err(other) => println!("Got different error: {:?}", other),
        Ok(_) => panic!("Should have failed"),
    }
}

/// Test complex file: default_pipeline.star
#[test]
fn test_complex_file() {
    let path = format!("{}/default_pipeline.star", TEST_DATA_DIR);
    println!("Testing complex file: {}", path);
    
    let data = read(&path, None).unwrap();
    println!("  Loaded {} data blocks", data.len());
    
    let stats = block_stats(&data);
    println!("  Statistics:");
    println!("    - Blocks: {} ({} simple, {} loop)", 
             stats.n_blocks, stats.n_simple_blocks, stats.n_loop_blocks);
    println!("    - Total loop rows: {}", stats.total_loop_rows);
    println!("    - Total simple entries: {}", stats.total_simple_entries);
    
    // Iterate and print details for each block
    for (name, block) in &data {
        match block {
            DataBlock::Simple(b) => {
                println!("  Block '{}': SimpleBlock ({} entries)", name, b.len());
            }
            DataBlock::Loop(b) => {
                println!("  Block '{}': LoopBlock ({} rows x {} cols)", 
                         name, b.row_count(), b.column_count());
                println!("    Columns: {:?}", b.columns());
            }
        }
    }
    
    assert!(!data.is_empty());
}

/// Test empty loop handling
#[test]
fn test_empty_loop() {
    let path = format!("{}/empty_loop.star", TEST_DATA_DIR);
    println!("Testing empty loop file: {}", path);
    
    let data = read(&path, None).unwrap();
    
    for (name, block) in &data {
        if let DataBlock::Loop(b) = block {
            println!("  Block '{}': {} rows x {} cols", name, b.row_count(), b.column_count());
            assert!(b.is_empty() || b.row_count() == 0);
        }
    }
}

/// Test DataBlock as_simple/as_loop/as_simple_mut/as_loop_mut
#[test]
fn test_datablock_accessors() {
    let files = vec![
        "basic_double_quote.star",
        "basic_single_quote.star",
        "default_pipeline.star",
        "empty_loop.star",
        "loop_double_quote.star",
        "loop_single_quote.star",
        "one_loop.star",
        "postprocess.star",
        "rln3.1_data_style.star",
        "single_line_end_of_multiblock.star",
        "single_line_middle_of_multiblock.star",
        "two_basic_blocks.star",
        "two_single_line_loop_blocks.star",
        "relion_tutorial/run_it025_optimiser_2D.star",
        "relion_tutorial/run_it025_optimiser_3D.star",
        "relion_tutorial/run_it025_sampling_2D.star",
        "relion_tutorial/run_it025_sampling_3D.star",
    ];

    for file in &files {
        let path = format!("{}/{}", TEST_DATA_DIR, file);
        let mut data = read(&path, None).unwrap();
    
    // Test immutable accessors
    for (_, block) in &data {
        if block.is_simple() {
            assert!(block.as_simple().is_some());
            assert!(block.as_loop().is_none());
        } else {
            assert!(block.as_loop().is_some());
            assert!(block.as_simple().is_none());
        }
    }
    
    // Test mutable accessors
    for (_, block) in &mut data {
        if block.is_simple() {
            if let Some(b) = block.as_simple_mut() {
                b.set("test", DataValue::Integer(1));
            }
        }
    }
    }
}

/// Test to_string conversion
#[test]
fn test_to_string_conversion() {
    let files = vec![
        "basic_double_quote.star",
        "basic_single_quote.star",
        "default_pipeline.star",
        "empty_loop.star",
        "loop_double_quote.star",
        "loop_single_quote.star",
        "one_loop.star",
        "postprocess.star",
        "rln3.1_data_style.star",
        "single_line_end_of_multiblock.star",
        "single_line_middle_of_multiblock.star",
        "two_basic_blocks.star",
        "two_single_line_loop_blocks.star",
        "relion_tutorial/run_it025_optimiser_2D.star",
        "relion_tutorial/run_it025_optimiser_3D.star",
        "relion_tutorial/run_it025_sampling_2D.star",
        "relion_tutorial/run_it025_sampling_3D.star",
    ];

    for file in &files {
        let path = format!("{}/{}", TEST_DATA_DIR, file);
        let data = read(&path, None).unwrap();
        
        let star_string = emstar::to_string(&data);
        assert!(star_string.is_ok());
        
        let s = star_string.unwrap();
        assert!(!s.is_empty());
        assert!(s.contains("data_"));
    }
}

/// Test list_blocks function
#[test]
fn test_list_blocks() {
    use emstar::list_blocks;
    
    let path = format!("{}/default_pipeline.star", TEST_DATA_DIR);
    let data = read(&path, None).unwrap();
    
    let blocks = list_blocks(&data);
    
    // Should return all block names with their types
    assert_eq!(blocks.len(), data.len());
    
    // Verify each block has correct type
    for (name, block_type) in &blocks {
        let actual_block = data.get(name).unwrap();
        assert_eq!(*block_type, actual_block.block_type());
        assert!(*block_type == "SimpleBlock" || *block_type == "LoopBlock");
    }
}

/// Test DataBlock expect methods
#[test]
fn test_datablock_expect_methods() {
    let mut simple_block = SimpleBlock::new();
    simple_block.set("key", DataValue::Integer(42));
    
    let mut loop_block = LoopBlock::new();
    loop_block.add_column("col1");
    loop_block.add_row(vec![DataValue::Integer(1)]).unwrap();
    
    let simple_db = DataBlock::Simple(simple_block);
    let loop_db = DataBlock::Loop(loop_block);
    
    // Test expect_simple - should work
    let _ = simple_db.expect_simple("Should be simple");
    
    // Test expect_loop - should work
    let _ = loop_db.expect_loop("Should be loop");
    
    // Test expect_simple on loop should panic
    let result = std::panic::catch_unwind(|| {
        let loop_db = DataBlock::Loop(LoopBlock::new());
        let _ = loop_db.expect_simple("This should panic");
    });
    assert!(result.is_err());
    
    // Test expect_loop on simple should panic
    let result = std::panic::catch_unwind(|| {
        let simple_db = DataBlock::Simple(SimpleBlock::new());
        let _ = simple_db.expect_loop("This should panic");
    });
    assert!(result.is_err());
}

/// Test DataBlock expect_mut methods
#[test]
fn test_datablock_expect_mut_methods() {
    let mut simple_block = SimpleBlock::new();
    simple_block.set("key", DataValue::Integer(42));
    
    let mut loop_block = LoopBlock::new();
    loop_block.add_column("col1");
    loop_block.add_row(vec![DataValue::Integer(1)]).unwrap();
    
    let mut simple_db = DataBlock::Simple(simple_block);
    let mut loop_db = DataBlock::Loop(loop_block);
    
    // Test expect_simple_mut - should work and allow modification
    {
        let simple = simple_db.expect_simple_mut("Should be simple");
        simple.set("new_key", DataValue::String("value".into()));
    }
    
    // Test expect_loop_mut - should work and allow modification
    {
        let loop_b = loop_db.expect_loop_mut("Should be loop");
        loop_b.add_row(vec![DataValue::Integer(2)]).unwrap();
    }
    
    // Test expect_simple_mut on loop should panic
    let result = std::panic::catch_unwind(|| {
        let mut loop_db = DataBlock::Loop(LoopBlock::new());
        let _ = loop_db.expect_simple_mut("This should panic");
    });
    assert!(result.is_err());
    
    // Test expect_loop_mut on simple should panic
    let result = std::panic::catch_unwind(|| {
        let mut simple_db = DataBlock::Simple(SimpleBlock::new());
        let _ = simple_db.expect_loop_mut("This should panic");
    });
    assert!(result.is_err());
}

/// Test LoopBlock update_row
#[test]
fn test_loopblock_update_row() {
    let mut block = LoopBlock::new();
    block.add_column("col1");
    block.add_column("col2");
    block.add_row(vec![DataValue::Integer(1), DataValue::Integer(2)]).unwrap();
    block.add_row(vec![DataValue::Integer(3), DataValue::Integer(4)]).unwrap();
    
    // Update row 0
    block.update_row(0, vec![DataValue::Integer(10), DataValue::Integer(20)]).unwrap();
    
    assert_eq!(block.get_by_name(0, "col1"), Some(DataValue::Integer(10)));
    assert_eq!(block.get_by_name(0, "col2"), Some(DataValue::Integer(20)));
    
    // Verify row 1 unchanged
    assert_eq!(block.get_by_name(1, "col1"), Some(DataValue::Integer(3)));
    assert_eq!(block.get_by_name(1, "col2"), Some(DataValue::Integer(4)));
    
    // Test out of bounds
    assert!(block.update_row(99, vec![DataValue::Integer(1)]).is_err());
    
    // Test wrong column count
    assert!(block.update_row(0, vec![DataValue::Integer(1)]).is_err());
}

/// Test LoopBlock clear_rows and clear
#[test]
fn test_loopblock_clear_methods() {
    let mut block = LoopBlock::new();
    block.add_column("col1");
    block.add_column("col2");
    block.add_row(vec![DataValue::Integer(1), DataValue::Integer(2)]).unwrap();
    block.add_row(vec![DataValue::Integer(3), DataValue::Integer(4)]).unwrap();
    
    assert_eq!(block.row_count(), 2);
    assert_eq!(block.column_count(), 2);
    
    // Test clear_rows - keeps columns
    block.clear_rows();
    assert_eq!(block.row_count(), 0);
    assert_eq!(block.column_count(), 2);
    assert!(block.is_empty());
    
    // Add row again
    block.add_row(vec![DataValue::Integer(5), DataValue::Integer(6)]).unwrap();
    assert_eq!(block.row_count(), 1);
    
    // Test clear - removes everything
    block.clear();
    assert_eq!(block.row_count(), 0);
    assert_eq!(block.column_count(), 0);
    assert!(block.is_empty());
}

/// Test SimpleBlock clear
#[test]
fn test_simpleblock_clear() {
    let mut block = SimpleBlock::new();
    block.set("key1", DataValue::Integer(1));
    block.set("key2", DataValue::Integer(2));
    block.set("key3", DataValue::Integer(3));
    
    assert_eq!(block.len(), 3);
    
    block.clear();
    
    assert_eq!(block.len(), 0);
    assert!(block.is_empty());
    assert!(block.get("key1").is_none());
}

/// Test LoopBlock enumerate_rows
#[test]
fn test_loopblock_enumerate_rows() {
    let mut block = LoopBlock::new();
    block.add_column("col1");
    block.add_column("col2");
    block.add_row(vec![DataValue::Integer(1), DataValue::Integer(2)]).unwrap();
    block.add_row(vec![DataValue::Integer(3), DataValue::Integer(4)]).unwrap();
    block.add_row(vec![DataValue::Integer(5), DataValue::Integer(6)]).unwrap();
    
    let enumerated: Vec<_> = block.enumerate_rows().collect();
    
    assert_eq!(enumerated.len(), 3);
    
    // Check indices and values
    assert_eq!(enumerated[0].0, 0);
    assert_eq!(enumerated[0].1[0], DataValue::Integer(1));
    assert_eq!(enumerated[0].1[1], DataValue::Integer(2));
    
    assert_eq!(enumerated[1].0, 1);
    assert_eq!(enumerated[1].1[0], DataValue::Integer(3));
    assert_eq!(enumerated[1].1[1], DataValue::Integer(4));
    
    assert_eq!(enumerated[2].0, 2);
    assert_eq!(enumerated[2].1[0], DataValue::Integer(5));
    assert_eq!(enumerated[2].1[1], DataValue::Integer(6));
}

/// Test LoopBlock column_iter methods
#[test]
fn test_loopblock_column_iter() {
    let mut block = LoopBlock::new();
    block.add_column("float_col");
    block.add_column("int_col");
    block.add_column("string_col");
    block.add_row(vec![
        DataValue::Float(1.5),
        DataValue::Integer(10),
        DataValue::String("hello".into()),
    ]).unwrap();
    block.add_row(vec![
        DataValue::Float(2.5),
        DataValue::Integer(20),
        DataValue::String("world".into()),
    ]).unwrap();
    block.add_row(vec![
        DataValue::Null,
        DataValue::Null,
        DataValue::Null,
    ]).unwrap();
    
    // Test column_iter_f64
    let float_values: Vec<_> = block.column_iter_f64("float_col").unwrap().collect();
    assert_eq!(float_values.len(), 3);
    assert_eq!(float_values[0], Some(1.5));
    assert_eq!(float_values[1], Some(2.5));
    assert_eq!(float_values[2], None); // Null value
    
    // Test column_iter_i64
    let int_values: Vec<_> = block.column_iter_i64("int_col").unwrap().collect();
    assert_eq!(int_values.len(), 3);
    assert_eq!(int_values[0], Some(10));
    assert_eq!(int_values[1], Some(20));
    assert_eq!(int_values[2], None); // Null value
    
    // Test column_iter_str
    let str_values: Vec<_> = block.column_iter_str("string_col").unwrap().collect();
    assert_eq!(str_values.len(), 3);
    assert_eq!(str_values[0], Some("hello"));
    assert_eq!(str_values[1], Some("world"));
    assert_eq!(str_values[2], None); // Null value
    
    // Test non-existent column returns None
    assert!(block.column_iter_f64("nonexistent").is_none());
}

/// Test LoopBlock get_f64/get_i64/get_string helpers
#[test]
fn test_loopblock_typed_getters() {
    let mut block = LoopBlock::new();
    block.add_column("float_col");
    block.add_column("int_col");
    block.add_column("string_col");
    block.add_row(vec![
        DataValue::Float(3.14),
        DataValue::Integer(42),
        DataValue::String("test".into()),
    ]).unwrap();
    
    // Test get_f64
    assert_eq!(block.get_f64(0, "float_col"), Some(3.14));
    assert_eq!(block.get_f64_or(0, "float_col", 0.0), 3.14);
    
    // Test get_i64
    assert_eq!(block.get_i64(0, "int_col"), Some(42));
    assert_eq!(block.get_i64_or(0, "int_col", 0), 42);
    
    // Test get_string
    assert_eq!(block.get_string(0, "string_col").map(|s| s.to_string()), Some("test".to_string()));
    assert_eq!(block.get_string_or(0, "string_col", "default").to_string(), "test".to_string());
    
    // Test default values for non-existent columns
    assert_eq!(block.get_f64_or(0, "nonexistent", 99.9), 99.9);
    assert_eq!(block.get_i64_or(0, "nonexistent", 99), 99);
    assert_eq!(block.get_string_or(0, "nonexistent", "default").to_string(), "default");
}

/// Test StarStats helper methods
#[test]
fn test_starstats_helper_methods() {
    use emstar::StarStats;
    
    let mut blocks = std::collections::HashMap::new();
    
    // Add simple blocks
    let mut simple1 = SimpleBlock::new();
    simple1.set("k1", DataValue::Integer(1));
    simple1.set("k2", DataValue::Integer(2));
    blocks.insert("simple1".to_string(), DataBlock::Simple(simple1));
    
    let mut simple2 = SimpleBlock::new();
    simple2.set("k3", DataValue::Integer(3));
    blocks.insert("simple2".to_string(), DataBlock::Simple(simple2));
    
    // Add loop blocks
    let mut loop1 = LoopBlock::new();
    loop1.add_column("col1");
    loop1.add_row(vec![DataValue::Integer(1)]).unwrap();
    loop1.add_row(vec![DataValue::Integer(2)]).unwrap();
    loop1.add_row(vec![DataValue::Integer(3)]).unwrap();
    blocks.insert("loop1".to_string(), DataBlock::Loop(loop1));
    
    let mut loop2 = LoopBlock::new();
    loop2.add_column("col1");
    loop2.add_column("col2");
    loop2.add_row(vec![DataValue::Integer(1), DataValue::Integer(2)]).unwrap();
    blocks.insert("loop2".to_string(), DataBlock::Loop(loop2));
    
    let stats = StarStats::from_blocks(&blocks);
    
    // Test has_loop_blocks
    assert!(stats.has_loop_blocks());
    
    // Test has_simple_blocks
    assert!(stats.has_simple_blocks());
    
    // Test avg_rows_per_loop
    assert_eq!(stats.avg_rows_per_loop(), 2.0); // (3 + 1) / 2 = 2.0
    
    // Test avg_cols_per_loop
    assert_eq!(stats.avg_cols_per_loop(), 1.5); // (1 + 2) / 2 = 1.5
    
    // Test get_block_stats for existing block
    let loop1_stats = stats.get_block_stats("loop1");
    assert!(loop1_stats.is_some());
    if let Some(DataBlockStats::Loop(l)) = loop1_stats {
        assert_eq!(l.n_rows, 3);
        assert_eq!(l.n_cols, 1);
    }
    
    // Test get_block_stats for non-existent block
    assert!(stats.get_block_stats("nonexistent").is_none());
    
    // Test with only simple blocks
    let mut simple_only = std::collections::HashMap::new();
    simple_only.insert("s1".to_string(), DataBlock::Simple(SimpleBlock::new()));
    let simple_stats = StarStats::from_blocks(&simple_only);
    
    assert!(!simple_stats.has_loop_blocks());
    assert!(simple_stats.has_simple_blocks());
    assert_eq!(simple_stats.avg_rows_per_loop(), 0.0);
    assert_eq!(simple_stats.avg_cols_per_loop(), 0.0);
}

/// Test all applicable APIs on all files
#[test]
fn test_all_apis_on_all_files() {
    let files = vec![
        "basic_double_quote.star",
        "basic_single_quote.star",
        "default_pipeline.star",
        "empty_loop.star",
        "loop_double_quote.star",
        "loop_single_quote.star",
        "one_loop.star",
        "postprocess.star",
        "rln3.1_data_style.star",
        "single_line_end_of_multiblock.star",
        "single_line_middle_of_multiblock.star",
        "two_basic_blocks.star",
        "two_single_line_loop_blocks.star",
        "relion_tutorial/run_it025_optimiser_2D.star",
        "relion_tutorial/run_it025_optimiser_3D.star",
        "relion_tutorial/run_it025_sampling_2D.star",
        "relion_tutorial/run_it025_sampling_3D.star",
    ];

    for file in &files {
        let path = format!("{}/{}", TEST_DATA_DIR, file);
        println!("Testing all APIs on: {}", path);
        
        // Verify file exists using standard library
        assert!(Path::new(&path).exists(), "File not found: {}", file);
        
        // Test read()
        let data = read(&path, None).expect(&format!("read() failed for {}", file));
        assert!(!data.is_empty(), "Data should not be empty for {}", file);
        
        // Test block_stats()
        let mem_stats = block_stats(&data);
        println!("  - {} blocks ({} simple, {} loop)", 
                 mem_stats.n_blocks, mem_stats.n_simple_blocks, mem_stats.n_loop_blocks);
        
        // Test stats() function
        let file_stats = stats(&path).expect(&format!("stats() failed for {}", file));
        assert_eq!(file_stats.n_blocks, mem_stats.n_blocks);
        
        // Test each block's APIs
        for (block_name, block) in &data {
            // Test count()
            let count = block.count();
            println!("  - Block '{}': count() = {}", block_name, count);
            
            // Test stats()
            let _block_stat = block.stats();
            
            // Test type-specific APIs
            if block.is_simple() {
                if let Some(simple) = block.as_simple() {
                    // Test SimpleBlock APIs
                    let len = simple.len();
                    let is_empty = simple.is_empty();
                    println!("    SimpleBlock: len()={}, is_empty()={}", len, is_empty);
                    
                    if len > 0 {
                        // Test keys()
                        let keys: Vec<_> = simple.keys().collect();
                        println!("    Keys: {:?}", keys);
                        
                        // Test contains_key() and get()
                        if let Some(first_key) = keys.first() {
                            assert!(simple.contains_key(*first_key));
                            let value = simple.get(*first_key);
                            assert!(value.is_some());
                        }
                    }
                }
            } else if block.is_loop() {
                if let Some(loop_block) = block.as_loop() {
                    // Test LoopBlock APIs
                    let nrows = loop_block.row_count();
                    let ncols = loop_block.column_count();
                    let is_empty = loop_block.is_empty();
                    println!("    LoopBlock: nrows()={}, ncols()={}, is_empty()={}", 
                             nrows, ncols, is_empty);
                    
                    if ncols > 0 {
                        // Test columns()
                        let cols = loop_block.columns();
                        println!("    Columns: {:?}", cols);
                        
                        // Test has_column()
                        if let Some(first_col) = cols.first() {
                            assert!(loop_block.has_column(first_col));
                            
                            // Test get_column()
                            let col_data = loop_block.get_column(first_col);
                            assert!(col_data.is_some());
                        }
                    }
                    
                    if nrows > 0 && ncols > 0 {
                        // Test get()
                        let value = loop_block.get(0, 0);
                        assert!(value.is_some());
                        
                        // Test iter_rows()
                        let row_count = loop_block.iter_rows().count();
                        assert_eq!(row_count, nrows);
                    }
                }
            }
            
            // Test block_type()
            let block_type = block.block_type();
            println!("    Type: {}", block_type);
        }
        
        // Test to_string()
        let star_string = emstar::to_string(&data);
        assert!(star_string.is_ok(), "to_string() failed for {}", file);
        let s = star_string.unwrap();
        assert!(!s.is_empty());
        assert!(s.contains("data_"));
        
        println!("  ✓ All APIs tested successfully for {}", file);
    }
}

// ============================================================================
// LoopBlockBuilder Tests
// ============================================================================

#[test]
fn test_loopblock_builder_basic() {
    let block = LoopBlock::builder()
        .columns(&["col1", "col2", "col3"])
        .row(vec![
            DataValue::Integer(1),
            DataValue::Integer(2),
            DataValue::Integer(3),
        ])
        .row(vec![
            DataValue::Integer(4),
            DataValue::Integer(5),
            DataValue::Integer(6),
        ])
        .build()
        .expect("Failed to build LoopBlock");

    assert_eq!(block.row_count(), 2);
    assert_eq!(block.column_count(), 3);
    assert_eq!(block.get_by_name(0, "col1"), Some(DataValue::Integer(1)));
    assert_eq!(block.get_by_name(1, "col2"), Some(DataValue::Integer(5)));
}

#[test]
fn test_loopblock_builder_column_method() {
    let block = LoopBlock::builder()
        .column("x")
        .column("y")
        .column("z")
        .row(vec![
            DataValue::Float(1.0),
            DataValue::Float(2.0),
            DataValue::Float(3.0),
        ])
        .build()
        .expect("Failed to build LoopBlock");

    assert_eq!(block.column_count(), 3);
    assert!(block.has_column("x"));
    assert!(block.has_column("y"));
    assert!(block.has_column("z"));
}

#[test]
fn test_loopblock_builder_rows_method() {
    let block = LoopBlock::builder()
        .columns(&["a", "b"])
        .rows(vec![
            vec![DataValue::Integer(1), DataValue::Integer(2)],
            vec![DataValue::Integer(3), DataValue::Integer(4)],
            vec![DataValue::Integer(5), DataValue::Integer(6)],
        ])
        .build()
        .expect("Failed to build LoopBlock");

    assert_eq!(block.row_count(), 3);
    assert_eq!(block.get_by_name(0, "a"), Some(DataValue::Integer(1)));
    assert_eq!(block.get_by_name(2, "b"), Some(DataValue::Integer(6)));
}

#[test]
fn test_loopblock_builder_empty() {
    let block = LoopBlock::builder().build().expect("Failed to build empty LoopBlock");

    assert_eq!(block.row_count(), 0);
    assert_eq!(block.column_count(), 0);
    assert!(block.is_empty());
}

#[test]
fn test_loopblock_builder_columns_only() {
    let block = LoopBlock::builder()
        .columns(&["col1", "col2"])
        .build()
        .expect("Failed to build LoopBlock");

    assert_eq!(block.row_count(), 0);
        assert_eq!(block.column_count(), 2);
}

#[test]
fn test_loopblock_builder_mixed_types() {
    let block = LoopBlock::builder()
        .columns(&["int_col", "float_col", "string_col", "null_col"])
        .row(vec![
            DataValue::Integer(42),
            DataValue::Float(3.14),
            DataValue::String("hello".into()),
            DataValue::Null,
        ])
        .build()
        .expect("Failed to build LoopBlock");

    assert_eq!(block.row_count(), 1);
    assert_eq!(block.get_by_name(0, "int_col"), Some(DataValue::Integer(42)));
    assert_eq!(block.get_by_name(0, "float_col"), Some(DataValue::Float(3.14)));
    assert_eq!(
        block.get_by_name(0, "string_col"),
        Some(DataValue::String("hello".into()))
    );
    assert_eq!(block.get_by_name(0, "null_col"), Some(DataValue::Null));
}

#[test]
fn test_loopblock_builder_write_read_roundtrip() {
    use std::collections::HashMap;

    let block = LoopBlock::builder()
        .columns(&["rlnCoordinateX", "rlnCoordinateY"])
        .row(vec![DataValue::Float(100.0), DataValue::Float(200.0)])
        .row(vec![DataValue::Float(300.0), DataValue::Float(400.0)])
        .build()
        .expect("Failed to build LoopBlock");

    let mut data = HashMap::new();
    data.insert("particles".to_string(), DataBlock::Loop(block));

    let test_file = "/tmp/test_builder_roundtrip.star";
    write(&data, test_file, None).expect("Failed to write file");

    let parsed = read(test_file, None).expect("Failed to read file");
    let parsed_block = parsed.get("particles").expect("Missing particles block");

    if let DataBlock::Loop(loop_block) = parsed_block {
        assert_eq!(loop_block.row_count(), 2);
        assert_eq!(loop_block.column_count(), 2);
        assert_eq!(
            loop_block.get_by_name(0, "rlnCoordinateX"),
            Some(DataValue::Float(100.0))
        );
        assert_eq!(
            loop_block.get_by_name(1, "rlnCoordinateY"),
            Some(DataValue::Float(400.0))
        );
    } else {
        panic!("Expected LoopBlock");
    }

    // Cleanup
    std::fs::remove_file(test_file).expect("Failed to delete test file");
}
