//! Comprehensive CRUD operations example for emstar
//!
//! This example demonstrates Create, Read, and Update operations
//! for working with STAR files.

use emstar::{
    read, write, DataBlock, DataValue, LoopBlock, SimpleBlock
};
use std::collections::HashMap;
use std::path::Path;

/// Helper function to create a LoopBlock using the builder pattern
fn create_particles_with_builder() -> emstar::Result<LoopBlock> {
    LoopBlock::builder()
        .columns(&["rlnCoordinateX", "rlnCoordinateY", "rlnAnglePsi", "rlnImageName"])
        .rows(vec![
            vec![DataValue::Float(125.5), DataValue::Float(340.2), DataValue::Float(45.3), DataValue::String("000001@particle.mrcs".into())],
            vec![DataValue::Float(200.0), DataValue::Float(150.5), DataValue::Float(90.0), DataValue::String("000002@particle.mrcs".into())],
            vec![DataValue::Float(300.7), DataValue::Float(420.1), DataValue::Float(180.0), DataValue::String("000003@particle.mrcs".into())],
        ])
        .build()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== emstar CRUD Operations Example ===\n");

    let file_path = "/tmp/crud_example.star";

    // =========================================================================
    // CREATE - Create a new STAR file with data blocks
    // =========================================================================
    println!("1. CREATE: Building STAR file with data blocks...");
    
    let mut data_blocks = HashMap::new();

    // Create a SimpleBlock (key-value pairs)
    let mut general = SimpleBlock::new();
    general.set("rlnImageSize", DataValue::Integer(256));
    general.set("rlnPixelSize", DataValue::Float(1.06));
    general.set("rlnDataDimensionality", DataValue::Integer(2));
    general.set("rlnMicrographOriginalPixelSize", DataValue::Float(0.53));
    data_blocks.insert("general".to_string(), DataBlock::Simple(general));

    // Create a LoopBlock using the builder pattern (fluent API)
    let particles = create_particles_with_builder()?;
    
    data_blocks.insert("particles".to_string(), DataBlock::Loop(particles));

    // Write to file (creates or overwrites)
    write(&data_blocks, file_path, None)?;
    println!("   ✓ Created STAR file with general metadata and {} particles\n", 3);

    // =========================================================================
    // READ - Read data from the STAR file
    // =========================================================================
    println!("2. READ: Reading data from STAR file...");
    
    let data_blocks = read(file_path, None)?;
    
    // Read SimpleBlock
    if let Some(DataBlock::Simple(general)) = data_blocks.get("general") {
        println!("   General block:");
        if let Some(DataValue::Integer(size)) = general.get("rlnImageSize") {
            println!("     - Image Size: {}", size);
        }
        if let Some(DataValue::Float(px)) = general.get("rlnPixelSize") {
            println!("     - Pixel Size: {}", px);
        }
        println!("     - Keys: {:?}", general.keys().collect::<Vec<_>>());
    }
    
    // Read LoopBlock
    if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
        println!("\n   Particles block:");
        println!("     - Rows: {}, Columns: {}", particles.row_count(), particles.column_count());
        println!("     - Column names: {:?}", particles.columns());
        
        // Read specific values
        println!("\n   First particle:");
        if let Some(x) = particles.get_by_name(0, "rlnCoordinateX") {
            println!("     - X coordinate: {:?}", x);
        }
        if let Some(y) = particles.get_by_name(0, "rlnCoordinateY") {
            println!("     - Y coordinate: {:?}", y);
        }
        
        // Read entire column
        if let Some(x_coords) = particles.get_column("rlnCoordinateX") {
            println!("\n   All X coordinates: {:?}", x_coords);
        }
        
        // Iterate over rows
        println!("\n   All particles (using iter_rows):");
        for (i, row) in particles.iter_rows().enumerate() {
            println!("     Row {}: {:?}", i, row);
        }
    }
    println!();

    // =========================================================================
    // UPDATE - Modify existing data
    // =========================================================================
    println!("3. UPDATE: Modifying data...");
    
    let mut data_blocks = read(file_path, None)?;
    
    // Update SimpleBlock
    if let Some(DataBlock::Simple(general)) = data_blocks.get_mut("general") {
        // Add a new key
        general.set("rlnNewParameter", DataValue::String("new_value".into()));
        // Update existing key
        general.set("rlnPixelSize", DataValue::Float(1.12));
        println!("   ✓ Updated general block (added new parameter, modified pixel size)");
    }
    
    // Update LoopBlock
    if let Some(DataBlock::Loop(particles)) = data_blocks.get_mut("particles") {
        // Update a single cell
        particles.set_by_name(0, "rlnCoordinateX", DataValue::Float(999.9))?;
        println!("   ✓ Updated X coordinate of first particle to 999.9");
        
        // Update entire row using set_by_name
        particles.set_by_name(1, "rlnCoordinateX", DataValue::Float(500.0))?;
        particles.set_by_name(1, "rlnCoordinateY", DataValue::Float(600.0))?;
        particles.set_by_name(1, "rlnAnglePsi", DataValue::Float(270.0))?;
        particles.set_by_name(1, "rlnImageName", DataValue::String("updated@particle.mrcs".into()))?;
        println!("   ✓ Updated entire second row");
        
        // Add a new row
        particles.add_row(vec![
            DataValue::Float(400.0),
            DataValue::Float(300.0),
            DataValue::Float(0.0),
            DataValue::String("000004@particle.mrcs".into()),
        ])?;
        println!("   ✓ Added fourth particle");
    }
    
    // Save updated data
    write(&data_blocks, file_path, None)?;
    println!("   ✓ Saved changes to file\n");

    // Verify updates
    let data_blocks = read(file_path, None)?;
    if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
        println!("   Verification after updates:");
        println!("     - Total particles: {}", particles.row_count());
        println!("     - First particle X: {:?}", particles.get_by_name(0, "rlnCoordinateX"));
    }
    println!();

    // =========================================================================
    // DELETE - Remove data from blocks
    // =========================================================================
    println!("4. DELETE: Removing data from blocks...");
    
    let mut data_blocks = read(file_path, None)?;
    
    if let Some(DataBlock::Simple(general)) = data_blocks.get_mut("general") {
        // Remove a key from SimpleBlock
        general.remove("rlnNewParameter");
        println!("   ✓ Removed 'rlnNewParameter' from general block");
    }
    
    if let Some(DataBlock::Loop(particles)) = data_blocks.get_mut("particles") {
        // Remove a row from LoopBlock
        particles.remove_row(0)?;
        println!("   ✓ Removed first particle row");
        
        // Remove a column (if needed)
        // particles.remove_column("rlnAnglePsi")?;
        // println!("   ✓ Removed rlnAnglePsi column");
    }
    
    write(&data_blocks, file_path, None)?;
    println!("   ✓ Saved changes\n");

    // Verify deletions
    let data_blocks = read(file_path, None)?;
    if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
        println!("   Verification after deletions:");
        println!("     - Total particles: {}", particles.row_count());
    }
    println!();

    // =========================================================================
    // Utility functions
    // =========================================================================
    println!("5. UTILITY FUNCTIONS:");
    
    // Check if file exists using standard library
    if Path::new(file_path).exists() {
        println!("   ✓ File exists: {}", file_path);
    }
    
    // Check column existence
    let data_blocks = read(file_path, None)?;
    if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
        println!("   ✓ Has 'rlnCoordinateX' column: {}", particles.columns().contains(&"rlnCoordinateX"));
        println!("   ✓ Has 'rlnNonExistent' column: {}", particles.columns().contains(&"rlnNonExistent"));
    }
    println!();

    // =========================================================================
    // Cleanup
    // =========================================================================
    println!("6. CLEANUP: Deleting the file...");
    std::fs::remove_file(file_path)?;
    println!("   ✓ File deleted: {}", file_path);
    println!("   ✓ File exists after delete: {}", Path::new(file_path).exists());

    println!("\n=== CRUD Operations Example Completed Successfully! ===");
    Ok(())
}
