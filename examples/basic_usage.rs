//! Basic usage example for emstar

use emstar::{read, write, DataBlock, DataValue, LoopBlock, SimpleBlock};
use polars::prelude::*;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== emstar Basic Usage Example ===\n");

    // Example 1: Create and write a simple block
    println!("Example 1: Creating a simple block");
    let mut simple_block = SimpleBlock::new();
    simple_block.set("rlnImageSize".into(), DataValue::Integer(256));
    simple_block.set("rlnPixelSize".into(), DataValue::Float(1.23));
    simple_block.set("rlnDataDimensionality".into(), DataValue::Integer(2));

    let mut data = HashMap::new();
    data.insert("general".to_string(), DataBlock::Simple(simple_block));

    write(&data, "example_simple.star", None)?;
    println!("✓ Wrote example_simple.star\n");


    // Example 2: Create and write a loop block (particle data) using Polars
    println!("Example 2: Creating a loop block with particle data using Polars");
    
    // Create Polars Series for each column
    let s_x = Series::new("rlnCoordinateX".into(), &[91.7987f64, 97.6358, 92.4152]);
    let s_y = Series::new("rlnCoordinateY".into(), &[83.6226f64, 80.4370, 88.8427]);
    let s_z = Series::new("rlnCoordinateZ".into(), &[203.34103f64, 203.13616, 210.66390]);
    let s_rot = Series::new("rlnAngleRot".into(), &[-51.74f64, 141.5, -78.75]);
    let s_tilt = Series::new("rlnAngleTilt".into(), &[173.93f64, 171.76, 173.93]);
    let s_psi = Series::new("rlnAnglePsi".into(), &[32.971f64, -134.68, 87.2632]);
    let s_mg = Series::new("rlnMicrographName".into(), &["01_10.00Apx.mrc", "01_10.00Apx.mrc", "02_15.00Apx.mrc"]);

    // Create DataFrame from Series
    let df = DataFrame::new(vec![s_x.into(), s_y.into(), s_z.into(), s_rot.into(), s_tilt.into(), s_psi.into(), s_mg.into()])?;
    let particles = LoopBlock::from_dataframe(df);

    data.insert("particles".to_string(), DataBlock::Loop(particles));

    write(&data, "example_particles.star", None)?;
    println!("✓ Wrote example_particles.star with {} particles\n", data["particles"].as_loop().unwrap().row_count());

    // Example 3: Read and modify existing data using Polars operations
    println!("Example 3: Reading and modifying data with Polars");
    let mut read_data = read("example_particles.star", None)?;

    if let Some(DataBlock::Loop(particles)) = read_data.get_mut("particles") {
        println!("✓ Read {} particles", particles.row_count());

        // Use Polars DataFrame operations to modify data
        let df = particles.as_dataframe_mut();
        
        // Shift X coordinates by 10.0 using Polars
        let col_x = df.column("rlnCoordinateX")?;
        let shifted_x = col_x.f64()? + 10.0;
        df.replace("rlnCoordinateX", shifted_x)?;
        
        println!("✓ Modified X coordinates (+10.0) using Polars\n");
    }

    write(&read_data, "example_modified.star", None)?;
    println!("✓ Wrote modified data to example_modified.star\n");

    // Example 4: Access specific data and demonstrate Polars filtering
    println!("Example 4: Accessing specific data and Polars filtering");
    let read_data = read("example_particles.star", None)?;

    if let Some(DataBlock::Loop(particles)) = read_data.get("particles") {
        println!("Particle metadata:");
        println!("  - Number of particles: {}", particles.row_count());
        println!("  - Number of columns: {}", particles.column_count());
        println!("  - Columns: {:?}", particles.columns());

        // Get first particle's X coordinate
        if let Some(DataValue::Float(x)) = particles.get_by_name(0, "rlnCoordinateX") {
            println!("  - First particle X coordinate: {}", x);
        }

        // Get micrograph name of second particle
        if let Some(DataValue::String(name)) = particles.get_by_name(1, "rlnMicrographName") {
            println!("  - Second particle micrograph: {}", name);
        }

        // Demonstrate Polars DataFrame filtering
        println!("\n  Demonstrating Polars filtering:");
        let df = particles.as_dataframe();
        
        // Filter particles with X > 92
        let x_col = df.column("rlnCoordinateX")?;
        let x_f64 = x_col.f64()?;
        let mask = x_f64.gt(92.0);
        let filtered = df.filter(&mask)?;
        println!("  - Particles with X > 92.0: {}", filtered.height());
        
        // Calculate statistics
        let x_mean = x_f64.mean().unwrap_or(0.0);
        println!("  - Mean X coordinate: {:.4}", x_mean);
    }

    println!("\n=== Examples completed successfully! ===");
    Ok(())
}
