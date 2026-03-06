//! Statistics API example for emstar
//!
//! This example demonstrates how to use the statistics API to analyze
//! STAR file contents without fully loading all data.

use emstar::{
    create, stats, block_stats,
    DataBlock, DataValue, LoopBlock, SimpleBlock,
    DataBlockStats, StarStats
};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== emstar Statistics API Example ===\n");

    let file_path = "/tmp/stats_example.star";

    // Create a STAR file with various data blocks
    println!("Creating STAR file with multiple data blocks...\n");
    
    let mut data_blocks = HashMap::new();

    // SimpleBlock 1: General metadata
    let mut general = SimpleBlock::new();
    general.set("rlnImageSize", DataValue::Integer(256));
    general.set("rlnPixelSize", DataValue::Float(1.06));
    general.set("rlnDataDimensionality", DataValue::Integer(2));
    general.set("rlnVoltage", DataValue::Float(300.0));
    general.set("rlnSphericalAberration", DataValue::Float(2.7));
    data_blocks.insert("general".to_string(), DataBlock::Simple(general));

    // SimpleBlock 2: Optimization parameters
    let mut optim = SimpleBlock::new();
    optim.set("rlnOptimizeParticles", DataValue::Integer(10000));
    optim.set("rlnIterations", DataValue::Integer(25));
    data_blocks.insert("optimization".to_string(), DataBlock::Simple(optim));

    // LoopBlock 1: Particles (100 particles, 5 columns)
    // Using traditional approach for bulk data
    let mut particles = LoopBlock::new();
    particles.add_column("rlnCoordinateX");
    particles.add_column("rlnCoordinateY");
    particles.add_column("rlnAnglePsi");
    particles.add_column("rlnDefocusU");
    particles.add_column("rlnDefocusV");
    
    for i in 0..100 {
        particles.add_row(vec![
            DataValue::Float(100.0 + i as f64),
            DataValue::Float(200.0 + i as f64),
            DataValue::Float((i * 3) as f64),
            DataValue::Float(5000.0 + i as f64 * 10.0),
            DataValue::Float(5200.0 + i as f64 * 10.0),
        ])?;
    }
    data_blocks.insert("particles".to_string(), DataBlock::Loop(particles));

    // LoopBlock 2: Micrographs (5 micrographs, 3 columns)
    // Using the builder pattern for cleaner, declarative construction
    let micrographs = LoopBlock::builder()
        .columns(&["rlnMicrographName", "rlnMotionModelCoeff", "rlnAccumMotionTotal"])
        .row(vec![
            DataValue::String("micrograph_000.mrc".into()),
            DataValue::Integer(5),
            DataValue::Float(15.5),
        ])
        .row(vec![
            DataValue::String("micrograph_001.mrc".into()),
            DataValue::Integer(5),
            DataValue::Float(16.5),
        ])
        .row(vec![
            DataValue::String("micrograph_002.mrc".into()),
            DataValue::Integer(5),
            DataValue::Float(17.5),
        ])
        .row(vec![
            DataValue::String("micrograph_003.mrc".into()),
            DataValue::Integer(5),
            DataValue::Float(18.5),
        ])
        .row(vec![
            DataValue::String("micrograph_004.mrc".into()),
            DataValue::Integer(5),
            DataValue::Float(19.5),
        ])
        .build()?;
    data_blocks.insert("micrographs".to_string(), DataBlock::Loop(micrographs));

    // Create the file
    create(&data_blocks, file_path)?;
    println!("✓ Created STAR file\n");

    // =========================================================================
    // Method 1: stats() - Read file and compute statistics
    // =========================================================================
    println!("1. Using stats() function to analyze file:");
    println!("   (Reads file and computes statistics)\n");
    
    let file_stats = stats(file_path)?;
    print_stats(&file_stats);

    // =========================================================================
    // Method 2: block_stats() - Compute statistics from in-memory data
    // =========================================================================
    println!("\n2. Using block_stats() function on existing data:");
    println!("   (Computes statistics from already-loaded HashMap)\n");
    
    let mem_stats = block_stats(&data_blocks);
    print_stats(&mem_stats);

    // =========================================================================
    // Detailed block-level statistics
    // =========================================================================
    println!("\n3. Detailed per-block statistics:\n");
    
    for (name, block_stat) in &file_stats.block_stats {
        match block_stat {
            DataBlockStats::Simple(s) => {
                println!("   Block '{}' (SimpleBlock):", name);
                println!("     - Entries: {}", s.n_entries);
            }
            DataBlockStats::Loop(l) => {
                println!("   Block '{}' (LoopBlock):", name);
                println!("     - Rows: {}, Columns: {}", l.n_rows, l.n_cols);
                println!("     - Total cells: {}", l.n_cells);
            }
        }
    }

    // =========================================================================
    // Access specific block stats
    // =========================================================================
    println!("\n4. Accessing specific block statistics:\n");
    
    if let Some(particle_stats) = file_stats.get_block_stats("particles") {
        if let DataBlockStats::Loop(l) = particle_stats {
            println!("   Particles block:");
            println!("     - Number of particles: {}", l.n_rows);
            println!("     - Number of features: {}", l.n_cols);
            println!("     - Total data points: {}", l.n_cells);
        }
    }

    if let Some(general_stats) = file_stats.get_block_stats("general") {
        if let DataBlockStats::Simple(s) = general_stats {
            println!("\n   General block:");
            println!("     - Metadata entries: {}", s.n_entries);
        }
    }

    // =========================================================================
    // Summary statistics
    // =========================================================================
    println!("\n5. Summary statistics:\n");
    println!("   - Has loop blocks: {}", file_stats.has_loop_blocks());
    println!("   - Has simple blocks: {}", file_stats.has_simple_blocks());
    println!("   - Avg rows per LoopBlock: {:.1}", file_stats.avg_rows_per_loop());
    println!("   - Avg cols per LoopBlock: {:.1}", file_stats.avg_cols_per_loop());

    // =========================================================================
    // DataBlock-level stats access
    // =========================================================================
    println!("\n6. DataBlock-level statistics:\n");
    
    if let Some(DataBlock::Loop(particles)) = data_blocks.get("particles") {
        let loop_stats = particles.stats();
        println!("   Particles.stats():");
        println!("     - rows: {}, cols: {}, cells: {}", 
                 loop_stats.n_rows, loop_stats.n_cols, loop_stats.n_cells);
    }

    if let Some(DataBlock::Simple(general)) = data_blocks.get("general") {
        let simple_stats = general.stats();
        println!("\n   General.stats():");
        println!("     - entries: {}", simple_stats.n_entries);
    }

    // Cleanup
    std::fs::remove_file(file_path)?;
    println!("\n✓ Cleanup completed");
    println!("\n=== Statistics API Example Completed! ===");

    Ok(())
}

fn print_stats(stats: &StarStats) {
    println!("   StarStats:");
    println!("     - Total data blocks: {}", stats.n_blocks);
    println!("     - SimpleBlocks: {}", stats.n_simple_blocks);
    println!("     - LoopBlocks: {}", stats.n_loop_blocks);
    println!("     - Total SimpleBlock entries: {}", stats.total_simple_entries);
    println!("     - Total LoopBlock rows: {}", stats.total_loop_rows);
    println!("     - Total LoopBlock columns: {}", stats.total_loop_cols);
}
