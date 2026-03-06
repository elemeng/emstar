//! Test data generators for benchmarking
//!
//! This module provides functions to generate large test datasets for performance testing:
//! - 3 simple blocks with 20 key-value pairs each
//! - 3 loop blocks with 5 million rows total (distributed) and 20 columns

use emstar::{DataBlock, DataValue, LoopBlock, SimpleBlock};
use std::collections::HashMap;

/// Generate 3 simple blocks with 20 key-value pairs each
/// Returns a HashMap suitable for writing to a STAR file
pub fn generate_simple_blocks() -> HashMap<String, DataBlock> {
    let mut data = HashMap::new();

    // Simple Block 1: General microscopy parameters
    let mut general = SimpleBlock::new();
    general.set("rlnImageSize", DataValue::Integer(256));
    general.set("rlnPixelSize", DataValue::Float(1.06));
    general.set("rlnVoltage", DataValue::Float(300.0));
    general.set("rlnSphericalAberration", DataValue::Float(2.7));
    general.set("rlnAmplitudeContrast", DataValue::Float(0.1));
    general.set("rlnMagnification", DataValue::Integer(10000));
    general.set("rlnDetectorPixelSize", DataValue::Float(5.0));
    general.set("rlnDataDimensionality", DataValue::Integer(2));
    general.set("rlnOriginalImageSize", DataValue::Integer(512));
    general.set("rlnCurrentResolution", DataValue::Float(3.5));
    general.set("rlnFinalResolution", DataValue::Float(3.2));
    general.set("rlnBfactorUsedForSharpening", DataValue::Float(0.0));
    general.set("rlnFscThreshold", DataValue::Float(0.143));
    general.set("rlnParticleBoxSize", DataValue::Integer(256));
    general.set("rlnParticleDiameter", DataValue::Float(200.0));
    general.set("rlnSymmetryGroup", DataValue::String("C1".into()));
    general.set("rlnNumberOfParticles", DataValue::Integer(5000000));
    general.set("rlnNumberOfClasses", DataValue::Integer(4));
    general.set("rlnNumberOfIterations", DataValue::Integer(25));
    general.set("rlnPipelineJobCounter", DataValue::Integer(32));
    data.insert("general".to_string(), DataBlock::Simple(general));

    // Simple Block 2: Optimization parameters
    let mut optim = SimpleBlock::new();
    optim.set("rlnOptimizeParticles", DataValue::Integer(5000000));
    optim.set("rlnOptimizeIterations", DataValue::Integer(25));
    optim.set("rlnOptimizeBatchSize", DataValue::Integer(1000));
    optim.set("rlnLearningRate", DataValue::Float(0.01));
    optim.set("rlnMomentum", DataValue::Float(0.9));
    optim.set("rlnWeightDecay", DataValue::Float(0.0001));
    optim.set("rlnDropoutRate", DataValue::Float(0.2));
    optim.set("rlnConvergenceTolerance", DataValue::Float(0.00001));
    optim.set("rlnMaxResolution", DataValue::Float(3.0));
    optim.set("rlnMinResolution", DataValue::Float(20.0));
    optim.set("rlnHelicalTwistInitial", DataValue::Float(27.5));
    optim.set("rlnHelicalRiseInitial", DataValue::Float(4.8));
    optim.set("rlnHelicalSymmetry", DataValue::String("n1".into()));
    optim.set("rlnCtfRefineDefocus", DataValue::Integer(1));
    optim.set("rlnCtfRefineAstigmatism", DataValue::Integer(1));
    optim.set("rlnCtfRefineBfactor", DataValue::Integer(0));
    optim.set("rlnCtfRefineScale", DataValue::Integer(0));
    optim.set("rlnParticleDisplayRadius", DataValue::Integer(128));
    optim.set("rlnNormCorrection", DataValue::Float(1.0));
    optim.set("rlnSigmaOffsets", DataValue::Float(3.0));
    data.insert("optimization".to_string(), DataBlock::Simple(optim));

    // Simple Block 3: Post-processing parameters
    let mut postprocess = SimpleBlock::new();
    postprocess.set("rlnRandomiseFrom", DataValue::Float(32.7));
    postprocess.set("rlnLowPassFilter", DataValue::Float(3.0));
    postprocess.set("rlnHighPassFilter", DataValue::Float(200.0));
    postprocess.set("rlnSharpeningBfactor", DataValue::Float(-100.0));
    postprocess.set("rlnMaskDiameter", DataValue::Float(180.0));
    postprocess.set("rlnMaskEdgeWidth", DataValue::Integer(6));
    postprocess.set("rlnSolventMask", DataValue::String("mask.mrc".into()));
    postprocess.set("rlnUnfilteredMap", DataValue::String("unfiltered.mrc".into()));
    postprocess.set("rlnFilteredMap", DataValue::String("filtered.mrc".into()));
    postprocess.set("rlnHalfMap1", DataValue::String("half1.mrc".into()));
    postprocess.set("rlnHalfMap2", DataValue::String("half2.mrc".into()));
    postprocess.set("rlnFscCurve", DataValue::String("fsc.star".into()));
    postprocess.set("rlnGuinierPlot", DataValue::String("guinier.star".into()));
    postprocess.set("rlnPostprocessVersion", DataValue::String("3.0.8".into()));
    postprocess.set("rlnPostprocessDate", DataValue::String("2024-01-15".into()));
    postprocess.set("rlnAngularSampling", DataValue::Float(3.0));
    postprocess.set("rlnOffsetSearchRange", DataValue::Float(15.0));
    postprocess.set("rlnOffsetSearchStep", DataValue::Float(1.5));
    postprocess.set("rlnHealpixOrder", DataValue::Integer(3));
    postprocess.set("rlnAutoLocalSearches", DataValue::Integer(1));
    postprocess.set("rlnSymmetry", DataValue::String("C1".into()));
    data.insert("postprocess".to_string(), DataBlock::Simple(postprocess));

    data
}

/// Generate loop block column names (20 columns with realistic RELION field names)
fn get_loop_columns() -> Vec<&'static str> {
    vec![
        "rlnCoordinateX",
        "rlnCoordinateY",
        "rlnCoordinateZ",
        "rlnAngleRot",
        "rlnAngleTilt",
        "rlnAnglePsi",
        "rlnOriginX",
        "rlnOriginY",
        "rlnOriginZ",
        "rlnDefocusU",
        "rlnDefocusV",
        "rlnDefocusAngle",
        "rlnPhaseShift",
        "rlnCtfBfactor",
        "rlnCtfScalefactor",
        "rlnMagnification",
        "rlnDetectorPixelSize",
        "rlnNormCorrection",
        "rlnLogLikeliContribution",
        "rlnMaxValueProbDistribution",
    ]
}

/// Generate a template row for particles
/// This will be repeated to create the full dataset
fn generate_particle_template(idx: usize) -> Vec<DataValue> {
    // Generate 100 unique templates that will be repeated
    // Use idx to create some variation
    let offset = idx as f64 * 10.0;
    
    vec![
        DataValue::Float(1000.0 + offset % 3000.0),
        DataValue::Float(1000.0 + (offset * 1.1) % 3000.0),
        DataValue::Float(200.0 + (offset * 0.5) % 800.0),
        DataValue::Float((offset * 3.7) % 360.0),
        DataValue::Float((offset * 2.3) % 180.0),
        DataValue::Float((offset * 1.7) % 360.0),
        DataValue::Float((offset.sin() * 5.0) % 10.0),
        DataValue::Float((offset.cos() * 5.0) % 10.0),
        DataValue::Float((offset.sin() * 2.0) % 5.0),
        DataValue::Float(8000.0 + (offset * 10.0) % 15000.0),
        DataValue::Float(8500.0 + (offset * 11.0) % 15000.0),
        DataValue::Float((offset * 4.5) % 180.0),
        DataValue::Float((offset * 1.0) % 180.0),
        DataValue::Float(-50.0 + (offset * 2.0) % 100.0),
        DataValue::Float(0.8 + (offset.sin() * 0.2 + 0.2) % 0.4),
        DataValue::Float(10000.0 + (offset * 1.0) % 5000.0),
        DataValue::Float(5.0 + (offset * 0.05) % 3.0),
        DataValue::Float(0.8 + (offset.cos() * 0.3 + 0.3) % 0.4),
        DataValue::Float(-5000.0 + (offset * 10.0) % 10000.0),
        DataValue::Float(0.1 + (offset.sin() * 0.4 + 0.4) % 0.8),
    ]
}

/// Generate 3 loop blocks with ~5 million rows total using repeated template rows
/// Block 1: 2,000,000 rows (particles)
/// Block 2: 2,000,000 rows (particles)  
/// Block 3: 1,000,000 rows (particles)
pub fn generate_loop_blocks() -> HashMap<String, DataBlock> {
    let mut data = HashMap::new();

    let columns = get_loop_columns();

    // Pre-generate 100 unique template rows
    let num_templates = 100;
    let mut templates = Vec::with_capacity(num_templates);
    for i in 0..num_templates {
        templates.push(generate_particle_template(i));
    }

    // Helper function to add repeated rows
    let add_repeated_rows = |block: &mut LoopBlock, num_rows: usize| {
        for i in 0..num_rows {
            let template_idx = i % num_templates;
            block.add_row(templates[template_idx].clone()).unwrap();
        }
    };

    // Loop Block 1: 2,000,000 particles
    println!("Generating loop block 1 (2,000,000 rows)...");
    let mut block1 = LoopBlock::new();
    for col in &columns {
        block1.add_column(col);
    }
    add_repeated_rows(&mut block1, 2_000_000);
    data.insert("particles_1".to_string(), DataBlock::Loop(block1));
    println!("  Done: 2,000,000 rows");

    // Loop Block 2: 2,000,000 particles
    println!("Generating loop block 2 (2,000,000 rows)...");
    let mut block2 = LoopBlock::new();
    for col in &columns {
        block2.add_column(col);
    }
    add_repeated_rows(&mut block2, 2_000_000);
    data.insert("particles_2".to_string(), DataBlock::Loop(block2));
    println!("  Done: 2,000,000 rows");

    // Loop Block 3: 1,000,000 particles
    println!("Generating loop block 3 (1,000,000 rows)...");
    let mut block3 = LoopBlock::new();
    for col in &columns {
        block3.add_column(col);
    }
    add_repeated_rows(&mut block3, 1_000_000);
    data.insert("particles_3".to_string(), DataBlock::Loop(block3));
    println!("  Done: 1,000,000 rows");

    println!("Total: 5,000,000 rows generated");
    data
}

/// Generate complete benchmark dataset with both simple and loop blocks
pub fn generate_benchmark_data() -> HashMap<String, DataBlock> {
    let mut data = HashMap::new();
    
    // Add simple blocks
    let simple_blocks = generate_simple_blocks();
    data.extend(simple_blocks);
    
    // Add loop blocks
    let loop_blocks = generate_loop_blocks();
    data.extend(loop_blocks);
    
    data
}

/// Write benchmark data to a STAR file
pub fn write_benchmark_file(path: &str) -> Result<(), emstar::Error> {
    let data = generate_benchmark_data();
    emstar::write(&data, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_generate_simple_blocks() {
        let data = generate_simple_blocks();
        assert_eq!(data.len(), 3);
        
        // Verify each block is a SimpleBlock
        for (_, block) in &data {
            assert!(block.is_simple());
        }
        
        // Verify block names
        assert!(data.contains_key("general"));
        assert!(data.contains_key("optimization"));
        assert!(data.contains_key("postprocess"));
        
        // Verify 20 entries each
        if let Some(DataBlock::Simple(block)) = data.get("general") {
            assert_eq!(block.len(), 20);
        }
    }
    
    #[test]
    fn test_generate_loop_blocks_small() {
        // Small test with only 1000 rows total
        let columns = get_loop_columns();
        let num_templates = 10;
        let mut templates = Vec::with_capacity(num_templates);
        for i in 0..num_templates {
            templates.push(generate_particle_template(i));
        }
        
        let mut block = LoopBlock::new();
        for col in &columns {
            block.add_column(col);
        }
        
        // Add 1000 rows using repeated templates
        for i in 0..1000 {
            let template_idx = i % num_templates;
            block.add_row(templates[template_idx].clone()).unwrap();
        }
        
        assert_eq!(block.row_count(), 1000);
        assert_eq!(block.column_count(), 20);
        
        // Verify first and last rows are different
        let first = block.get(0, 0);
        let last = block.get(999, 0);
        assert_ne!(first, last);
    }
    
    #[test]
    fn test_generate_loop_blocks() {
        // For actual test, use smaller dataset to avoid long test time
        let columns = get_loop_columns();
        
        // Generate small blocks for testing
        let mut block1 = LoopBlock::new();
        for col in &columns {
            block1.add_column(col);
        }
        
        let templates = vec![generate_particle_template(0), generate_particle_template(1)];
        for i in 0..100 {
            block1.add_row(templates[i % 2].clone()).unwrap();
        }
        
        assert_eq!(block1.row_count(), 100);
        assert_eq!(block1.column_count(), 20);
    }
    
    #[test]
    fn test_generate_benchmark_data() {
        // Use small dataset for testing
        let mut data = HashMap::new();
        
        // Add simple blocks
        data.extend(generate_simple_blocks());
        
        // Add small loop block for testing
        let columns = get_loop_columns();
        let mut block = LoopBlock::new();
        for col in &columns {
            block.add_column(col);
        }
        let template = generate_particle_template(0);
        for _ in 0..100 {
            block.add_row(template.clone()).unwrap();
        }
        data.insert("test_particles".to_string(), DataBlock::Loop(block));
        
        assert_eq!(data.len(), 4); // 3 simple + 1 loop
        
        let simple_count = data.values().filter(|b| b.is_simple()).count();
        let loop_count = data.values().filter(|b| b.is_loop()).count();
        
        assert_eq!(simple_count, 3);
        assert_eq!(loop_count, 1);
    }
}