//! Benchmark for writing STAR files

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use emstar::{write, DataBlock, DataValue, LoopBlock};
use std::collections::HashMap;

fn create_large_loop_block(n_rows: usize) -> LoopBlock {
    let mut block = LoopBlock::new();
    block.add_column("rlnCoordinateX".into());
    block.add_column("rlnCoordinateY".into());
    block.add_column("rlnCoordinateZ".into());
    block.add_column("rlnAngleRot".into());
    block.add_column("rlnAngleTilt".into());
    block.add_column("rlnAnglePsi".into());
    block.add_column("rlnMicrographName".into());

    for i in 0..n_rows {
        block
            .add_row(vec![
                DataValue::Float(91.7987 + i as f64 * 0.1),
                DataValue::Float(83.6226 + i as f64 * 0.1),
                DataValue::Float(203.34103 + i as f64 * 0.1),
                DataValue::Float(-51.74 + i as f64 * 0.1),
                DataValue::Float(173.93 + i as f64 * 0.1),
                DataValue::Float(32.971 + i as f64 * 0.1),
                DataValue::String(format!("{:02}.mrc", i).into()),
            ])
            .unwrap();
    }

    block
}

fn bench_write_large_file(c: &mut Criterion) {
    let block = create_large_loop_block(10000);
    let mut data_blocks = HashMap::new();
    data_blocks.insert("particles".to_string(), DataBlock::Loop(block));

    c.bench_function("write_10000_rows", |b| {
        b.iter(|| {
            write(black_box(&data_blocks), black_box("/tmp/bench_write_large.star"), None)
                .unwrap();
        })
    });
}

fn bench_write_medium_file(c: &mut Criterion) {
    let block = create_large_loop_block(1000);
    let mut data_blocks = HashMap::new();
    data_blocks.insert("particles".to_string(), DataBlock::Loop(block));

    c.bench_function("write_1000_rows", |b| {
        b.iter(|| {
            write(black_box(&data_blocks), black_box("/tmp/bench_write_medium.star"), None)
                .unwrap();
        })
    });
}

fn bench_write_small_file(c: &mut Criterion) {
    let block = create_large_loop_block(100);
    let mut data_blocks = HashMap::new();
    data_blocks.insert("particles".to_string(), DataBlock::Loop(block));

    c.bench_function("write_100_rows", |b| {
        b.iter(|| {
            write(black_box(&data_blocks), black_box("/tmp/bench_write_small.star"), None)
                .unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_write_small_file,
    bench_write_medium_file,
    bench_write_large_file
);
criterion_main!(benches);