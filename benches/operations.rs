//! Benchmark for CRUD operations on SimpleBlock and LoopBlock
//!
//! This benchmarks performance of critical operations that may have
//! performance issues:
//! - LoopBlock: add_row (O(1) amortized), get_by_name, set_by_name (O(n)), iter_rows (allocates per row), get_column (allocates)
//! - SimpleBlock: get, set, remove (O(1))
//! - stats() function

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use emstar::{DataBlock, DataValue, LoopBlock, SimpleBlock, stats};
use std::collections::HashMap;

// ============================================================================
// LoopBlock Benchmarks
// ============================================================================

fn create_loop_block(n_rows: usize, n_cols: usize) -> LoopBlock {
    let mut block = LoopBlock::new();

    // Add columns
    for col_idx in 0..n_cols {
        block.add_column(&format!("col{}", col_idx));
    }

    // Add rows
    for row_idx in 0..n_rows {
        let mut row = Vec::with_capacity(n_cols);
        for col_idx in 0..n_cols {
            let value = match col_idx % 3 {
                0 => DataValue::Float((row_idx * col_idx) as f64),
                1 => DataValue::Integer((row_idx * col_idx) as i64),
                _ => DataValue::String(format!("val_{}_{}", row_idx, col_idx).into()),
            };
            row.push(value);
        }
        block.add_row(row).unwrap();
    }

    block
}

fn bench_loopblock_add_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("LoopBlock::add_row");

    group.bench_function("1000_rows", |b| {
        b.iter(|| {
            let mut block = LoopBlock::new();
            block.add_column("col1");
            block.add_column("col2");

            for i in 0..1000 {
                block
                    .add_row(vec![
                        DataValue::Float(i as f64),
                        DataValue::Float((i * 2) as f64),
                    ])
                    .unwrap();
            }
        })
    });

    group.bench_function("10000_rows", |b| {
        b.iter(|| {
            let mut block = LoopBlock::new();
            block.add_column("col1");
            block.add_column("col2");

            for i in 0..10000 {
                block
                    .add_row(vec![
                        DataValue::Float(i as f64),
                        DataValue::Float((i * 2) as f64),
                    ])
                    .unwrap();
            }
        })
    });

    group.finish();
}

fn bench_loopblock_get(c: &mut Criterion) {
    let block = create_loop_block(10000, 20);

    let mut group = c.benchmark_group("LoopBlock::get");

    group.bench_function("by_index", |b| {
        b.iter(|| {
            // Get random cells
            for i in 0..1000 {
                let row = (i * 37) % 10000;
                let col = (i * 13) % 20;
                black_box(block.get(row, col));
            }
        })
    });

    group.bench_function("by_name", |b| {
        b.iter(|| {
            // Get cells by column name
            for i in 0..1000 {
                let row = (i * 37) % 10000;
                let col_name = format!("col{}", (i * 13) % 20);
                black_box(block.get_by_name(row, &col_name));
            }
        })
    });

    group.finish();
}

fn bench_loopblock_set_by_name(c: &mut Criterion) {
    let mut block = create_loop_block(10000, 20);

    let mut group = c.benchmark_group("LoopBlock::set_by_name");

    group.bench_function("single_cell", |b| {
        b.iter(|| {
            let row = 5000;
            let col_name = "col10";
            black_box(block.set_by_name(row, col_name, DataValue::Float(999.9)));
        })
    });

    group.bench_function("batch_update_100", |b| {
        b.iter(|| {
            for i in 0..100 {
                let row = (i * 97) % 10000;
                let col_name = format!("col{}", i % 20);
                black_box(block.set_by_name(row, &col_name, DataValue::Float(i as f64)));
            }
        })
    });

    group.finish();
}

fn bench_loopblock_iter_rows(c: &mut Criterion) {
    let block = create_loop_block(10000, 20);

    let mut group = c.benchmark_group("LoopBlock::iter_rows");

    group.bench_function("full_iteration", |b| {
        b.iter(|| {
            let count = black_box(block.iter_rows().count());
            black_box(count);
        })
    });

    group.bench_function("partial_iteration_100", |b| {
        b.iter(|| {
            let count = black_box(block.iter_rows().take(100).count());
            black_box(count);
        })
    });

    group.finish();
}

fn bench_loopblock_get_column(c: &mut Criterion) {
    let block = create_loop_block(10000, 20);

    let mut group = c.benchmark_group("LoopBlock::get_column");

    group.bench_function("full_column", |b| {
        b.iter(|| {
            black_box(block.get_column("col10"));
        })
    });

    group.bench_function("multiple_columns_5", |b| {
        b.iter(|| {
            for i in 0..5 {
                black_box(block.get_column(&format!("col{}", i * 3)));
            }
        })
    });

    group.finish();
}

fn bench_loopblock_update_row(c: &mut Criterion) {
    let mut block = create_loop_block(10000, 20);

    let mut group = c.benchmark_group("LoopBlock::update_row");

    group.bench_function("single_update", |b| {
        b.iter(|| {
            let new_row = vec![DataValue::Float(999.9); 20];
            black_box(block.update_row(5000, new_row));
        })
    });

    group.bench_function("batch_update_100", |b| {
        b.iter(|| {
            for i in 0..100 {
                let row_idx = (i * 97) % 10000;
                let new_row = vec![DataValue::Float(i as f64); 20];
                black_box(block.update_row(row_idx, new_row));
            }
        })
    });

    group.finish();
}

// ============================================================================
// SimpleBlock Benchmarks
// ============================================================================

fn create_simple_block(n_entries: usize) -> SimpleBlock {
    let mut block = SimpleBlock::new();

    for i in 0..n_entries {
        let key = format!("key{}", i);
        let value = match i % 3 {
            0 => DataValue::Integer(i as i64),
            1 => DataValue::Float(i as f64),
            _ => DataValue::String(format!("value_{}", i).into()),
        };
        block.set(&key, value);
    }

    block
}

fn bench_simpleblock_get(c: &mut Criterion) {
    let block = create_simple_block(1000);

    let mut group = c.benchmark_group("SimpleBlock::get");

    group.bench_function("single_lookup", |b| {
        b.iter(|| {
            black_box(block.get("key500"));
        })
    });

    group.bench_function("batch_lookup_100", |b| {
        b.iter(|| {
            for i in 0..100 {
                black_box(block.get(&format!("key{}", i * 7)));
            }
        })
    });

    group.finish();
}

fn bench_simpleblock_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("SimpleBlock::set");

    group.bench_function("single_insert", |b| {
        b.iter(|| {
            let mut block = SimpleBlock::new();
            black_box(block.set("new_key", DataValue::Integer(42)));
        })
    });

    group.bench_function("batch_insert_100", |b| {
        b.iter(|| {
            let mut block = SimpleBlock::new();
            for i in 0..100 {
                black_box(block.set(&format!("key{}", i), DataValue::Integer(i)));
            }
        })
    });

    group.finish();
}

fn bench_simpleblock_remove(c: &mut Criterion) {
    let mut block = create_simple_block(1000);

    let mut group = c.benchmark_group("SimpleBlock::remove");

    group.bench_function("single_remove", |b| {
        b.iter(|| {
            let mut block = create_simple_block(1000);
            black_box(block.remove("key500"));
        })
    });

    group.bench_function("batch_remove_100", |b| {
        b.iter(|| {
            let mut block = create_simple_block(1000);
            for i in 0..100 {
                black_box(block.remove(&format!("key{}", i * 7)));
            }
        })
    });

    group.finish();
}

// ============================================================================
// Stats Benchmarks
// ============================================================================

fn create_benchmark_file(path: &str, n_rows: usize) {
    let mut data = HashMap::new();

    // Add simple blocks
    let mut general = SimpleBlock::new();
    general.set("rlnImageSize", DataValue::Integer(256));
    general.set("rlnPixelSize", DataValue::Float(1.06));
    data.insert("general".to_string(), DataBlock::Simple(general));

    // Add loop block
    let mut loop_block = LoopBlock::new();
    for col_idx in 0..10 {
        loop_block.add_column(&format!("col{}", col_idx));
    }

    for row_idx in 0..n_rows {
        let mut row = Vec::new();
        for col_idx in 0..10 {
            row.push(DataValue::Float((row_idx * col_idx) as f64));
        }
        loop_block.add_row(row).unwrap();
    }
    data.insert("particles".to_string(), DataBlock::Loop(loop_block));

    emstar::write(&data, path).unwrap();
}

fn bench_stats(c: &mut Criterion) {
    let mut group = c.benchmark_group("stats");

    // Create test files of different sizes
    create_benchmark_file("/tmp/bench_stats_1k.star", 1000);
    create_benchmark_file("/tmp/bench_stats_10k.star", 10000);
    create_benchmark_file("/tmp/bench_stats_100k.star", 100000);

    group.bench_function("1000_rows", |b| {
        b.iter(|| {
            black_box(stats("/tmp/bench_stats_1k.star"));
        })
    });

    group.bench_function("10000_rows", |b| {
        b.iter(|| {
            black_box(stats("/tmp/bench_stats_10k.star"));
        })
    });

    group.bench_function("100000_rows", |b| {
        b.iter(|| {
            black_box(stats("/tmp/bench_stats_100k.star"));
        })
    });

    group.finish();
}

// ============================================================================
// Criterion Main
// ============================================================================

criterion_group!(
    benches,
    bench_loopblock_add_row,
    bench_loopblock_get,
    bench_loopblock_set_by_name,
    bench_loopblock_iter_rows,
    bench_loopblock_get_column,
    bench_loopblock_update_row,
    bench_simpleblock_get,
    bench_simpleblock_set,
    bench_simpleblock_remove,
    bench_stats,
);
criterion_main!(benches);
