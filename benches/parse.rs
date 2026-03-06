//! Benchmark for parsing STAR files

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use emstar::read;
use std::fs::File;
use std::io::Write;

fn create_large_star_file(path: &str, n_rows: usize) {
    let mut file = File::create(path).unwrap();

    writeln!(file, "data_particles").unwrap();
    writeln!(file).unwrap();
    writeln!(file, "loop_").unwrap();
    writeln!(file, "_rlnCoordinateX #1").unwrap();
    writeln!(file, "_rlnCoordinateY #2").unwrap();
    writeln!(file, "_rlnCoordinateZ #3").unwrap();
    writeln!(file, "_rlnAngleRot #4").unwrap();
    writeln!(file, "_rlnAngleTilt #5").unwrap();
    writeln!(file, "_rlnAnglePsi #6").unwrap();
    writeln!(file, "_rlnMicrographName #7").unwrap();

    for i in 0..n_rows {
        writeln!(
            file,
            "{}\t{}\t{}\t{}\t{}\t{}\t{:02}.mrc",
            91.7987 + i as f64 * 0.1,
            83.6226 + i as f64 * 0.1,
            203.34103 + i as f64 * 0.1,
            -51.74 + i as f64 * 0.1,
            173.93 + i as f64 * 0.1,
            32.971 + i as f64 * 0.1,
            i
        )
        .unwrap();
    }
}

fn bench_parse_large_file(c: &mut Criterion) {
    let path = "/tmp/bench_large.star";
    create_large_star_file(path, 10000);

    c.bench_function("parse_10000_rows", |b| {
        b.iter(|| {
            let data = read(black_box(path)).unwrap();
            black_box(data);
        })
    });
}

fn bench_parse_medium_file(c: &mut Criterion) {
    let path = "/tmp/bench_medium.star";
    create_large_star_file(path, 1000);

    c.bench_function("parse_1000_rows", |b| {
        b.iter(|| {
            let data = read(black_box(path)).unwrap();
            black_box(data);
        })
    });
}

fn bench_parse_small_file(c: &mut Criterion) {
    let path = "/tmp/bench_small.star";
    create_large_star_file(path, 100);

    c.bench_function("parse_100_rows", |b| {
        b.iter(|| {
            let data = read(black_box(path)).unwrap();
            black_box(data);
        })
    });
}

criterion_group!(
    benches,
    bench_parse_small_file,
    bench_parse_medium_file,
    bench_parse_large_file
);
criterion_main!(benches);