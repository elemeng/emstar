use std::io::Write;
use std::process::Command;

const BINARY: &str = env!("CARGO_BIN_EXE_emstar");

/// A realistic multi-block STAR file like those used in RELION.
const EXAMPLE_STAR: &str = r#"data_general
_rlnImageSize 256
_rlnPixelSize 1.06
_rlnVoltage 300

data_particles
loop_
_rlnCoordinateX #1
_rlnCoordinateY #2
_rlnAngleRot #3
91.7987	83.6226	-51.74
97.6358	80.4370	141.5

data_optimisation
_rlnIteration 25
_rlnConverged Yes
"#;

fn run(args: &[&str]) -> (String, String, bool) {
    let output = Command::new(BINARY).args(args).output().expect("failed to run");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (stdout, stderr, output.status.success())
}

fn write_star(path: &str, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

// ── CLI ─────────────────────────────────────────────────────────────────

#[test]
fn cli_read_shows_blocks() {
    write_star("/tmp/emstar_cli_read.star", EXAMPLE_STAR);
    let (out, err, ok) = run(&["read", "/tmp/emstar_cli_read.star"]);
    assert!(ok, "stderr: {}", err);
    assert!(out.contains("general"));
    assert!(out.contains("particles"));
    assert!(out.contains("optimisation"));
    assert!(out.contains("Simple"));
    assert!(out.contains("Loop"));
}

#[test]
fn cli_stats_shows_counts() {
    write_star("/tmp/emstar_cli_stats.star", EXAMPLE_STAR);
    let (out, err, ok) = run(&["stats", "/tmp/emstar_cli_stats.star"]);
    assert!(ok, "stderr: {}", err);
    assert!(out.contains("Blocks: 3"));
    assert!(out.contains("simple"));
    assert!(out.contains("loop"));
    assert!(out.contains("Loop rows: 2"));
    assert!(out.contains("Simple entries: 5"));
}

#[test]
fn cli_validate_ok() {
    write_star("/tmp/emstar_cli_v_ok.star", EXAMPLE_STAR);
    let (out, err, ok) = run(&["validate", "/tmp/emstar_cli_v_ok.star"]);
    assert!(ok, "stderr: {}", err);
    assert!(out.contains("is valid"));
    assert!(out.contains("3 data blocks"));
}

#[test]
fn cli_validate_missing() {
    let (_out, err, ok) = run(&["validate", "/tmp/emstar_cli_nonexistent.star"]);
    assert!(!ok);
    assert!(err.contains("not found"));
}

// ── Library API ─────────────────────────────────────────────────────────

#[test]
fn lib_read_file() {
    write_star("/tmp/emstar_lib_read.star", EXAMPLE_STAR);
    let sf = emstar::read_file("/tmp/emstar_lib_read.star".as_ref()).unwrap();
    assert_eq!(sf.blocks.len(), 3);
    assert!(sf.get("general").is_some());
    assert!(sf.get("particles").is_some());
    assert!(sf.get("optimisation").is_some());
}

#[test]
fn lib_write_read_roundtrip() {
    write_star("/tmp/emstar_lib_rt_in.star", EXAMPLE_STAR);
    let sf = emstar::read_file("/tmp/emstar_lib_rt_in.star".as_ref()).unwrap();
    emstar::write_file(&sf, "/tmp/emstar_lib_rt_out.star".as_ref()).unwrap();
    let sf2 = emstar::read_file("/tmp/emstar_lib_rt_out.star".as_ref()).unwrap();
    assert_eq!(sf.blocks.len(), sf2.blocks.len());

    // Check a simple value survived roundtrip
    if let Some(emstar::DataBlock::Simple(s)) = sf2.get("general") {
        assert_eq!(s.get("rlnImageSize"), Some(&emstar::DataValue::Integer(256)));
    } else {
        panic!("expected SimpleBlock");
    }
}

#[test]
fn lib_to_string_contains_data() {
    use emstar::{DataBlock, SimpleBlock, StarFile};

    let sf = StarFile {
        blocks: vec![
            ("block_a".into(), DataBlock::Simple(SimpleBlock {
                entries: vec![("key".into(), emstar::DataValue::Integer(42))],
            })),
        ],
    };
    let s = emstar::to_string(&sf);
    assert!(s.contains("block_a"));
    assert!(s.contains("42"));
}

#[test]
fn lib_stats() {
    write_star("/tmp/emstar_lib_stats.star", EXAMPLE_STAR);
    let sf = emstar::read_file("/tmp/emstar_lib_stats.star".as_ref()).unwrap();
    let s = sf.stats();
    assert_eq!(s.n_blocks, 3);
    assert_eq!(s.n_simple, 2);
    assert_eq!(s.n_loop, 1);
    assert_eq!(s.total_loop_rows, 2);
    assert_eq!(s.total_simple_entries, 5);
}

#[test]
fn lib_loop_block_access() {
    use emstar::{DataValue, LoopBlock};

    let lb = LoopBlock {
        col_names: vec!["x".into(), "y".into()],
        col_data: vec![
            vec![DataValue::Float(1.0), DataValue::Float(2.0)],
            vec![DataValue::Float(3.0), DataValue::Float(4.0)],
        ],
    };
    assert_eq!(lb.row_count(), 2);
    assert_eq!(lb.column_count(), 2);
    assert_eq!(lb.get(0, 0), Some(&DataValue::Float(1.0)));
    assert_eq!(lb.get(1, 1), Some(&DataValue::Float(4.0)));
}

#[test]
fn lib_simple_block_get() {
    use emstar::{DataValue, SimpleBlock};

    let b = SimpleBlock {
        entries: vec![
            ("name".into(), DataValue::String("hello".into())),
        ],
    };
    assert_eq!(b.get("name"), Some(&DataValue::String("hello".into())));
    assert_eq!(b.get("nonexistent"), None);
}

#[test]
fn lib_string_quoting() {
    let mut sf = emstar::StarFile::new();
    sf.blocks.push(("t".into(), emstar::DataBlock::Simple(emstar::SimpleBlock {
        entries: vec![
            ("s".into(), emstar::DataValue::String("hello world".into())),
            ("n".into(), emstar::DataValue::Null),
        ],
    })));
    let s = emstar::to_string(&sf);
    assert!(s.contains("\"hello world\""));
    assert!(s.contains("<NA>"));
}

#[test]
fn read_real_data_files() {
    let files = vec![
        ("basic_double_quote.star", 1),
        ("basic_single_quote.star", 1),
        ("default_pipeline.star", 5),
        ("empty_loop.star", 1),
        ("loop_double_quote.star", 1),
        ("loop_single_quote.star", 1),
        ("one_loop.star", 1),
        ("postprocess.star", 3),
        ("rln3.1_data_style.star", 3),
        ("single_line_end_of_multiblock.star", 2),
        ("single_line_middle_of_multiblock.star", 2),
        ("two_basic_blocks.star", 2),
        ("two_single_line_loop_blocks.star", 2),
        ("relion_tutorial/run_it025_optimiser_2D.star", 1),
        ("relion_tutorial/run_it025_optimiser_3D.star", 1),
        ("relion_tutorial/run_it025_sampling_2D.star", 1),
        ("relion_tutorial/run_it025_sampling_3D.star", 2),
    ];
    for (name, n_blocks) in &files {
        let path = format!("tests/data/{}", name);
        let sf = emstar::read_file(path.as_ref()).unwrap_or_else(|_| panic!("failed to read {}", name));
        assert_eq!(sf.blocks.len(), *n_blocks, "block count mismatch for {}", name);

        // Roundtrip: write to string and re-parse
        let out = emstar::to_string(&sf);
        let sf2 = emstar::parse_reader(out.as_bytes())
            .unwrap_or_else(|_| panic!("failed to re-parse {}", name));
        assert_eq!(sf.blocks.len(), sf2.blocks.len(), "roundtrip block count mismatch for {}", name);
    }
}

#[cfg(feature = "polars")]
#[test]
fn lib_polars_conversion() {
    use emstar::{DataValue, LoopBlock};

    // LoopBlock → DataFrame
    let lb = LoopBlock {
        col_names: vec!["x".into(), "y".into()],
        col_data: vec![
            vec![DataValue::Float(1.0), DataValue::Float(2.0)],
            vec![DataValue::Float(3.0), DataValue::Float(4.0)],
        ],
    };
    let df: polars::prelude::DataFrame = lb.clone().into();
    assert_eq!(df.width(), 2);
    assert_eq!(df.height(), 2);

    // DataFrame → LoopBlock
    let lb2: LoopBlock = df.into();
    assert_eq!(lb2.col_names, lb.col_names);
    assert_eq!(lb2.col_data, lb.col_data);
}

#[test]
fn cli_on_real_data() {
    let files = vec![
        ("basic_double_quote.star", 1),
        ("basic_single_quote.star", 1),
        ("default_pipeline.star", 5),
        ("empty_loop.star", 1),
        ("loop_double_quote.star", 1),
        ("loop_single_quote.star", 1),
        ("one_loop.star", 1),
        ("postprocess.star", 3),
        ("rln3.1_data_style.star", 3),
        ("single_line_end_of_multiblock.star", 2),
        ("single_line_middle_of_multiblock.star", 2),
        ("two_basic_blocks.star", 2),
        ("two_single_line_loop_blocks.star", 2),
        ("relion_tutorial/run_it025_optimiser_2D.star", 1),
        ("relion_tutorial/run_it025_optimiser_3D.star", 1),
        ("relion_tutorial/run_it025_sampling_2D.star", 1),
        ("relion_tutorial/run_it025_sampling_3D.star", 2),
    ];
    for (name, n_blocks) in &files {
        let path = format!("tests/data/{}", name);

        // emstar read
        let (out, err, ok) = run(&["read", &path]);
        assert!(ok, "read failed on {}: stderr={}", name, err);
        assert!(out.contains("File:"), "read missing file header for {}", name);
        if *n_blocks > 0 {
            assert!(out.contains("[Simple]") || out.contains("[Loop]"),
                "read missing block info for {}: {:?}", name, out);
        }

        // emstar stats
        let (out, err, ok) = run(&["stats", &path]);
        assert!(ok, "stats failed on {}: stderr={}", name, err);
        assert!(out.contains(&format!("Blocks: {}", n_blocks)),
            "stats block count mismatch for {}: got {:?}", name, out);

        // emstar validate
        let (out, err, ok) = run(&["validate", &path]);
        assert!(ok, "validate failed on {}: stderr={}", name, err);
        assert!(out.contains("is valid"), "validate failed for {}: {:?}", name, out);
        assert!(out.contains(&format!("{} data blocks", n_blocks)),
            "validate block count mismatch for {}: got {:?}", name, out);
    }
}
