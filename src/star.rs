//! Core STAR file types, parser, and writer.
//!
//! A STAR file is a simple text format used in cryo-EM (RELION):
//! ```text
//! data_general
//! _rlnImageSize 256
//!
//! data_particles
//! loop_
//! _rlnCoordinateX #1
//! _rlnCoordinateY #2
//! 91.8  83.6
//! ```
use crate::error::{Result, StarError};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

// ── Types ───────────────────────────────────────────────────────────────────

/// A value in a STAR file. Parsed from text automatically.
///
/// | Text | Variant |
/// |------|---------|
/// | `42` | `Integer(42)` |
/// | `3.14` | `Float(3.14)` |
/// | `true` / `yes` | `Bool(true)` |
/// | `<NA>` / `nan` | `Null` |
/// | anything else | `String(…)` |
#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Null,
}

/// A key-value metadata block. Entries are insertion-ordered.
///
/// ```
/// use emstar::SimpleBlock;
/// let b = SimpleBlock { entries: vec![] };
/// assert!(b.is_empty());
/// ```
#[derive(Debug, Clone)]
pub struct SimpleBlock {
    /// Key-value pairs in insertion order.
    pub entries: Vec<(String, DataValue)>,
}

impl Default for SimpleBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleBlock {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Look up a value by key.
    pub fn get(&self, key: &str) -> Option<&DataValue> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate `(key, value)` in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &DataValue)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }
}

/// A tabular data block (`loop_`). Columns and data are public fields.
///
/// ```
/// use emstar::{LoopBlock, DataValue};
/// let lb = LoopBlock {
///     col_names: vec!["x".into()],
///     col_data: vec![vec![DataValue::Float(1.0)]],
/// };
/// assert_eq!(lb.row_count(), 1);
/// ```
#[derive(Debug, Clone)]
pub struct LoopBlock {
    pub col_names: Vec<String>,
    pub col_data: Vec<Vec<DataValue>>,
}

impl Default for LoopBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl LoopBlock {
    pub fn new() -> Self {
        Self {
            col_names: Vec::new(),
            col_data: Vec::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.col_data.first().map_or(0, |c| c.len())
    }

    pub fn column_count(&self) -> usize {
        self.col_names.len()
    }

    /// Get a cell `(row, col)`. Returns `None` if out of bounds.
    pub fn get(&self, row: usize, col: usize) -> Option<&DataValue> {
        self.col_data.get(col).and_then(|c| c.get(row))
    }
}

/// Convert a Polars [`DataFrame`] into a [`LoopBlock`].
#[cfg(feature = "polars")]
impl From<polars::prelude::DataFrame> for LoopBlock {
    fn from(df: polars::prelude::DataFrame) -> Self {
        use polars::prelude::*;
        let col_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        let mut col_data: Vec<Vec<DataValue>> = Vec::with_capacity(col_names.len());
        for col in df.get_columns() {
            let vals: Vec<DataValue> = match col.dtype() {
                DataType::Float64 => {
                    col.f64().map(|ca| ca.into_iter()
                        .map(|opt| opt.map(DataValue::Float).unwrap_or(DataValue::Null)).collect()
                    ).unwrap_or_default()
                }
                DataType::Int64 => {
                    col.i64().map(|ca| ca.into_iter()
                        .map(|opt| opt.map(DataValue::Integer).unwrap_or(DataValue::Null)).collect()
                    ).unwrap_or_default()
                }
                DataType::String => {
                    col.str().map(|ca| ca.into_iter()
                        .map(|opt| opt.map(|s| DataValue::String(s.to_string())).unwrap_or(DataValue::Null)).collect()
                    ).unwrap_or_default()
                }
                DataType::Boolean => {
                    col.bool().map(|ca| ca.into_iter()
                        .map(|opt| opt.map(DataValue::Bool).unwrap_or(DataValue::Null)).collect()
                    ).unwrap_or_default()
                }
                _ => {
                    (0..col.len()).map(|i| col.get(i).ok().map_or(DataValue::Null, |av| {
                        if av.is_null() { DataValue::Null } else { DataValue::String(av.to_string()) }
                    })).collect()
                }
            };
            col_data.push(vals);
        }
        LoopBlock { col_names, col_data }
    }
}

/// Convert a [`LoopBlock`] into a Polars [`DataFrame`].
#[cfg(feature = "polars")]
impl From<LoopBlock> for polars::prelude::DataFrame {
    fn from(lb: LoopBlock) -> Self {
        use polars::prelude::*;
        let mut series_vec: Vec<Series> = Vec::with_capacity(lb.column_count());
        for (i, col_name) in lb.col_names.iter().enumerate() {
            let values = &lb.col_data[i];
            let has_float = values.iter().any(|v| matches!(v, DataValue::Float(_)));
            let has_int = values.iter().any(|v| matches!(v, DataValue::Integer(_)));
            let has_string = values.iter().any(|v| matches!(v, DataValue::String(_)));
            let has_bool = values.iter().any(|v| matches!(v, DataValue::Bool(_)));

            let series: Series = if has_string || (!has_float && !has_int && !has_bool) {
                let v: Vec<Option<String>> = values.iter().map(|dv| match dv {
                    DataValue::String(s) => Some(s.clone()),
                    DataValue::Null => None,
                    _ => Some(fmt_value(dv)),
                }).collect();
                Series::new(col_name.into(), v)
            } else if has_float {
                let v: Vec<Option<f64>> = values.iter().map(|dv| match dv {
                    DataValue::Float(f) => Some(*f),
                    DataValue::Integer(i) => Some(*i as f64),
                    DataValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
                    DataValue::Null => None,
                    DataValue::String(s) => s.parse::<f64>().ok(),
                }).collect();
                Series::new(col_name.into(), v)
            } else if has_int {
                let v: Vec<Option<i64>> = values.iter().map(|dv| match dv {
                    DataValue::Integer(i) => Some(*i),
                    DataValue::Bool(b) => Some(if *b { 1 } else { 0 }),
                    DataValue::Null => None,
                    _ => None,
                }).collect();
                Series::new(col_name.into(), v)
            } else {
                let v: Vec<Option<bool>> = values.iter().map(|dv| match dv {
                    DataValue::Bool(b) => Some(*b),
                    DataValue::Null => None,
                    _ => None,
                }).collect();
                Series::new(col_name.into(), v)
            };
            series_vec.push(series);
        }
        let columns: Vec<Column> = series_vec.into_iter().map(|s| s.into()).collect();
        DataFrame::new(columns).expect("Polars creation should not fail")
    }
}

/// A data block: `Simple` (key-value) or `Loop` (table).
#[derive(Debug, Clone)]
pub enum DataBlock {
    Simple(SimpleBlock),
    Loop(LoopBlock),
}

/// In-memory STAR file. [`blocks`](StarFile::blocks) is public.
///
/// ```
/// use emstar::StarFile;
/// let sf = StarFile::new();
/// assert!(sf.blocks.is_empty());
/// ```
#[derive(Debug, Clone)]
pub struct StarFile {
    /// All blocks in file order: `(name, block)`.
    pub blocks: Vec<(String, DataBlock)>,
}

impl Default for StarFile {
    fn default() -> Self {
        Self::new()
    }
}

impl StarFile {
    pub fn new() -> Self {
        Self { blocks: Vec::new() }
    }

    /// Find a block by name. Returns `None` if not found.
    pub fn get(&self, name: &str) -> Option<&DataBlock> {
        self.blocks.iter().find(|(n, _)| n == name).map(|(_, b)| b)
    }
}

// ── Statistics ──────────────────────────────────────────────────────────────

/// Per-block stats.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BlockStats {
    Simple { entries: usize },
    Loop { rows: usize, cols: usize },
}

/// File-level statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct StarStats {
    pub n_blocks: usize,
    pub n_simple: usize,
    pub n_loop: usize,
    pub total_loop_rows: usize,
    pub total_simple_entries: usize,
    pub block_stats: Vec<(String, BlockStats)>,
}

impl StarFile {
    /// Compute statistics for this file.
    pub fn stats(&self) -> StarStats {
        let mut n_simple = 0;
        let mut n_loop = 0;
        let mut total_loop_rows = 0;
        let mut total_simple_entries = 0;
        let mut block_stats = Vec::new();
        for (name, block) in &self.blocks {
            match block {
                DataBlock::Simple(s) => {
                    n_simple += 1;
                    total_simple_entries += s.len();
                    block_stats.push((name.clone(), BlockStats::Simple { entries: s.len() }));
                }
                DataBlock::Loop(l) => {
                    n_loop += 1;
                    total_loop_rows += l.row_count();
                    block_stats.push((
                        name.clone(),
                        BlockStats::Loop {
                            rows: l.row_count(),
                            cols: l.column_count(),
                        },
                    ));
                }
            }
        }
        StarStats {
            n_blocks: self.blocks.len(),
            n_simple,
            n_loop,
            total_loop_rows,
            total_simple_entries,
            block_stats,
        }
    }
}

// ── Parser ──────────────────────────────────────────────────────────────────

/// Read a STAR file from disk.
pub fn read_file(path: &Path) -> Result<StarFile> {
    if !path.exists() {
        return Err(StarError::FileNotFound(path.to_path_buf()));
    }
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    parse_reader(reader)
}

/// Parse STAR data from any `BufRead` source.
pub fn parse_reader<R: BufRead>(reader: R) -> Result<StarFile> {
    let mut lines = reader.lines().enumerate().peekable();
    let mut blocks = Vec::new();

    while let Some((line_num, line)) = lines.next() {
        let line = line.map_err(|e| StarError::Parse {
            line: line_num + 1,
            message: e.to_string(),
        })?;
        let trimmed = line.trim().to_string();

        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if let Some(name) = trimmed.strip_prefix("data_") {
            let name = name.trim().to_string();
            let block = parse_block_content(&mut lines)?;
            blocks.push((name, block));
        }
    }

    Ok(StarFile { blocks })
}

/// Parse content between a `data_<name>` and the next `data_` (or EOF).
fn parse_block_content<I: Iterator<Item = (usize, std::result::Result<String, std::io::Error>)>>(
    lines: &mut std::iter::Peekable<I>,
) -> Result<DataBlock> {
    let mut content_lines: Vec<(usize, String)> = Vec::new();

    loop {
        let is_next_data = match lines.peek() {
            Some((_, Ok(l))) => l.trim().starts_with("data_"),
            Some((_, Err(_))) => false,
            None => true,
        };
        if is_next_data {
            break;
        }

        match lines.next() {
            Some((ln, line)) => {
                let l = line.map_err(|e| StarError::Parse {
                    line: ln + 1,
                    message: e.to_string(),
                })?;
                let t = l.trim().to_string();
                if !t.is_empty() && !t.starts_with('#') {
                    content_lines.push((ln, t));
                }
            }
            None => break,
        }
    }

    if content_lines.is_empty() {
        return Ok(DataBlock::Simple(SimpleBlock::new()));
    }

    let loop_pos = content_lines.iter().position(|(_, l)| l == "loop_");

    match loop_pos {
        None => {
            let mut block = SimpleBlock::new();
            for (_ln, l) in &content_lines {
                if l.starts_with('_') {
                    parse_simple_line(l, &mut block)?;
                }
            }
            Ok(DataBlock::Simple(block))
        }
        Some(pos) => {
            let loop_lines: Vec<&str> = content_lines[pos + 1..]
                .iter()
                .map(|(_, l)| l.as_str())
                .collect();
            match parse_loop_from_lines(&loop_lines) {
                Some(lb) => Ok(DataBlock::Loop(lb)),
                None => Ok(DataBlock::Simple(SimpleBlock::new())),
            }
        }
    }
}

fn parse_loop_from_lines(lines: &[&str]) -> Option<LoopBlock> {
    let mut col_names: Vec<String> = Vec::new();
    let mut col_data: Vec<Vec<DataValue>> = Vec::new();
    let mut idx = 0;

    while idx < lines.len() {
        let l = lines[idx].trim();
        if l.starts_with('_') {
            let name = l.split_whitespace().next().unwrap_or(l);
            let name = name[1..].trim();
            col_names.push(name.to_string());
            col_data.push(Vec::new());
            idx += 1;
        } else {
            break;
        }
    }

    if col_names.is_empty() {
        return None;
    }

    let ncols = col_names.len();
    for line in &lines[idx..] {
        let values = parse_star_values(line);
        if values.len() != ncols {
            continue;
        }
        for (i, v) in values.into_iter().enumerate() {
            col_data[i].push(v);
        }
    }

    Some(LoopBlock {
        col_names,
        col_data,
    })
}

/// Split a STAR data line into values, respecting quotes.
fn parse_star_values(line: &str) -> Vec<DataValue> {
    let mut result = Vec::new();
    let mut current = String::with_capacity(64);
    let mut in_quote: Option<char> = None;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match in_quote {
            None => {
                if c == '"' || c == '\'' {
                    in_quote = Some(c);
                } else if c.is_whitespace() {
                    if !current.is_empty() {
                        result.push(parse_value(&current));
                        current.clear();
                    }
                } else {
                    current.push(c);
                }
            }
            Some(q) => {
                if c == q {
                    if chars.peek() == Some(&q) {
                        current.push(c);
                        chars.next();
                    } else {
                        result.push(parse_value(&current));
                        current.clear();
                        in_quote = None;
                    }
                } else {
                    current.push(c);
                }
            }
        }
    }
    if !current.is_empty() {
        result.push(parse_value(&current));
    }
    result
}

/// Parse a `_key value` line.
fn parse_simple_line(line: &str, block: &mut SimpleBlock) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return Ok(());
    }
    let key = parts[0][1..].trim();
    let val_str = parts[1..].join(" ");
    let value = parse_value(&val_str);
    block.entries.push((key.to_string(), value));
    Ok(())
}

/// Parse a single text token into a `DataValue`.
fn parse_value(s: &str) -> DataValue {
    let t = s.trim();
    if t.eq_ignore_ascii_case("<NA>") || t.eq_ignore_ascii_case("nan") {
        return DataValue::Null;
    }
    match t.to_lowercase().as_str() {
        "true" | "yes" => return DataValue::Bool(true),
        "false" | "no" => return DataValue::Bool(false),
        _ => {}
    }
    if let Ok(i) = t.parse::<i64>() {
        return DataValue::Integer(i);
    }
    if let Ok(f) = t.parse::<f64>()
        && !f.is_infinite()
    {
        return DataValue::Float(f);
    }
    DataValue::String(remove_quotes(t).to_string())
}

fn remove_quotes(s: &str) -> &str {
    if s.len() < 2 {
        return s;
    }
    let first = s.as_bytes()[0];
    let last = s.as_bytes()[s.len() - 1];
    if (first == b'"' || first == b'\'') && first == last {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

// ── Writer ──────────────────────────────────────────────────────────────────

/// Write a `StarFile` to a file.
pub fn write_file(file: &StarFile, path: &Path) -> Result<()> {
    let mut out = File::create(path)?;
    out.write_all(to_string(file).as_bytes())?;
    Ok(())
}

/// Render a `StarFile` as a STAR format string.
pub fn to_string(file: &StarFile) -> String {
    if file.blocks.is_empty() {
        return String::new();
    }
    let mut result = String::new();
    result.push_str("# Created by emstar\n\n");

    for (name, block) in &file.blocks {
        match block {
            DataBlock::Simple(s) => write_simple(name, s, &mut result),
            DataBlock::Loop(l) => write_loop(name, l, &mut result),
        }
    }
    result
}

fn write_simple(name: &str, block: &SimpleBlock, out: &mut String) {
    out.push_str(&format!("data_{}\n", name));
    for (key, value) in block.iter() {
        out.push_str(&format!("_{}\t\t\t{}\n", key, fmt_value(value)));
    }
    out.push_str("\n\n");
}

fn write_loop(name: &str, block: &LoopBlock, out: &mut String) {
    out.push_str(&format!("data_{}\n\nloop_\n", name));
    for (i, col) in block.col_names.iter().enumerate() {
        out.push_str(&format!("_{} #{}\n", col, i + 1));
    }
    for row in 0..block.row_count() {
        for col in 0..block.column_count() {
            if col > 0 {
                out.push('\t');
            }
            match block.get(row, col) {
                Some(v) => out.push_str(&fmt_value(v)),
                None => out.push_str("<NA>"),
            }
        }
        out.push('\n');
    }
    out.push_str("\n\n");
}

fn fmt_value(v: &DataValue) -> String {
    match v {
        DataValue::String(s) => {
            if s.contains(' ') || s.is_empty() {
                format!("\"{}\"", s)
            } else {
                s.clone()
            }
        }
        DataValue::Integer(i) => i.to_string(),
        DataValue::Float(f) => format!("{}", f),
        DataValue::Bool(b) => b.to_string(),
        DataValue::Null => "<NA>".to_string(),
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![expect(clippy::approx_constant)]
    use super::*;

    #[test]
    fn test_data_value_integer() {
        assert_eq!(fmt_value(&DataValue::Integer(42)), "42");
    }

    #[test]
    fn test_data_value_float() {
        assert!(fmt_value(&DataValue::Float(3.14)).starts_with("3.14"));
    }

    #[test]
    fn test_data_value_string() {
        assert_eq!(fmt_value(&DataValue::String("hello".into())), "hello");
    }

    #[test]
    fn test_data_value_string_with_spaces() {
        assert_eq!(
            fmt_value(&DataValue::String("hello world".into())),
            "\"hello world\""
        );
    }

    #[test]
    fn test_data_value_bool() {
        assert_eq!(fmt_value(&DataValue::Bool(true)), "true");
        assert_eq!(fmt_value(&DataValue::Bool(false)), "false");
    }

    #[test]
    fn test_data_value_null() {
        assert_eq!(fmt_value(&DataValue::Null), "<NA>");
    }

    #[test]
    fn test_simple_block() {
        let b = SimpleBlock {
            entries: vec![("k".into(), DataValue::Integer(1))],
        };
        assert_eq!(b.get("k"), Some(&DataValue::Integer(1)));
        assert_eq!(b.get("nonexistent"), None);
    }

    #[test]
    fn test_simple_block_iter() {
        let b = SimpleBlock {
            entries: vec![
                ("a".into(), DataValue::Integer(1)),
                ("b".into(), DataValue::Integer(2)),
            ],
        };
        assert_eq!(b.iter().count(), 2);
    }

    #[test]
    fn test_loop_basic() {
        let lb = LoopBlock {
            col_names: vec!["x".into()],
            col_data: vec![vec![DataValue::Float(1.0), DataValue::Float(2.0)]],
        };
        assert_eq!(lb.row_count(), 2);
        assert_eq!(lb.column_count(), 1);
        assert_eq!(lb.get(0, 0), Some(&DataValue::Float(1.0)));
        assert_eq!(lb.get(5, 0), None);
    }

    #[test]
    fn test_starfile_new() {
        let sf = StarFile::new();
        assert!(sf.blocks.is_empty());
    }

    #[test]
    fn test_starfile_get() {
        let sf = StarFile {
            blocks: vec![("a".into(), DataBlock::Simple(SimpleBlock::new()))],
        };
        assert!(sf.get("a").is_some());
        assert!(sf.get("b").is_none());
    }

    #[test]
    fn test_starfile_stats() {
        let sf = StarFile {
            blocks: vec![
                (
                    "s".into(),
                    DataBlock::Simple(SimpleBlock {
                        entries: vec![("k".into(), DataValue::Integer(1))],
                    }),
                ),
                (
                    "l".into(),
                    DataBlock::Loop(LoopBlock {
                        col_names: vec!["x".into()],
                        col_data: vec![vec![DataValue::Float(1.0), DataValue::Float(2.0)]],
                    }),
                ),
            ],
        };
        let s = sf.stats();
        assert_eq!(s.n_blocks, 2);
        assert_eq!(s.n_simple, 1);
        assert_eq!(s.n_loop, 1);
        assert_eq!(s.total_loop_rows, 2);
        assert_eq!(s.total_simple_entries, 1);
    }

    #[test]
    fn test_parse_value_integer() {
        assert_eq!(parse_value("42"), DataValue::Integer(42));
        assert_eq!(parse_value("-5"), DataValue::Integer(-5));
    }

    #[test]
    fn test_parse_value_float() {
        assert!(matches!(parse_value("3.14"), DataValue::Float(_)));
    }

    #[test]
    fn test_parse_value_string() {
        assert_eq!(parse_value("hello"), DataValue::String("hello".into()));
    }

    #[test]
    fn test_parse_value_null() {
        assert_eq!(parse_value("<NA>"), DataValue::Null);
        assert_eq!(parse_value("nan"), DataValue::Null);
    }

    #[test]
    fn test_parse_value_bool() {
        assert_eq!(parse_value("true"), DataValue::Bool(true));
        assert_eq!(parse_value("false"), DataValue::Bool(false));
        assert_eq!(parse_value("yes"), DataValue::Bool(true));
        assert_eq!(parse_value("no"), DataValue::Bool(false));
    }

    #[test]
    fn test_parse_value_empty_string() {
        assert_eq!(parse_value("\"\""), DataValue::String("".into()));
    }

    #[test]
    fn test_parse_star_values_basic() {
        let vals = parse_star_values("1 2 3");
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], DataValue::Integer(1));
    }

    #[test]
    fn test_parse_star_values_quoted() {
        let vals = parse_star_values("hello \"hello world\" 42");
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[1], DataValue::String("hello world".into()));
    }

    #[test]
    fn test_parse_star_values_tabs() {
        assert_eq!(parse_star_values("1\t2\t3").len(), 3);
    }

    #[test]
    fn test_parse_simple() {
        let sf = parse_reader(b"data_test\n_k1 hello\n_k2 42\n" as &[u8]).unwrap();
        if let DataBlock::Simple(s) = &sf.blocks[0].1 {
            assert_eq!(s.get("k1"), Some(&DataValue::String("hello".into())));
        } else {
            panic!("expected SimpleBlock");
        }
    }

    #[test]
    fn test_parse_loop() {
        let sf = parse_reader(b"data_p\n\nloop_\n_x #1\n_y #2\n1 2\n3 4\n" as &[u8]).unwrap();
        if let DataBlock::Loop(l) = &sf.blocks[0].1 {
            assert_eq!(l.row_count(), 2);
            assert_eq!(l.column_count(), 2);
        } else {
            panic!("expected LoopBlock");
        }
    }

    #[test]
    fn test_parse_multiple_blocks() {
        let sf = parse_reader(b"data_a\n_k1 v1\n\ndata_b\n_k2 v2\n" as &[u8]).unwrap();
        assert_eq!(sf.blocks.len(), 2);
    }

    #[test]
    fn test_parse_empty() {
        assert_eq!(parse_reader("".as_bytes()).unwrap().blocks.len(), 0);
    }

    #[test]
    fn test_parse_comments() {
        assert_eq!(
            parse_reader(b"# c\ndata_x\n_k 1\n" as &[u8])
                .unwrap()
                .blocks
                .len(),
            1
        );
    }

    #[test]
    fn test_roundtrip_simple() {
        let sf = parse_reader(b"data_t\n_k hello\n" as &[u8]).unwrap();
        let sf2 = parse_reader(to_string(&sf).as_bytes()).unwrap();
        assert_eq!(sf.blocks.len(), sf2.blocks.len());
    }

    #[test]
    fn test_roundtrip_loop() {
        let input = "data_p\n\nloop_\n_x #1\n_y #2\n1 2\n3 4\n";
        let sf = parse_reader(input.as_bytes()).unwrap();
        let sf2 = parse_reader(to_string(&sf).as_bytes()).unwrap();
        assert_eq!(sf.blocks.len(), sf2.blocks.len());
    }

    #[test]
    fn test_empty_file_writes_nothing() {
        assert_eq!(to_string(&StarFile::new()), "");
    }

    #[test]
    fn test_writer_adds_header() {
        let sf = StarFile {
            blocks: vec![("t".into(), DataBlock::Simple(SimpleBlock::new()))],
        };
        assert!(to_string(&sf).contains("# Created by emstar"));
    }
}
