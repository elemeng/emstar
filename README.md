# emstar

Read, write, and inspect [STAR files](https://www.iucr.org/__data/assets/file/0013/11416/star.5.html) used in cryo-EM/ET (RELION ect).

**I/O only** — turns bytes into structs and back.
Data manipulation is left to your code (or Polars, pandas, NumPy, etc.).

We provide a **Rust library** (zero dependencies) and a **user-friendly Python binding** via PyO3.
Both share the same core: fast, pure-Rust STAR file parsing, no heavy dependencies.

## Python (via PyO3)

```bash
pip install emstar
```

```python
import emstar

# Read — auto-returns DataFrames if polars or pandas is installed
data = emstar.read("particles.star")
df = data["particles"]          # polars.DataFrame | pandas.DataFrame | dict of lists

# List blocks
emstar.block_names("particles.star")  # ["general", "particles"]

# Write — accepts both dicts and DataFrames
emstar.write(data, "out.star")
emstar.write({"x": [1.0, 2.0]}, "out.star")

# Inspect
emstar.stats("particles.star")

# Validate
emstar.validate("particles.star")
```

### Build from source

```bash
pip install maturin
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --features python
uv pip install target/wheels/emstar-*.whl
python tests/test_python.py
```

## Rust library (zero dependencies)

```toml
[dependencies]
emstar = "0.2"
```

```rust
use emstar::{read_file, write_file, DataBlock, DataValue};

let sf = read_file("particles.star".as_ref())?;

// Inspect
for (name, block) in &sf.blocks {
    match block {
        DataBlock::Simple(s) => println!("  [{}] {} entries", name, s.len()),
        DataBlock::Loop(l)   => println!("  [{}] {} rows × {} cols", name, l.row_count(), l.column_count()),
    }
}

// Modify — all fields are pub
if let Some((_, DataBlock::Loop(p))) = sf.blocks.iter_mut().find(|(n, _)| n == "particles") {
    for v in p.col_data[0].iter_mut() {
        if let DataValue::Float(x) = v { *x += 10.0; }
    }
}

write_file(&sf, "output.star".as_ref())?;
# Ok::<(), emstar::StarError>(())
```

### Polars (optional)

```toml
[dependencies]
emstar = { version = "0.2", features = ["polars"] }
polars = "0.45"
```

```rust,ignore
use emstar::{read_file, DataBlock};

let sf = read_file("particles.star".as_ref())?;
if let Some(DataBlock::Loop(p)) = sf.get("particles") {
    let df: polars::prelude::DataFrame = p.clone().into();
    // Polars: filter, group, join ...
    let result: emstar::LoopBlock = df.filter(...).into();
}
```

### Statistics

```rust,no_run
use emstar::read_file;
let sf = read_file("particles.star".as_ref())?;
let s = sf.stats();
println!("{} blocks, {} loop rows", s.n_blocks, s.total_loop_rows);
```

## CLI

```bash
cargo install emstar --features cli

emstar read file.star      # list blocks
emstar stats file.star     # block counts
emstar validate file.star  # validate format
```

## API (Rust)

| Type | Description |
|------|-------------|
| `StarFile { blocks }` | `pub Vec<(String, DataBlock)>` |
| `DataBlock::Simple(SimpleBlock)` | Key-value metadata |
| `DataBlock::Loop(LoopBlock)` | Tabular data |
| `SimpleBlock { entries }` | `pub Vec<(String, DataValue)>` |
| `LoopBlock { col_names, col_data }` | `pub Vec<String>` + `pub Vec<Vec<DataValue>>` |
| `DataValue` | `String / Integer / Float / Bool / Null` |

| Function | Description |
|----------|-------------|
| `read_file()` | Read from path |
| `parse_reader()` | Read from any `BufRead` |
| `write_file()` | Write to path |
| `to_string()` | Render as STAR string |
| `sf.stats()` | → `StarStats` |

## Testing

51 tests, roundtrip on 17 real RELION STAR files:

```
tests/data/
├── basic_double_quote.star
├── default_pipeline.star       (5 blocks)
├── one_loop.star
├── postprocess.star            (3 blocks)
├── relion_tutorial/
│   ├── run_it025_optimiser_2D.star
│   ├── run_it025_optimiser_3D.star
│   ├── run_it025_sampling_2D.star
│   └── run_it025_sampling_3D.star (2 blocks)
└── ... (17 files total)
```

## Dependency matrix

| Use case | `Cargo.toml` | Deps |
|----------|-------------|------|
| Library | `emstar = "0.2"` | **zero** |
| + Polars | `features = ["polars"]` | `polars` |
| CLI | `cargo install --features cli` | `clap` |
| Python | `maturin build --features python` | `pyo3` |

## Design

- **Zero dependency** by default — pure std
- **No mutation API** — construct data via public fields
- **Buffered writer** — `to_string()` builds a string, fine for <1 GB files
- **Optional Polars** — `LoopBlock ↔ DataFrame` conversion
- **Pure std parsing** — no `lexical`/`ryu`/`itoa`

## License

MIT
