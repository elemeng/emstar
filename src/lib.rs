//! # emstar
//!
//! Read, write, and inspect [STAR files](https://en.wikipedia.org/wiki/Self-defining_Text_Archive_and_Retrieval)
//! used in cryo-EM (RELION).
//!
//! emstar is **I/O only** — it turns bytes into Rust structs and back.
//! Data manipulation is done on the structs directly (or via Polars).
//!
//! ## Data model
//!
//! A STAR file has named blocks, each being either key-value or tabular:
//!
//! ```text
//! emstar::StarFile {
//!     blocks: Vec<(name, emstar::DataBlock)>,
//! }                    ├── DataBlock::Simple(SimpleBlock { entries: Vec<(key, DataValue)> })
//!                      └── DataBlock::Loop(LoopBlock { col_names, col_data })
//! ```
//!
//! All fields are `pub` — construct or destructure directly.
//!
//! ## STAR format examples
//!
//! A file with a **simple block** (key-value metadata):
//!
//! ```text
//! data_general
//! _rlnImageSize 256
//! _rlnPixelSize 1.06
//! _rlnVoltage 300
//! ```
//!
//! A file with a **loop block** (tabular particle data):
//!
//! ```text
//! data_particles
//!
//! loop_
//! _rlnCoordinateX #1
//! _rlnCoordinateY #2
//! _rlnAngleRot #3
//! 91.7987  83.6226  -51.74
//! 97.6358  80.4370  141.50
//! ```
//!
//! A file with **multiple blocks** (metadata + data):
//!
//! ```text
//! data_general
//! _rlnImageSize 256
//!
//! data_particles
//! loop_
//! _rlnCoordinateX #1
//! _rlnCoordinateY #2
//! 91.8  83.6
//! 97.6  80.4
//!
//! data_optimisation
//! _rlnIteration 25
//! ```
//!
//! All of these can be read with [`read_file`] and written with [`write_file`].
//!
//! ## Quick start — build → write
//!
//! ```rust
//! use emstar::{StarFile, SimpleBlock, LoopBlock, DataBlock, DataValue, write_file};
//!
//! let mut sf = StarFile::new();
//!
//! // Simple block (key-value)
//! sf.blocks.push(("general".into(), DataBlock::Simple(SimpleBlock {
//!     entries: vec![
//!         ("rlnImageSize".into(), DataValue::Integer(256)),
//!     ],
//! })));
//!
//! // Loop block (tabular)
//! sf.blocks.push(("particles".into(), DataBlock::Loop(LoopBlock {
//!     col_names: vec!["x".into(), "y".into()],
//!     col_data: vec![
//!         vec![DataValue::Float(1.0), DataValue::Float(2.0)],
//!         vec![DataValue::Float(3.0), DataValue::Float(4.0)],
//!     ],
//! })));
//!
//! write_file(&sf, "output.star".as_ref())?;
//! # Ok::<(), emstar::StarError>(())
//! ```
//!
//! ## Read → modify → write
//!
//! ```rust,no_run
//! use emstar::{read_file, write_file, DataBlock, DataValue};
//!
//! let mut sf = read_file("particles.star".as_ref())?;
//!
//! // Modify: shift X coordinates by 10
//! if let Some((_, DataBlock::Loop(p))) = sf.blocks.iter_mut().find(|(n, _)| n == "particles") {
//!     for v in p.col_data[0].iter_mut() {
//!         if let DataValue::Float(x) = v { *x += 10.0; }
//!     }
//! }
//!
//! write_file(&sf, "shifted.star".as_ref())?;
//! # Ok::<(), emstar::StarError>(())
//! ```
//!
//! ## Statistics
//!
//! ```rust,no_run
//! use emstar::read_file;
//!
//! let sf = read_file("particles.star".as_ref())?;
//! let s = sf.stats();
//! println!("{} blocks ({} simple, {} loop)", s.n_blocks, s.n_simple, s.n_loop);
//! println!("{} loop rows, {} simple entries", s.total_loop_rows, s.total_simple_entries);
//! # Ok::<(), emstar::StarError>(())
//! ```
//!
//! ## Polars integration (optional)
//!
//! ```toml
//! [dependencies]
//! emstar = { version = "0.2", features = ["polars"] }
//! polars = "0.45"
//! ```
//!
//! ```rust,no_run
//! # #[cfg(feature = "polars")] {
//! use emstar::{read_file, DataBlock};
//!
//! let sf = read_file("particles.star".as_ref())?;
//! if let Some(DataBlock::Loop(p)) = sf.blocks.iter().find(|(n, _)| n == "particles").map(|(_, b)| b) {
//!     let df: polars::prelude::DataFrame = p.clone().into();
//!     println!("{} × {}", df.width(), df.height());
//! }
//! # }
//! # Ok::<(), emstar::StarError>(())
//! ```
//!
//! ## CLI
//!
//! ```bash
//! emstar read file.star      # list blocks
//! emstar stats file.star     # block counts
//! emstar validate file.star  # check format
//! cargo install emstar --features cli
//! ```

pub mod error;
pub mod star;

pub use error::{Result, StarError};
pub use star::{
    BlockStats, DataBlock, DataValue, LoopBlock, SimpleBlock, StarFile, StarStats, parse_reader,
    read_file, to_string, write_file,
};
