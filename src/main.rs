#[cfg(feature = "cli")]
mod cli {
    use clap::Parser;
    use emstar::DataBlock;
    use emstar::DataValue;
    use emstar::read_file;
    use emstar::write_file;
    use std::path::{Path, PathBuf};
    use std::process;

    #[derive(Parser)]
    #[command(name = "emstar", version, about = "STAR file tool for cryo-EM")]
    enum Cli {
        /// Show file structure
        Read { file: PathBuf },
        /// Show file statistics
        Stats { file: PathBuf },
        /// Validate file structure
        Validate { file: PathBuf },
        /// Center particles on a position (Angstrom, converted to pixels via --angpix)
        Center {
            file: PathBuf,
            /// X coordinate in Angstrom
            #[arg(long)]
            x: f64,
            /// Y coordinate in Angstrom
            #[arg(long)]
            y: f64,
            /// Z coordinate in Angstrom (optional)
            #[arg(long)]
            z: Option<f64>,
            /// Pixel size in Angstrom
            #[arg(long, default_value = "1.0")]
            angpix: f64,
            /// Output file
            #[arg(short, long, default_value = "out.star")]
            output: PathBuf,
        },
        /// Remove a column from all loop blocks
        RemoveCol {
            file: PathBuf,
            /// Column name to remove (e.g. rlnSomeLabel)
            #[arg(long)]
            column: String,
            /// Output file
            #[arg(short, long, default_value = "out.star")]
            output: PathBuf,
        },
    }

    pub fn run() {
        match Cli::parse() {
            Cli::Read { file } => cmd_read(file.as_path()),
            Cli::Stats { file } => cmd_stats(file.as_path()),
            Cli::Validate { file } => cmd_validate(file.as_path()),
            Cli::Center { file, x, y, z, angpix, output } => cmd_center(file.as_path(), x, y, z, angpix, output.as_path()),
            Cli::RemoveCol { file, column, output } => cmd_remove_col(file.as_path(), &column, output.as_path()),
        }
    }

    fn cmd_read(path: &Path) {
        match read_file(path) {
            Ok(sf) => {
                println!("File: {}", path.display());
                for (name, block) in &sf.blocks {
                    match block {
                        DataBlock::Simple(s) => println!("  [Simple] {} ({} entries)", name, s.len()),
                        DataBlock::Loop(l) => println!("  [Loop]   {} ({} rows × {} cols)", name, l.row_count(), l.column_count()),
                    }
                }
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    fn cmd_stats(path: &Path) {
        match read_file(path) {
            Ok(sf) => {
                let s = sf.stats();
                println!("File: {}", path.display());
                println!("  Blocks: {} total ({} simple, {} loop)", s.n_blocks, s.n_simple, s.n_loop);
                println!("  Loop rows: {}", s.total_loop_rows);
                println!("  Simple entries: {}", s.total_simple_entries);
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    fn cmd_validate(path: &Path) {
        match read_file(path) {
            Ok(sf) => {
                println!("✓ {} is valid", path.display());
                println!("  {} data blocks found", sf.blocks.len());
                for (name, block) in &sf.blocks {
                    match block {
                        DataBlock::Simple(s) => println!("  - [Simple] {}: {} entries", name, s.len()),
                        DataBlock::Loop(l) => println!("  - [Loop]   {}: {} rows × {} cols", name, l.row_count(), l.column_count()),
                    }
                }
            }
            Err(e) => {
                eprintln!("✗ {} is INVALID", path.display());
                eprintln!("  {}", e);
                process::exit(1);
            }
        }
    }

    fn cmd_center(path: &Path, cx: f64, cy: f64, cz: Option<f64>, angpix: f64, out: &Path) {
        let mut sf = match read_file(path) {
            Ok(sf) => sf,
            Err(e) => { eprintln!("Error: {}", e); process::exit(1); }
        };

        let cx_pix = cx / angpix;
        let cy_pix = cy / angpix;
        let cz_pix = cz.map(|z| z / angpix);

        let lb = match sf.blocks.iter_mut().find(|(n, _)| n == "particles") {
            Some((_, DataBlock::Loop(lb))) => lb,
            _ => {
                eprintln!("Error: no block named 'particles' found");
                process::exit(1);
            }
        };

        let xi = lb.col_names.iter().position(|c| c == "rlnCoordinateX")
            .unwrap_or_else(|| { eprintln!("Error: rlnCoordinateX not found in 'particles'"); process::exit(1); });
        let yi = lb.col_names.iter().position(|c| c == "rlnCoordinateY")
            .unwrap_or_else(|| { eprintln!("Error: rlnCoordinateY not found in 'particles'"); process::exit(1); });

        let n = lb.row_count();
        for row in 0..n {
            if let Some(DataValue::Float(v)) = lb.col_data[xi].get_mut(row) {
                *v -= cx_pix;
            }
            if let Some(DataValue::Float(v)) = lb.col_data[yi].get_mut(row) {
                *v -= cy_pix;
            }
        }

        if let Some(cz) = cz_pix {
            if let Some(zi) = lb.col_names.iter().position(|c| c == "rlnCoordinateZ") {
                for row in 0..n {
                    if let Some(DataValue::Float(v)) = lb.col_data[zi].get_mut(row) {
                        *v -= cz;
                    }
                }
            }
        }

        println!("  Centered 'particles' at ({:.3}, {:.3}{}) pixels", cx_pix, cy_pix,
            cz_pix.map(|z| format!(", {:.3}", z)).unwrap_or_default());

        if let Err(e) = write_file(&sf, out) {
            eprintln!("Error writing: {}", e);
            process::exit(1);
        }
        println!("  Wrote {}", out.display());
    }

    fn cmd_remove_col(path: &Path, col_name: &str, out: &Path) {
        let mut sf = match read_file(path) {
            Ok(sf) => sf,
            Err(e) => { eprintln!("Error: {}", e); process::exit(1); }
        };

        let lb = match sf.blocks.iter_mut().find(|(n, _)| n == "particles") {
            Some((_, DataBlock::Loop(lb))) => lb,
            _ => {
                eprintln!("Error: no block named 'particles' found");
                process::exit(1);
            }
        };

        let idx = lb.col_names.iter().position(|c| c == col_name)
            .unwrap_or_else(|| { eprintln!("Error: column '{}' not found in 'particles'", col_name); process::exit(1); });

        lb.col_names.remove(idx);
        lb.col_data.remove(idx);
        println!("  Removed '{}' from 'particles'", col_name);

        if let Err(e) = write_file(&sf, out) {
            eprintln!("Error writing: {}", e);
            process::exit(1);
        }
        println!("  Wrote {}", out.display());
    }
}

#[cfg(feature = "cli")]
fn main() {
    cli::run();
}

#[cfg(not(feature = "cli"))]
fn main() {
    eprintln!("emstar: built as library only. Enable 'cli' feature for the CLI.");
}
