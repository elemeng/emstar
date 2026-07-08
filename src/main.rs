#[cfg(feature = "cli")]
mod cli {
    use clap::Parser;
    use emstar::DataBlock;
    use emstar::read_file;
    use std::path::{Path, PathBuf};
    use std::process;

    #[derive(Parser)]
    #[command(name = "emstar", version, about = "Simple STAR file tool for cryo-EM")]
    enum Cli {
        Read { file: PathBuf },
        Stats { file: PathBuf },
        Validate { file: PathBuf },
    }

    pub fn run() {
        match Cli::parse() {
            Cli::Read { file } => cmd_read(file.as_path()),
            Cli::Stats { file } => cmd_stats(file.as_path()),
            Cli::Validate { file } => cmd_validate(file.as_path()),
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
}

#[cfg(feature = "cli")]
fn main() {
    cli::run();
}

#[cfg(not(feature = "cli"))]
fn main() {
    eprintln!("emstar: built as library only. Enable 'cli' feature for the CLI.");
}
