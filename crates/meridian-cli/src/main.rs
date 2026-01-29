//! Meridian CLI

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "meridian")]
#[command(author = "Katie <blackabee@gmail.com>")]
#[command(version = "0.1.0")]
#[command(about = "A language for data transformation pipelines")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check syntax and types
    Check {
        /// File to check
        file: PathBuf,
    },
    /// Run a pipeline
    Run {
        /// File to run
        file: PathBuf,
        /// Specific pipeline to run
        #[arg(long)]
        pipeline: Option<String>,
    },
    /// Run tests
    Test {
        /// Filter tests by name
        #[arg(long)]
        filter: Option<String>,
    },
    /// Format source code
    Fmt {
        /// File to format
        file: PathBuf,
        /// Check formatting without modifying
        #[arg(long)]
        check: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {}", e);
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Check { file } => {
            let source = std::fs::read_to_string(&file)?;
            match meridian_parser::parse(&source) {
                Ok(program) => {
                    println!("Parsed {} items", program.items.len());
                    Ok(())
                }
                Err(errors) => {
                    for error in errors {
                        eprintln!("{}", error);
                    }
                    Err("parse failed".into())
                }
            }
        }
        Command::Run { file, pipeline: _ } => {
            let source = std::fs::read_to_string(&file)?;
            let _program = meridian_parser::parse(&source).map_err(|e| {
                for err in &e {
                    eprintln!("{}", err);
                }
                "parse failed"
            })?;
            
            println!("Running...");
            // TODO: Implement full pipeline
            let executor = meridian_runtime::Executor::new()?;
            let stats = executor.execute("SELECT 1")?;
            println!("Done. {:?}", stats);
            Ok(())
        }
        Command::Test { filter: _ } => {
            println!("Running tests...");
            // TODO: Implement test runner
            println!("No tests found.");
            Ok(())
        }
        Command::Fmt { file, check } => {
            if check {
                println!("Checking {}", file.display());
            } else {
                println!("Formatting {}", file.display());
            }
            // TODO: Implement formatter
            Ok(())
        }
    }
}
