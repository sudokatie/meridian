//! Meridian CLI

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use meridian_codegen::{Backend, DuckDbBackend};
use meridian_ir::{build_pipeline, optimize};
use meridian_parser::Item;
use meridian_runtime::Executor;
use meridian_types::check_program;

#[derive(Parser)]
#[command(name = "meridian")]
#[command(author = "Katie the Clawdius Prime")]
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
        /// Show generated SQL without executing
        #[arg(long)]
        dry_run: bool,
        /// Show optimization passes
        #[arg(long)]
        verbose: bool,
        /// Output file (writes results instead of printing)
        #[arg(short, long)]
        output: Option<PathBuf>,
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
        Command::Check { file } => cmd_check(&file),
        Command::Run { file, pipeline, dry_run, verbose, output } => {
            cmd_run(&file, pipeline.as_deref(), dry_run, verbose, output.as_deref())
        }
        Command::Test { filter } => cmd_test(filter.as_deref()),
        Command::Fmt { file, check } => cmd_fmt(&file, check),
    }
}

fn cmd_check(file: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let source = std::fs::read_to_string(file)?;
    
    // Parse
    let program = match meridian_parser::parse(&source) {
        Ok(p) => p,
        Err(errors) => {
            for error in errors {
                eprintln!("parse error: {}", error);
            }
            return Err("parsing failed".into());
        }
    };
    
    // Count items
    let schema_count = program.items.iter().filter(|i| matches!(i, Item::Schema(_))).count();
    let source_count = program.items.iter().filter(|i| matches!(i, Item::Source(_))).count();
    let pipeline_count = program.items.iter().filter(|i| matches!(i, Item::Pipeline(_))).count();
    let function_count = program.items.iter().filter(|i| matches!(i, Item::Function(_))).count();
    
    // Type check
    match check_program(&program) {
        Ok(_env) => {
            println!("OK: {} schemas, {} sources, {} pipelines, {} functions",
                schema_count, source_count, pipeline_count, function_count);
            Ok(())
        }
        Err(errors) => {
            for error in errors {
                eprintln!("type error: {}", error);
            }
            Err("type checking failed".into())
        }
    }
}

fn cmd_run(
    file: &PathBuf,
    pipeline_name: Option<&str>,
    dry_run: bool,
    verbose: bool,
    output: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = std::fs::read_to_string(file)?;
    let base_dir = file.parent().unwrap_or(Path::new("."));
    
    // Parse
    let program = match meridian_parser::parse(&source) {
        Ok(p) => p,
        Err(errors) => {
            for error in errors {
                eprintln!("parse error: {}", error);
            }
            return Err("parsing failed".into());
        }
    };
    
    // Type check
    let env = match check_program(&program) {
        Ok(env) => env,
        Err(errors) => {
            for error in errors {
                eprintln!("type error: {}", error);
            }
            return Err("type checking failed".into());
        }
    };
    
    // Create executor
    let executor = if !dry_run {
        Some(Executor::new()?)
    } else {
        None
    };

    // Load sources
    if let Some(ref exec) = executor {
        for item in &program.items {
            if let Item::Source(src) = item {
                let source_path = base_dir.join(&src.path);
                if verbose {
                    println!("Loading source '{}' from {}", src.name.name, source_path.display());
                }
                match exec.load_source(&src.name.name, &source_path) {
                    Ok(stats) => {
                        if verbose {
                            println!("  Loaded {} rows in {}ms", stats.rows_read, stats.duration_ms);
                        }
                    }
                    Err(e) => {
                        eprintln!("warning: failed to load source '{}': {}", src.name.name, e);
                    }
                }
            }
        }
    }
    
    // Find pipelines to run
    let pipelines: Vec<_> = program
        .items
        .iter()
        .filter_map(|item| {
            if let Item::Pipeline(p) = item {
                if pipeline_name.is_none() || pipeline_name == Some(p.name.name.as_str()) {
                    Some(p)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();
    
    if pipelines.is_empty() {
        if let Some(name) = pipeline_name {
            return Err(format!("pipeline '{}' not found", name).into());
        } else {
            return Err("no pipelines found".into());
        }
    }
    
    let backend = DuckDbBackend;
    
    for pipeline in pipelines {
        if verbose {
            println!("\n=== Pipeline: {} ===", pipeline.name.name);
        }
        
        // Build IR
        let ir = match build_pipeline(pipeline, &env) {
            Ok(ir) => ir,
            Err(e) => {
                eprintln!("IR build error for '{}': {}", pipeline.name.name, e);
                continue;
            }
        };
        
        if verbose {
            println!("IR built successfully");
        }
        
        // Optimize
        let optimized = optimize(ir);
        
        if verbose {
            println!("Optimization complete");
        }
        
        // Generate SQL
        let sql = match backend.generate(&optimized) {
            Ok(sql) => sql,
            Err(e) => {
                eprintln!("codegen error for '{}': {}", pipeline.name.name, e);
                continue;
            }
        };
        
        if dry_run || verbose {
            println!("Generated SQL:\n{}\n", sql);
        }
        
        // Execute
        if let Some(ref exec) = executor {
            if let Some(out_path) = output {
                // Write to file
                match exec.execute_to_file(&sql, out_path) {
                    Ok(stats) => {
                        println!(
                            "Pipeline '{}': wrote {} rows to {} in {}ms",
                            pipeline.name.name,
                            stats.rows_written,
                            out_path.display(),
                            stats.duration_ms
                        );
                    }
                    Err(e) => {
                        eprintln!("execution error for '{}': {}", pipeline.name.name, e);
                    }
                }
            } else {
                // Print results
                match exec.query_print(&sql) {
                    Ok(count) => {
                        println!("\n({} rows)", count);
                    }
                    Err(e) => {
                        eprintln!("execution error for '{}': {}", pipeline.name.name, e);
                    }
                }
            }
        }
    }
    
    Ok(())
}

fn cmd_test(filter: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    println!("Running tests{}...", 
        filter.map(|f| format!(" (filter: {})", f)).unwrap_or_default());
    
    // TODO: Implement test discovery and runner
    // 1. Find all *_test.mer and test_*.mer files
    // 2. Parse and extract test blocks
    // 3. Run each test in isolation
    // 4. Report results
    
    println!("Test framework not yet implemented.");
    Ok(())
}

fn cmd_fmt(file: &PathBuf, check: bool) -> Result<(), Box<dyn std::error::Error>> {
    let _source = std::fs::read_to_string(file)?;
    
    if check {
        println!("Checking formatting: {}", file.display());
    } else {
        println!("Formatting: {}", file.display());
    }
    
    // TODO: Implement formatter
    // 1. Parse to AST
    // 2. Pretty-print AST with consistent formatting
    // 3. If check mode, compare and exit non-zero if different
    // 4. If format mode, write back to file
    
    println!("Formatter not yet implemented.");
    Ok(())
}
