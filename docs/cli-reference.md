# Meridian CLI Reference

Complete reference for Meridian command-line interface.

---

## Usage

```bash
meridian [OPTIONS] <COMMAND>
```

---

## Commands

### check

Check syntax and types without executing.

```bash
meridian check <FILE>
```

**Arguments:**
- `FILE` - Path to Meridian file (.mer)

**Examples:**
```bash
meridian check pipeline.mer
meridian check src/pipelines/*.mer
```

**Exit Codes:**
- `0` - No errors
- `1` - Syntax or type errors found

---

### run

Execute a pipeline.

```bash
meridian run <FILE> [OPTIONS]
```

**Arguments:**
- `FILE` - Path to Meridian file (.mer)

**Options:**
| Option | Description |
|--------|-------------|
| `--pipeline <NAME>` | Run specific pipeline (if file has multiple) |
| `--output <PATH>` | Override output path |
| `--format <FMT>` | Output format: csv, json, parquet |
| `--limit <N>` | Limit output rows |
| `--target <TARGET>` | Code generation target: duckdb (default), spark |
| `--dry-run` | Show generated code without executing |
| `--verbose` | Show optimization and compilation details |

**Examples:**
```bash
# Run file (executes first/only pipeline)
meridian run daily.mer

# Run specific pipeline
meridian run pipelines.mer --pipeline daily_revenue

# Override output
meridian run report.mer --output results.csv --format csv

# Preview with limit
meridian run big_query.mer --limit 10

# Generate PySpark code
meridian run pipeline.mer --target spark > pipeline.py

# Show generated SQL without executing
meridian run pipeline.mer --dry-run
```

---

### test

Run tests.

```bash
meridian test [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--filter <PATTERN>` | Run tests matching pattern |
| `--file <PATH>` | Run tests in specific file |
| `--verbose` | Show detailed output |

**Examples:**
```bash
# Run all tests
meridian test

# Run tests matching pattern
meridian test --filter "classify"

# Run tests in specific file
meridian test --file tests/test_revenue.mer

# Verbose output
meridian test --verbose
```

**Test Discovery:**

Tests are discovered from:
- Files matching `*_test.mer`
- Files matching `test_*.mer`
- Files in `tests/` directory

---

### fmt

Format source code.

```bash
meridian fmt <FILE> [OPTIONS]
```

**Arguments:**
- `FILE` - Path to Meridian file (.mer)

**Options:**
| Option | Description |
|--------|-------------|
| `--check` | Check if formatted (no changes) |
| `--write` | Write changes in place (default) |

**Examples:**
```bash
# Format file
meridian fmt pipeline.mer

# Check formatting (CI)
meridian fmt pipeline.mer --check

# Format all files
find . -name "*.mer" -exec meridian fmt {} \;
```

---

### sql

Show generated SQL without executing.

```bash
meridian sql <FILE> [OPTIONS]
```

**Arguments:**
- `FILE` - Path to Meridian file (.mer)

**Options:**
| Option | Description |
|--------|-------------|
| `--pipeline <NAME>` | Show SQL for specific pipeline |

**Examples:**
```bash
# Show generated SQL
meridian sql daily.mer

# For specific pipeline
meridian sql pipelines.mer --pipeline daily_revenue
```

---

### version

Show version information.

```bash
meridian --version
meridian -V
```

---

### help

Show help.

```bash
meridian --help
meridian -h
meridian <COMMAND> --help
```

---

## Global Options

| Option | Description |
|--------|-------------|
| `--help`, `-h` | Show help |
| `--version`, `-V` | Show version |
| `--verbose`, `-v` | Verbose output |
| `--quiet`, `-q` | Suppress non-error output |
| `--color <WHEN>` | Color output: auto, always, never |

---

## Configuration

### Project Config (meridian.toml)

```toml
[project]
name = "my_analytics"
version = "0.1.0"

[execution]
backend = "duckdb"
parallelism = 4

[sources.orders]
path = "data/input/orders.parquet"
format = "parquet"

[sinks.output]
path = "data/output/"
format = "parquet"
mode = "overwrite"
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `MERIDIAN_CONFIG` | Path to config file |
| `MERIDIAN_BACKEND` | Override execution backend |
| `NO_COLOR` | Disable color output |

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (syntax, type, or runtime) |
| 2 | Invalid arguments |

---

## Examples

### Development Workflow

```bash
# Check code as you write
meridian check pipeline.mer

# Run tests
meridian test

# Preview results
meridian run pipeline.mer --limit 10

# Full execution
meridian run pipeline.mer
```

### CI/CD Integration

```bash
#!/bin/bash
set -e

# Check all files
for f in src/**/*.mer; do
    meridian check "$f"
done

# Run tests
meridian test

# Check formatting
for f in src/**/*.mer; do
    meridian fmt "$f" --check
done
```
