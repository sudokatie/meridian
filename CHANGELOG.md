# Changelog

All notable changes to Meridian will be documented in this file.

## [0.1.0] - 2026-01-29

Initial release of Meridian - a domain-specific language for data transformation pipelines.

### Added

- Full lexer with span tracking for error messages
- Recursive descent parser for complete grammar
- Type system with inference and checking
  - Primitives: string, int, bigint, float, double, decimal(p,s), bool
  - Temporal: timestamp, date, time, interval
  - Complex: list<T>, map<K,V>, struct{...}
  - Special: nullable<T>, enum(...), null
- IR generation with dataflow graph representation
- Optimization passes
  - Constant folding (evaluate 2+3 at compile time)
  - Predicate pushdown (move filters closer to source)
  - Projection pushdown (limit columns read)
  - Common subexpression elimination
  - Dead code elimination
- DuckDB SQL code generation
- CLI commands
  - `check` - Validate syntax and types
  - `run` - Execute pipeline
  - `test` - Run test blocks
  - `fmt` - Format source code
- Test framework with assert/given/expect
- Documentation
  - Getting started guide
  - Language reference
  - CLI reference
  - Example files

### Language Features

- Schema definitions with typed fields
- Source declarations (CSV, JSON, Parquet)
- Pipeline operations: from, where, select, group by, order by, limit, join (inner/left/right/full), union
- User-defined functions with match expressions
- Aggregates: count, sum, avg, min, max, first, last
- Expressions: arithmetic, comparison, boolean, null coalesce (??), is null, is not null
- String concatenation with ++
- Built-in function library (string, numeric, temporal, collection)
