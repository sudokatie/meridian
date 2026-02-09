# Meridian

A language for data transformation pipelines. SQL meets functional programming.

---

## The Pitch

Data pipelines are everywhere. ETL jobs. Stream processing. Feature engineering. Analytics. The tooling? A mess.

SQL is great until you need real programming logic. Python is flexible until you need performance. Spark is powerful until you need to actually understand it. dbt brought engineering discipline to SQL, but you're still writing SQL.

Meridian is a language built for one thing: transforming data.

```meridian
source orders from file("orders.parquet") {
  schema: Order
}

pipeline daily_revenue {
  from orders
  where status == "completed"
  group by date(created_at)
  select {
    date: date(created_at),
    total: sum(amount),
    order_count: count()
  }
}

sink daily_revenue to file("output/revenue.parquet")
```

You describe what you want. Meridian figures out how.

---

## Features

- Declarative pipeline syntax (readable as SQL, powerful as a real language)
- Strong typing with inference (catch errors at compile time)
- Multi-backend execution (DuckDB now, Spark/Flink later)
- First-class testing (test your transformations like code)
- Unified batch and streaming (same syntax, different execution)

---

## Status

**v0.1.0** - Complete

Features:
- Full lexer and recursive descent parser
- Type system with inference
- IR with optimization passes
- DuckDB SQL code generation
- CLI: check, run, test, fmt
- Test framework

## Roadmap

### v0.2 (Planned)
- [ ] Streaming support with windowing
- [ ] Spark backend (PySpark generation)
- [ ] Match expression to SQL CASE conversion
- [ ] Source schema type checking improvements

### v0.3 (Planned)
- [ ] Flink backend for production streaming
- [ ] Language Server Protocol (LSP) for IDE support

See FEATURE-BACKLOG.md in the clawd repo for detailed acceptance criteria.

---

## Installation

```bash
# From source (Rust required)
cargo install --path crates/meridian-cli
```

---

## Quick Start

1. Create a file `hello.mer`:

```meridian
source data from file("input.csv")

pipeline doubled {
  from data
  select {
    id,
    value: amount * 2
  }
}

sink doubled to file("output.csv")
```

2. Run it:

```bash
meridian run hello.mer
```

---

## CLI

```bash
meridian check file.mer     # Check syntax and types
meridian run file.mer       # Execute pipeline
meridian test               # Run tests
meridian fmt file.mer       # Format code
```

---

## Language Overview

### Schemas

```meridian
schema Order {
  order_id: string
  amount: decimal(10, 2)
  status: enum("pending", "completed", "cancelled")
  created_at: timestamp
}
```

### Pipelines

```meridian
pipeline clean_orders {
  from orders
  where status == "completed"
  select {
    order_id,
    amount,
    day: date(created_at)
  }
}
```

### Functions

```meridian
fn classify(amount: decimal) -> string {
  match {
    amount >= 1000 -> "large"
    amount >= 100 -> "medium"
    _ -> "small"
  }
}
```

### Tests

```meridian
test "classify handles edge cases" {
  assert classify(1000) == "large"
  assert classify(100) == "medium"
  assert classify(99) == "small"
}
```

---

## Documentation

- [Getting Started](docs/getting-started.md)
- [Language Reference](docs/language-reference.md)
- [CLI Reference](docs/cli-reference.md)
- [Examples](examples/)

---

## Philosophy

1. **Declarative over imperative** - Describe the transformation, not the execution.
2. **Types catch bugs** - Strong typing prevents runtime surprises.
3. **Test everything** - Transformations are code; test them like code.
4. **Readable by default** - If you can read SQL, you can read Meridian.
5. **Backend agnostic** - Write once, run anywhere (eventually).

---

## License

MIT

---

## Author

Katie

*Data pipelines deserve their own language. This is it.*
