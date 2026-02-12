# Meridian Language Reference

Complete reference for the Meridian language syntax and semantics.

---

## Lexical Elements

### Comments

```meridian
-- Single line comment

-- Comments can appear anywhere
pipeline foo {  -- even inline
    from data
}
```

### Identifiers

Names for schemas, sources, pipelines, fields, and variables:

```
identifier = letter (letter | digit | "_")*
letter     = "a".."z" | "A".."Z" | "_"
digit      = "0".."9"
```

Examples: `orders`, `user_id`, `dailyRevenue`, `_private`

### Literals

**Strings:**
```meridian
"hello world"
"with \"escapes\""
"line one\nline two"
```

**Numbers:**
```meridian
42          -- integer
3.14        -- float
-100        -- negative
1_000_000   -- underscores allowed
```

**Booleans:**
```meridian
true
false
```

---

## Types

### Primitive Types

| Type | Description | Example |
|------|-------------|---------|
| `string` | Text | `"hello"` |
| `int` | 32-bit integer | `42` |
| `bigint` | 64-bit integer | `9223372036854775807` |
| `float` | 32-bit float | `3.14` |
| `double` | 64-bit float | `3.141592653589793` |
| `decimal(p, s)` | Fixed precision | `decimal(10, 2)` |
| `bool` | Boolean | `true`, `false` |

### Temporal Types

| Type | Description | Example |
|------|-------------|---------|
| `timestamp` | Date and time | `2024-01-15T10:30:00Z` |
| `date` | Date only | `2024-01-15` |
| `time` | Time only | `10:30:00` |

### Complex Types

**List:**
```meridian
list<string>     -- list of strings
list<int>        -- list of integers
list<OrderItem>  -- list of structs
```

**Struct (inline):**
```meridian
struct {
    name: string
    age: int
}
```

**Enum:**
```meridian
enum("pending", "completed", "cancelled")
```

**Nullable:**
```meridian
nullable<string>  -- string that can be null
```

---

## Schemas

Define reusable data structures:

```meridian
schema Order {
    order_id: string
    user_id: string
    amount: decimal(10, 2)
    status: enum("pending", "completed", "cancelled")
    items: list<OrderItem>
    created_at: timestamp
}

schema OrderItem {
    product_id: string
    quantity: int
    price: decimal(10, 2)
}
```

---

## Sources

Declare data inputs:

```meridian
source orders from file("data/orders.parquet") {
    schema: Order
}

source events from file("events.csv") {
    schema: Event
    format: csv
}

source config from file("settings.json") {
    schema: Settings
    format: json
}
```

### Source Options

| Option | Description |
|--------|-------------|
| `schema` | Schema to apply |
| `format` | File format (csv, json, parquet) |

---

## Pipelines

Transform data through operations:

```meridian
pipeline daily_revenue {
    from orders
    where status == "completed"
    group by date(created_at)
    select {
        day: date(created_at),
        total: sum(amount),
        order_count: count()
    }
    order by day desc
    limit 30
}
```

### Pipeline Operations

#### from

Specifies the input source or pipeline:

```meridian
from orders              -- from a source
from completed_orders    -- from another pipeline
```

#### where

Filters rows by a predicate:

```meridian
where status == "completed"
where amount > 100 and created_at > date("2024-01-01")
where user_id is not null
```

#### select

Projects and transforms columns:

```meridian
-- Simple projection
select { order_id, amount }

-- With aliases
select {
    id: order_id,
    total: amount * quantity,
    status
}

-- With expressions
select {
    order_id,
    subtotal: sum(items.price),
    tax: sum(items.price) * 0.08,
    total: sum(items.price) * 1.08
}
```

#### group by

Groups rows for aggregation:

```meridian
group by user_id
select {
    user_id,
    total_orders: count(),
    total_spent: sum(amount)
}

-- Multiple grouping columns
group by category, date(created_at)
```

#### order by

Sorts results:

```meridian
order by created_at          -- ascending (default)
order by amount desc         -- descending
order by status, amount desc -- multiple columns
```

#### limit

Caps result count:

```meridian
limit 100
limit 10
```

#### join

Combines datasets:

```meridian
-- Inner join (default)
join users on orders.user_id == users.id

-- Left join
left join users on orders.user_id == users.id

-- Right join
right join users on orders.user_id == users.id

-- With field selection
pipeline order_details {
    from orders
    join users on orders.user_id == users.id
    select {
        order_id: orders.order_id,
        user_name: users.name,
        amount: orders.amount
    }
}
```

---

## Sinks

Define output destinations:

```meridian
sink daily_revenue to file("output/revenue.parquet") {
    format: parquet
    mode: overwrite
}

sink alerts to file("output/alerts.json") {
    format: json
}
```

### Sink Options

| Option | Description |
|--------|-------------|
| `format` | Output format (csv, json, parquet) |
| `mode` | Write mode (overwrite, append) |

---

## Streaming

Meridian supports streaming data processing with windows and watermarks.

### Streaming Sources

Declare streaming data inputs:

```meridian
stream events from kafka("events-topic") {
    schema: Event
    watermark: event_time - 5.minutes
}

stream clicks from pubsub("clicks-subscription") {
    schema: ClickEvent
    watermark: timestamp
}
```

### Duration Literals

Durations are used for windows, watermarks, and temporal joins:

```meridian
5.seconds
10.minutes
1.hour
24.hours
7.days
```

### Watermarks

Watermarks define how out-of-order events are handled:

```meridian
stream events from kafka("topic") {
    schema: Event
    watermark: event_time - 5.minutes  -- Allow 5 min late data
}
```

The watermark expression must be a timestamp column, optionally minus a duration for allowed lateness.

### Windows

Group streaming data into time-based windows:

**Tumbling Windows:**
Fixed-size, non-overlapping windows.

```meridian
pipeline hourly_counts {
    from events
    window tumbling(1.hour) on event_time
    group by category
    select {
        window_start,
        window_end,
        category,
        count: count()
    }
}
```

**Sliding Windows:**
Fixed-size, overlapping windows.

```meridian
pipeline rolling_avg {
    from events
    window sliding(1.hour, 15.minutes) on event_time  -- 1h window, 15m slide
    select {
        window_start,
        window_end,
        avg_value: avg(value)
    }
}
```

**Session Windows:**
Dynamic windows based on activity gaps.

```meridian
pipeline user_sessions {
    from clicks
    window session(30.minutes) on timestamp  -- 30min inactivity gap
    group by user_id
    select {
        window_start,
        window_end,
        user_id,
        page_views: count()
    }
}
```

### Emit Modes

Control when results are emitted:

```meridian
pipeline updates_stream {
    from events
    window tumbling(1.hour) on event_time
    emit { mode: updates, allowed_lateness: 5.minutes }
    select { window_start, total: sum(value) }
}
```

| Mode | Description |
|------|-------------|
| `final` | Emit only when window closes |
| `updates` | Emit updates as data arrives |
| `append` | Emit only new complete windows |

### Temporal Joins

Join streaming sources with temporal bounds:

```meridian
pipeline enriched_events {
    from clicks
    join impressions within 5.minutes on clicks.user_id == impressions.user_id
    select {
        click_time: clicks.timestamp,
        impression_time: impressions.timestamp,
        user_id: clicks.user_id
    }
}
```

Stream-stream joins require the `within` clause to bound the join window.

---

## Functions

### User-Defined Functions

```meridian
fn classify(amount: decimal) -> string {
    match {
        amount >= 1000 -> "large"
        amount >= 100 -> "medium"
        _ -> "small"
    }
}

fn full_name(first: string, last: string) -> string {
    first ++ " " ++ last
}
```

### Built-in Functions

**String Functions:**
| Function | Description |
|----------|-------------|
| `length(s)` | String length |
| `upper(s)` | Uppercase |
| `lower(s)` | Lowercase |
| `trim(s)` | Remove whitespace |
| `concat(a, b)` | Concatenate |
| `substring(s, start, len)` | Substring |

**Numeric Functions:**
| Function | Description |
|----------|-------------|
| `abs(n)` | Absolute value |
| `ceil(n)` | Ceiling |
| `floor(n)` | Floor |
| `round(n, d)` | Round to d decimals |
| `sqrt(n)` | Square root |

**Temporal Functions:**
| Function | Description |
|----------|-------------|
| `date(ts)` | Extract date |
| `year(ts)` | Extract year |
| `month(ts)` | Extract month |
| `day(ts)` | Extract day |
| `hour(ts)` | Extract hour |
| `now()` | Current timestamp |

**Aggregate Functions:**
| Function | Description |
|----------|-------------|
| `count()` | Row count |
| `sum(x)` | Sum |
| `avg(x)` | Average |
| `min(x)` | Minimum |
| `max(x)` | Maximum |
| `first(x)` | First value |
| `last(x)` | Last value |

---

## Expressions

### Operators

**Arithmetic:**
| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |

**Comparison:**
| Operator | Description |
|----------|-------------|
| `==` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `<=` | Less or equal |
| `>` | Greater than |
| `>=` | Greater or equal |

**Logical:**
| Operator | Description |
|----------|-------------|
| `and` | Logical AND |
| `or` | Logical OR |
| `not` | Logical NOT |

**Other:**
| Operator | Description |
|----------|-------------|
| `++` | String concatenation |
| `??` | Null coalesce |
| `is null` | Null check |
| `is not null` | Not null check |

### Operator Precedence

From highest to lowest:
1. Function calls, field access (`.`)
2. Unary: `not`, `-`
3. Multiplicative: `*`, `/`, `%`
4. Additive: `+`, `-`, `++`
5. Comparison: `==`, `!=`, `<`, `<=`, `>`, `>=`
6. Logical AND: `and`
7. Logical OR: `or`
8. Null coalesce: `??`

### Match Expressions

Pattern matching for conditional logic:

```meridian
match {
    amount >= 1000 -> "large"
    amount >= 100 -> "medium"
    _ -> "small"
}
```

---

## Tests

Write inline tests:

```meridian
test "function works correctly" {
    assert classify(1000) == "large"
    assert classify(500) == "medium"
    assert classify(50) == "small"
}

test "math operations" {
    assert 2 + 2 == 4
    assert 10 * 3 == 30
    assert not (5 > 10)
}
```

---

## Code Generation Targets

Meridian compiles to different backends for execution.

### DuckDB (Default)

Generates SQL for local execution with DuckDB. Best for development and smaller datasets.

```bash
meridian run pipeline.mer
```

### Spark

Generates PySpark code for distributed processing. Use `--target spark` to generate code.

```bash
meridian run pipeline.mer --target spark > pipeline.py
spark-submit pipeline.py
```

Generated code includes:
- SparkSession initialization
- DataFrame operations mapping to Meridian pipeline
- Window functions for streaming pipelines
- Proper type mappings

Example output:
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Meridian Pipeline") \
    .getOrCreate()

result = spark.read.parquet("orders")
result = result.filter((F.col("status") == F.lit("completed")))
result = result.select(F.col("id"), (F.col("amount") * F.lit(2)).alias("total"))
result = result.orderBy(F.col("total").desc())
result = result.limit(10)
```

### Flink

Generates PyFlink Table API code for streaming and batch processing. Use `--target flink` to generate code.

```bash
meridian run pipeline.mer --target flink > pipeline.py
python pipeline.py
```

Generated code includes:
- TableEnvironment setup (streaming mode by default)
- Table API operations mapping to Meridian pipeline
- Window functions (Tumble, Slide, Session)
- Proper type mappings for Flink DataTypes

Example output:
```python
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble, Slide, Session

settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)

result = t_env.from_path("orders")
result = result.where((col("status") == lit("completed")))
result = result.select(col("id"), (col("amount") * lit(2)).alias("total"))
result = result.order_by(col("total").desc)
result = result.fetch(10)
```

Flink-specific features:
- Tumbling windows: `Tumble.over(lit(ms).millis).on(col("time_col"))`
- Sliding windows: `Slide.over(...).every(...).on(...)`
- Session windows: `Session.with_gap(...).on(...)`
- Emit modes: retract, upsert, append

---

## Grammar Summary (EBNF)

```ebnf
program     = item*
item        = schema | source | pipeline | sink | function | test

schema      = "schema" IDENT "{" field_def* "}"
field_def   = IDENT ":" type

source      = "source" IDENT "from" "file" "(" STRING ")" block?
sink        = "sink" IDENT "to" "file" "(" STRING ")" block?

pipeline    = "pipeline" IDENT "{" statement* "}"
statement   = from_stmt | where_stmt | select_stmt | group_stmt |
              order_stmt | limit_stmt | join_stmt

from_stmt   = "from" IDENT
where_stmt  = "where" expr
select_stmt = "select" "{" (IDENT (":" expr)? ",")* "}"
group_stmt  = "group" "by" expr ("," expr)*
order_stmt  = "order" "by" order_term ("," order_term)*
limit_stmt  = "limit" INTEGER
join_stmt   = ("left" | "right")? "join" IDENT "on" expr

function    = "fn" IDENT "(" params? ")" "->" type expr
test        = "test" STRING "{" assertion* "}"
assertion   = "assert" expr

type        = primitive_type | complex_type | IDENT
expr        = literal | IDENT | binary | unary | call | match | "(" expr ")"
```
