# Getting Started with Meridian

This guide walks you through installing Meridian and writing your first data pipeline.

---

## Installation

### From Source (Rust Required)

Meridian is written in Rust. Make sure you have Rust installed:

```bash
# Install Rust if needed
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone and build Meridian
git clone https://github.com/sudokatie/meridian.git
cd meridian
cargo install --path crates/meridian-cli
```

After installation, verify it works:

```bash
meridian --version
```

---

## Hello World

Create a file called `hello.mer`:

```meridian
-- Define a schema for our data
schema Order {
    id: string
    amount: decimal(10, 2)
    status: enum("pending", "completed", "cancelled")
}

-- Declare a data source
source orders from file("orders.csv") {
    schema: Order
}

-- Transform the data
pipeline completed_orders {
    from orders
    where status == "completed"
    select {
        id,
        total: amount * 2
    }
    limit 10
}
```

Check your code for errors:

```bash
meridian check hello.mer
```

Run it (requires an `orders.csv` file):

```bash
meridian run hello.mer
```

---

## Core Concepts

### Schemas

Schemas define the structure of your data. Think of them as type definitions:

```meridian
schema Customer {
    customer_id: string
    name: string
    email: string
    created_at: timestamp
    is_active: bool
}
```

### Sources

Sources declare where data comes from:

```meridian
source customers from file("data/customers.csv") {
    schema: Customer
}

source orders from file("data/orders.parquet")
```

Supported file formats: CSV, JSON, Parquet.

### Pipelines

Pipelines transform data. They read from a source and apply operations:

```meridian
pipeline active_customers {
    from customers
    where is_active == true
    select {
        customer_id,
        name,
        email
    }
    order by name
}
```

### Sinks

Sinks define where results go:

```meridian
sink active_customers to file("output/active.parquet") {
    format: parquet
}
```

---

## Pipeline Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| `from` | Input source or pipeline | `from orders` |
| `where` | Filter rows | `where amount > 100` |
| `select` | Choose/transform columns | `select { id, total: price * qty }` |
| `group by` | Group for aggregation | `group by category` |
| `order by` | Sort results | `order by created_at desc` |
| `limit` | Cap result count | `limit 100` |
| `join` | Combine datasets | `join users on orders.user_id == users.id` |

---

## Expressions

### Arithmetic

```meridian
select {
    subtotal: price * quantity,
    tax: price * quantity * 0.08,
    total: price * quantity * 1.08
}
```

### Comparisons

```meridian
where amount >= 100 and status == "completed"
where created_at > date("2024-01-01")
```

### Aggregations

```meridian
group by category
select {
    category,
    total: sum(amount),
    count: count(),
    average: avg(amount)
}
```

---

## Functions

Define reusable logic:

```meridian
fn classify(amount: decimal) -> string {
    match {
        amount >= 1000 -> "large"
        amount >= 100 -> "medium"
        _ -> "small"
    }
}

pipeline classified_orders {
    from orders
    select {
        id,
        amount,
        tier: classify(amount)
    }
}
```

---

## Testing

Write tests to verify your transformations:

```meridian
test "classify handles boundaries" {
    assert classify(1000) == "large"
    assert classify(999) == "medium"
    assert classify(100) == "medium"
    assert classify(99) == "small"
}
```

Run tests:

```bash
meridian test
```

---

## Streaming Quickstart

Meridian supports streaming data processing with windows and watermarks.

### Streaming Source

Declare a streaming source with a watermark:

```meridian
schema Event {
    user_id: string
    action: string
    timestamp: timestamp
}

stream events from kafka("events-topic") {
    schema: Event
    watermark: timestamp - 5.minutes
}
```

### Windowed Aggregation

Use windows to group streaming data by time:

```meridian
pipeline hourly_stats {
    from events
    window tumbling(1.hour) on timestamp
    group by action
    select {
        window_start,
        window_end,
        action,
        count: count()
    }
}
```

### Window Types

- **Tumbling**: Fixed, non-overlapping windows - `tumbling(1.hour)`
- **Sliding**: Overlapping windows - `sliding(1.hour, 15.minutes)`
- **Session**: Activity-based windows - `session(30.minutes)`

### Temporal Joins

Join streaming sources with time bounds:

```meridian
pipeline enriched_clicks {
    from clicks
    join impressions within 5.minutes on clicks.user_id == impressions.user_id
}
```

---

## Project Structure

For larger projects, organize your code:

```
my_project/
    meridian.toml          # Project config
    src/
        schemas/
            order.mer
            customer.mer
        pipelines/
            daily_revenue.mer
            customer_segments.mer
    tests/
        test_revenue.mer
    data/
        input/
        output/
```

---

## Next Steps

- Read the [Language Reference](language-reference.md) for complete syntax
- See the [CLI Reference](cli-reference.md) for all commands
- Check out the [examples/](../examples/) directory
