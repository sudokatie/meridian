//! Backend trait and implementations.

use meridian_ir::{AggFunc, BinOp, IrExpr, IrLiteral, IrNode, JoinKind, SortOrder, UnaryOp};

/// A code generation backend.
pub trait Backend {
    /// Generate code from an IR graph.
    fn generate(&self, ir: &IrNode) -> Result<String, CodegenError>;
}

/// Code generation error.
#[derive(Debug, thiserror::Error)]
pub enum CodegenError {
    #[error("unsupported IR node: {0}")]
    UnsupportedNode(String),

    #[error("empty IR graph")]
    EmptyGraph,
}

/// DuckDB SQL backend.
pub struct DuckDbBackend;

impl Backend for DuckDbBackend {
    fn generate(&self, ir: &IrNode) -> Result<String, CodegenError> {
        let sql = generate_sql(ir)?;
        Ok(sql)
    }
}

/// Generate SQL from an IR tree.
fn generate_sql(ir: &IrNode) -> Result<String, CodegenError> {
    match ir {
        IrNode::Scan { source, columns } => {
            let cols = if columns.is_empty() {
                "*".to_string()
            } else {
                columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            Ok(format!("SELECT {} FROM {}", cols, quote_ident(source)))
        }

        IrNode::Filter { input, predicate } => {
            let inner = generate_sql(input)?;
            let pred = generate_expr(predicate);
            
            // If the input is a simple scan, add WHERE directly
            if matches!(input.as_ref(), IrNode::Scan { .. }) {
                Ok(format!("{} WHERE {}", inner, pred))
            } else {
                // Wrap in subquery
                Ok(format!("SELECT * FROM ({}) AS _t WHERE {}", inner, pred))
            }
        }

        IrNode::Project { input, columns } => {
            let inner = generate_sql(input)?;
            let cols = columns
                .iter()
                .map(|(name, expr)| {
                    let expr_sql = generate_expr(expr);
                    if expr_sql == quote_ident(name) {
                        expr_sql
                    } else {
                        format!("{} AS {}", expr_sql, quote_ident(name))
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");

            // If input is a scan, replace the SELECT clause
            if let IrNode::Scan { source, .. } = input.as_ref() {
                Ok(format!("SELECT {} FROM {}", cols, quote_ident(source)))
            } else if let IrNode::Filter { input: filter_input, predicate } = input.as_ref() {
                // SELECT cols FROM (inner) WHERE pred
                if let IrNode::Scan { source, .. } = filter_input.as_ref() {
                    let pred = generate_expr(predicate);
                    Ok(format!("SELECT {} FROM {} WHERE {}", cols, quote_ident(source), pred))
                } else {
                    let inner_sql = generate_sql(filter_input)?;
                    let pred = generate_expr(predicate);
                    Ok(format!("SELECT {} FROM ({}) AS _t WHERE {}", cols, inner_sql, pred))
                }
            } else {
                Ok(format!("SELECT {} FROM ({}) AS _t", cols, inner))
            }
        }

        IrNode::Aggregate { input, group_by, aggregates } => {
            let inner = generate_sql(input)?;
            
            let mut select_parts: Vec<String> = Vec::new();
            
            // Add group by columns to select
            for expr in group_by {
                select_parts.push(generate_expr(expr));
            }
            
            // Add aggregates
            for agg in aggregates {
                let func = match agg.function {
                    AggFunc::Count => "COUNT",
                    AggFunc::Sum => "SUM",
                    AggFunc::Avg => "AVG",
                    AggFunc::Min => "MIN",
                    AggFunc::Max => "MAX",
                };
                let arg = generate_expr(&agg.arg);
                select_parts.push(format!("{}({}) AS {}", func, arg, quote_ident(&agg.name)));
            }
            
            let select_clause = if select_parts.is_empty() {
                "*".to_string()
            } else {
                select_parts.join(", ")
            };
            
            let group_clause = if group_by.is_empty() {
                String::new()
            } else {
                let keys: Vec<String> = group_by.iter().map(generate_expr).collect();
                format!(" GROUP BY {}", keys.join(", "))
            };
            
            // If input is a scan, use it directly
            if let IrNode::Scan { source, .. } = input.as_ref() {
                Ok(format!("SELECT {} FROM {}{}", select_clause, quote_ident(source), group_clause))
            } else {
                Ok(format!("SELECT {} FROM ({}) AS _t{}", select_clause, inner, group_clause))
            }
        }

        IrNode::Join { kind, left, right, on } => {
            let left_sql = generate_sql(left)?;
            let right_sql = generate_sql(right)?;
            let on_sql = generate_expr(on);
            
            let join_type = match kind {
                JoinKind::Inner => "INNER JOIN",
                JoinKind::Left => "LEFT JOIN",
                JoinKind::Right => "RIGHT JOIN",
            };
            
            // Wrap both sides in subqueries for safety
            Ok(format!(
                "SELECT * FROM ({}) AS _l {} ({}) AS _r ON {}",
                left_sql, join_type, right_sql, on_sql
            ))
        }

        IrNode::Sort { input, by } => {
            let inner = generate_sql(input)?;
            
            let order_parts: Vec<String> = by
                .iter()
                .map(|(expr, order)| {
                    let expr_sql = generate_expr(expr);
                    let dir = match order {
                        SortOrder::Asc => "ASC",
                        SortOrder::Desc => "DESC",
                    };
                    format!("{} {}", expr_sql, dir)
                })
                .collect();
            
            let order_clause = order_parts.join(", ");
            
            // Check if we can append directly
            if can_append_clause(input) {
                Ok(format!("{} ORDER BY {}", inner, order_clause))
            } else {
                Ok(format!("SELECT * FROM ({}) AS _t ORDER BY {}", inner, order_clause))
            }
        }

        IrNode::Limit { input, count } => {
            let inner = generate_sql(input)?;
            
            // Check if we can append directly
            if can_append_clause(input) {
                Ok(format!("{} LIMIT {}", inner, count))
            } else {
                Ok(format!("SELECT * FROM ({}) AS _t LIMIT {}", inner, count))
            }
        }
    }
}

/// Check if we can append a clause to this node's SQL.
fn can_append_clause(node: &IrNode) -> bool {
    matches!(
        node,
        IrNode::Scan { .. }
            | IrNode::Filter { .. }
            | IrNode::Project { .. }
            | IrNode::Aggregate { .. }
            | IrNode::Sort { .. }
    )
}

/// Generate SQL expression.
fn generate_expr(expr: &IrExpr) -> String {
    match expr {
        IrExpr::Column(name) => quote_ident(name),
        
        IrExpr::Literal(lit) => match lit {
            IrLiteral::Int(n) => n.to_string(),
            IrLiteral::Float(n) => format!("{}", n),
            IrLiteral::String(s) => quote_string(s),
            IrLiteral::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            IrLiteral::Null => "NULL".to_string(),
        },
        
        IrExpr::BinaryOp(left, op, right) => {
            let left_sql = generate_expr(left);
            let right_sql = generate_expr(right);
            let op_str = match op {
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Eq => "=",
                BinOp::Ne => "<>",
                BinOp::Lt => "<",
                BinOp::Le => "<=",
                BinOp::Gt => ">",
                BinOp::Ge => ">=",
                BinOp::And => "AND",
                BinOp::Or => "OR",
            };
            format!("({} {} {})", left_sql, op_str, right_sql)
        }
        
        IrExpr::UnaryOp(op, operand) => {
            let operand_sql = generate_expr(operand);
            match op {
                UnaryOp::Neg => format!("(-{})", operand_sql),
                UnaryOp::Not => format!("(NOT {})", operand_sql),
            }
        }
        
        IrExpr::Call(name, args) => {
            let func = map_function_name(name);
            let args_sql: Vec<String> = args.iter().map(generate_expr).collect();
            format!("{}({})", func, args_sql.join(", "))
        }
    }
}

/// Map Meridian function names to DuckDB SQL functions.
fn map_function_name(name: &str) -> &str {
    match name {
        // String functions
        "length" => "LENGTH",
        "upper" => "UPPER",
        "lower" => "LOWER",
        "trim" => "TRIM",
        "concat" => "CONCAT",
        "substring" => "SUBSTRING",
        "split" => "STRING_SPLIT",
        
        // Numeric functions
        "abs" => "ABS",
        "ceil" => "CEIL",
        "floor" => "FLOOR",
        "round" => "ROUND",
        "sqrt" => "SQRT",
        "power" => "POWER",
        
        // Temporal functions
        "extract" => "EXTRACT",
        "date_add" => "DATE_ADD",
        "date_diff" => "DATE_DIFF",
        "parse_timestamp" => "STRPTIME",
        "format_timestamp" => "STRFTIME",
        
        // Aggregate functions
        "count" => "COUNT",
        "sum" => "SUM",
        "avg" => "AVG",
        "min" => "MIN",
        "max" => "MAX",
        "first" => "FIRST",
        "last" => "LAST",
        
        // Collection functions
        "size" => "LEN",
        "contains" => "CONTAINS",
        "flatten" => "FLATTEN",
        
        // Null handling
        "coalesce" => "COALESCE",
        
        // Default: pass through
        _ => name,
    }
}

/// Quote an identifier to prevent SQL injection.
fn quote_ident(name: &str) -> String {
    // Handle dotted names like "table.column"
    if name.contains('.') {
        name.split('.')
            .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(".")
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Quote a string literal.
fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_ir::{AggExpr, IrNode};

    #[test]
    fn test_generate_simple_scan() {
        let ir = IrNode::Scan {
            source: "orders".to_string(),
            columns: vec![],
        };
        let backend = DuckDbBackend;
        let sql = backend.generate(&ir).unwrap();
        assert_eq!(sql, "SELECT * FROM \"orders\"");
    }

    #[test]
    fn test_generate_scan_with_columns() {
        let ir = IrNode::Scan {
            source: "orders".to_string(),
            columns: vec!["id".to_string(), "amount".to_string()],
        };
        let backend = DuckDbBackend;
        let sql = backend.generate(&ir).unwrap();
        assert_eq!(sql, "SELECT \"id\", \"amount\" FROM \"orders\"");
    }

    #[test]
    fn test_generate_filter() {
        let ir = IrNode::Filter {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            predicate: IrExpr::BinaryOp(
                Box::new(IrExpr::Column("amount".to_string())),
                BinOp::Gt,
                Box::new(IrExpr::Literal(IrLiteral::Int(100))),
            ),
        };
        let backend = DuckDbBackend;
        let sql = backend.generate(&ir).unwrap();
        assert_eq!(sql, "SELECT * FROM \"orders\" WHERE (\"amount\" > 100)");
    }

    #[test]
    fn test_generate_project() {
        let ir = IrNode::Project {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            columns: vec![
                ("id".to_string(), IrExpr::Column("id".to_string())),
                (
                    "doubled".to_string(),
                    IrExpr::BinaryOp(
                        Box::new(IrExpr::Column("amount".to_string())),
                        BinOp::Mul,
                        Box::new(IrExpr::Literal(IrLiteral::Int(2))),
                    ),
                ),
            ],
        };
        let backend = DuckDbBackend;
        let sql = backend.generate(&ir).unwrap();
        assert_eq!(
            sql,
            "SELECT \"id\", (\"amount\" * 2) AS \"doubled\" FROM \"orders\""
        );
    }

    #[test]
    fn test_generate_aggregate() {
        let ir = IrNode::Aggregate {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            group_by: vec![IrExpr::Column("status".to_string())],
            aggregates: vec![AggExpr {
                name: "total".to_string(),
                function: AggFunc::Sum,
                arg: IrExpr::Column("amount".to_string()),
            }],
        };
        let backend = DuckDbBackend;
        let sql = backend.generate(&ir).unwrap();
        assert_eq!(
            sql,
            "SELECT \"status\", SUM(\"amount\") AS \"total\" FROM \"orders\" GROUP BY \"status\""
        );
    }

    #[test]
    fn test_generate_join() {
        let ir = IrNode::Join {
            kind: JoinKind::Inner,
            left: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            right: Box::new(IrNode::Scan {
                source: "users".to_string(),
                columns: vec![],
            }),
            on: IrExpr::BinaryOp(
                Box::new(IrExpr::Column("orders.user_id".to_string())),
                BinOp::Eq,
                Box::new(IrExpr::Column("users.id".to_string())),
            ),
        };
        let backend = DuckDbBackend;
        let sql = backend.generate(&ir).unwrap();
        assert!(sql.contains("INNER JOIN"));
        assert!(sql.contains("\"orders\".\"user_id\""));
    }

    #[test]
    fn test_generate_sort() {
        let ir = IrNode::Sort {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            by: vec![(IrExpr::Column("amount".to_string()), SortOrder::Desc)],
        };
        let backend = DuckDbBackend;
        let sql = backend.generate(&ir).unwrap();
        assert_eq!(sql, "SELECT * FROM \"orders\" ORDER BY \"amount\" DESC");
    }

    #[test]
    fn test_generate_limit() {
        let ir = IrNode::Limit {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            count: 10,
        };
        let backend = DuckDbBackend;
        let sql = backend.generate(&ir).unwrap();
        assert_eq!(sql, "SELECT * FROM \"orders\" LIMIT 10");
    }

    #[test]
    fn test_generate_complex_pipeline() {
        // SELECT id, (amount * 2) AS doubled FROM orders WHERE status = 'completed' ORDER BY doubled DESC LIMIT 10
        let ir = IrNode::Limit {
            input: Box::new(IrNode::Sort {
                input: Box::new(IrNode::Project {
                    input: Box::new(IrNode::Filter {
                        input: Box::new(IrNode::Scan {
                            source: "orders".to_string(),
                            columns: vec![],
                        }),
                        predicate: IrExpr::BinaryOp(
                            Box::new(IrExpr::Column("status".to_string())),
                            BinOp::Eq,
                            Box::new(IrExpr::Literal(IrLiteral::String("completed".to_string()))),
                        ),
                    }),
                    columns: vec![
                        ("id".to_string(), IrExpr::Column("id".to_string())),
                        (
                            "doubled".to_string(),
                            IrExpr::BinaryOp(
                                Box::new(IrExpr::Column("amount".to_string())),
                                BinOp::Mul,
                                Box::new(IrExpr::Literal(IrLiteral::Int(2))),
                            ),
                        ),
                    ],
                }),
                by: vec![(IrExpr::Column("doubled".to_string()), SortOrder::Desc)],
            }),
            count: 10,
        };
        let backend = DuckDbBackend;
        let sql = backend.generate(&ir).unwrap();
        
        // Should produce valid SQL
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("'completed'"));
    }

    #[test]
    fn test_quote_ident_injection() {
        // Test SQL injection prevention
        let result = quote_ident("users; DROP TABLE users;--");
        assert_eq!(result, "\"users; DROP TABLE users;--\"");
    }

    #[test]
    fn test_quote_string_injection() {
        let result = quote_string("'; DROP TABLE users;--");
        assert_eq!(result, "'''; DROP TABLE users;--'");
    }

    #[test]
    fn test_function_mapping() {
        let expr = IrExpr::Call(
            "upper".to_string(),
            vec![IrExpr::Column("name".to_string())],
        );
        let sql = generate_expr(&expr);
        assert_eq!(sql, "UPPER(\"name\")");
    }
}
