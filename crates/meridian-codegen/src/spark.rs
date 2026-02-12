//! PySpark code generation backend.

use meridian_ir::{AggFunc, BinOp, IrExpr, IrLiteral, IrNode, JoinKind, SortOrder, UnaryOp};

use crate::backend::{Backend, CodegenError};

/// PySpark code generation backend.
pub struct SparkBackend {
    /// Session variable name.
    session_var: String,
}

impl Default for SparkBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBackend {
    /// Create a new Spark backend.
    pub fn new() -> Self {
        Self {
            session_var: "spark".to_string(),
        }
    }

    /// Create with custom session variable name.
    pub fn with_session(session_var: &str) -> Self {
        Self {
            session_var: session_var.to_string(),
        }
    }
}

impl Backend for SparkBackend {
    fn generate(&self, ir: &IrNode) -> Result<String, CodegenError> {
        let imports = self.generate_imports();
        let session = self.generate_session();
        let df_code = self.generate_df(ir, "result")?;
        
        Ok(format!(
            "{}\n\n{}\n\n{}",
            imports, session, df_code
        ))
    }
}

impl SparkBackend {
    fn generate_imports(&self) -> String {
        r#"from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window"#.to_string()
    }

    fn generate_session(&self) -> String {
        format!(
            r#"{} = SparkSession.builder \
    .appName("Meridian Pipeline") \
    .getOrCreate()"#,
            self.session_var
        )
    }

    fn generate_df(&self, ir: &IrNode, var_name: &str) -> Result<String, CodegenError> {
        match ir {
            IrNode::Scan { source, columns } => {
                let read_expr = format!(
                    "{} = {}.read.parquet(\"{}\")",
                    var_name, self.session_var, source
                );
                if columns.is_empty() {
                    Ok(read_expr)
                } else {
                    let cols = columns.iter()
                        .map(|c| format!("\"{}\"", c))
                        .collect::<Vec<_>>()
                        .join(", ");
                    Ok(format!("{}\n{} = {}.select({})", read_expr, var_name, var_name, cols))
                }
            }

            IrNode::Filter { input, predicate } => {
                let input_code = self.generate_df(input, var_name)?;
                let pred_expr = self.generate_expr(predicate);
                Ok(format!("{}\n{} = {}.filter({})", input_code, var_name, var_name, pred_expr))
            }

            IrNode::Project { input, columns } => {
                let input_code = self.generate_df(input, var_name)?;
                let cols = columns.iter()
                    .map(|(name, expr)| {
                        let expr_code = self.generate_expr(expr);
                        if let IrExpr::Column(col) = expr {
                            if col == name {
                                return format!("F.col(\"{}\")", name);
                            }
                        }
                        format!("{}.alias(\"{}\")", expr_code, name)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{}\n{} = {}.select({})", input_code, var_name, var_name, cols))
            }

            IrNode::Aggregate { input, group_by, aggregates } => {
                let input_code = self.generate_df(input, var_name)?;
                
                let group_cols = group_by.iter()
                    .map(|e| self.generate_expr(e))
                    .collect::<Vec<_>>()
                    .join(", ");
                
                let agg_exprs = aggregates.iter()
                    .map(|agg| {
                        let func = match agg.function {
                            AggFunc::Count => "F.count",
                            AggFunc::Sum => "F.sum",
                            AggFunc::Avg => "F.avg",
                            AggFunc::Min => "F.min",
                            AggFunc::Max => "F.max",
                            AggFunc::First => "F.first",
                            AggFunc::Last => "F.last",
                        };
                        let arg = self.generate_expr(&agg.arg);
                        format!("{}({}).alias(\"{}\")", func, arg, agg.name)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                
                if group_by.is_empty() {
                    Ok(format!("{}\n{} = {}.agg({})", input_code, var_name, var_name, agg_exprs))
                } else {
                    Ok(format!(
                        "{}\n{} = {}.groupBy({}).agg({})",
                        input_code, var_name, var_name, group_cols, agg_exprs
                    ))
                }
            }

            IrNode::Join { kind, left, right, on } => {
                let left_code = self.generate_df(left, "left_df")?;
                let right_code = self.generate_df(right, "right_df")?;
                let on_expr = self.generate_expr(on);
                
                let join_type = match kind {
                    JoinKind::Inner => "inner",
                    JoinKind::Left => "left",
                    JoinKind::Right => "right",
                    JoinKind::Full => "full",
                };
                
                Ok(format!(
                    "{}\n{}\n{} = left_df.join(right_df, {}, \"{}\")",
                    left_code, right_code, var_name, on_expr, join_type
                ))
            }

            IrNode::Sort { input, by } => {
                let input_code = self.generate_df(input, var_name)?;
                let sort_cols = by.iter()
                    .map(|(expr, order)| {
                        let col_expr = self.generate_expr(expr);
                        match order {
                            SortOrder::Asc => format!("{}.asc()", col_expr),
                            SortOrder::Desc => format!("{}.desc()", col_expr),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{}\n{} = {}.orderBy({})", input_code, var_name, var_name, sort_cols))
            }

            IrNode::Limit { input, count } => {
                let input_code = self.generate_df(input, var_name)?;
                Ok(format!("{}\n{} = {}.limit({})", input_code, var_name, var_name, count))
            }

            IrNode::Union { left, right } => {
                let left_code = self.generate_df(left, "left_df")?;
                let right_code = self.generate_df(right, "right_df")?;
                Ok(format!("{}\n{}\n{} = left_df.union(right_df)", left_code, right_code, var_name))
            }

            IrNode::Sink { input, destination, format } => {
                let input_code = self.generate_df(input, var_name)?;
                let write_method = match format.as_str() {
                    "csv" => "csv",
                    "json" => "json",
                    "parquet" | _ => "parquet",
                };
                Ok(format!(
                    "{}\n{}.write.mode(\"overwrite\").{}(\"{}\")",
                    input_code, var_name, write_method, destination
                ))
            }

            IrNode::Window { input, window_type, time_column } => {
                let input_code = self.generate_df(input, var_name)?;
                let time_col = format!("F.col(\"{}\")", time_column);
                
                let (window_expr, comment) = match window_type {
                    meridian_ir::IrWindowType::Tumbling { size_ms } => {
                        let interval = format_interval(*size_ms);
                        (
                            format!("F.window({}, \"{}\")", time_col, interval),
                            format!("# Tumbling window: {}", interval)
                        )
                    }
                    meridian_ir::IrWindowType::Sliding { size_ms, slide_ms } => {
                        let size = format_interval(*size_ms);
                        let slide = format_interval(*slide_ms);
                        (
                            format!("F.window({}, \"{}\", \"{}\")", time_col, size, slide),
                            format!("# Sliding window: {} size, {} slide", size, slide)
                        )
                    }
                    meridian_ir::IrWindowType::Session { gap_ms } => {
                        // Spark doesn't have native session windows, use tumbling as approximation
                        let interval = format_interval(*gap_ms);
                        (
                            format!("F.window({}, \"{}\")", time_col, interval),
                            format!("# Session window approximation: {} gap", interval)
                        )
                    }
                };
                
                Ok(format!(
                    "{}\n{}\n{} = {}.withColumn(\"window\", {})\n{} = {}.select(\"*\", F.col(\"window.start\").alias(\"window_start\"), F.col(\"window.end\").alias(\"window_end\")).drop(\"window\")",
                    input_code, comment, var_name, var_name, window_expr, var_name, var_name
                ))
            }

            IrNode::Emit { input, mode, allowed_lateness_ms } => {
                let input_code = self.generate_df(input, var_name)?;
                let mode_str = match mode {
                    meridian_ir::IrEmitMode::Final => "complete",
                    meridian_ir::IrEmitMode::Updates => "update",
                    meridian_ir::IrEmitMode::Append => "append",
                };
                let lateness = allowed_lateness_ms
                    .map(|ms| format!(", lateness: {}ms", ms))
                    .unwrap_or_default();
                Ok(format!(
                    "{}\n# Emit mode: {}{}\n# Note: outputMode would be set on writeStream",
                    input_code, mode_str, lateness
                ))
            }
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn generate_expr(&self, expr: &IrExpr) -> String {
        match expr {
            IrExpr::Column(name) => {
                if name.contains('.') {
                    // Handle qualified names like "table.column"
                    format!("F.col(\"{}\")", name)
                } else {
                    format!("F.col(\"{}\")", name)
                }
            }
            
            IrExpr::Literal(lit) => match lit {
                IrLiteral::Int(n) => format!("F.lit({})", n),
                IrLiteral::Float(n) => format!("F.lit({})", n),
                IrLiteral::String(s) => format!("F.lit(\"{}\")", s.replace('"', "\\\"")),
                IrLiteral::Bool(b) => format!("F.lit({})", if *b { "True" } else { "False" }),
                IrLiteral::Null => "F.lit(None)".to_string(),
                IrLiteral::Duration(ms) => format!("F.expr(\"INTERVAL {} MILLISECONDS\")", ms),
            },
            
            IrExpr::BinaryOp(left, op, right) => {
                let left_code = self.generate_expr(left);
                let right_code = self.generate_expr(right);
                match op {
                    BinOp::Add => format!("({} + {})", left_code, right_code),
                    BinOp::Sub => format!("({} - {})", left_code, right_code),
                    BinOp::Mul => format!("({} * {})", left_code, right_code),
                    BinOp::Div => format!("({} / {})", left_code, right_code),
                    BinOp::Mod => format!("({} % {})", left_code, right_code),
                    BinOp::Eq => format!("({} == {})", left_code, right_code),
                    BinOp::Ne => format!("({} != {})", left_code, right_code),
                    BinOp::Lt => format!("({} < {})", left_code, right_code),
                    BinOp::Le => format!("({} <= {})", left_code, right_code),
                    BinOp::Gt => format!("({} > {})", left_code, right_code),
                    BinOp::Ge => format!("({} >= {})", left_code, right_code),
                    BinOp::And => format!("({} & {})", left_code, right_code),
                    BinOp::Or => format!("({} | {})", left_code, right_code),
                    BinOp::Concat => format!("F.concat({}, {})", left_code, right_code),
                }
            }
            
            IrExpr::UnaryOp(op, operand) => {
                let operand_code = self.generate_expr(operand);
                match op {
                    UnaryOp::Neg => format!("(-{})", operand_code),
                    UnaryOp::Not => format!("(~{})", operand_code),
                    UnaryOp::IsNull => format!("{}.isNull()", operand_code),
                    UnaryOp::IsNotNull => format!("{}.isNotNull()", operand_code),
                }
            }
            
            IrExpr::Call(name, args) => {
                let func = map_spark_function(name);
                let args_code: Vec<String> = args.iter()
                    .map(|a| self.generate_expr(a))
                    .collect();
                format!("{}({})", func, args_code.join(", "))
            }
            
            IrExpr::Case { when_clauses, else_clause } => {
                let mut code = "F.when(".to_string();
                for (i, (condition, result)) in when_clauses.iter().enumerate() {
                    let cond_code = self.generate_expr(condition);
                    let result_code = self.generate_expr(result);
                    if i == 0 {
                        code.push_str(&format!("{}, {})", cond_code, result_code));
                    } else {
                        code.push_str(&format!(".when({}, {})", cond_code, result_code));
                    }
                }
                if let Some(else_expr) = else_clause {
                    let else_code = self.generate_expr(else_expr);
                    code.push_str(&format!(".otherwise({})", else_code));
                }
                code
            }
        }
    }
}

/// Format milliseconds as Spark interval string.
fn format_interval(ms: i64) -> String {
    if ms >= 86400000 && ms % 86400000 == 0 {
        format!("{} days", ms / 86400000)
    } else if ms >= 3600000 && ms % 3600000 == 0 {
        format!("{} hours", ms / 3600000)
    } else if ms >= 60000 && ms % 60000 == 0 {
        format!("{} minutes", ms / 60000)
    } else if ms >= 1000 && ms % 1000 == 0 {
        format!("{} seconds", ms / 1000)
    } else {
        format!("{} milliseconds", ms)
    }
}

/// Map Meridian function names to PySpark functions.
fn map_spark_function(name: &str) -> &str {
    match name {
        // String functions
        "length" => "F.length",
        "upper" => "F.upper",
        "lower" => "F.lower",
        "trim" => "F.trim",
        "concat" => "F.concat",
        "substring" => "F.substring",
        
        // Numeric functions
        "abs" => "F.abs",
        "ceil" => "F.ceil",
        "floor" => "F.floor",
        "round" => "F.round",
        "sqrt" => "F.sqrt",
        "power" => "F.pow",
        
        // Aggregate functions
        "count" => "F.count",
        "sum" => "F.sum",
        "avg" => "F.avg",
        "min" => "F.min",
        "max" => "F.max",
        "first" => "F.first",
        "last" => "F.last",
        
        // Null handling
        "coalesce" => "F.coalesce",
        
        // Default: pass through with F. prefix
        _ => name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_ir::{AggExpr, IrNode};

    #[test]
    fn test_spark_simple_scan() {
        let ir = IrNode::Scan {
            source: "orders".to_string(),
            columns: vec![],
        };
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("spark.read.parquet(\"orders\")"));
    }

    #[test]
    fn test_spark_filter() {
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
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".filter("));
        assert!(code.contains("F.col(\"amount\")"));
        assert!(code.contains("> F.lit(100)"));
    }

    #[test]
    fn test_spark_project() {
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
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".select("));
        assert!(code.contains(".alias(\"doubled\")"));
    }

    #[test]
    fn test_spark_aggregate() {
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
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".groupBy("));
        assert!(code.contains(".agg("));
        assert!(code.contains("F.sum("));
    }

    #[test]
    fn test_spark_sort() {
        let ir = IrNode::Sort {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            by: vec![(IrExpr::Column("amount".to_string()), SortOrder::Desc)],
        };
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".orderBy("));
        assert!(code.contains(".desc()"));
    }

    #[test]
    fn test_spark_join() {
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
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".join("));
        assert!(code.contains("\"inner\""));
    }

    #[test]
    fn test_spark_tumbling_window() {
        let ir = IrNode::Window {
            input: Box::new(IrNode::Scan {
                source: "events".to_string(),
                columns: vec![],
            }),
            window_type: meridian_ir::IrWindowType::Tumbling { size_ms: 3600000 },
            time_column: "event_time".to_string(),
        };
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("F.window("));
        assert!(code.contains("\"1 hours\""));
        assert!(code.contains("window_start"));
        assert!(code.contains("window_end"));
    }

    #[test]
    fn test_format_interval() {
        assert_eq!(format_interval(86400000), "1 days");
        assert_eq!(format_interval(3600000), "1 hours");
        assert_eq!(format_interval(60000), "1 minutes");
        assert_eq!(format_interval(1000), "1 seconds");
        assert_eq!(format_interval(500), "500 milliseconds");
    }

    #[test]
    fn test_spark_limit() {
        let ir = IrNode::Limit {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            count: 10,
        };
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".limit(10)"));
    }

    #[test]
    fn test_spark_union() {
        let ir = IrNode::Union {
            left: Box::new(IrNode::Scan {
                source: "orders_2023".to_string(),
                columns: vec![],
            }),
            right: Box::new(IrNode::Scan {
                source: "orders_2024".to_string(),
                columns: vec![],
            }),
        };
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("left_df.union(right_df)"));
    }

    #[test]
    fn test_spark_sink() {
        let ir = IrNode::Sink {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            destination: "output/orders".to_string(),
            format: "parquet".to_string(),
        };
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".write.mode(\"overwrite\").parquet("));
    }

    #[test]
    fn test_spark_case_expression() {
        let ir = IrNode::Project {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            columns: vec![(
                "category".to_string(),
                IrExpr::Case {
                    when_clauses: vec![
                        (
                            IrExpr::BinaryOp(
                                Box::new(IrExpr::Column("amount".to_string())),
                                BinOp::Ge,
                                Box::new(IrExpr::Literal(IrLiteral::Int(1000))),
                            ),
                            IrExpr::Literal(IrLiteral::String("large".to_string())),
                        ),
                        (
                            IrExpr::BinaryOp(
                                Box::new(IrExpr::Column("amount".to_string())),
                                BinOp::Ge,
                                Box::new(IrExpr::Literal(IrLiteral::Int(100))),
                            ),
                            IrExpr::Literal(IrLiteral::String("medium".to_string())),
                        ),
                    ],
                    else_clause: Some(Box::new(IrExpr::Literal(IrLiteral::String("small".to_string())))),
                },
            )],
        };
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("F.when("));
        assert!(code.contains(".when("));
        assert!(code.contains(".otherwise("));
    }

    #[test]
    fn test_spark_sliding_window() {
        let ir = IrNode::Window {
            input: Box::new(IrNode::Scan {
                source: "events".to_string(),
                columns: vec![],
            }),
            window_type: meridian_ir::IrWindowType::Sliding { 
                size_ms: 3600000,  // 1 hour
                slide_ms: 900000,  // 15 min
            },
            time_column: "event_time".to_string(),
        };
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("F.window("));
        assert!(code.contains("\"1 hours\""));
        assert!(code.contains("\"15 minutes\""));
    }

    #[test]
    fn test_spark_complex_pipeline() {
        // Filter -> Project -> Sort -> Limit
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
                            "total".to_string(),
                            IrExpr::BinaryOp(
                                Box::new(IrExpr::Column("amount".to_string())),
                                BinOp::Mul,
                                Box::new(IrExpr::Literal(IrLiteral::Float(1.1))),
                            ),
                        ),
                    ],
                }),
                by: vec![(IrExpr::Column("total".to_string()), SortOrder::Desc)],
            }),
            count: 10,
        };
        let backend = SparkBackend::new();
        let code = backend.generate(&ir).unwrap();
        // Verify the chain of operations
        assert!(code.contains(".filter("));
        assert!(code.contains(".select("));
        assert!(code.contains(".orderBy("));
        assert!(code.contains(".limit(10)"));
        assert!(code.contains("\"completed\""));
    }

    #[test]
    fn test_spark_custom_session_var() {
        let ir = IrNode::Scan {
            source: "orders".to_string(),
            columns: vec![],
        };
        let backend = SparkBackend::with_session("my_spark");
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("my_spark = SparkSession.builder"));
        assert!(code.contains("my_spark.read.parquet"));
    }
}
