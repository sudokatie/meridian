//! PyFlink code generation backend.
//!
//! Generates PyFlink Table API code from Meridian IR.

use meridian_ir::{AggFunc, BinOp, IrExpr, IrLiteral, IrNode, JoinKind, SortOrder, UnaryOp};

use crate::backend::{Backend, CodegenError};

/// PyFlink code generation backend.
pub struct FlinkBackend {
    /// Table environment variable name.
    table_env_var: String,
}

impl Default for FlinkBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl FlinkBackend {
    /// Create a new Flink backend.
    pub fn new() -> Self {
        Self {
            table_env_var: "t_env".to_string(),
        }
    }
}

impl Backend for FlinkBackend {
    fn generate(&self, ir: &IrNode) -> Result<String, CodegenError> {
        let imports = self.generate_imports();
        let env = self.generate_env();
        let table_code = self.generate_table(ir, "result")?;
        
        Ok(format!(
            "{}\n\n{}\n\n{}",
            imports, env, table_code
        ))
    }
}

impl FlinkBackend {
    fn generate_imports(&self) -> String {
        r#"from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble, Slide, Session"#.to_string()
    }

    fn generate_env(&self) -> String {
        format!(
            r#"settings = EnvironmentSettings.in_streaming_mode()
{} = TableEnvironment.create(settings)"#,
            self.table_env_var
        )
    }

    fn generate_table(&self, ir: &IrNode, var_name: &str) -> Result<String, CodegenError> {
        match ir {
            IrNode::Scan { source, columns } => {
                // For Flink, we'd typically read from a registered table
                let read_expr = format!(
                    "{} = {}.from_path(\"{}\")",
                    var_name, self.table_env_var, source
                );
                if columns.is_empty() {
                    Ok(read_expr)
                } else {
                    let cols = columns.iter()
                        .map(|c| format!("col(\"{}\")", c))
                        .collect::<Vec<_>>()
                        .join(", ");
                    Ok(format!("{}\n{} = {}.select({})", read_expr, var_name, var_name, cols))
                }
            }

            IrNode::Filter { input, predicate } => {
                let input_code = self.generate_table(input, var_name)?;
                let pred_expr = self.generate_expr(predicate);
                Ok(format!("{}\n{} = {}.where({})", input_code, var_name, var_name, pred_expr))
            }

            IrNode::Project { input, columns } => {
                let input_code = self.generate_table(input, var_name)?;
                let cols = columns.iter()
                    .map(|(name, expr)| {
                        let expr_code = self.generate_expr(expr);
                        if let IrExpr::Column(col) = expr {
                            if col == name {
                                return format!("col(\"{}\")", name);
                            }
                        }
                        format!("{}.alias(\"{}\")", expr_code, name)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{}\n{} = {}.select({})", input_code, var_name, var_name, cols))
            }

            IrNode::Aggregate { input, group_by, aggregates } => {
                let input_code = self.generate_table(input, var_name)?;
                
                let group_cols = group_by.iter()
                    .map(|e| self.generate_expr(e))
                    .collect::<Vec<_>>()
                    .join(", ");
                
                let agg_exprs = aggregates.iter()
                    .map(|agg| {
                        let func = match agg.function {
                            AggFunc::Count => "count",
                            AggFunc::Sum => "sum",
                            AggFunc::Avg => "avg",
                            AggFunc::Min => "min",
                            AggFunc::Max => "max",
                            AggFunc::First => "first_value",
                            AggFunc::Last => "last_value",
                        };
                        let arg = self.generate_expr(&agg.arg);
                        format!("{}.{}().alias(\"{}\")", arg, func, agg.name)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                
                if group_by.is_empty() {
                    Ok(format!("{}\n{} = {}.select({})", input_code, var_name, var_name, agg_exprs))
                } else {
                    Ok(format!(
                        "{}\n{} = {}.group_by({}).select({}, {})",
                        input_code, var_name, var_name, group_cols, group_cols, agg_exprs
                    ))
                }
            }

            IrNode::Join { kind, left, right, on } => {
                let left_code = self.generate_table(left, "left_t")?;
                let right_code = self.generate_table(right, "right_t")?;
                let on_expr = self.generate_expr(on);
                
                let join_method = match kind {
                    JoinKind::Inner => "join",
                    JoinKind::Left => "left_outer_join",
                    JoinKind::Right => "right_outer_join",
                    JoinKind::Full => "full_outer_join",
                };
                
                Ok(format!(
                    "{}\n{}\n{} = left_t.{}(right_t).where({})",
                    left_code, right_code, var_name, join_method, on_expr
                ))
            }

            IrNode::Sort { input, by } => {
                let input_code = self.generate_table(input, var_name)?;
                let order_cols = by.iter()
                    .map(|(expr, order)| {
                        let col_expr = self.generate_expr(expr);
                        match order {
                            SortOrder::Asc => format!("{}.asc", col_expr),
                            SortOrder::Desc => format!("{}.desc", col_expr),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{}\n{} = {}.order_by({})", input_code, var_name, var_name, order_cols))
            }

            IrNode::Limit { input, count } => {
                let input_code = self.generate_table(input, var_name)?;
                Ok(format!("{}\n{} = {}.fetch({})", input_code, var_name, var_name, count))
            }

            IrNode::Union { left, right } => {
                let left_code = self.generate_table(left, "left_t")?;
                let right_code = self.generate_table(right, "right_t")?;
                Ok(format!("{}\n{}\n{} = left_t.union_all(right_t)", left_code, right_code, var_name))
            }

            IrNode::Sink { input, destination, format } => {
                let input_code = self.generate_table(input, var_name)?;
                Ok(format!(
                    "{}\n# Sink to {} ({} format)\n{}.execute_insert(\"{}\")",
                    input_code, destination, format, var_name, destination
                ))
            }

            IrNode::Window { input, window_type, time_column } => {
                let input_code = self.generate_table(input, var_name)?;
                let time_col = format!("col(\"{}\")", time_column);
                
                let (window_expr, comment) = match window_type {
                    meridian_ir::IrWindowType::Tumbling { size_ms } => {
                        let interval = format_flink_interval(*size_ms);
                        (
                            format!("Tumble.over(lit({}).millis).on({})", size_ms, time_col),
                            format!("# Tumbling window: {}", interval)
                        )
                    }
                    meridian_ir::IrWindowType::Sliding { size_ms, slide_ms } => {
                        (
                            format!("Slide.over(lit({}).millis).every(lit({}).millis).on({})", 
                                size_ms, slide_ms, time_col),
                            format!("# Sliding window: {}ms size, {}ms slide", size_ms, slide_ms)
                        )
                    }
                    meridian_ir::IrWindowType::Session { gap_ms } => {
                        (
                            format!("Session.with_gap(lit({}).millis).on({})", gap_ms, time_col),
                            format!("# Session window: {}ms gap", gap_ms)
                        )
                    }
                };
                
                Ok(format!(
                    "{}\n{}\n{} = {}.window({}).alias(\"w\")",
                    input_code, comment, var_name, var_name, window_expr
                ))
            }

            IrNode::Emit { input, mode, allowed_lateness_ms } => {
                let input_code = self.generate_table(input, var_name)?;
                let mode_str = match mode {
                    meridian_ir::IrEmitMode::Final => "retract",
                    meridian_ir::IrEmitMode::Updates => "upsert", 
                    meridian_ir::IrEmitMode::Append => "append",
                };
                let lateness = allowed_lateness_ms
                    .map(|ms| format!("\n# Allowed lateness: {}ms", ms))
                    .unwrap_or_default();
                Ok(format!(
                    "{}\n# Emit mode: {}{}",
                    input_code, mode_str, lateness
                ))
            }
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn generate_expr(&self, expr: &IrExpr) -> String {
        match expr {
            IrExpr::Column(name) => format!("col(\"{}\")", name),
            
            IrExpr::Literal(lit) => match lit {
                IrLiteral::Int(n) => format!("lit({})", n),
                IrLiteral::Float(n) => format!("lit({})", n),
                IrLiteral::String(s) => format!("lit(\"{}\")", s.replace('"', "\\\"")),
                IrLiteral::Bool(b) => format!("lit({})", if *b { "True" } else { "False" }),
                IrLiteral::Null => "lit(None)".to_string(),
                IrLiteral::Duration(ms) => format!("lit({}).millis", ms),
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
                    BinOp::Concat => format!("{}.cast(DataTypes.STRING()) + {}.cast(DataTypes.STRING())", left_code, right_code),
                }
            }
            
            IrExpr::UnaryOp(op, operand) => {
                let operand_code = self.generate_expr(operand);
                match op {
                    UnaryOp::Neg => format!("(-{})", operand_code),
                    UnaryOp::Not => format!("(~{})", operand_code),
                    UnaryOp::IsNull => format!("{}.is_null", operand_code),
                    UnaryOp::IsNotNull => format!("{}.is_not_null", operand_code),
                }
            }
            
            IrExpr::Call(name, args) => {
                let args_code: Vec<String> = args.iter()
                    .map(|a| self.generate_expr(a))
                    .collect();
                format!("{}({})", name, args_code.join(", "))
            }
            
            IrExpr::Case { when_clauses, else_clause } => {
                // PyFlink uses if_then_else
                let mut code = String::new();
                for (i, (condition, result)) in when_clauses.iter().enumerate() {
                    let cond_code = self.generate_expr(condition);
                    let result_code = self.generate_expr(result);
                    if i == 0 {
                        code.push_str(&format!("if_then_else({}, {}", cond_code, result_code));
                    } else {
                        code.push_str(&format!(", if_then_else({}, {}", cond_code, result_code));
                    }
                }
                if let Some(else_expr) = else_clause {
                    let else_code = self.generate_expr(else_expr);
                    code.push_str(&format!(", {}", else_code));
                }
                // Close all parentheses
                for _ in 0..when_clauses.len() {
                    code.push(')');
                }
                code
            }
        }
    }
}

/// Format milliseconds as human-readable interval.
fn format_flink_interval(ms: i64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_ir::AggExpr;

    #[test]
    fn test_flink_simple_scan() {
        let ir = IrNode::Scan {
            source: "orders".to_string(),
            columns: vec![],
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("TableEnvironment.create"));
        assert!(code.contains("from_path(\"orders\")"));
    }

    #[test]
    fn test_flink_filter() {
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
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".where("));
        assert!(code.contains("col(\"amount\")"));
    }

    #[test]
    fn test_flink_aggregate() {
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
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".group_by("));
        assert!(code.contains(".sum()"));
    }

    #[test]
    fn test_flink_tumbling_window() {
        let ir = IrNode::Window {
            input: Box::new(IrNode::Scan {
                source: "events".to_string(),
                columns: vec![],
            }),
            window_type: meridian_ir::IrWindowType::Tumbling { size_ms: 3600000 },
            time_column: "event_time".to_string(),
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("Tumble.over("));
        assert!(code.contains(".on(col(\"event_time\"))"));
    }

    #[test]
    fn test_flink_join() {
        let ir = IrNode::Join {
            kind: JoinKind::Inner,
            left: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            right: Box::new(IrNode::Scan {
                source: "customers".to_string(),
                columns: vec![],
            }),
            on: IrExpr::BinaryOp(
                Box::new(IrExpr::Column("customer_id".to_string())),
                BinOp::Eq,
                Box::new(IrExpr::Column("id".to_string())),
            ),
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".join("));
        assert!(code.contains("left_t"));
        assert!(code.contains("right_t"));
    }

    #[test]
    fn test_flink_left_join() {
        let ir = IrNode::Join {
            kind: JoinKind::Left,
            left: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            right: Box::new(IrNode::Scan {
                source: "customers".to_string(),
                columns: vec![],
            }),
            on: IrExpr::BinaryOp(
                Box::new(IrExpr::Column("customer_id".to_string())),
                BinOp::Eq,
                Box::new(IrExpr::Column("id".to_string())),
            ),
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".left_outer_join("));
    }

    #[test]
    fn test_flink_sort() {
        let ir = IrNode::Sort {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            by: vec![
                (IrExpr::Column("amount".to_string()), SortOrder::Desc),
            ],
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".order_by("));
        assert!(code.contains(".desc"));
    }

    #[test]
    fn test_flink_limit() {
        let ir = IrNode::Limit {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            count: 10,
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".fetch(10)"));
    }

    #[test]
    fn test_flink_union() {
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
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".union_all("));
    }

    #[test]
    fn test_flink_sink() {
        let ir = IrNode::Sink {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            destination: "output_table".to_string(),
            format: "parquet".to_string(),
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".execute_insert(\"output_table\")"));
        assert!(code.contains("# Sink to output_table (parquet format)"));
    }

    #[test]
    fn test_flink_sliding_window() {
        let ir = IrNode::Window {
            input: Box::new(IrNode::Scan {
                source: "events".to_string(),
                columns: vec![],
            }),
            window_type: meridian_ir::IrWindowType::Sliding { 
                size_ms: 3600000, 
                slide_ms: 900000,
            },
            time_column: "event_time".to_string(),
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("Slide.over("));
        assert!(code.contains(".every("));
        assert!(code.contains("# Sliding window"));
    }

    #[test]
    fn test_flink_session_window() {
        let ir = IrNode::Window {
            input: Box::new(IrNode::Scan {
                source: "events".to_string(),
                columns: vec![],
            }),
            window_type: meridian_ir::IrWindowType::Session { gap_ms: 1800000 },
            time_column: "event_time".to_string(),
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("Session.with_gap("));
        assert!(code.contains("# Session window"));
    }

    #[test]
    fn test_flink_emit_modes() {
        let ir = IrNode::Emit {
            input: Box::new(IrNode::Scan {
                source: "events".to_string(),
                columns: vec![],
            }),
            mode: meridian_ir::IrEmitMode::Updates,
            allowed_lateness_ms: Some(300000),
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("# Emit mode: upsert"));
        assert!(code.contains("# Allowed lateness: 300000ms"));
    }

    #[test]
    fn test_flink_project_with_alias() {
        let ir = IrNode::Project {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            columns: vec![
                ("id".to_string(), IrExpr::Column("id".to_string())),
                ("total".to_string(), IrExpr::BinaryOp(
                    Box::new(IrExpr::Column("amount".to_string())),
                    BinOp::Mul,
                    Box::new(IrExpr::Literal(IrLiteral::Int(2))),
                )),
            ],
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".select("));
        assert!(code.contains(".alias(\"total\")"));
    }

    #[test]
    fn test_flink_case_expression() {
        let ir = IrNode::Project {
            input: Box::new(IrNode::Scan {
                source: "orders".to_string(),
                columns: vec![],
            }),
            columns: vec![
                ("status_label".to_string(), IrExpr::Case {
                    when_clauses: vec![
                        (
                            IrExpr::BinaryOp(
                                Box::new(IrExpr::Column("status".to_string())),
                                BinOp::Eq,
                                Box::new(IrExpr::Literal(IrLiteral::String("completed".to_string()))),
                            ),
                            IrExpr::Literal(IrLiteral::String("Done".to_string())),
                        ),
                    ],
                    else_clause: Some(Box::new(IrExpr::Literal(IrLiteral::String("Pending".to_string())))),
                }),
            ],
        };
        let backend = FlinkBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("if_then_else"));
    }
}
