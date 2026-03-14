//! Python (Polars) code generation backend.

use crate::backend::{Backend, CodegenError};
use meridian_ir::{AggFunc, BinOp, IrExpr, IrLiteral, IrNode, JoinKind, SortOrder, UnaryOp};

/// Polars Python backend.
pub struct PolarsBackend {
    /// Variable counter for generating unique names.
    var_counter: std::cell::RefCell<usize>,
}

impl PolarsBackend {
    /// Create a new Polars backend.
    pub fn new() -> Self {
        Self {
            var_counter: std::cell::RefCell::new(0),
        }
    }

    /// Generate a unique variable name.
    fn next_var(&self) -> String {
        let mut counter = self.var_counter.borrow_mut();
        let name = format!("df{}", *counter);
        *counter += 1;
        name
    }
}

impl Default for PolarsBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Backend for PolarsBackend {
    fn generate(&self, ir: &IrNode) -> Result<String, CodegenError> {
        let mut lines = vec![
            "import polars as pl".to_string(),
            "from datetime import datetime, timedelta".to_string(),
            "".to_string(),
        ];

        let (code, result_var) = self.generate_node(ir)?;
        lines.extend(code);
        lines.push(format!("result = {}", result_var));

        Ok(lines.join("\n"))
    }
}

impl PolarsBackend {
    /// Generate code for an IR node, returning (lines, result_var).
    fn generate_node(&self, ir: &IrNode) -> Result<(Vec<String>, String), CodegenError> {
        match ir {
            IrNode::Scan { source, columns } => {
                let var = self.next_var();
                let read = format!("{} = pl.scan_parquet(\"{}\")", var, source);

                if columns.is_empty() {
                    Ok((vec![read], var))
                } else {
                    let cols: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c)).collect();
                    let select = format!("{} = {}.select([{}])", var, var, cols.join(", "));
                    Ok((vec![read, select], var))
                }
            }

            IrNode::Filter { input, predicate } => {
                let (mut lines, input_var) = self.generate_node(input)?;
                let pred = self.generate_expr(predicate);
                let var = self.next_var();
                lines.push(format!("{} = {}.filter({})", var, input_var, pred));
                Ok((lines, var))
            }

            IrNode::Project { input, columns } => {
                let (mut lines, input_var) = self.generate_node(input)?;
                let var = self.next_var();

                let cols: Vec<String> = columns
                    .iter()
                    .map(|(name, expr)| {
                        let expr_code = self.generate_expr(expr);
                        if matches!(expr, IrExpr::Column(c) if c == name) {
                            format!("pl.col(\"{}\")", name)
                        } else {
                            format!("{}.alias(\"{}\")", expr_code, name)
                        }
                    })
                    .collect();

                lines.push(format!("{} = {}.select([{}])", var, input_var, cols.join(", ")));
                Ok((lines, var))
            }

            IrNode::Aggregate { input, group_by, aggregates } => {
                let (mut lines, input_var) = self.generate_node(input)?;
                let var = self.next_var();

                let agg_exprs: Vec<String> = aggregates
                    .iter()
                    .map(|agg| {
                        let func = match agg.function {
                            AggFunc::Count => "count",
                            AggFunc::Sum => "sum",
                            AggFunc::Avg => "mean",
                            AggFunc::Min => "min",
                            AggFunc::Max => "max",
                            AggFunc::First => "first",
                            AggFunc::Last => "last",
                        };
                        let arg = self.generate_expr(&agg.arg);
                        format!("{}.{}().alias(\"{}\")", arg, func, agg.name)
                    })
                    .collect();

                if group_by.is_empty() {
                    lines.push(format!(
                        "{} = {}.select([{}])",
                        var,
                        input_var,
                        agg_exprs.join(", ")
                    ));
                } else {
                    let group_cols: Vec<String> = group_by
                        .iter()
                        .map(|e| self.generate_expr(e))
                        .collect();
                    lines.push(format!(
                        "{} = {}.group_by([{}]).agg([{}])",
                        var,
                        input_var,
                        group_cols.join(", "),
                        agg_exprs.join(", ")
                    ));
                }
                Ok((lines, var))
            }

            IrNode::Join { kind, left, right, on } => {
                let (mut lines, left_var) = self.generate_node(left)?;
                let (right_lines, right_var) = self.generate_node(right)?;
                lines.extend(right_lines);

                let var = self.next_var();
                let how = match kind {
                    JoinKind::Inner => "inner",
                    JoinKind::Left => "left",
                    JoinKind::Right => "right",
                    JoinKind::Full => "full",
                };

                // Extract join columns from the ON expression
                let on_expr = self.generate_expr(on);
                lines.push(format!(
                    "{} = {}.join({}, on={}, how=\"{}\")",
                    var, left_var, right_var, on_expr, how
                ));
                Ok((lines, var))
            }

            IrNode::Sort { input, by } => {
                let (mut lines, input_var) = self.generate_node(input)?;
                let var = self.next_var();

                let cols: Vec<String> = by
                    .iter()
                    .map(|(expr, _)| self.generate_expr(expr))
                    .collect();
                let descending: Vec<String> = by
                    .iter()
                    .map(|(_, order)| match order {
                        SortOrder::Asc => "False".to_string(),
                        SortOrder::Desc => "True".to_string(),
                    })
                    .collect();

                lines.push(format!(
                    "{} = {}.sort([{}], descending=[{}])",
                    var,
                    input_var,
                    cols.join(", "),
                    descending.join(", ")
                ));
                Ok((lines, var))
            }

            IrNode::Limit { input, count } => {
                let (mut lines, input_var) = self.generate_node(input)?;
                let var = self.next_var();
                lines.push(format!("{} = {}.head({})", var, input_var, count));
                Ok((lines, var))
            }

            IrNode::Union { left, right } => {
                let (mut lines, left_var) = self.generate_node(left)?;
                let (right_lines, right_var) = self.generate_node(right)?;
                lines.extend(right_lines);

                let var = self.next_var();
                lines.push(format!("{} = pl.concat([{}, {}])", var, left_var, right_var));
                Ok((lines, var))
            }

            IrNode::Sink { input, destination, format } => {
                let (mut lines, input_var) = self.generate_node(input)?;

                let write = match format.as_str() {
                    "parquet" => format!("{}.collect().write_parquet(\"{}\")", input_var, destination),
                    "csv" => format!("{}.collect().write_csv(\"{}\")", input_var, destination),
                    "json" => format!("{}.collect().write_json(\"{}\")", input_var, destination),
                    _ => format!("{}.collect().write_parquet(\"{}\")", input_var, destination),
                };
                lines.push(write);
                Ok((lines, input_var))
            }

            IrNode::Window { input, window_type, time_column } => {
                let (mut lines, input_var) = self.generate_node(input)?;
                let var = self.next_var();

                let size_str = match window_type {
                    meridian_ir::IrWindowType::Tumbling { size_ms } => format!("{}ms", size_ms),
                    meridian_ir::IrWindowType::Sliding { size_ms, .. } => format!("{}ms", size_ms),
                    meridian_ir::IrWindowType::Session { gap_ms } => format!("{}ms", gap_ms),
                };

                lines.push(format!(
                    "# Window: {}\n{} = {}.with_columns([\n    pl.col(\"{}\").dt.truncate(\"{}\").alias(\"window_start\"),\n    (pl.col(\"{}\").dt.truncate(\"{}\") + pl.duration(milliseconds={})).alias(\"window_end\")\n])",
                    match window_type {
                        meridian_ir::IrWindowType::Tumbling { size_ms } => format!("tumbling {}ms", size_ms),
                        meridian_ir::IrWindowType::Sliding { size_ms, slide_ms } => format!("sliding {}ms/{}ms", size_ms, slide_ms),
                        meridian_ir::IrWindowType::Session { gap_ms } => format!("session {}ms", gap_ms),
                    },
                    var,
                    input_var,
                    time_column,
                    size_str,
                    time_column,
                    size_str,
                    match window_type {
                        meridian_ir::IrWindowType::Tumbling { size_ms } => *size_ms,
                        meridian_ir::IrWindowType::Sliding { size_ms, .. } => *size_ms,
                        meridian_ir::IrWindowType::Session { gap_ms } => *gap_ms,
                    }
                ));
                Ok((lines, var))
            }

            IrNode::Emit { input, mode, allowed_lateness_ms } => {
                let (mut lines, input_var) = self.generate_node(input)?;
                // Emit is a streaming concept - add as comment
                let mode_str = match mode {
                    meridian_ir::IrEmitMode::Final => "final",
                    meridian_ir::IrEmitMode::Updates => "updates",
                    meridian_ir::IrEmitMode::Append => "append",
                };
                let lateness = allowed_lateness_ms
                    .map(|ms| format!(", lateness: {}ms", ms))
                    .unwrap_or_default();
                lines.push(format!("# Emit mode: {}{}", mode_str, lateness));
                Ok((lines, input_var))
            }
        }
    }

    /// Generate a Polars expression.
    fn generate_expr(&self, expr: &IrExpr) -> String {
        match expr {
            IrExpr::Column(name) => format!("pl.col(\"{}\")", name),

            IrExpr::Literal(lit) => match lit {
                IrLiteral::Int(n) => format!("pl.lit({})", n),
                IrLiteral::Float(n) => format!("pl.lit({:.6})", n),
                IrLiteral::String(s) => format!("pl.lit(\"{}\")", s.replace('"', "\\\"")),
                IrLiteral::Bool(b) => format!("pl.lit({})", if *b { "True" } else { "False" }),
                IrLiteral::Null => "pl.lit(None)".to_string(),
                IrLiteral::Duration(ms) => format!("pl.duration(milliseconds={})", ms),
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
                    BinOp::Concat => format!("pl.concat_str([{}, {}])", left_code, right_code),
                }
            }

            IrExpr::UnaryOp(op, operand) => {
                let operand_code = self.generate_expr(operand);
                match op {
                    UnaryOp::Neg => format!("(-{})", operand_code),
                    UnaryOp::Not => format!("(~{})", operand_code),
                    UnaryOp::IsNull => format!("{}.is_null()", operand_code),
                    UnaryOp::IsNotNull => format!("{}.is_not_null()", operand_code),
                }
            }

            IrExpr::Call(name, args) => {
                let args_code: Vec<String> = args.iter().map(|a| self.generate_expr(a)).collect();
                self.map_function(name, &args_code)
            }

            IrExpr::Case { when_clauses, else_clause } => {
                // Build pl.when().then().when().then()...otherwise() chain
                let mut code = String::new();
                for (i, (condition, result)) in when_clauses.iter().enumerate() {
                    let cond = self.generate_expr(condition);
                    let res = self.generate_expr(result);
                    if i == 0 {
                        code = format!("pl.when({}).then({})", cond, res);
                    } else {
                        code = format!("{}.when({}).then({})", code, cond, res);
                    }
                }
                if let Some(else_expr) = else_clause {
                    let else_code = self.generate_expr(else_expr);
                    format!("{}.otherwise({})", code, else_code)
                } else {
                    format!("{}.otherwise(pl.lit(None))", code)
                }
            }
        }
    }

    /// Map function name to Polars equivalent.
    fn map_function(&self, name: &str, args: &[String]) -> String {
        match name {
            // String functions
            "length" => format!("{}.str.len_chars()", args[0]),
            "upper" => format!("{}.str.to_uppercase()", args[0]),
            "lower" => format!("{}.str.to_lowercase()", args[0]),
            "trim" => format!("{}.str.strip_chars()", args[0]),
            "concat" => format!("pl.concat_str([{}])", args.join(", ")),
            "substring" => {
                if args.len() == 3 {
                    format!("{}.str.slice({}, {})", args[0], args[1], args[2])
                } else {
                    format!("{}.str.slice({}, None)", args[0], args[1])
                }
            }

            // Numeric functions
            "abs" => format!("{}.abs()", args[0]),
            "ceil" => format!("{}.ceil()", args[0]),
            "floor" => format!("{}.floor()", args[0]),
            "round" => {
                if args.len() == 2 {
                    format!("{}.round({})", args[0], args[1])
                } else {
                    format!("{}.round(0)", args[0])
                }
            }
            "sqrt" => format!("{}.sqrt()", args[0]),
            "power" => format!("{}.pow({})", args[0], args[1]),

            // Temporal functions
            "extract" => format!("{}.dt.{}", args[1], args[0].trim_matches('"').to_lowercase()),
            "parse_timestamp" => format!("{}.str.to_datetime()", args[0]),
            "format_timestamp" => format!("{}.dt.strftime({})", args[0], args[1]),

            // Null handling
            "coalesce" => format!("pl.coalesce([{}])", args.join(", ")),

            // Collection functions
            "size" => format!("{}.list.len()", args[0]),
            "contains" => format!("{}.list.contains({})", args[0], args[1]),

            // Default: pass through as method call
            _ => {
                if args.len() == 1 {
                    format!("{}.{}()", args[0], name)
                } else {
                    format!("{}({}, {})", name, args[0], args[1..].join(", "))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_ir::{AggExpr, IrNode};

    #[test]
    fn test_generate_simple_scan() {
        let ir = IrNode::Scan {
            source: "orders.parquet".to_string(),
            columns: vec![],
        };
        let backend = PolarsBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains("import polars as pl"));
        assert!(code.contains("pl.scan_parquet"));
        assert!(code.contains("orders.parquet"));
    }

    #[test]
    fn test_generate_scan_with_columns() {
        let ir = IrNode::Scan {
            source: "orders.parquet".to_string(),
            columns: vec!["id".to_string(), "amount".to_string()],
        };
        let backend = PolarsBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".select(["));
        assert!(code.contains("\"id\""));
        assert!(code.contains("\"amount\""));
    }

    #[test]
    fn test_generate_filter() {
        let ir = IrNode::Filter {
            input: Box::new(IrNode::Scan {
                source: "orders.parquet".to_string(),
                columns: vec![],
            }),
            predicate: IrExpr::BinaryOp(
                Box::new(IrExpr::Column("amount".to_string())),
                BinOp::Gt,
                Box::new(IrExpr::Literal(IrLiteral::Int(100))),
            ),
        };
        let backend = PolarsBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".filter("));
        assert!(code.contains("pl.col(\"amount\")"));
        assert!(code.contains("> pl.lit(100)"));
    }

    #[test]
    fn test_generate_project() {
        let ir = IrNode::Project {
            input: Box::new(IrNode::Scan {
                source: "orders.parquet".to_string(),
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
        let backend = PolarsBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".select(["));
        assert!(code.contains(".alias(\"doubled\")"));
    }

    #[test]
    fn test_generate_aggregate() {
        let ir = IrNode::Aggregate {
            input: Box::new(IrNode::Scan {
                source: "orders.parquet".to_string(),
                columns: vec![],
            }),
            group_by: vec![IrExpr::Column("status".to_string())],
            aggregates: vec![AggExpr {
                name: "total".to_string(),
                function: AggFunc::Sum,
                arg: IrExpr::Column("amount".to_string()),
            }],
        };
        let backend = PolarsBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".group_by(["));
        assert!(code.contains(".agg(["));
        assert!(code.contains(".sum()"));
        assert!(code.contains(".alias(\"total\")"));
    }

    #[test]
    fn test_generate_sort() {
        let ir = IrNode::Sort {
            input: Box::new(IrNode::Scan {
                source: "orders.parquet".to_string(),
                columns: vec![],
            }),
            by: vec![(IrExpr::Column("amount".to_string()), SortOrder::Desc)],
        };
        let backend = PolarsBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".sort(["));
        assert!(code.contains("descending=[True]"));
    }

    #[test]
    fn test_generate_limit() {
        let ir = IrNode::Limit {
            input: Box::new(IrNode::Scan {
                source: "orders.parquet".to_string(),
                columns: vec![],
            }),
            count: 10,
        };
        let backend = PolarsBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".head(10)"));
    }

    #[test]
    fn test_generate_join() {
        let ir = IrNode::Join {
            kind: JoinKind::Inner,
            left: Box::new(IrNode::Scan {
                source: "orders.parquet".to_string(),
                columns: vec![],
            }),
            right: Box::new(IrNode::Scan {
                source: "users.parquet".to_string(),
                columns: vec![],
            }),
            on: IrExpr::BinaryOp(
                Box::new(IrExpr::Column("user_id".to_string())),
                BinOp::Eq,
                Box::new(IrExpr::Column("id".to_string())),
            ),
        };
        let backend = PolarsBackend::new();
        let code = backend.generate(&ir).unwrap();
        assert!(code.contains(".join("));
        assert!(code.contains("how=\"inner\""));
    }

    #[test]
    fn test_generate_case_expression() {
        let expr = IrExpr::Case {
            when_clauses: vec![
                (
                    IrExpr::BinaryOp(
                        Box::new(IrExpr::Column("x".to_string())),
                        BinOp::Gt,
                        Box::new(IrExpr::Literal(IrLiteral::Int(0))),
                    ),
                    IrExpr::Literal(IrLiteral::String("positive".to_string())),
                ),
            ],
            else_clause: Some(Box::new(IrExpr::Literal(IrLiteral::String("non-positive".to_string())))),
        };
        let backend = PolarsBackend::new();
        let code = backend.generate_expr(&expr);
        assert!(code.contains("pl.when("));
        assert!(code.contains(".then("));
        assert!(code.contains(".otherwise("));
    }

    #[test]
    fn test_function_mapping() {
        let backend = PolarsBackend::new();

        // String functions
        let upper = backend.map_function("upper", &["pl.col(\"name\")".to_string()]);
        assert!(upper.contains(".str.to_uppercase()"));

        // Numeric functions
        let sqrt = backend.map_function("sqrt", &["pl.col(\"x\")".to_string()]);
        assert!(sqrt.contains(".sqrt()"));
    }
}
