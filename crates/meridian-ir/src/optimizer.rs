//! Optimization passes for the IR.

use crate::ir::{BinOp, IrExpr, IrLiteral, IrNode};

/// A trait for optimization passes.
pub trait Pass {
    /// Returns the name of this pass.
    fn name(&self) -> &str;

    /// Run the pass on an IR node, returning the optimized version.
    fn run(&self, ir: IrNode) -> IrNode;
}

/// Predicate pushdown optimization.
///
/// Pushes filter predicates closer to data sources to reduce
/// the amount of data processed by later stages.
pub struct PredicatePushdown;

impl Pass for PredicatePushdown {
    fn name(&self) -> &str {
        "predicate-pushdown"
    }

    fn run(&self, ir: IrNode) -> IrNode {
        push_predicates(ir)
    }
}

fn push_predicates(node: IrNode) -> IrNode {
    match node {
        // Push filter through project if predicate only references projected columns
        IrNode::Filter {
            input,
            predicate,
        } => {
            let input = *input;
            match input {
                IrNode::Project { input: proj_input, columns } => {
                    // Check if predicate can be pushed through project
                    if can_push_through_project(&predicate, &columns) {
                        // Rewrite predicate to use source column names and push down
                        let rewritten = rewrite_predicate(&predicate, &columns);
                        let pushed = IrNode::Filter {
                            input: Box::new(push_predicates(*proj_input)),
                            predicate: rewritten,
                        };
                        IrNode::Project {
                            input: Box::new(pushed),
                            columns,
                        }
                    } else {
                        IrNode::Filter {
                            input: Box::new(IrNode::Project {
                                input: Box::new(push_predicates(*proj_input)),
                                columns,
                            }),
                            predicate,
                        }
                    }
                }
                // Recursively optimize the input
                other => IrNode::Filter {
                    input: Box::new(push_predicates(other)),
                    predicate,
                },
            }
        }
        // Recursively process all other nodes
        IrNode::Project { input, columns } => IrNode::Project {
            input: Box::new(push_predicates(*input)),
            columns,
        },
        IrNode::Aggregate { input, group_by, aggregates } => IrNode::Aggregate {
            input: Box::new(push_predicates(*input)),
            group_by,
            aggregates,
        },
        IrNode::Join { kind, left, right, on } => IrNode::Join {
            kind,
            left: Box::new(push_predicates(*left)),
            right: Box::new(push_predicates(*right)),
            on,
        },
        IrNode::Sort { input, by } => IrNode::Sort {
            input: Box::new(push_predicates(*input)),
            by,
        },
        IrNode::Limit { input, count } => IrNode::Limit {
            input: Box::new(push_predicates(*input)),
            count,
        },
        // Scan nodes are leaves
        scan @ IrNode::Scan { .. } => scan,
    }
}

fn can_push_through_project(predicate: &IrExpr, columns: &[(String, IrExpr)]) -> bool {
    let refs = collect_column_refs(predicate);
    refs.iter().all(|col| {
        columns.iter().any(|(name, expr)| {
            name == col && matches!(expr, IrExpr::Column(_))
        })
    })
}

fn rewrite_predicate(predicate: &IrExpr, columns: &[(String, IrExpr)]) -> IrExpr {
    match predicate {
        IrExpr::Column(name) => {
            // Find the source column
            for (proj_name, expr) in columns {
                if proj_name == name {
                    if let IrExpr::Column(src) = expr {
                        return IrExpr::Column(src.clone());
                    }
                }
            }
            IrExpr::Column(name.clone())
        }
        IrExpr::BinaryOp(left, op, right) => IrExpr::BinaryOp(
            Box::new(rewrite_predicate(left, columns)),
            *op,
            Box::new(rewrite_predicate(right, columns)),
        ),
        IrExpr::UnaryOp(op, expr) => IrExpr::UnaryOp(
            *op,
            Box::new(rewrite_predicate(expr, columns)),
        ),
        IrExpr::Call(name, args) => IrExpr::Call(
            name.clone(),
            args.iter().map(|a| rewrite_predicate(a, columns)).collect(),
        ),
        IrExpr::Literal(lit) => IrExpr::Literal(lit.clone()),
    }
}

fn collect_column_refs(expr: &IrExpr) -> Vec<String> {
    let mut refs = Vec::new();
    collect_column_refs_inner(expr, &mut refs);
    refs
}

fn collect_column_refs_inner(expr: &IrExpr, refs: &mut Vec<String>) {
    match expr {
        IrExpr::Column(name) => refs.push(name.clone()),
        IrExpr::BinaryOp(left, _, right) => {
            collect_column_refs_inner(left, refs);
            collect_column_refs_inner(right, refs);
        }
        IrExpr::UnaryOp(_, inner) => collect_column_refs_inner(inner, refs),
        IrExpr::Call(_, args) => {
            for arg in args {
                collect_column_refs_inner(arg, refs);
            }
        }
        IrExpr::Literal(_) => {}
    }
}

/// Projection pushdown optimization.
///
/// Tracks required columns and adds column lists to Scan nodes
/// to avoid reading unnecessary data.
pub struct ProjectionPushdown;

impl Pass for ProjectionPushdown {
    fn name(&self) -> &str {
        "projection-pushdown"
    }

    fn run(&self, ir: IrNode) -> IrNode {
        let required = collect_all_columns(&ir);
        push_projections(ir, &required)
    }
}

fn collect_all_columns(node: &IrNode) -> Vec<String> {
    let mut cols = Vec::new();
    collect_columns_inner(node, &mut cols);
    cols.sort();
    cols.dedup();
    cols
}

fn collect_columns_inner(node: &IrNode, cols: &mut Vec<String>) {
    match node {
        IrNode::Scan { columns, .. } => {
            cols.extend(columns.iter().cloned());
        }
        IrNode::Filter { input, predicate } => {
            collect_columns_inner(input, cols);
            collect_expr_columns(predicate, cols);
        }
        IrNode::Project { input, columns } => {
            collect_columns_inner(input, cols);
            for (_, expr) in columns {
                collect_expr_columns(expr, cols);
            }
        }
        IrNode::Aggregate { input, group_by, aggregates } => {
            collect_columns_inner(input, cols);
            for expr in group_by {
                collect_expr_columns(expr, cols);
            }
            for agg in aggregates {
                collect_expr_columns(&agg.arg, cols);
            }
        }
        IrNode::Join { left, right, on, .. } => {
            collect_columns_inner(left, cols);
            collect_columns_inner(right, cols);
            collect_expr_columns(on, cols);
        }
        IrNode::Sort { input, by } => {
            collect_columns_inner(input, cols);
            for (expr, _) in by {
                collect_expr_columns(expr, cols);
            }
        }
        IrNode::Limit { input, .. } => {
            collect_columns_inner(input, cols);
        }
    }
}

fn collect_expr_columns(expr: &IrExpr, cols: &mut Vec<String>) {
    match expr {
        IrExpr::Column(name) => cols.push(name.clone()),
        IrExpr::BinaryOp(left, _, right) => {
            collect_expr_columns(left, cols);
            collect_expr_columns(right, cols);
        }
        IrExpr::UnaryOp(_, inner) => collect_expr_columns(inner, cols),
        IrExpr::Call(_, args) => {
            for arg in args {
                collect_expr_columns(arg, cols);
            }
        }
        IrExpr::Literal(_) => {}
    }
}

fn push_projections(node: IrNode, required: &[String]) -> IrNode {
    match node {
        IrNode::Scan { source, columns } => {
            // Keep only required columns
            let filtered: Vec<String> = if columns.is_empty() {
                required.to_vec()
            } else {
                columns.into_iter()
                    .filter(|c| required.contains(c))
                    .collect()
            };
            IrNode::Scan {
                source,
                columns: filtered,
            }
        }
        IrNode::Filter { input, predicate } => IrNode::Filter {
            input: Box::new(push_projections(*input, required)),
            predicate,
        },
        IrNode::Project { input, columns } => {
            // Collect columns needed by project expressions
            let mut needed = Vec::new();
            for (_, expr) in &columns {
                collect_expr_columns(expr, &mut needed);
            }
            needed.sort();
            needed.dedup();
            IrNode::Project {
                input: Box::new(push_projections(*input, &needed)),
                columns,
            }
        }
        IrNode::Aggregate { input, group_by, aggregates } => {
            let mut needed = Vec::new();
            for expr in &group_by {
                collect_expr_columns(expr, &mut needed);
            }
            for agg in &aggregates {
                collect_expr_columns(&agg.arg, &mut needed);
            }
            needed.sort();
            needed.dedup();
            IrNode::Aggregate {
                input: Box::new(push_projections(*input, &needed)),
                group_by,
                aggregates,
            }
        }
        IrNode::Join { kind, left, right, on } => IrNode::Join {
            kind,
            left: Box::new(push_projections(*left, required)),
            right: Box::new(push_projections(*right, required)),
            on,
        },
        IrNode::Sort { input, by } => IrNode::Sort {
            input: Box::new(push_projections(*input, required)),
            by,
        },
        IrNode::Limit { input, count } => IrNode::Limit {
            input: Box::new(push_projections(*input, required)),
            count,
        },
    }
}

/// Constant folding optimization.
///
/// Evaluates constant expressions at compile time and
/// simplifies boolean expressions.
pub struct ConstantFolding;

impl Pass for ConstantFolding {
    fn name(&self) -> &str {
        "constant-folding"
    }

    fn run(&self, ir: IrNode) -> IrNode {
        fold_constants(ir)
    }
}

fn fold_constants(node: IrNode) -> IrNode {
    match node {
        IrNode::Filter { input, predicate } => {
            let folded = fold_expr(predicate);
            // If predicate is always true, remove the filter
            if let IrExpr::Literal(IrLiteral::Bool(true)) = &folded {
                return fold_constants(*input);
            }
            IrNode::Filter {
                input: Box::new(fold_constants(*input)),
                predicate: folded,
            }
        }
        IrNode::Project { input, columns } => IrNode::Project {
            input: Box::new(fold_constants(*input)),
            columns: columns.into_iter()
                .map(|(name, expr)| (name, fold_expr(expr)))
                .collect(),
        },
        IrNode::Aggregate { input, group_by, aggregates } => IrNode::Aggregate {
            input: Box::new(fold_constants(*input)),
            group_by: group_by.into_iter().map(fold_expr).collect(),
            aggregates,
        },
        IrNode::Join { kind, left, right, on } => IrNode::Join {
            kind,
            left: Box::new(fold_constants(*left)),
            right: Box::new(fold_constants(*right)),
            on: fold_expr(on),
        },
        IrNode::Sort { input, by } => IrNode::Sort {
            input: Box::new(fold_constants(*input)),
            by: by.into_iter()
                .map(|(expr, order)| (fold_expr(expr), order))
                .collect(),
        },
        IrNode::Limit { input, count } => IrNode::Limit {
            input: Box::new(fold_constants(*input)),
            count,
        },
        scan @ IrNode::Scan { .. } => scan,
    }
}

fn fold_expr(expr: IrExpr) -> IrExpr {
    match expr {
        IrExpr::BinaryOp(left, op, right) => {
            let left = fold_expr(*left);
            let right = fold_expr(*right);

            // Try to evaluate if both are literals
            if let (IrExpr::Literal(l), IrExpr::Literal(r)) = (&left, &right) {
                if let Some(result) = eval_binary(l, op, r) {
                    return IrExpr::Literal(result);
                }
            }

            // Boolean simplifications
            match (&left, op, &right) {
                // x AND true => x
                (_, BinOp::And, IrExpr::Literal(IrLiteral::Bool(true))) => left,
                (IrExpr::Literal(IrLiteral::Bool(true)), BinOp::And, _) => right,
                // x AND false => false
                (_, BinOp::And, IrExpr::Literal(IrLiteral::Bool(false))) |
                (IrExpr::Literal(IrLiteral::Bool(false)), BinOp::And, _) => {
                    IrExpr::Literal(IrLiteral::Bool(false))
                }
                // x OR false => x
                (_, BinOp::Or, IrExpr::Literal(IrLiteral::Bool(false))) => left,
                (IrExpr::Literal(IrLiteral::Bool(false)), BinOp::Or, _) => right,
                // x OR true => true
                (_, BinOp::Or, IrExpr::Literal(IrLiteral::Bool(true))) |
                (IrExpr::Literal(IrLiteral::Bool(true)), BinOp::Or, _) => {
                    IrExpr::Literal(IrLiteral::Bool(true))
                }
                _ => IrExpr::BinaryOp(Box::new(left), op, Box::new(right)),
            }
        }
        IrExpr::UnaryOp(op, inner) => {
            let inner = fold_expr(*inner);
            if let IrExpr::Literal(lit) = &inner {
                if let Some(result) = eval_unary(op, lit) {
                    return IrExpr::Literal(result);
                }
            }
            IrExpr::UnaryOp(op, Box::new(inner))
        }
        IrExpr::Call(name, args) => IrExpr::Call(
            name,
            args.into_iter().map(fold_expr).collect(),
        ),
        other => other,
    }
}

fn eval_binary(left: &IrLiteral, op: BinOp, right: &IrLiteral) -> Option<IrLiteral> {
    match (left, op, right) {
        // Integer arithmetic
        (IrLiteral::Int(l), BinOp::Add, IrLiteral::Int(r)) => Some(IrLiteral::Int(l + r)),
        (IrLiteral::Int(l), BinOp::Sub, IrLiteral::Int(r)) => Some(IrLiteral::Int(l - r)),
        (IrLiteral::Int(l), BinOp::Mul, IrLiteral::Int(r)) => Some(IrLiteral::Int(l * r)),
        (IrLiteral::Int(l), BinOp::Div, IrLiteral::Int(r)) if *r != 0 => Some(IrLiteral::Int(l / r)),
        (IrLiteral::Int(l), BinOp::Mod, IrLiteral::Int(r)) if *r != 0 => Some(IrLiteral::Int(l % r)),

        // Integer comparisons
        (IrLiteral::Int(l), BinOp::Eq, IrLiteral::Int(r)) => Some(IrLiteral::Bool(l == r)),
        (IrLiteral::Int(l), BinOp::Ne, IrLiteral::Int(r)) => Some(IrLiteral::Bool(l != r)),
        (IrLiteral::Int(l), BinOp::Lt, IrLiteral::Int(r)) => Some(IrLiteral::Bool(l < r)),
        (IrLiteral::Int(l), BinOp::Le, IrLiteral::Int(r)) => Some(IrLiteral::Bool(l <= r)),
        (IrLiteral::Int(l), BinOp::Gt, IrLiteral::Int(r)) => Some(IrLiteral::Bool(l > r)),
        (IrLiteral::Int(l), BinOp::Ge, IrLiteral::Int(r)) => Some(IrLiteral::Bool(l >= r)),

        // Float arithmetic
        (IrLiteral::Float(l), BinOp::Add, IrLiteral::Float(r)) => Some(IrLiteral::Float(l + r)),
        (IrLiteral::Float(l), BinOp::Sub, IrLiteral::Float(r)) => Some(IrLiteral::Float(l - r)),
        (IrLiteral::Float(l), BinOp::Mul, IrLiteral::Float(r)) => Some(IrLiteral::Float(l * r)),
        (IrLiteral::Float(l), BinOp::Div, IrLiteral::Float(r)) if *r != 0.0 => Some(IrLiteral::Float(l / r)),

        // Boolean logic
        (IrLiteral::Bool(l), BinOp::And, IrLiteral::Bool(r)) => Some(IrLiteral::Bool(*l && *r)),
        (IrLiteral::Bool(l), BinOp::Or, IrLiteral::Bool(r)) => Some(IrLiteral::Bool(*l || *r)),
        (IrLiteral::Bool(l), BinOp::Eq, IrLiteral::Bool(r)) => Some(IrLiteral::Bool(l == r)),
        (IrLiteral::Bool(l), BinOp::Ne, IrLiteral::Bool(r)) => Some(IrLiteral::Bool(l != r)),

        // String comparisons
        (IrLiteral::String(l), BinOp::Eq, IrLiteral::String(r)) => Some(IrLiteral::Bool(l == r)),
        (IrLiteral::String(l), BinOp::Ne, IrLiteral::String(r)) => Some(IrLiteral::Bool(l != r)),

        _ => None,
    }
}

fn eval_unary(op: crate::ir::UnaryOp, lit: &IrLiteral) -> Option<IrLiteral> {
    match (op, lit) {
        (crate::ir::UnaryOp::Neg, IrLiteral::Int(n)) => Some(IrLiteral::Int(-n)),
        (crate::ir::UnaryOp::Neg, IrLiteral::Float(n)) => Some(IrLiteral::Float(-n)),
        (crate::ir::UnaryOp::Not, IrLiteral::Bool(b)) => Some(IrLiteral::Bool(!b)),
        _ => None,
    }
}

/// Run all optimization passes on an IR tree.
pub fn optimize(ir: IrNode) -> IrNode {
    let passes: Vec<Box<dyn Pass>> = vec![
        Box::new(ConstantFolding),
        Box::new(PredicatePushdown),
        Box::new(ProjectionPushdown),
    ];

    let mut result = ir;
    for pass in passes {
        result = pass.run(result);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::*;

    #[test]
    fn constant_folding_arithmetic() {
        // 2 + 3 => 5
        let expr = IrExpr::BinaryOp(
            Box::new(IrExpr::Literal(IrLiteral::Int(2))),
            BinOp::Add,
            Box::new(IrExpr::Literal(IrLiteral::Int(3))),
        );
        let result = fold_expr(expr);
        assert!(matches!(result, IrExpr::Literal(IrLiteral::Int(5))));
    }

    #[test]
    fn constant_folding_boolean_and_true() {
        // x AND true => x
        let expr = IrExpr::BinaryOp(
            Box::new(IrExpr::Column("x".to_string())),
            BinOp::And,
            Box::new(IrExpr::Literal(IrLiteral::Bool(true))),
        );
        let result = fold_expr(expr);
        assert!(matches!(result, IrExpr::Column(ref s) if s == "x"));
    }

    #[test]
    fn constant_folding_boolean_or_false() {
        // x OR false => x
        let expr = IrExpr::BinaryOp(
            Box::new(IrExpr::Column("x".to_string())),
            BinOp::Or,
            Box::new(IrExpr::Literal(IrLiteral::Bool(false))),
        );
        let result = fold_expr(expr);
        assert!(matches!(result, IrExpr::Column(ref s) if s == "x"));
    }

    #[test]
    fn constant_folding_removes_true_filter() {
        // Filter with true predicate should be removed
        let ir = IrNode::Filter {
            input: Box::new(IrNode::Scan {
                source: "users".to_string(),
                columns: vec![],
            }),
            predicate: IrExpr::Literal(IrLiteral::Bool(true)),
        };
        let result = fold_constants(ir);
        assert!(matches!(result, IrNode::Scan { .. }));
    }

    #[test]
    fn predicate_pushdown_through_project() {
        // Filter(Project(Scan, [a as x]), x > 10)
        // => Project(Filter(Scan, a > 10), [a as x])
        let ir = IrNode::Filter {
            input: Box::new(IrNode::Project {
                input: Box::new(IrNode::Scan {
                    source: "t".to_string(),
                    columns: vec![],
                }),
                columns: vec![
                    ("x".to_string(), IrExpr::Column("a".to_string())),
                ],
            }),
            predicate: IrExpr::BinaryOp(
                Box::new(IrExpr::Column("x".to_string())),
                BinOp::Gt,
                Box::new(IrExpr::Literal(IrLiteral::Int(10))),
            ),
        };

        let result = push_predicates(ir);

        // Should be Project(Filter(Scan, a > 10), ...)
        match result {
            IrNode::Project { input, .. } => {
                assert!(matches!(*input, IrNode::Filter { .. }));
            }
            _ => panic!("Expected Project at top level"),
        }
    }

    #[test]
    fn projection_pushdown_limits_scan_columns() {
        // Project selecting only 'a' should result in Scan with only 'a'
        let ir = IrNode::Project {
            input: Box::new(IrNode::Scan {
                source: "t".to_string(),
                columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            }),
            columns: vec![
                ("x".to_string(), IrExpr::Column("a".to_string())),
            ],
        };

        let pass = ProjectionPushdown;
        let result = pass.run(ir);

        match result {
            IrNode::Project { input, .. } => {
                match *input {
                    IrNode::Scan { columns, .. } => {
                        assert_eq!(columns, vec!["a".to_string()]);
                    }
                    _ => panic!("Expected Scan"),
                }
            }
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn optimize_runs_all_passes() {
        // A complex tree that exercises multiple passes
        let ir = IrNode::Filter {
            input: Box::new(IrNode::Project {
                input: Box::new(IrNode::Scan {
                    source: "t".to_string(),
                    columns: vec!["a".to_string(), "b".to_string()],
                }),
                columns: vec![
                    ("x".to_string(), IrExpr::Column("a".to_string())),
                ],
            }),
            predicate: IrExpr::BinaryOp(
                Box::new(IrExpr::Column("x".to_string())),
                BinOp::And,
                Box::new(IrExpr::Literal(IrLiteral::Bool(true))),
            ),
        };

        let result = optimize(ir);

        // Constant folding should simplify AND true
        // Predicate pushdown should push filter through project
        match result {
            IrNode::Project { input, .. } => {
                match *input {
                    IrNode::Filter { input: filter_input, .. } => {
                        match *filter_input {
                            IrNode::Scan { columns, .. } => {
                                // Projection pushdown should reduce columns
                                assert_eq!(columns, vec!["a".to_string()]);
                            }
                            _ => panic!("Expected Scan under Filter"),
                        }
                    }
                    _ => panic!("Expected Filter under Project"),
                }
            }
            _ => panic!("Expected Project at top"),
        }
    }
}
