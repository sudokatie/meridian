//! AST to IR lowering.
//!
//! Converts typed Meridian AST to dataflow IR.

use std::collections::HashMap;

use meridian_parser::{
    BinOp as AstBinOp, Expr, Pipeline, SelectField, SortOrder as AstSortOrder, Statement,
    UnaryOp as AstUnaryOp,
};
use meridian_types::TypeEnv;
use thiserror::Error;

use crate::ir::{BinOp, IrExpr, IrLiteral, IrNode, JoinKind, SortOrder, UnaryOp};

/// Errors that can occur during IR building.
#[derive(Error, Debug)]
pub enum BuildError {
    #[error("pipeline has no 'from' statement")]
    NoFromStatement,

    #[error("undefined source: {0}")]
    UndefinedSource(String),

    #[error("unsupported expression in IR")]
    UnsupportedExpression,
}

/// IR builder state.
pub struct IrBuilder<'a> {
    #[allow(dead_code)] // Reserved for future use in type resolution
    env: &'a TypeEnv,
    sources: HashMap<String, ()>,
}

impl<'a> IrBuilder<'a> {
    /// Create a new IR builder.
    pub fn new(env: &'a TypeEnv) -> Self {
        Self {
            env,
            sources: HashMap::new(),
        }
    }

    /// Register a source.
    pub fn register_source(&mut self, name: &str) {
        self.sources.insert(name.to_string(), ());
    }

    /// Build IR from a pipeline.
    pub fn build_pipeline(&self, pipeline: &Pipeline) -> Result<IrNode, BuildError> {
        let mut current: Option<IrNode> = None;

        for stmt in &pipeline.statements {
            current = Some(self.build_statement(stmt, current)?);
        }

        current.ok_or(BuildError::NoFromStatement)
    }

    fn build_statement(
        &self,
        stmt: &Statement,
        input: Option<IrNode>,
    ) -> Result<IrNode, BuildError> {
        match stmt {
            Statement::From(from) => {
                let source = &from.source.name;
                // In a real implementation, we'd look up the source schema
                // For now, just create a Scan with no specific columns
                Ok(IrNode::Scan {
                    source: source.clone(),
                    columns: vec![],
                })
            }

            Statement::Where(where_stmt) => {
                let input = input.ok_or(BuildError::NoFromStatement)?;
                let predicate = self.build_expr(&where_stmt.condition)?;
                Ok(IrNode::Filter {
                    input: Box::new(input),
                    predicate,
                })
            }

            Statement::Select(select) => {
                let input = input.ok_or(BuildError::NoFromStatement)?;
                let columns = self.build_select_fields(&select.fields)?;
                Ok(IrNode::Project {
                    input: Box::new(input),
                    columns,
                })
            }

            Statement::GroupBy(group) => {
                let input = input.ok_or(BuildError::NoFromStatement)?;
                let group_by: Vec<IrExpr> = group
                    .keys
                    .iter()
                    .map(|e| self.build_expr(e))
                    .collect::<Result<_, _>>()?;
                Ok(IrNode::Aggregate {
                    input: Box::new(input),
                    group_by,
                    aggregates: vec![], // Aggregates are built from select
                })
            }

            Statement::OrderBy(order) => {
                let input = input.ok_or(BuildError::NoFromStatement)?;
                let by: Vec<(IrExpr, SortOrder)> = order
                    .keys
                    .iter()
                    .map(|(e, sort_order)| {
                        let expr = self.build_expr(e)?;
                        let order = match sort_order {
                            AstSortOrder::Asc => SortOrder::Asc,
                            AstSortOrder::Desc => SortOrder::Desc,
                        };
                        Ok((expr, order))
                    })
                    .collect::<Result<_, BuildError>>()?;
                Ok(IrNode::Sort {
                    input: Box::new(input),
                    by,
                })
            }

            Statement::Limit(limit) => {
                let input = input.ok_or(BuildError::NoFromStatement)?;
                Ok(IrNode::Limit {
                    input: Box::new(input),
                    count: limit.count as usize,
                })
            }

            Statement::Join(join) => {
                let input = input.ok_or(BuildError::NoFromStatement)?;
                let right = IrNode::Scan {
                    source: join.source.name.clone(),
                    columns: vec![],
                };
                let on = self.build_expr(&join.condition)?;
                let kind = match join.kind {
                    meridian_parser::JoinKind::Inner => JoinKind::Inner,
                    meridian_parser::JoinKind::Left => JoinKind::Left,
                    meridian_parser::JoinKind::Right => JoinKind::Right,
                };
                Ok(IrNode::Join {
                    kind,
                    left: Box::new(input),
                    right: Box::new(right),
                    on,
                })
            }
        }
    }

    fn build_select_fields(
        &self,
        fields: &[SelectField],
    ) -> Result<Vec<(String, IrExpr)>, BuildError> {
        let mut result = Vec::new();

        for field in fields {
            match field {
                SelectField::Named { name, expr, .. } => {
                    let ir_expr = self.build_expr(expr)?;
                    result.push((name.name.clone(), ir_expr));
                }
                SelectField::Expr { expr, .. } => {
                    // For unnamed expressions, try to derive a name
                    let name = self.derive_column_name(expr);
                    let ir_expr = self.build_expr(expr)?;
                    result.push((name, ir_expr));
                }
                SelectField::Spread { source, .. } => {
                    // Spread all columns from source - simplified handling
                    result.push((format!("{}.*", source.name), IrExpr::Column(source.name.clone())));
                }
            }
        }

        Ok(result)
    }

    fn derive_column_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Ident(ident) => ident.name.clone(),
            Expr::Field(_, field, _) => field.name.clone(),
            _ => "expr".to_string(),
        }
    }

    fn build_expr(&self, expr: &Expr) -> Result<IrExpr, BuildError> {
        match expr {
            Expr::Int(value, _) => Ok(IrExpr::Literal(IrLiteral::Int(*value))),
            Expr::Float(value, _) => Ok(IrExpr::Literal(IrLiteral::Float(*value))),
            Expr::String(value, _) => Ok(IrExpr::Literal(IrLiteral::String(value.clone()))),
            Expr::Bool(value, _) => Ok(IrExpr::Literal(IrLiteral::Bool(*value))),

            Expr::Ident(ident) => Ok(IrExpr::Column(ident.name.clone())),

            Expr::Field(base, field, _) => {
                // For field access like `order.amount`, generate `order.amount` column ref
                if let Expr::Ident(base_ident) = base.as_ref() {
                    Ok(IrExpr::Column(format!("{}.{}", base_ident.name, field.name)))
                } else {
                    Err(BuildError::UnsupportedExpression)
                }
            }

            Expr::Binary(left, op, right, _) => {
                let left_ir = self.build_expr(left)?;
                let right_ir = self.build_expr(right)?;
                let bin_op = self.convert_bin_op(*op);
                Ok(IrExpr::BinaryOp(Box::new(left_ir), bin_op, Box::new(right_ir)))
            }

            Expr::Unary(op, operand, _) => {
                let operand_ir = self.build_expr(operand)?;
                let unary_op = self.convert_unary_op(*op);
                Ok(IrExpr::UnaryOp(unary_op, Box::new(operand_ir)))
            }

            Expr::Call(name, args, _) => {
                let args_ir: Vec<IrExpr> = args
                    .iter()
                    .map(|a| self.build_expr(a))
                    .collect::<Result<_, _>>()?;
                Ok(IrExpr::Call(name.name.clone(), args_ir))
            }

            Expr::List(_elements, _) => {
                // Lists aren't directly representable in SQL IR
                // For now, return an error
                Err(BuildError::UnsupportedExpression)
            }

            Expr::Match(_, _, _) => {
                // Match expressions need to be converted to CASE WHEN
                // TODO: implement match -> CASE conversion
                Err(BuildError::UnsupportedExpression)
            }

            Expr::NullCoalesce(left, right, _) => {
                // COALESCE(left, right)
                let left_ir = self.build_expr(left)?;
                let right_ir = self.build_expr(right)?;
                Ok(IrExpr::Call("coalesce".to_string(), vec![left_ir, right_ir]))
            }
        }
    }

    fn convert_bin_op(&self, op: AstBinOp) -> BinOp {
        match op {
            AstBinOp::Add => BinOp::Add,
            AstBinOp::Sub => BinOp::Sub,
            AstBinOp::Mul => BinOp::Mul,
            AstBinOp::Div => BinOp::Div,
            AstBinOp::Mod => BinOp::Mod,
            AstBinOp::Eq => BinOp::Eq,
            AstBinOp::Ne => BinOp::Ne,
            AstBinOp::Lt => BinOp::Lt,
            AstBinOp::Le => BinOp::Le,
            AstBinOp::Gt => BinOp::Gt,
            AstBinOp::Ge => BinOp::Ge,
            AstBinOp::And => BinOp::And,
            AstBinOp::Or => BinOp::Or,
        }
    }

    fn convert_unary_op(&self, op: AstUnaryOp) -> UnaryOp {
        match op {
            AstUnaryOp::Neg => UnaryOp::Neg,
            AstUnaryOp::Not => UnaryOp::Not,
        }
    }
}

/// Build IR from a pipeline.
pub fn build_pipeline(pipeline: &Pipeline, env: &TypeEnv) -> Result<IrNode, BuildError> {
    let builder = IrBuilder::new(env);
    builder.build_pipeline(pipeline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_parser::parse;
    use meridian_types::TypeEnv;

    // Test helper - just parses pipeline and builds IR
    fn build_from_source(source: &str) -> Result<IrNode, BuildError> {
        let program = parse(source).expect("parse failed");
        
        // Create environment with orders source registered
        let mut env = TypeEnv::new();
        env.register_builtins();
        
        // Manually register the source with a struct type
        let order_fields = vec![
            ("id".to_string(), meridian_types::Type::String),
            ("amount".to_string(), meridian_types::Type::Float),
            ("status".to_string(), meridian_types::Type::String),
            ("total".to_string(), meridian_types::Type::Float),
        ];
        env.define_source("orders", None, meridian_types::Type::Struct(order_fields));

        // Find the first pipeline
        for item in &program.items {
            if let meridian_parser::Item::Pipeline(pipeline) = item {
                return build_pipeline(pipeline, &env);
            }
        }
        Err(BuildError::NoFromStatement)
    }

    #[test]
    fn test_simple_pipeline() {
        let ir = build_from_source(
            r#"
            pipeline simple {
                from orders
                where amount > 100
            }
        "#,
        )
        .unwrap();

        // Should be Filter(Scan)
        assert!(matches!(ir, IrNode::Filter { .. }));
    }

    #[test]
    fn test_pipeline_with_select() {
        let ir = build_from_source(
            r#"
            pipeline projected {
                from orders
                select {
                    id,
                    total: amount * 2
                }
            }
        "#,
        )
        .unwrap();

        // Should be Project(Scan)
        assert!(matches!(ir, IrNode::Project { .. }));
    }

    #[test]
    fn test_pipeline_with_limit() {
        let ir = build_from_source(
            r#"
            pipeline limited {
                from orders
                limit 10
            }
        "#,
        )
        .unwrap();

        // Should be Limit(Scan)
        assert!(matches!(ir, IrNode::Limit { count: 10, .. }));
    }

    #[test]
    fn test_pipeline_with_order() {
        let ir = build_from_source(
            r#"
            pipeline ordered {
                from orders
                order by amount desc
            }
        "#,
        )
        .unwrap();

        // Should be Sort(Scan)
        assert!(matches!(ir, IrNode::Sort { .. }));
    }

    #[test]
    fn test_complex_pipeline() {
        let ir = build_from_source(
            r#"
            pipeline complex {
                from orders
                where status == "completed"
                select {
                    id,
                    total: amount * 2
                }
                order by total desc
                limit 10
            }
        "#,
        )
        .unwrap();

        // Should be Limit(Sort(Project(Filter(Scan))))
        assert!(matches!(ir, IrNode::Limit { .. }));
    }

    #[test]
    fn test_empty_pipeline_error() {
        let program = parse("pipeline empty {}").expect("parse failed");
        let env = TypeEnv::new();

        for item in &program.items {
            if let meridian_parser::Item::Pipeline(pipeline) = item {
                let result = build_pipeline(pipeline, &env);
                assert!(result.is_err());
                return;
            }
        }
        panic!("No pipeline found");
    }
}
