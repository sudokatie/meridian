//! Type inference for Meridian expressions.

use meridian_parser::{Expr, BinOp, UnaryOp};

use crate::env::TypeEnv;
use crate::error::TypeError;
use crate::types::Type;

/// Infer the type of an expression.
pub fn infer_expr(expr: &Expr, env: &TypeEnv) -> Result<Type, TypeError> {
    match expr {
        // Literals
        Expr::Int(_, _) => Ok(Type::Int),
        Expr::Float(_, _) => Ok(Type::Float),
        Expr::String(_, _) => Ok(Type::String),
        Expr::Bool(_, _) => Ok(Type::Bool),

        // Lists
        Expr::List(elements, _span) => {
            if elements.is_empty() {
                Ok(Type::List(Box::new(Type::Unknown)))
            } else {
                let elem_ty = infer_expr(&elements[0], env)?;
                Ok(Type::List(Box::new(elem_ty)))
            }
        }

        // Identifiers
        Expr::Ident(ident) => {
            // Return Unknown for undefined variables (allows code generation to proceed)
            // TODO: This should error once schema fields are properly wired to scope
            Ok(env.resolve(&ident.name).unwrap_or(Type::Unknown))
        }

        // Field access
        Expr::Field(base, field, _span) => {
            let base_ty = infer_expr(base, env)?;
            // Return Unknown for unknown fields (allows code generation to proceed)
            // TODO: This should error once schema fields are properly wired
            Ok(env.get_field_type(&base_ty, &field.name).unwrap_or(Type::Unknown))
        }

        // Binary operations
        Expr::Binary(left, op, right, span) => {
            let left_ty = infer_expr(left, env)?;
            let right_ty = infer_expr(right, env)?;
            infer_binary_op(*op, &left_ty, &right_ty, *span)
        }

        // Unary operations
        Expr::Unary(op, operand, span) => {
            let operand_ty = infer_expr(operand, env)?;
            infer_unary_op(*op, &operand_ty, *span)
        }

        // Function calls
        Expr::Call(name, args, span) => {
            let func = env.get_function(&name.name).ok_or_else(|| TypeError::UndefinedFunction {
                name: name.name.clone(),
                span: name.span,
            })?;

            if args.len() != func.params.len() {
                return Err(TypeError::WrongArity {
                    expected: func.params.len(),
                    found: args.len(),
                    span: *span,
                });
            }

            Ok(func.return_type.clone())
        }

        // Match expressions
        Expr::Match(arms, default, _span) => {
            if let Some((_, body)) = arms.first() {
                infer_expr(body, env)
            } else if let Some(default_expr) = default {
                infer_expr(default_expr, env)
            } else {
                Ok(Type::Unknown)
            }
        }

        // Null coalesce
        Expr::NullCoalesce(left, _right, _span) => {
            let left_ty = infer_expr(left, env)?;
            // Result type is non-nullable version of left
            match left_ty {
                Type::Nullable(inner) => Ok(*inner),
                other => Ok(other),
            }
        }

        // Non-null assertion
        Expr::NonNullAssert(inner, _span) => {
            let inner_ty = infer_expr(inner, env)?;
            // Result is non-nullable version of inner
            match inner_ty {
                Type::Nullable(inner) => Ok(*inner),
                other => Ok(other),
            }
        }
    }
}

/// Infer result type of a binary operation.
fn infer_binary_op(
    op: BinOp,
    left: &Type,
    right: &Type,
    span: meridian_parser::Span,
) -> Result<Type, TypeError> {
    match op {
        // Arithmetic: numeric operands, numeric result
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            if is_numeric(left) && is_numeric(right) {
                Ok(promote_numeric(left, right))
            } else {
                Err(TypeError::InvalidOperator {
                    op: format!("{:?}", op),
                    left: left.clone(),
                    right: right.clone(),
                    span,
                })
            }
        }

        // Comparison: any comparable operands, boolean result
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            if types_comparable(left, right) {
                Ok(Type::Bool)
            } else {
                Err(TypeError::InvalidOperator {
                    op: format!("{:?}", op),
                    left: left.clone(),
                    right: right.clone(),
                    span,
                })
            }
        }

        // Logical: boolean operands, boolean result
        BinOp::And | BinOp::Or => {
            if matches!(left, Type::Bool | Type::Unknown) && matches!(right, Type::Bool | Type::Unknown) {
                Ok(Type::Bool)
            } else {
                Err(TypeError::InvalidOperator {
                    op: format!("{:?}", op),
                    left: left.clone(),
                    right: right.clone(),
                    span,
                })
            }
        }

        // String concatenation
        BinOp::Concat => {
            if matches!(left, Type::String | Type::Unknown) && matches!(right, Type::String | Type::Unknown) {
                Ok(Type::String)
            } else {
                Err(TypeError::InvalidOperator {
                    op: "++".to_string(),
                    left: left.clone(),
                    right: right.clone(),
                    span,
                })
            }
        }
    }
}

/// Infer result type of a unary operation.
fn infer_unary_op(
    op: UnaryOp,
    operand: &Type,
    span: meridian_parser::Span,
) -> Result<Type, TypeError> {
    match op {
        UnaryOp::Neg => {
            if is_numeric(operand) {
                Ok(operand.clone())
            } else {
                Err(TypeError::InvalidNegation {
                    ty: operand.clone(),
                    span,
                })
            }
        }
        UnaryOp::Not => {
            if matches!(operand, Type::Bool | Type::Unknown) {
                Ok(Type::Bool)
            } else {
                Err(TypeError::InvalidNot {
                    ty: operand.clone(),
                    span,
                })
            }
        }
        // is null / is not null always return bool
        UnaryOp::IsNull | UnaryOp::IsNotNull => Ok(Type::Bool),
    }
}

/// Check if a type is numeric.
fn is_numeric(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Int | Type::BigInt | Type::Float | Type::Double | Type::Decimal { .. } | Type::Unknown
    )
}

/// Promote two numeric types to a common type.
fn promote_numeric(left: &Type, right: &Type) -> Type {
    // Simple promotion: Float > Int
    match (left, right) {
        (Type::Double, _) | (_, Type::Double) => Type::Double,
        (Type::Float, _) | (_, Type::Float) => Type::Float,
        (Type::Decimal { precision, scale }, _) | (_, Type::Decimal { precision, scale }) => {
            Type::Decimal {
                precision: *precision,
                scale: *scale,
            }
        }
        (Type::BigInt, _) | (_, Type::BigInt) => Type::BigInt,
        _ => Type::Int,
    }
}

/// Check if two types can be compared.
fn types_comparable(left: &Type, right: &Type) -> bool {
    // Same types are comparable
    if left == right {
        return true;
    }

    // Numeric types are mutually comparable
    if is_numeric(left) && is_numeric(right) {
        return true;
    }

    // Unknown is comparable to anything
    if matches!(left, Type::Unknown) || matches!(right, Type::Unknown) {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_parser::Span;

    fn span() -> Span {
        Span::new(0, 0)
    }

    #[test]
    fn test_infer_int_literal() {
        let env = TypeEnv::new();
        let expr = Expr::Int(42, span());
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::Int);
    }

    #[test]
    fn test_infer_float_literal() {
        let env = TypeEnv::new();
        let expr = Expr::Float(3.14, span());
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::Float);
    }

    #[test]
    fn test_infer_string_literal() {
        let env = TypeEnv::new();
        let expr = Expr::String("hello".into(), span());
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::String);
    }

    #[test]
    fn test_infer_bool_literal() {
        let env = TypeEnv::new();
        let expr = Expr::Bool(true, span());
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::Bool);
    }

    #[test]
    fn test_infer_addition() {
        let env = TypeEnv::new();
        let expr = Expr::Binary(
            Box::new(Expr::Int(1, span())),
            BinOp::Add,
            Box::new(Expr::Int(2, span())),
            span(),
        );
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::Int);
    }

    #[test]
    fn test_infer_float_promotion() {
        let env = TypeEnv::new();
        let expr = Expr::Binary(
            Box::new(Expr::Int(1, span())),
            BinOp::Add,
            Box::new(Expr::Float(2.5, span())),
            span(),
        );
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::Float);
    }

    #[test]
    fn test_infer_comparison() {
        let env = TypeEnv::new();
        let expr = Expr::Binary(
            Box::new(Expr::Int(1, span())),
            BinOp::Lt,
            Box::new(Expr::Int(2, span())),
            span(),
        );
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::Bool);
    }

    #[test]
    fn test_infer_undefined_variable() {
        let env = TypeEnv::new();
        let expr = Expr::Ident(meridian_parser::Ident::new("x", span()));
        // Undefined variables return Unknown (lenient for code generation)
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::Unknown);
    }

    #[test]
    fn test_infer_defined_variable() {
        let mut env = TypeEnv::new();
        env.define_local("x", Type::Int);
        let expr = Expr::Ident(meridian_parser::Ident::new("x", span()));
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::Int);
    }

    #[test]
    fn test_infer_function_call() {
        let mut env = TypeEnv::new();
        env.register_builtins();
        let expr = Expr::Call(
            meridian_parser::Ident::new("length", span()),
            vec![Expr::String("test".into(), span())],
            span(),
        );
        assert_eq!(infer_expr(&expr, &env).unwrap(), Type::Int);
    }

    #[test]
    fn test_invalid_operator() {
        let env = TypeEnv::new();
        let expr = Expr::Binary(
            Box::new(Expr::String("a".into(), span())),
            BinOp::Add,
            Box::new(Expr::Int(1, span())),
            span(),
        );
        assert!(infer_expr(&expr, &env).is_err());
    }
}
