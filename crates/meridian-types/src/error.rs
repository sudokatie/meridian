//! Type error definitions.

use meridian_parser::Span;
use thiserror::Error;

use crate::types::Type;

/// A type checking error.
#[derive(Error, Debug, Clone)]
pub enum TypeError {
    #[error("type mismatch: expected {expected:?}, found {found:?}")]
    TypeMismatch {
        expected: Type,
        found: Type,
        span: Span,
    },

    #[error("undefined variable: {name}")]
    UndefinedVariable { name: String, span: Span },

    #[error("undefined schema: {name}")]
    UndefinedSchema { name: String, span: Span },

    #[error("undefined field '{field}' on type {on_type:?}")]
    UndefinedField {
        field: String,
        on_type: Type,
        span: Span,
    },

    #[error("undefined function: {name}")]
    UndefinedFunction { name: String, span: Span },

    #[error("wrong number of arguments: expected {expected}, found {found}")]
    WrongArity {
        expected: usize,
        found: usize,
        span: Span,
    },

    #[error("cannot apply operator {op} to types {left:?} and {right:?}")]
    InvalidOperator {
        op: String,
        left: Type,
        right: Type,
        span: Span,
    },

    #[error("cannot negate type {ty:?}")]
    InvalidNegation { ty: Type, span: Span },

    #[error("cannot apply 'not' to type {ty:?}")]
    InvalidNot { ty: Type, span: Span },

    #[error("non-boolean condition: expected Bool, found {found:?}")]
    NonBooleanCondition { found: Type, span: Span },

    #[error("duplicate definition: {name}")]
    DuplicateDefinition { name: String, span: Span },

    #[error("stream-stream join requires 'within' temporal bounds")]
    StreamJoinMissingWithin { span: Span },

    #[error("watermark expression must be a timestamp minus a duration")]
    InvalidWatermark { span: Span },
}

impl TypeError {
    /// Get the source span of this error.
    pub fn span(&self) -> Span {
        match self {
            TypeError::TypeMismatch { span, .. } => *span,
            TypeError::UndefinedVariable { span, .. } => *span,
            TypeError::UndefinedSchema { span, .. } => *span,
            TypeError::UndefinedField { span, .. } => *span,
            TypeError::UndefinedFunction { span, .. } => *span,
            TypeError::WrongArity { span, .. } => *span,
            TypeError::InvalidOperator { span, .. } => *span,
            TypeError::InvalidNegation { span, .. } => *span,
            TypeError::InvalidNot { span, .. } => *span,
            TypeError::NonBooleanCondition { span, .. } => *span,
            TypeError::DuplicateDefinition { span, .. } => *span,
            TypeError::StreamJoinMissingWithin { span, .. } => *span,
            TypeError::InvalidWatermark { span, .. } => *span,
        }
    }
}
