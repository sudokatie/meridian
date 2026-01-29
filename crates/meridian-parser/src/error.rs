//! Parse error types.

use crate::span::Span;
use thiserror::Error;

/// A parse error.
#[derive(Error, Debug, Clone)]
pub enum ParseError {
    #[error("unexpected token at {span:?}: expected {expected}, found {found}")]
    UnexpectedToken {
        span: Span,
        expected: String,
        found: String,
    },

    #[error("unexpected end of file: expected {expected}")]
    UnexpectedEof { expected: String },

    #[error("invalid number at {span:?}: {message}")]
    InvalidNumber { span: Span, message: String },

    #[error("invalid string at {span:?}: {message}")]
    InvalidString { span: Span, message: String },
}

impl ParseError {
    pub fn unexpected_token(span: Span, expected: impl Into<String>, found: impl Into<String>) -> Self {
        ParseError::UnexpectedToken {
            span,
            expected: expected.into(),
            found: found.into(),
        }
    }

    pub fn unexpected_eof(expected: impl Into<String>) -> Self {
        ParseError::UnexpectedEof {
            expected: expected.into(),
        }
    }
}
