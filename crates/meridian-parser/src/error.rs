//! Parse error types with miette integration.

use crate::span::Span;
use miette::{Diagnostic, SourceSpan};
use thiserror::Error;

/// A parse error with source context.
#[derive(Error, Debug, Clone, Diagnostic)]
pub enum ParseError {
    #[error("unexpected token: expected {expected}, found {found}")]
    #[diagnostic(code(parse::unexpected_token))]
    UnexpectedToken {
        #[source_code]
        src: String,
        #[label("found {found} here")]
        span: SourceSpan,
        expected: String,
        found: String,
    },

    #[error("unexpected end of file: expected {expected}")]
    #[diagnostic(code(parse::unexpected_eof))]
    UnexpectedEof { expected: String },

    #[error("invalid number: {message}")]
    #[diagnostic(code(parse::invalid_number))]
    InvalidNumber {
        #[source_code]
        src: String,
        #[label("invalid number")]
        span: SourceSpan,
        message: String,
    },

    #[error("invalid string: {message}")]
    #[diagnostic(code(parse::invalid_string))]
    InvalidString {
        #[source_code]
        src: String,
        #[label("invalid string")]
        span: SourceSpan,
        message: String,
    },
}

impl ParseError {
    pub fn unexpected_token(span: Span, expected: impl Into<String>, found: impl Into<String>) -> Self {
        ParseError::UnexpectedToken {
            src: String::new(), // Will be filled in later
            span: (span.start, span.end - span.start).into(),
            expected: expected.into(),
            found: found.into(),
        }
    }

    pub fn unexpected_eof(expected: impl Into<String>) -> Self {
        ParseError::UnexpectedEof {
            expected: expected.into(),
        }
    }

    /// Add source code context to the error.
    pub fn with_source(self, source: &str) -> Self {
        match self {
            ParseError::UnexpectedToken { span, expected, found, .. } => {
                ParseError::UnexpectedToken {
                    src: source.to_string(),
                    span,
                    expected,
                    found,
                }
            }
            ParseError::InvalidNumber { span, message, .. } => {
                ParseError::InvalidNumber {
                    src: source.to_string(),
                    span,
                    message,
                }
            }
            ParseError::InvalidString { span, message, .. } => {
                ParseError::InvalidString {
                    src: source.to_string(),
                    span,
                    message,
                }
            }
            other => other,
        }
    }
}

/// Legacy constructor for tests that use Span directly.
impl ParseError {
    pub fn from_span(span: Span, expected: impl Into<String>, found: impl Into<String>) -> Self {
        Self::unexpected_token(span, expected, found)
    }
}
