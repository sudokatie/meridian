//! Meridian Parser
//!
//! Lexer and parser for the Meridian data transformation language.

pub mod span;
pub mod token;
pub mod lexer;
pub mod ast;
pub mod error;
pub mod parser;
pub mod formatter;

pub use ast::*;
pub use error::ParseError;
pub use parser::parse;
pub use span::Span;
pub use formatter::format_program;
