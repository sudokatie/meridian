//! Type system for the Meridian data transformation language.
//!
//! This crate provides:
//! - Type definitions (`types`)
//! - Type environment for symbol tracking (`env`)
//! - Type inference for expressions (`inference`)
//! - Type checking for programs (`checker`)

pub mod types;
pub mod env;
pub mod error;
pub mod inference;
pub mod checker;

pub use types::Type;
pub use env::TypeEnv;
pub use error::TypeError;
pub use inference::infer_expr;
pub use checker::check_program;
