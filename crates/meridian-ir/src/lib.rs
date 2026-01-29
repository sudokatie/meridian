//! Meridian Intermediate Representation
//!
//! Dataflow IR for Meridian pipelines.

pub mod ir;
pub mod builder;

pub use ir::*;
pub use builder::{build_pipeline, BuildError, IrBuilder};
