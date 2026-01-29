//! Meridian Intermediate Representation
//!
//! Dataflow IR for Meridian pipelines.

pub mod ir;
pub mod builder;
pub mod optimizer;

pub use ir::*;
pub use builder::{build_pipeline, BuildError, IrBuilder};
pub use optimizer::{optimize, Pass, PredicatePushdown, ProjectionPushdown, ConstantFolding};
