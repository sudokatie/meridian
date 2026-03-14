//! Meridian Code Generation
//!
//! Backend code generators for Meridian.

pub mod backend;
pub mod flink;
pub mod python;
pub mod spark;

pub use backend::*;
pub use flink::FlinkBackend;
pub use python::PolarsBackend;
pub use spark::SparkBackend;
