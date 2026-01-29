//! Backend trait and implementations.

use meridian_ir::IrNode;

/// A code generation backend.
pub trait Backend {
    /// Generate code from an IR graph.
    fn generate(&self, ir: &IrNode) -> Result<String, CodegenError>;
}

/// Code generation error.
#[derive(Debug, thiserror::Error)]
pub enum CodegenError {
    #[error("unsupported IR node: {0}")]
    UnsupportedNode(String),
}

/// DuckDB SQL backend.
pub struct DuckDbBackend;

impl Backend for DuckDbBackend {
    fn generate(&self, _ir: &IrNode) -> Result<String, CodegenError> {
        // TODO: Implement SQL generation
        Ok("SELECT 1".to_string())
    }
}
