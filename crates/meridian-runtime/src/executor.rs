//! Pipeline executor.

use duckdb::Connection;

/// Pipeline executor using DuckDB.
pub struct Executor {
    conn: Connection,
}

/// Execution error.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("execution error: {0}")]
    Execution(String),
}

/// Execution statistics.
#[derive(Debug, Default)]
pub struct ExecutionStats {
    pub rows_read: usize,
    pub rows_written: usize,
    pub duration_ms: u64,
}

impl Executor {
    /// Create a new executor with an in-memory database.
    pub fn new() -> Result<Self, ExecutorError> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    /// Execute a SQL query.
    pub fn execute(&self, sql: &str) -> Result<ExecutionStats, ExecutorError> {
        self.conn.execute_batch(sql)?;
        Ok(ExecutionStats::default())
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new().expect("failed to create executor")
    }
}
