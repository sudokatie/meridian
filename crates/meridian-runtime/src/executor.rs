//! Pipeline executor using DuckDB.

use std::path::Path;
use std::time::Instant;

use duckdb::{Connection, Result as DuckResult};

/// Pipeline executor using DuckDB.
pub struct Executor {
    conn: Connection,
}

/// Execution error.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("unsupported file format: {0}")]
    UnsupportedFormat(String),

    #[error("source not found: {0}")]
    SourceNotFound(String),

    #[error("execution error: {0}")]
    Execution(String),
}

/// Execution statistics.
#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    pub rows_read: usize,
    pub rows_written: usize,
    pub duration_ms: u64,
}

/// Supported file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    Json,
    Parquet,
}

impl FileFormat {
    /// Detect format from file extension.
    pub fn from_path(path: &Path) -> Option<Self> {
        path.extension()
            .and_then(|ext| ext.to_str())
            .and_then(|ext| match ext.to_lowercase().as_str() {
                "csv" => Some(FileFormat::Csv),
                "json" | "jsonl" | "ndjson" => Some(FileFormat::Json),
                "parquet" | "pq" => Some(FileFormat::Parquet),
                _ => None,
            })
    }

    /// Get DuckDB read function for this format.
    pub fn read_function(&self) -> &'static str {
        match self {
            FileFormat::Csv => "read_csv_auto",
            FileFormat::Json => "read_json_auto",
            FileFormat::Parquet => "read_parquet",
        }
    }

    /// Get DuckDB COPY format name.
    pub fn copy_format(&self) -> &'static str {
        match self {
            FileFormat::Csv => "CSV",
            FileFormat::Json => "JSON",
            FileFormat::Parquet => "PARQUET",
        }
    }
}

impl Executor {
    /// Create a new executor with an in-memory database.
    pub fn new() -> Result<Self, ExecutorError> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    /// Create an executor with a file-backed database.
    pub fn with_file(path: &Path) -> Result<Self, ExecutorError> {
        let conn = Connection::open(path)?;
        Ok(Self { conn })
    }

    /// Load a source file into a DuckDB table.
    pub fn load_source(&self, name: &str, path: &Path) -> Result<ExecutionStats, ExecutorError> {
        let start = Instant::now();

        // Check file exists
        if !path.exists() {
            return Err(ExecutorError::SourceNotFound(path.display().to_string()));
        }

        // Detect format
        let format = FileFormat::from_path(path)
            .ok_or_else(|| ExecutorError::UnsupportedFormat(
                path.extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("unknown")
                    .to_string()
            ))?;

        // Create table from file
        let sql = format!(
            "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM {}('{}')",
            name,
            format.read_function(),
            path.display()
        );

        self.conn.execute_batch(&sql)?;

        // Get row count
        let rows_read = self.count_rows(name)?;

        Ok(ExecutionStats {
            rows_read,
            rows_written: 0,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Load a source with explicit format.
    pub fn load_source_with_format(
        &self,
        name: &str,
        path: &Path,
        format: FileFormat,
    ) -> Result<ExecutionStats, ExecutorError> {
        let start = Instant::now();

        if !path.exists() {
            return Err(ExecutorError::SourceNotFound(path.display().to_string()));
        }

        let sql = format!(
            "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM {}('{}')",
            name,
            format.read_function(),
            path.display()
        );

        self.conn.execute_batch(&sql)?;

        let rows_read = self.count_rows(name)?;

        Ok(ExecutionStats {
            rows_read,
            rows_written: 0,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Execute a SQL query and return statistics.
    pub fn execute(&self, sql: &str) -> Result<ExecutionStats, ExecutorError> {
        let start = Instant::now();

        self.conn.execute_batch(sql)?;

        Ok(ExecutionStats {
            rows_read: 0,
            rows_written: 0,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Execute a query and return the result as a new table.
    pub fn execute_into_table(&self, sql: &str, table_name: &str) -> Result<ExecutionStats, ExecutorError> {
        let start = Instant::now();

        let create_sql = format!(
            "CREATE OR REPLACE TABLE \"{}\" AS {}",
            table_name, sql
        );

        self.conn.execute_batch(&create_sql)?;

        let rows_written = self.count_rows(table_name)?;

        Ok(ExecutionStats {
            rows_read: 0,
            rows_written,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Execute a query and write results to a file.
    pub fn execute_to_file(&self, sql: &str, path: &Path) -> Result<ExecutionStats, ExecutorError> {
        let start = Instant::now();

        // Detect format from output path
        let format = FileFormat::from_path(path)
            .ok_or_else(|| ExecutorError::UnsupportedFormat(
                path.extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("unknown")
                    .to_string()
            ))?;

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Use COPY to write results
        let copy_sql = format!(
            "COPY ({}) TO '{}' (FORMAT {})",
            sql,
            path.display(),
            format.copy_format()
        );

        self.conn.execute_batch(&copy_sql)?;

        // Count rows by running the query again (not ideal but simple)
        let count_sql = format!("SELECT COUNT(*) FROM ({})", sql);
        let rows_written = self.query_count(&count_sql)?;

        Ok(ExecutionStats {
            rows_read: 0,
            rows_written,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Write a table to a file.
    pub fn write_table_to_file(&self, table_name: &str, path: &Path) -> Result<ExecutionStats, ExecutorError> {
        let start = Instant::now();

        let format = FileFormat::from_path(path)
            .ok_or_else(|| ExecutorError::UnsupportedFormat(
                path.extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("unknown")
                    .to_string()
            ))?;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let rows = self.count_rows(table_name)?;

        let copy_sql = format!(
            "COPY \"{}\" TO '{}' (FORMAT {})",
            table_name,
            path.display(),
            format.copy_format()
        );

        self.conn.execute_batch(&copy_sql)?;

        Ok(ExecutionStats {
            rows_read: rows,
            rows_written: rows,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// List all tables in the database.
    pub fn list_tables(&self) -> Result<Vec<String>, ExecutorError> {
        let mut stmt = self.conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        
        let mut tables = Vec::new();
        for row in rows {
            tables.push(row?);
        }
        Ok(tables)
    }

    /// Get the schema of a table.
    pub fn describe_table(&self, table_name: &str) -> Result<Vec<(String, String)>, ExecutorError> {
        let sql = format!("DESCRIBE \"{}\"", table_name);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut columns = Vec::new();
        for row in rows {
            columns.push(row?);
        }
        Ok(columns)
    }

    /// Count rows in a table.
    fn count_rows(&self, table_name: &str) -> Result<usize, ExecutorError> {
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
        self.query_count(&sql)
    }

    /// Execute a count query.
    fn query_count(&self, sql: &str) -> Result<usize, ExecutorError> {
        let mut stmt = self.conn.prepare(sql)?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count as usize)
    }

    /// Execute a query and print results to stdout (for debugging).
    pub fn query_print(&self, sql: &str) -> Result<usize, ExecutorError> {
        let mut stmt = self.conn.prepare(sql)?;
        let column_count = stmt.column_count();
        
        // Print header
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).map_or("?".to_string(), |s| s.to_string()))
            .collect();
        println!("{}", column_names.join("\t"));
        println!("{}", "-".repeat(column_names.iter().map(|s| s.len()).sum::<usize>() + column_count));

        // Print rows
        let mut rows = stmt.query([])?;
        let mut count = 0;
        while let Some(row) = rows.next()? {
            let values: Vec<String> = (0..column_count)
                .map(|i| {
                    row.get::<_, String>(i)
                        .unwrap_or_else(|_| "NULL".to_string())
                })
                .collect();
            println!("{}", values.join("\t"));
            count += 1;
        }

        Ok(count)
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new().expect("failed to create executor")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_csv(dir: &TempDir, name: &str, content: &str) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn test_executor_new() {
        let exec = Executor::new();
        assert!(exec.is_ok());
    }

    #[test]
    fn test_execute_simple_query() {
        let exec = Executor::new().unwrap();
        let stats = exec.execute("SELECT 1 + 1 AS result").unwrap();
        assert!(stats.duration_ms < 1000);
    }

    #[test]
    fn test_load_csv_source() {
        let dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&dir, "test.csv", "id,name,value\n1,alice,100\n2,bob,200\n3,charlie,300\n");

        let exec = Executor::new().unwrap();
        let stats = exec.load_source("test_data", &csv_path).unwrap();

        assert_eq!(stats.rows_read, 3);
    }

    #[test]
    fn test_execute_with_loaded_source() {
        let dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&dir, "orders.csv", "id,amount,status\n1,100,completed\n2,200,pending\n3,300,completed\n");

        let exec = Executor::new().unwrap();
        exec.load_source("orders", &csv_path).unwrap();

        // Execute a query on the loaded data
        let stats = exec.execute_into_table(
            "SELECT * FROM orders WHERE status = 'completed'",
            "completed_orders"
        ).unwrap();

        assert_eq!(stats.rows_written, 2);
    }

    #[test]
    fn test_write_to_csv() {
        let dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&dir, "input.csv", "id,value\n1,100\n2,200\n");
        let output_path = dir.path().join("output.csv");

        let exec = Executor::new().unwrap();
        exec.load_source("input", &csv_path).unwrap();

        let stats = exec.execute_to_file(
            "SELECT id, value * 2 AS doubled FROM input",
            &output_path
        ).unwrap();

        assert_eq!(stats.rows_written, 2);
        assert!(output_path.exists());

        // Verify content
        let content = std::fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("200"));
        assert!(content.contains("400"));
    }

    #[test]
    fn test_write_to_parquet() {
        let dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&dir, "input.csv", "id,value\n1,100\n2,200\n");
        let output_path = dir.path().join("output.parquet");

        let exec = Executor::new().unwrap();
        exec.load_source("input", &csv_path).unwrap();

        let stats = exec.execute_to_file(
            "SELECT * FROM input",
            &output_path
        ).unwrap();

        assert_eq!(stats.rows_written, 2);
        assert!(output_path.exists());
    }

    #[test]
    fn test_list_tables() {
        let dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&dir, "test.csv", "id,name\n1,alice\n");

        let exec = Executor::new().unwrap();
        exec.load_source("my_table", &csv_path).unwrap();

        let tables = exec.list_tables().unwrap();
        assert!(tables.contains(&"my_table".to_string()));
    }

    #[test]
    fn test_describe_table() {
        let dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&dir, "test.csv", "id,name,value\n1,alice,100\n");

        let exec = Executor::new().unwrap();
        exec.load_source("test_table", &csv_path).unwrap();

        let columns = exec.describe_table("test_table").unwrap();
        assert_eq!(columns.len(), 3);
        
        let column_names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
        assert!(column_names.contains(&"id"));
        assert!(column_names.contains(&"name"));
        assert!(column_names.contains(&"value"));
    }

    #[test]
    fn test_source_not_found() {
        let exec = Executor::new().unwrap();
        let result = exec.load_source("test", Path::new("/nonexistent/file.csv"));
        assert!(matches!(result, Err(ExecutorError::SourceNotFound(_))));
    }

    #[test]
    fn test_unsupported_format() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.xyz");
        std::fs::write(&path, "data").unwrap();

        let exec = Executor::new().unwrap();
        let result = exec.load_source("test", &path);
        assert!(matches!(result, Err(ExecutorError::UnsupportedFormat(_))));
    }

    #[test]
    fn test_file_format_detection() {
        assert_eq!(FileFormat::from_path(Path::new("data.csv")), Some(FileFormat::Csv));
        assert_eq!(FileFormat::from_path(Path::new("data.CSV")), Some(FileFormat::Csv));
        assert_eq!(FileFormat::from_path(Path::new("data.json")), Some(FileFormat::Json));
        assert_eq!(FileFormat::from_path(Path::new("data.jsonl")), Some(FileFormat::Json));
        assert_eq!(FileFormat::from_path(Path::new("data.parquet")), Some(FileFormat::Parquet));
        assert_eq!(FileFormat::from_path(Path::new("data.pq")), Some(FileFormat::Parquet));
        assert_eq!(FileFormat::from_path(Path::new("data.txt")), None);
    }
}
