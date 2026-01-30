//! Test framework for Meridian.
//!
//! Discovers and runs test blocks in .mer files.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use meridian_parser::{parse, Item, TestStatement, Expr};

/// A discovered test case.
#[derive(Debug, Clone)]
pub struct TestCase {
    /// Name of the test.
    pub name: String,
    /// Source file containing the test.
    pub file: PathBuf,
    /// Line number where test starts.
    pub line: usize,
    /// The test body statements.
    pub body: Vec<TestStatement>,
}

/// Result of running a single test.
#[derive(Debug, Clone)]
pub enum TestResult {
    Pass {
        duration: Duration,
    },
    Fail {
        message: String,
        duration: Duration,
    },
    Skip {
        reason: String,
    },
}

impl TestResult {
    #[allow(dead_code)]
    pub fn is_pass(&self) -> bool {
        matches!(self, TestResult::Pass { .. })
    }

    #[allow(dead_code)]
    pub fn is_fail(&self) -> bool {
        matches!(self, TestResult::Fail { .. })
    }
}

/// Summary of test run results.
#[derive(Debug, Default)]
pub struct TestSummary {
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub total_duration: Duration,
    pub results: Vec<(TestCase, TestResult)>,
}

impl TestSummary {
    #[allow(dead_code)]
    pub fn total(&self) -> usize {
        self.passed + self.failed + self.skipped
    }

    pub fn success(&self) -> bool {
        self.failed == 0
    }
}

/// Test runner.
pub struct TestRunner {
    /// Filter tests by name (substring match).
    filter: Option<String>,
    /// Verbose output.
    verbose: bool,
}

impl TestRunner {
    pub fn new() -> Self {
        Self {
            filter: None,
            verbose: false,
        }
    }

    pub fn with_filter(mut self, filter: Option<String>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    /// Discover test files in a directory.
    pub fn discover_files(&self, dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        self.discover_files_recursive(dir, &mut files);
        files.sort();
        files
    }

    fn discover_files_recursive(&self, dir: &Path, files: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    self.discover_files_recursive(&path, files);
                } else if path.is_file() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        // Match *_test.mer or test_*.mer
                        if name.ends_with("_test.mer") || name.starts_with("test_") && name.ends_with(".mer") {
                            files.push(path);
                        }
                    }
                }
            }
        }
    }

    /// Discover test cases in a file.
    pub fn discover_tests(&self, file: &Path) -> Result<Vec<TestCase>, String> {
        let source = std::fs::read_to_string(file)
            .map_err(|e| format!("failed to read {}: {}", file.display(), e))?;

        let program = parse(&source)
            .map_err(|errors| {
                errors.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
            })?;

        let mut tests = Vec::new();
        for item in program.items {
            if let Item::Test(test) = item {
                // Apply filter if set
                if let Some(ref filter) = self.filter {
                    if !test.name.contains(filter) {
                        continue;
                    }
                }

                tests.push(TestCase {
                    name: test.name,
                    file: file.to_path_buf(),
                    line: test.span.start, // Approximate line from byte offset
                    body: test.body,
                });
            }
        }

        Ok(tests)
    }

    /// Run all discovered tests.
    pub fn run(&self, tests: &[TestCase]) -> TestSummary {
        let start = Instant::now();
        let mut summary = TestSummary::default();

        for test in tests {
            if self.verbose {
                print!("test {} ... ", test.name);
            }

            let result = self.run_test(test);

            match &result {
                TestResult::Pass { duration } => {
                    summary.passed += 1;
                    if self.verbose {
                        println!("ok ({:.2?})", duration);
                    }
                }
                TestResult::Fail { message, duration } => {
                    summary.failed += 1;
                    if self.verbose {
                        println!("FAILED ({:.2?})", duration);
                        println!("  {}", message);
                    }
                }
                TestResult::Skip { reason } => {
                    summary.skipped += 1;
                    if self.verbose {
                        println!("skipped: {}", reason);
                    }
                }
            }

            summary.results.push((test.clone(), result));
        }

        summary.total_duration = start.elapsed();
        summary
    }

    /// Run a single test.
    fn run_test(&self, test: &TestCase) -> TestResult {
        let start = Instant::now();

        // Process each statement
        for stmt in &test.body {
            match stmt {
                TestStatement::Assert(expr, _span) => {
                    match self.evaluate_assert(expr) {
                        Ok(true) => continue,
                        Ok(false) => {
                            return TestResult::Fail {
                                message: format!("assertion failed: {}", format_expr(expr)),
                                duration: start.elapsed(),
                            };
                        }
                        Err(e) => {
                            return TestResult::Fail {
                                message: format!("assertion error: {}", e),
                                duration: start.elapsed(),
                            };
                        }
                    }
                }
                TestStatement::Given { name, value: _, .. } => {
                    // For now, skip given/expect (requires runtime)
                    return TestResult::Skip {
                        reason: format!("given/expect not yet supported ({})", name.name),
                    };
                }
                TestStatement::Expect { pipeline, .. } => {
                    return TestResult::Skip {
                        reason: format!("given/expect not yet supported ({})", pipeline.name),
                    };
                }
            }
        }

        TestResult::Pass {
            duration: start.elapsed(),
        }
    }

    /// Evaluate an assertion expression.
    fn evaluate_assert(&self, expr: &Expr) -> Result<bool, String> {
        match expr {
            // Literal boolean
            Expr::Bool(value, _) => Ok(*value),

            // Comparison: evaluate both sides
            Expr::Binary(left, op, right, _) => {
                let left_val = self.evaluate_expr(left)?;
                let right_val = self.evaluate_expr(right)?;

                use meridian_parser::BinOp;
                match op {
                    BinOp::Eq => Ok(left_val == right_val),
                    BinOp::Ne => Ok(left_val != right_val),
                    BinOp::Lt => Ok(compare_values(&left_val, &right_val)? < 0),
                    BinOp::Le => Ok(compare_values(&left_val, &right_val)? <= 0),
                    BinOp::Gt => Ok(compare_values(&left_val, &right_val)? > 0),
                    BinOp::Ge => Ok(compare_values(&left_val, &right_val)? >= 0),
                    BinOp::And => {
                        let l = left_val.as_bool()?;
                        let r = right_val.as_bool()?;
                        Ok(l && r)
                    }
                    BinOp::Or => {
                        let l = left_val.as_bool()?;
                        let r = right_val.as_bool()?;
                        Ok(l || r)
                    }
                    _ => Err(format!("unsupported operator in assertion: {:?}", op)),
                }
            }

            // Unary not
            Expr::Unary(op, inner, _) => {
                use meridian_parser::UnaryOp;
                match op {
                    UnaryOp::Not => {
                        let val = self.evaluate_assert(inner)?;
                        Ok(!val)
                    }
                    _ => Err(format!("unsupported unary operator in assertion: {:?}", op)),
                }
            }

            _ => Err(format!("unsupported expression in assertion: {}", format_expr(expr))),
        }
    }

    /// Evaluate an expression to a value.
    fn evaluate_expr(&self, expr: &Expr) -> Result<Value, String> {
        match expr {
            Expr::Int(n, _) => Ok(Value::Int(*n)),
            Expr::Float(n, _) => Ok(Value::Float(*n)),
            Expr::String(s, _) => Ok(Value::String(s.clone())),
            Expr::Bool(b, _) => Ok(Value::Bool(*b)),

            Expr::Binary(left, op, right, _) => {
                let l = self.evaluate_expr(left)?;
                let r = self.evaluate_expr(right)?;

                use meridian_parser::BinOp;
                match op {
                    BinOp::Add => l.add(&r),
                    BinOp::Sub => l.sub(&r),
                    BinOp::Mul => l.mul(&r),
                    BinOp::Div => l.div(&r),
                    BinOp::Mod => l.modulo(&r),
                    BinOp::Eq => Ok(Value::Bool(l == r)),
                    BinOp::Ne => Ok(Value::Bool(l != r)),
                    BinOp::Lt => Ok(Value::Bool(compare_values(&l, &r)? < 0)),
                    BinOp::Le => Ok(Value::Bool(compare_values(&l, &r)? <= 0)),
                    BinOp::Gt => Ok(Value::Bool(compare_values(&l, &r)? > 0)),
                    BinOp::Ge => Ok(Value::Bool(compare_values(&l, &r)? >= 0)),
                    BinOp::And => Ok(Value::Bool(l.as_bool()? && r.as_bool()?)),
                    BinOp::Or => Ok(Value::Bool(l.as_bool()? || r.as_bool()?)),
                    BinOp::Concat => l.concat(&r),
                }
            }

            Expr::Unary(op, inner, _) => {
                let val = self.evaluate_expr(inner)?;
                use meridian_parser::UnaryOp;
                match op {
                    UnaryOp::Neg => val.neg(),
                    UnaryOp::Not => Ok(Value::Bool(!val.as_bool()?)),
                    UnaryOp::IsNull => Ok(Value::Bool(val == Value::Null)),
                    UnaryOp::IsNotNull => Ok(Value::Bool(val != Value::Null)),
                }
            }

            Expr::Call(name, args, _) => {
                // Support basic functions
                let func = name.name.as_str();
                let arg_vals: Result<Vec<_>, _> = args.iter()
                    .map(|a| self.evaluate_expr(a))
                    .collect();
                let arg_vals = arg_vals?;

                match func {
                    "abs" => {
                        if arg_vals.len() != 1 {
                            return Err("abs expects 1 argument".into());
                        }
                        arg_vals[0].abs()
                    }
                    "length" | "len" => {
                        if arg_vals.len() != 1 {
                            return Err("length expects 1 argument".into());
                        }
                        match &arg_vals[0] {
                            Value::String(s) => Ok(Value::Int(s.len() as i64)),
                            _ => Err("length expects string argument".into()),
                        }
                    }
                    _ => Err(format!("unknown function: {}", func)),
                }
            }

            _ => Err(format!("cannot evaluate expression: {}", format_expr(expr))),
        }
    }
}

impl Default for TestRunner {
    fn default() -> Self {
        Self::new()
    }
}

/// A runtime value for test evaluation.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

impl Value {
    fn as_bool(&self) -> Result<bool, String> {
        match self {
            Value::Bool(b) => Ok(*b),
            _ => Err(format!("expected bool, got {:?}", self)),
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
            (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
            _ => Err(format!("cannot add {:?} and {:?}", self, other)),
        }
    }

    fn sub(&self, other: &Value) -> Result<Value, String> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - *b as f64)),
            _ => Err(format!("cannot subtract {:?} and {:?}", self, other)),
        }
    }

    fn mul(&self, other: &Value) -> Result<Value, String> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * *b as f64)),
            _ => Err(format!("cannot multiply {:?} and {:?}", self, other)),
        }
    }

    fn div(&self, other: &Value) -> Result<Value, String> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) if *b != 0 => Ok(Value::Int(a / b)),
            (Value::Float(a), Value::Float(b)) if *b != 0.0 => Ok(Value::Float(a / b)),
            (Value::Int(a), Value::Float(b)) if *b != 0.0 => Ok(Value::Float(*a as f64 / b)),
            (Value::Float(a), Value::Int(b)) if *b != 0 => Ok(Value::Float(a / *b as f64)),
            (_, Value::Int(0)) | (_, Value::Float(0.0)) => Err("division by zero".into()),
            _ => Err(format!("cannot divide {:?} and {:?}", self, other)),
        }
    }

    fn modulo(&self, other: &Value) -> Result<Value, String> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) if *b != 0 => Ok(Value::Int(a % b)),
            (_, Value::Int(0)) => Err("modulo by zero".into()),
            _ => Err(format!("cannot modulo {:?} and {:?}", self, other)),
        }
    }

    fn neg(&self) -> Result<Value, String> {
        match self {
            Value::Int(n) => Ok(Value::Int(-n)),
            Value::Float(n) => Ok(Value::Float(-n)),
            _ => Err(format!("cannot negate {:?}", self)),
        }
    }

    fn abs(&self) -> Result<Value, String> {
        match self {
            Value::Int(n) => Ok(Value::Int(n.abs())),
            Value::Float(n) => Ok(Value::Float(n.abs())),
            _ => Err(format!("cannot take abs of {:?}", self)),
        }
    }

    fn concat(&self, other: &Value) -> Result<Value, String> {
        match (self, other) {
            (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
            _ => Err(format!("cannot concatenate {:?} and {:?}", self, other)),
        }
    }
}

/// Compare two values, returning -1, 0, or 1.
fn compare_values(left: &Value, right: &Value) -> Result<i8, String> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b) as i8),
        (Value::Float(a), Value::Float(b)) => {
            Ok(a.partial_cmp(b).map(|o| o as i8).unwrap_or(0))
        }
        (Value::Int(a), Value::Float(b)) => {
            let a = *a as f64;
            Ok(a.partial_cmp(b).map(|o| o as i8).unwrap_or(0))
        }
        (Value::Float(a), Value::Int(b)) => {
            let b = *b as f64;
            Ok(a.partial_cmp(&b).map(|o| o as i8).unwrap_or(0))
        }
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b) as i8),
        _ => Err(format!("cannot compare {:?} and {:?}", left, right)),
    }
}

/// Format an expression for display.
fn format_expr(expr: &Expr) -> String {
    match expr {
        Expr::Int(n, _) => n.to_string(),
        Expr::Float(n, _) => n.to_string(),
        Expr::String(s, _) => format!("\"{}\"", s),
        Expr::Bool(b, _) => b.to_string(),
        Expr::Ident(id) => id.name.clone(),
        Expr::Binary(l, op, r, _) => {
            format!("({} {:?} {})", format_expr(l), op, format_expr(r))
        }
        Expr::Unary(op, e, _) => format!("{:?} {}", op, format_expr(e)),
        Expr::Call(name, args, _) => {
            let args_str: Vec<_> = args.iter().map(format_expr).collect();
            format!("{}({})", name.name, args_str.join(", "))
        }
        _ => "<expr>".to_string(),
    }
}

/// Print test summary.
pub fn print_summary(summary: &TestSummary) {
    println!();
    
    // Print failures first
    for (test, result) in &summary.results {
        if let TestResult::Fail { message, .. } = result {
            println!("---- {} ----", test.name);
            println!("  file: {}:{}", test.file.display(), test.line);
            println!("  {}", message);
            println!();
        }
    }

    // Print summary line
    let status = if summary.success() { "ok" } else { "FAILED" };
    println!(
        "test result: {}. {} passed; {} failed; {} skipped; finished in {:.2?}",
        status,
        summary.passed,
        summary.failed,
        summary.skipped,
        summary.total_duration
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_arithmetic() {
        let a = Value::Int(10);
        let b = Value::Int(3);
        
        assert_eq!(a.add(&b).unwrap(), Value::Int(13));
        assert_eq!(a.sub(&b).unwrap(), Value::Int(7));
        assert_eq!(a.mul(&b).unwrap(), Value::Int(30));
        assert_eq!(a.div(&b).unwrap(), Value::Int(3));
        assert_eq!(a.modulo(&b).unwrap(), Value::Int(1));
    }

    #[test]
    fn test_value_float_arithmetic() {
        let a = Value::Float(10.0);
        let b = Value::Float(3.0);
        
        assert_eq!(a.add(&b).unwrap(), Value::Float(13.0));
        assert_eq!(a.mul(&b).unwrap(), Value::Float(30.0));
    }

    #[test]
    fn test_value_comparison() {
        let a = Value::Int(10);
        let b = Value::Int(20);
        
        assert_eq!(compare_values(&a, &b).unwrap(), -1);
        assert_eq!(compare_values(&b, &a).unwrap(), 1);
        assert_eq!(compare_values(&a, &a).unwrap(), 0);
    }

    #[test]
    fn test_division_by_zero() {
        let a = Value::Int(10);
        let b = Value::Int(0);
        
        assert!(a.div(&b).is_err());
    }

    #[test]
    fn test_runner_evaluate_simple() {
        let runner = TestRunner::new();
        
        // Simple comparison
        let expr = Expr::Binary(
            Box::new(Expr::Int(1, meridian_parser::Span::new(0, 1))),
            meridian_parser::BinOp::Eq,
            Box::new(Expr::Int(1, meridian_parser::Span::new(0, 1))),
            meridian_parser::Span::new(0, 1),
        );
        
        assert!(runner.evaluate_assert(&expr).unwrap());
    }

    #[test]
    fn test_runner_evaluate_arithmetic() {
        let runner = TestRunner::new();
        
        // 2 + 3 == 5
        let expr = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(Expr::Int(2, meridian_parser::Span::new(0, 1))),
                meridian_parser::BinOp::Add,
                Box::new(Expr::Int(3, meridian_parser::Span::new(0, 1))),
                meridian_parser::Span::new(0, 1),
            )),
            meridian_parser::BinOp::Eq,
            Box::new(Expr::Int(5, meridian_parser::Span::new(0, 1))),
            meridian_parser::Span::new(0, 1),
        );
        
        assert!(runner.evaluate_assert(&expr).unwrap());
    }
}
