//! Type checker for Meridian programs.

use meridian_parser::{
    Program, Item, Schema, Pipeline, Function, Source, StreamSource, Test,
    Statement, Expr, TypeExpr as AstTypeExpr,
};

use crate::env::TypeEnv;
use crate::error::TypeError;
use crate::inference::infer_expr;
use crate::types::Type;

/// Type check a complete program.
pub fn check_program(program: &Program) -> Result<TypeEnv, Vec<TypeError>> {
    let mut checker = Checker::new();
    checker.check_program(program);

    if checker.errors.is_empty() {
        Ok(checker.env)
    } else {
        Err(checker.errors)
    }
}

/// The type checker state.
struct Checker {
    env: TypeEnv,
    errors: Vec<TypeError>,
}

impl Checker {
    fn new() -> Self {
        let mut env = TypeEnv::new();
        env.register_builtins();
        Self {
            env,
            errors: Vec::new(),
        }
    }

    fn check_program(&mut self, program: &Program) {
        // First pass: register all definitions
        for item in &program.items {
            self.register_item(item);
        }

        // Second pass: type check bodies
        for item in &program.items {
            self.check_item(item);
        }
    }

    fn register_item(&mut self, item: &Item) {
        match item {
            Item::Schema(schema) => self.register_schema(schema),
            Item::Source(source) => self.register_source(source),
            Item::Stream(stream) => self.register_stream(stream),
            Item::Sink(_) => {} // Sinks registered during check
            Item::Pipeline(pipeline) => self.register_pipeline(pipeline),
            Item::Function(func) => self.register_function(func),
            Item::Test(_) => {} // Tests don't need registration
        }
    }
    
    fn register_stream(&mut self, stream: &StreamSource) {
        // Similar to register_source but marks as streaming
        let schema_name = stream.config.iter().find(|(k, _)| k.name == "schema");
        
        let inner_ty = if let Some((_, Expr::Ident(schema_ident))) = schema_name {
            if let Some(schema) = self.env.get_schema(&schema_ident.name) {
                Type::Struct(schema.fields.clone())
            } else {
                self.errors.push(TypeError::UndefinedSchema {
                    name: schema_ident.name.clone(),
                    span: schema_ident.span,
                });
                Type::Unknown
            }
        } else {
            Type::Unknown
        };
        
        // Validate watermark expression if present
        // Watermark should be: timestamp_column - duration
        if let Some((_, watermark_expr)) = stream.config.iter().find(|(k, _)| k.name == "watermark") {
            self.validate_watermark(watermark_expr, &inner_ty);
        }
        
        // Wrap in Stream type to indicate streaming source
        self.env.define_source(&stream.name.name, Some("stream".to_string()), Type::Stream(Box::new(inner_ty)));
    }
    
    /// Validate a watermark expression (must be timestamp - duration).
    fn validate_watermark(&mut self, expr: &Expr, schema_ty: &Type) {
        // Watermark must be: timestamp_column - duration
        // or just a timestamp column (for event-time without delay)
        match expr {
            Expr::Binary(left, meridian_parser::BinOp::Sub, right, span) => {
                // Left side must resolve to timestamp
                let left_valid = match left.as_ref() {
                    Expr::Ident(ident) => {
                        if let Type::Struct(fields) = schema_ty {
                            fields.iter().any(|(name, ty)| {
                                name == &ident.name && matches!(ty, Type::Timestamp)
                            })
                        } else {
                            true // Allow Unknown schemas
                        }
                    }
                    _ => false,
                };
                
                // Right side must be a duration
                let right_valid = matches!(right.as_ref(), Expr::Duration(_));
                
                if !left_valid || !right_valid {
                    self.errors.push(TypeError::InvalidWatermark { span: *span });
                }
            }
            Expr::Ident(ident) => {
                // Just a timestamp column (no delay)
                if let Type::Struct(fields) = schema_ty {
                    let is_timestamp = fields.iter().any(|(name, ty)| {
                        name == &ident.name && matches!(ty, Type::Timestamp)
                    });
                    if !is_timestamp {
                        self.errors.push(TypeError::InvalidWatermark { span: ident.span });
                    }
                }
            }
            _ => {
                self.errors.push(TypeError::InvalidWatermark { span: expr.span() });
            }
        }
    }

    fn register_schema(&mut self, schema: &Schema) {
        let fields: Vec<(String, Type)> = schema
            .fields
            .iter()
            .map(|f| (f.name.name.clone(), self.resolve_type(&f.ty)))
            .collect();

        self.env.define_schema(&schema.name.name, fields);
    }

    fn register_source(&mut self, source: &Source) {
        // Try to get schema from config
        let schema_name = source.config.iter().find(|(k, _)| k.name == "schema");

        let ty = if let Some((_, Expr::Ident(schema_ident))) = schema_name {
            // Look up schema fields
            if let Some(schema) = self.env.get_schema(&schema_ident.name) {
                Type::Struct(schema.fields.clone())
            } else {
                self.errors.push(TypeError::UndefinedSchema {
                    name: schema_ident.name.clone(),
                    span: schema_ident.span,
                });
                Type::Unknown
            }
        } else {
            Type::Unknown
        };

        self.env
            .define_source(&source.name.name, None, ty);
    }

    fn register_pipeline(&mut self, pipeline: &Pipeline) {
        // For now, pipelines produce unknown type until we analyze them
        self.env
            .define_pipeline(&pipeline.name.name, Type::Unknown);
    }

    fn register_function(&mut self, func: &Function) {
        let params: Vec<(String, Type)> = func
            .params
            .iter()
            .map(|p| (p.name.name.clone(), self.resolve_type(&p.ty)))
            .collect();

        let return_type = self.resolve_type(&func.return_type);

        self.env
            .define_function(&func.name.name, params, return_type);
    }

    fn check_item(&mut self, item: &Item) {
        match item {
            Item::Pipeline(pipeline) => self.check_pipeline(pipeline),
            Item::Function(func) => self.check_function(func),
            Item::Test(test) => self.check_test(test),
            _ => {} // Schemas, sources, sinks don't need body checking
        }
    }

    fn check_pipeline(&mut self, pipeline: &Pipeline) {
        let mut scope = self.env.child();

        for stmt in &pipeline.statements {
            self.check_statement(stmt, &mut scope);
        }
    }

    fn check_statement(&mut self, stmt: &Statement, scope: &mut TypeEnv) {
        match stmt {
            Statement::From(from) => {
                // Look up the source and wire its schema fields to scope
                // Clone the source info to avoid borrow conflicts
                let source_data = scope.get_source(&from.source.name).map(|s| {
                    (s.ty.clone(), from.source.name.clone())
                });
                
                if let Some((source_ty, source_name)) = source_data {
                    // If the source has a struct type (from a schema), add fields to scope
                    if let Type::Struct(fields) = &source_ty {
                        for (field_name, field_type) in fields {
                            scope.define_local(field_name, field_type.clone());
                        }
                    }
                    // Also add the source itself to scope for qualified access (e.g., orders.id)
                    scope.define_local(&source_name, source_ty);
                }
                // Allow unknown sources for forward references and code generation
            }

            Statement::Where(where_stmt) => {
                // Condition must be boolean (or Unknown if we couldn't infer)
                match infer_expr(&where_stmt.condition, scope) {
                    Ok(Type::Bool) | Ok(Type::Unknown) => {}
                    Ok(other) => {
                        self.errors.push(TypeError::NonBooleanCondition {
                            found: other,
                            span: where_stmt.span,
                        });
                    }
                    Err(e) => self.errors.push(e),
                }
            }

            Statement::Select(select) => {
                // Check each field expression
                for field in &select.fields {
                    match field {
                        meridian_parser::SelectField::Named { expr, .. } => {
                            if let Err(e) = infer_expr(expr, scope) {
                                self.errors.push(e);
                            }
                        }
                        meridian_parser::SelectField::Expr { expr, .. } => {
                            if let Err(e) = infer_expr(expr, scope) {
                                self.errors.push(e);
                            }
                        }
                        meridian_parser::SelectField::Spread { source, span } => {
                            if scope.resolve(&source.name).is_none() {
                                self.errors.push(TypeError::UndefinedVariable {
                                    name: source.name.clone(),
                                    span: *span,
                                });
                            }
                        }
                    }
                }
            }

            Statement::GroupBy(group) => {
                // Check each key expression
                for key in &group.keys {
                    if let Err(e) = infer_expr(key, scope) {
                        self.errors.push(e);
                    }
                }
            }

            Statement::OrderBy(order) => {
                // Check each key expression
                for (key, _) in &order.keys {
                    if let Err(e) = infer_expr(key, scope) {
                        self.errors.push(e);
                    }
                }
            }

            Statement::Limit(_) => {} // Nothing to check

            Statement::Join(join) => {
                // Check if join source is a stream
                let join_source_is_stream = scope.get_source(&join.source.name)
                    .map(|s| matches!(s.ty, Type::Stream(_)))
                    .unwrap_or(false);
                
                // Verify join source exists
                if scope.resolve(&join.source.name).is_none() {
                    self.errors.push(TypeError::UndefinedVariable {
                        name: join.source.name.clone(),
                        span: join.source.span,
                    });
                }

                // Condition must be boolean
                match infer_expr(&join.condition, scope) {
                    Ok(Type::Bool) | Ok(Type::Unknown) => {}
                    Ok(other) => {
                        self.errors.push(TypeError::NonBooleanCondition {
                            found: other,
                            span: join.span,
                        });
                    }
                    Err(e) => self.errors.push(e),
                }
                
                // Stream-stream joins require 'within' temporal bounds
                // (we check if join source is stream; pipeline source is tracked via scope)
                if join_source_is_stream && join.within.is_none() {
                    // Check if the pipeline's from source is also a stream
                    // This is a simplified check - in practice we'd track pipeline source type
                    self.errors.push(TypeError::StreamJoinMissingWithin { span: join.span });
                }
            }

            Statement::Union(union_stmt) => {
                // Verify union target pipeline exists (lenient for now)
                let _ = scope.resolve(&union_stmt.pipeline.name);
            }
            
            Statement::Window(window_stmt) => {
                // Verify time column exists and is timestamp type
                match scope.resolve(&window_stmt.time_column.name) {
                    Some(Type::Timestamp) | Some(Type::Unknown) => {}
                    Some(other) => {
                        self.errors.push(TypeError::TypeMismatch {
                            expected: Type::Timestamp,
                            found: other,
                            span: window_stmt.time_column.span,
                        });
                    }
                    None => {
                        self.errors.push(TypeError::UndefinedVariable {
                            name: window_stmt.time_column.name.clone(),
                            span: window_stmt.time_column.span,
                        });
                    }
                }
                
                // Add window_start and window_end to scope
                scope.define_local("window_start", Type::Timestamp);
                scope.define_local("window_end", Type::Timestamp);
            }
            
            Statement::Emit(_emit_stmt) => {
                // Emit configuration is validated at IR level
                // (must follow a window statement)
            }
        }
    }

    fn check_function(&mut self, func: &Function) {
        let mut scope = self.env.child();

        // Add parameters to scope
        for param in &func.params {
            let ty = self.resolve_type(&param.ty);
            scope.define_local(&param.name.name, ty);
        }

        // Check body expression
        let expected = self.resolve_type(&func.return_type);
        match infer_expr(&func.body, &scope) {
            Ok(actual) if actual == expected => {}
            Ok(_) if matches!(expected, Type::Unknown) => {}
            Ok(actual) => {
                self.errors.push(TypeError::TypeMismatch {
                    expected,
                    found: actual,
                    span: func.body.span(),
                });
            }
            Err(e) => self.errors.push(e),
        }
    }

    fn check_test(&mut self, test: &Test) {
        let scope = self.env.child();

        for stmt in &test.body {
            match stmt {
                meridian_parser::TestStatement::Assert(expr, span) => {
                    match infer_expr(expr, &scope) {
                        Ok(Type::Bool) => {}
                        Ok(other) => {
                            self.errors.push(TypeError::NonBooleanCondition {
                                found: other,
                                span: *span,
                            });
                        }
                        Err(e) => self.errors.push(e),
                    }
                }
                meridian_parser::TestStatement::Given { value, .. } => {
                    if let Err(e) = infer_expr(value, &scope) {
                        self.errors.push(e);
                    }
                }
                meridian_parser::TestStatement::Expect { value, .. } => {
                    if let Err(e) = infer_expr(value, &scope) {
                        self.errors.push(e);
                    }
                }
            }
        }
    }

    fn resolve_type(&self, ty: &AstTypeExpr) -> Type {
        match ty {
            AstTypeExpr::Named(ident) => match ident.name.as_str() {
                "string" => Type::String,
                "int" => Type::Int,
                "bigint" => Type::BigInt,
                "float" => Type::Float,
                "double" => Type::Double,
                "bool" => Type::Bool,
                "timestamp" => Type::Timestamp,
                "date" => Type::Date,
                "time" => Type::Time,
                "interval" => Type::Interval,
                "null" => Type::Null,
                _ => {
                    // Could be a schema reference
                    if let Some(schema) = self.env.get_schema(&ident.name) {
                        Type::Struct(schema.fields.clone())
                    } else {
                        Type::Unknown
                    }
                }
            },
            AstTypeExpr::Decimal {
                precision, scale, ..
            } => Type::Decimal {
                precision: *precision,
                scale: *scale,
            },
            AstTypeExpr::List(inner, _) => Type::List(Box::new(self.resolve_type(inner))),
            AstTypeExpr::Map(key, value, _) => Type::Map(
                Box::new(self.resolve_type(key)),
                Box::new(self.resolve_type(value)),
            ),
            AstTypeExpr::Enum(values, _) => Type::Enum(values.clone()),
            AstTypeExpr::Nullable(inner, _) => Type::Nullable(Box::new(self.resolve_type(inner))),
            AstTypeExpr::Struct(fields, _) => {
                let resolved_fields: Vec<(String, Type)> = fields
                    .iter()
                    .map(|(name, ty)| (name.name.clone(), self.resolve_type(ty)))
                    .collect();
                Type::Struct(resolved_fields)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_parser::parse;

    #[test]
    fn test_check_empty_program() {
        let program = parse("").unwrap();
        let result = check_program(&program);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_schema() {
        let source = r#"
            schema Order {
                id: string
                amount: float
            }
        "#;
        let program = parse(source).unwrap();
        let env = check_program(&program).unwrap();
        assert!(env.get_schema("Order").is_some());
    }

    #[test]
    fn test_check_pipeline_undefined_source() {
        // Pipeline referencing undefined source is allowed (lenient mode)
        // This supports code generation for external sources defined at runtime
        let source = r#"
            pipeline clean {
                from nonexistent
            }
        "#;
        let program = parse(source).unwrap();
        let result = check_program(&program);
        // Lenient - allows undefined sources for external/dynamic sources
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_function() {
        let source = r#"
            fn double(x: int) -> int {
                x * 2
            }
        "#;
        let program = parse(source).unwrap();
        let result = check_program(&program);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_non_boolean_where() {
        // where clause with non-boolean expression
        let source = r#"
            pipeline bad {
                from data
                where 42
            }
        "#;
        let program = parse(source).unwrap();
        let result = check_program(&program);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        // Should have errors for both undefined 'data' and non-boolean condition
        assert!(errors.iter().any(|e| matches!(e, TypeError::NonBooleanCondition { .. })));
    }

    #[test]
    fn test_check_schema_with_list_type() {
        let source = r#"
            schema Order {
                items: list<string>
            }
        "#;
        let program = parse(source).unwrap();
        let env = check_program(&program).unwrap();
        let schema = env.get_schema("Order").unwrap();
        assert!(matches!(schema.fields[0].1, Type::List(_)));
    }

    #[test]
    fn test_check_schema_with_map_type() {
        let source = r#"
            schema Config {
                settings: map<string, int>
            }
        "#;
        let program = parse(source).unwrap();
        let env = check_program(&program).unwrap();
        let schema = env.get_schema("Config").unwrap();
        assert!(matches!(schema.fields[0].1, Type::Map(_, _)));
    }

    #[test]
    fn test_check_schema_with_interval_type() {
        let source = r#"
            schema Window {
                duration: interval
            }
        "#;
        let program = parse(source).unwrap();
        let env = check_program(&program).unwrap();
        let schema = env.get_schema("Window").unwrap();
        assert_eq!(schema.fields[0].1, Type::Interval);
    }

    #[test]
    fn test_check_schema_with_decimal_type() {
        let source = r#"
            schema Order {
                amount: decimal(10, 2)
            }
        "#;
        let program = parse(source).unwrap();
        let env = check_program(&program).unwrap();
        let schema = env.get_schema("Order").unwrap();
        assert!(matches!(schema.fields[0].1, Type::Decimal { precision: 10, scale: 2 }));
    }

    #[test]
    fn test_check_schema_with_nullable_type() {
        let source = r#"
            schema User {
                email: nullable<string>
            }
        "#;
        let program = parse(source).unwrap();
        let env = check_program(&program).unwrap();
        let schema = env.get_schema("User").unwrap();
        if let Type::Nullable(inner) = &schema.fields[0].1 {
            assert_eq!(**inner, Type::String);
        } else {
            panic!("Expected Nullable type");
        }
    }

    #[test]
    fn test_check_schema_with_enum_type() {
        let source = r#"
            schema Order {
                status: enum("pending", "completed")
            }
        "#;
        let program = parse(source).unwrap();
        let env = check_program(&program).unwrap();
        let schema = env.get_schema("Order").unwrap();
        if let Type::Enum(values) = &schema.fields[0].1 {
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected Enum type");
        }
    }

    #[test]
    fn test_schema_fields_wired_to_pipeline_scope() {
        // Schema fields should be available in pipeline scope after `from`
        let source = r#"
            schema Order {
                id: string
                amount: float
                is_valid: bool
            }

            source orders from file("data/orders.csv") {
                schema: Order
            }

            pipeline valid_orders {
                from orders
                where is_valid
                where amount > 100.0
            }
        "#;
        let program = parse(source).unwrap();
        let result = check_program(&program);
        // Should succeed because is_valid (bool) and amount (float) are in scope
        assert!(result.is_ok());
    }

    #[test]
    fn test_window_adds_bounds_to_scope() {
        // Window statement should add window_start and window_end to scope
        let source = r#"
            schema Event {
                event_time: timestamp
                value: int
            }

            source events from file("events.csv") {
                schema: Event
            }

            pipeline windowed {
                from events
                window tumbling(1.hours) on event_time
                select { window_start, window_end, total: sum(value) }
            }
        "#;
        let program = parse(source).unwrap();
        let result = check_program(&program);
        // Should succeed because window_start and window_end are added to scope
        assert!(result.is_ok());
    }

    #[test]
    fn test_window_requires_timestamp_column() {
        // Window on non-timestamp column should fail
        let source = r#"
            schema Event {
                id: string
                value: int
            }

            source events from file("events.csv") {
                schema: Event
            }

            pipeline windowed {
                from events
                window tumbling(1.hours) on id
            }
        "#;
        let program = parse(source).unwrap();
        let result = check_program(&program);
        // Should fail because id is string, not timestamp
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| matches!(e, TypeError::TypeMismatch { .. })));
    }

    #[test]
    fn test_stream_join_requires_within() {
        // Stream-stream join without within should fail
        let source = r#"
            schema Event {
                id: string
                ts: timestamp
            }

            stream left_events from kafka("left") {
                schema: Event
            }

            stream right_events from kafka("right") {
                schema: Event
            }

            pipeline joined {
                from left_events
                join right_events on left_events.id == right_events.id
            }
        "#;
        let program = parse(source).unwrap();
        let result = check_program(&program);
        // Should fail because stream-stream join requires within
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| matches!(e, TypeError::StreamJoinMissingWithin { .. })));
    }

    #[test]
    fn test_stream_join_with_within_ok() {
        // Stream-stream join with within should pass
        let source = r#"
            schema Event {
                id: string
                ts: timestamp
            }

            stream left_events from kafka("left") {
                schema: Event
            }

            stream right_events from kafka("right") {
                schema: Event
            }

            pipeline joined {
                from left_events
                join right_events within 5.minutes on left_events.id == right_events.id
            }
        "#;
        let program = parse(source).unwrap();
        let result = check_program(&program);
        // Should succeed because within is provided
        assert!(result.is_ok());
    }

    #[test]
    fn test_qualified_field_access_in_pipeline() {
        // Access fields via source name (orders.amount)
        let source = r#"
            schema Order {
                id: string
                amount: float
            }

            source orders from file("data/orders.csv") {
                schema: Order
            }

            pipeline high_value {
                from orders
                where orders.amount > 1000.0
            }
        "#;
        let program = parse(source).unwrap();
        let result = check_program(&program);
        // Should succeed because orders.amount resolves to float
        assert!(result.is_ok());
    }
}
