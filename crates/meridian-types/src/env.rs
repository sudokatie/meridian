//! Type environment for symbol resolution and scoping.

#![allow(dead_code)] // Some fields reserved for future use

use std::collections::HashMap;

use crate::types::Type;

/// A type environment that tracks all defined symbols.
#[derive(Debug, Clone, Default)]
pub struct TypeEnv {
    /// Schema definitions.
    schemas: HashMap<String, SchemaInfo>,
    /// Source definitions.
    sources: HashMap<String, SourceInfo>,
    /// Sink definitions.
    sinks: HashMap<String, SinkInfo>,
    /// Pipeline definitions.
    pipelines: HashMap<String, PipelineInfo>,
    /// Function definitions.
    functions: HashMap<String, FunctionInfo>,
    /// Local variables in current scope.
    locals: HashMap<String, Type>,
    /// Parent scope for nested lookups.
    parent: Option<Box<TypeEnv>>,
}

/// Information about a schema.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub name: String,
    pub fields: Vec<(String, Type)>,
}

/// Information about a source.
#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub name: String,
    pub schema: Option<String>,
    pub ty: Type,
}

/// Information about a sink.
#[derive(Debug, Clone)]
pub struct SinkInfo {
    pub name: String,
    pub pipeline: String,
}

/// Information about a pipeline.
#[derive(Debug, Clone)]
pub struct PipelineInfo {
    pub name: String,
    pub output_type: Type,
}

/// Information about a function.
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub params: Vec<(String, Type)>,
    pub return_type: Type,
}

impl TypeEnv {
    /// Create a new empty type environment.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a child scope that inherits from this environment.
    pub fn child(&self) -> Self {
        Self {
            schemas: HashMap::new(),
            sources: HashMap::new(),
            sinks: HashMap::new(),
            pipelines: HashMap::new(),
            functions: HashMap::new(),
            locals: HashMap::new(),
            parent: Some(Box::new(self.clone())),
        }
    }

    /// Register a schema.
    pub fn define_schema(&mut self, name: &str, fields: Vec<(String, Type)>) {
        self.schemas.insert(
            name.to_string(),
            SchemaInfo {
                name: name.to_string(),
                fields,
            },
        );
    }

    /// Look up a schema by name.
    pub fn get_schema(&self, name: &str) -> Option<&SchemaInfo> {
        self.schemas
            .get(name)
            .or_else(|| self.parent.as_ref().and_then(|p| p.get_schema(name)))
    }

    /// Register a source.
    pub fn define_source(&mut self, name: &str, schema: Option<String>, ty: Type) {
        self.sources.insert(
            name.to_string(),
            SourceInfo {
                name: name.to_string(),
                schema,
                ty,
            },
        );
    }

    /// Look up a source by name.
    pub fn get_source(&self, name: &str) -> Option<&SourceInfo> {
        self.sources
            .get(name)
            .or_else(|| self.parent.as_ref().and_then(|p| p.get_source(name)))
    }

    /// Register a pipeline.
    pub fn define_pipeline(&mut self, name: &str, output_type: Type) {
        self.pipelines.insert(
            name.to_string(),
            PipelineInfo {
                name: name.to_string(),
                output_type,
            },
        );
    }

    /// Look up a pipeline by name.
    pub fn get_pipeline(&self, name: &str) -> Option<&PipelineInfo> {
        self.pipelines
            .get(name)
            .or_else(|| self.parent.as_ref().and_then(|p| p.get_pipeline(name)))
    }

    /// Register a function.
    pub fn define_function(&mut self, name: &str, params: Vec<(String, Type)>, return_type: Type) {
        self.functions.insert(
            name.to_string(),
            FunctionInfo {
                name: name.to_string(),
                params,
                return_type,
            },
        );
    }

    /// Look up a function by name.
    pub fn get_function(&self, name: &str) -> Option<&FunctionInfo> {
        self.functions
            .get(name)
            .or_else(|| self.parent.as_ref().and_then(|p| p.get_function(name)))
    }

    /// Define a local variable.
    pub fn define_local(&mut self, name: &str, ty: Type) {
        self.locals.insert(name.to_string(), ty);
    }

    /// Look up a local variable.
    pub fn get_local(&self, name: &str) -> Option<&Type> {
        self.locals
            .get(name)
            .or_else(|| self.parent.as_ref().and_then(|p| p.get_local(name)))
    }

    /// Look up any symbol (local, source, pipeline, function).
    pub fn resolve(&self, name: &str) -> Option<Type> {
        // Try locals first
        if let Some(ty) = self.get_local(name) {
            return Some(ty.clone());
        }

        // Try sources
        if let Some(source) = self.get_source(name) {
            return Some(source.ty.clone());
        }

        // Try pipelines
        if let Some(pipeline) = self.get_pipeline(name) {
            return Some(pipeline.output_type.clone());
        }

        None
    }

    /// Get field type from a struct type.
    pub fn get_field_type(&self, ty: &Type, field: &str) -> Option<Type> {
        match ty {
            Type::Struct(fields) => {
                fields.iter().find(|(n, _)| n == field).map(|(_, t)| t.clone())
            }
            _ => None,
        }
    }

    /// Register built-in functions.
    pub fn register_builtins(&mut self) {
        // String functions
        self.define_function("length", vec![("s".into(), Type::String)], Type::Int);
        self.define_function("upper", vec![("s".into(), Type::String)], Type::String);
        self.define_function("lower", vec![("s".into(), Type::String)], Type::String);
        self.define_function("trim", vec![("s".into(), Type::String)], Type::String);

        // Numeric functions
        self.define_function("abs", vec![("n".into(), Type::Float)], Type::Float);
        self.define_function("ceil", vec![("n".into(), Type::Float)], Type::Int);
        self.define_function("floor", vec![("n".into(), Type::Float)], Type::Int);
        self.define_function("round", vec![("n".into(), Type::Float)], Type::Int);
        self.define_function("sqrt", vec![("n".into(), Type::Float)], Type::Float);

        // Aggregate functions
        self.define_function("count", vec![("x".into(), Type::Unknown)], Type::Int);
        self.define_function("sum", vec![("x".into(), Type::Float)], Type::Float);
        self.define_function("avg", vec![("x".into(), Type::Float)], Type::Float);
        self.define_function("min", vec![("x".into(), Type::Unknown)], Type::Unknown);
        self.define_function("max", vec![("x".into(), Type::Unknown)], Type::Unknown);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_define_and_get_schema() {
        let mut env = TypeEnv::new();
        env.define_schema(
            "Order",
            vec![
                ("id".into(), Type::String),
                ("amount".into(), Type::Float),
            ],
        );

        let schema = env.get_schema("Order").unwrap();
        assert_eq!(schema.name, "Order");
        assert_eq!(schema.fields.len(), 2);
    }

    #[test]
    fn test_child_scope_inherits() {
        let mut parent = TypeEnv::new();
        parent.define_local("x", Type::Int);

        let child = parent.child();
        assert_eq!(child.get_local("x"), Some(&Type::Int));
    }

    #[test]
    fn test_child_scope_shadows() {
        let mut parent = TypeEnv::new();
        parent.define_local("x", Type::Int);

        let mut child = parent.child();
        child.define_local("x", Type::String);

        assert_eq!(child.get_local("x"), Some(&Type::String));
    }

    #[test]
    fn test_resolve_source() {
        let mut env = TypeEnv::new();
        env.define_source(
            "orders",
            Some("Order".into()),
            Type::Struct(vec![("id".into(), Type::String)]),
        );

        let ty = env.resolve("orders").unwrap();
        assert!(matches!(ty, Type::Struct(_)));
    }

    #[test]
    fn test_get_field_type() {
        let env = TypeEnv::new();
        let struct_ty = Type::Struct(vec![
            ("id".into(), Type::String),
            ("count".into(), Type::Int),
        ]);

        assert_eq!(env.get_field_type(&struct_ty, "id"), Some(Type::String));
        assert_eq!(env.get_field_type(&struct_ty, "count"), Some(Type::Int));
        assert_eq!(env.get_field_type(&struct_ty, "missing"), None);
    }

    #[test]
    fn test_builtins() {
        let mut env = TypeEnv::new();
        env.register_builtins();

        let length = env.get_function("length").unwrap();
        assert_eq!(length.return_type, Type::Int);

        let sum = env.get_function("sum").unwrap();
        assert_eq!(sum.return_type, Type::Float);
    }
}
