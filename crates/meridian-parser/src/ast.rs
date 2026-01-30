//! Abstract Syntax Tree definitions for Meridian.

use crate::span::Span;

/// A complete Meridian program.
#[derive(Debug, Clone)]
pub struct Program {
    /// Top-level items in the program.
    pub items: Vec<Item>,
}

/// A top-level item in a Meridian program.
#[derive(Debug, Clone)]
pub enum Item {
    Schema(Schema),
    Source(Source),
    Sink(Sink),
    Pipeline(Pipeline),
    Function(Function),
    Test(Test),
}

/// A schema definition.
#[derive(Debug, Clone)]
pub struct Schema {
    pub name: Ident,
    pub fields: Vec<FieldDef>,
    pub span: Span,
}

/// A field definition in a schema.
#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: Ident,
    pub ty: TypeExpr,
    pub span: Span,
}

/// A type expression.
#[derive(Debug, Clone)]
pub enum TypeExpr {
    Named(Ident),
    Decimal { precision: u8, scale: u8, span: Span },
    List(Box<TypeExpr>, Span),
    Map(Box<TypeExpr>, Box<TypeExpr>, Span),
    Enum(Vec<String>, Span),
    Nullable(Box<TypeExpr>, Span),
}

/// A source definition.
#[derive(Debug, Clone)]
pub struct Source {
    pub name: Ident,
    pub source_type: String,
    pub path: String,
    pub config: Vec<(Ident, Expr)>,
    pub span: Span,
}

/// A sink definition.
#[derive(Debug, Clone)]
pub struct Sink {
    pub pipeline: Ident,
    pub sink_type: String,
    pub path: String,
    pub config: Vec<(Ident, Expr)>,
    pub span: Span,
}

/// A pipeline definition.
#[derive(Debug, Clone)]
pub struct Pipeline {
    pub name: Ident,
    pub statements: Vec<Statement>,
    pub span: Span,
}

/// A statement in a pipeline.
#[derive(Debug, Clone)]
pub enum Statement {
    From(FromStmt),
    Where(WhereStmt),
    Select(SelectStmt),
    GroupBy(GroupByStmt),
    OrderBy(OrderByStmt),
    Limit(LimitStmt),
    Join(JoinStmt),
    Union(UnionStmt),
}

/// A from statement.
#[derive(Debug, Clone)]
pub struct FromStmt {
    pub source: Ident,
    pub span: Span,
}

/// A where statement.
#[derive(Debug, Clone)]
pub struct WhereStmt {
    pub condition: Expr,
    pub span: Span,
}

/// A select statement.
#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub fields: Vec<SelectField>,
    pub span: Span,
}

/// A field in a select statement.
#[derive(Debug, Clone)]
pub enum SelectField {
    Named { name: Ident, expr: Expr, span: Span },
    Expr { expr: Expr, span: Span },
    Spread { source: Ident, span: Span },
}

/// A group by statement.
#[derive(Debug, Clone)]
pub struct GroupByStmt {
    pub keys: Vec<Expr>,
    pub span: Span,
}

/// An order by statement.
#[derive(Debug, Clone)]
pub struct OrderByStmt {
    pub keys: Vec<(Expr, SortOrder)>,
    pub span: Span,
}

/// Sort order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// A limit statement.
#[derive(Debug, Clone)]
pub struct LimitStmt {
    pub count: u64,
    pub span: Span,
}

/// A join statement.
#[derive(Debug, Clone)]
pub struct JoinStmt {
    pub kind: JoinKind,
    pub source: Ident,
    pub condition: Expr,
    pub span: Span,
}

/// Kind of join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

/// A union statement.
#[derive(Debug, Clone)]
pub struct UnionStmt {
    pub pipeline: Ident,
    pub span: Span,
}

/// A function definition.
#[derive(Debug, Clone)]
pub struct Function {
    pub name: Ident,
    pub params: Vec<Param>,
    pub return_type: TypeExpr,
    pub body: Expr,
    pub span: Span,
}

/// A function parameter.
#[derive(Debug, Clone)]
pub struct Param {
    pub name: Ident,
    pub ty: TypeExpr,
    pub span: Span,
}

/// A test definition.
#[derive(Debug, Clone)]
pub struct Test {
    pub name: String,
    pub body: Vec<TestStatement>,
    pub span: Span,
}

/// A statement in a test.
#[derive(Debug, Clone)]
pub enum TestStatement {
    Assert(Expr, Span),
    Given { name: Ident, value: Expr, span: Span },
    Expect { pipeline: Ident, value: Expr, span: Span },
}

/// An identifier.
#[derive(Debug, Clone)]
pub struct Ident {
    pub name: String,
    pub span: Span,
}

impl Ident {
    pub fn new(name: impl Into<String>, span: Span) -> Self {
        Self {
            name: name.into(),
            span,
        }
    }
}

/// An expression.
#[derive(Debug, Clone)]
pub enum Expr {
    // Literals
    Int(i64, Span),
    Float(f64, Span),
    String(String, Span),
    Bool(bool, Span),
    List(Vec<Expr>, Span),

    // References
    Ident(Ident),
    Field(Box<Expr>, Ident, Span),

    // Operations
    Binary(Box<Expr>, BinOp, Box<Expr>, Span),
    Unary(UnaryOp, Box<Expr>, Span),
    Call(Ident, Vec<Expr>, Span),

    // Control
    Match(Vec<(Expr, Expr)>, Option<Box<Expr>>, Span),
    NullCoalesce(Box<Expr>, Box<Expr>, Span),
    
    // Non-null assertion (expr!)
    NonNullAssert(Box<Expr>, Span),
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Expr::Int(_, s) => *s,
            Expr::Float(_, s) => *s,
            Expr::String(_, s) => *s,
            Expr::Bool(_, s) => *s,
            Expr::List(_, s) => *s,
            Expr::Ident(i) => i.span,
            Expr::Field(_, _, s) => *s,
            Expr::Binary(_, _, _, s) => *s,
            Expr::Unary(_, _, s) => *s,
            Expr::Call(_, _, s) => *s,
            Expr::Match(_, _, s) => *s,
            Expr::NullCoalesce(_, _, s) => *s,
            Expr::NonNullAssert(_, s) => *s,
        }
    }
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Concat,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
    IsNull,
    IsNotNull,
}
