//! IR node definitions.

/// A dataflow IR node.
#[derive(Debug, Clone)]
pub enum IrNode {
    Scan {
        source: String,
        columns: Vec<String>,
    },
    Filter {
        input: Box<IrNode>,
        predicate: IrExpr,
    },
    Project {
        input: Box<IrNode>,
        columns: Vec<(String, IrExpr)>,
    },
    Aggregate {
        input: Box<IrNode>,
        group_by: Vec<IrExpr>,
        aggregates: Vec<AggExpr>,
    },
    Join {
        kind: JoinKind,
        left: Box<IrNode>,
        right: Box<IrNode>,
        on: IrExpr,
    },
    Sort {
        input: Box<IrNode>,
        by: Vec<(IrExpr, SortOrder)>,
    },
    Limit {
        input: Box<IrNode>,
        count: usize,
    },
    Union {
        left: Box<IrNode>,
        right: Box<IrNode>,
    },
    Sink {
        input: Box<IrNode>,
        destination: String,
        format: String,
    },
}

/// An IR expression.
#[derive(Debug, Clone)]
pub enum IrExpr {
    Column(String),
    Literal(IrLiteral),
    BinaryOp(Box<IrExpr>, BinOp, Box<IrExpr>),
    UnaryOp(UnaryOp, Box<IrExpr>),
    Call(String, Vec<IrExpr>),
}

/// An IR literal value.
#[derive(Debug, Clone)]
pub enum IrLiteral {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

/// An aggregate expression.
#[derive(Debug, Clone)]
pub struct AggExpr {
    pub name: String,
    pub function: AggFunc,
    pub arg: IrExpr,
}

/// Aggregate functions.
#[derive(Debug, Clone, Copy)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
}

/// Binary operators.
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Neg,
    Not,
    IsNull,
    IsNotNull,
}

/// Join kinds.
#[derive(Debug, Clone, Copy)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

/// Sort order.
#[derive(Debug, Clone, Copy)]
pub enum SortOrder {
    Asc,
    Desc,
}
