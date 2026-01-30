//! Token definitions for the Meridian lexer.

use logos::Logos;

/// Token kinds produced by the lexer.
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\n\r]+")]
#[logos(skip r"--[^\n]*")]
pub enum TokenKind {
    // Keywords
    #[token("schema")]
    Schema,
    #[token("struct")]
    Struct,
    #[token("source")]
    Source,
    #[token("sink")]
    Sink,
    #[token("pipeline")]
    Pipeline,
    #[token("from")]
    From,
    #[token("where")]
    Where,
    #[token("select")]
    Select,
    #[token("group")]
    Group,
    #[token("by")]
    By,
    #[token("order")]
    Order,
    #[token("limit")]
    Limit,
    #[token("join")]
    Join,
    #[token("left")]
    Left,
    #[token("right")]
    Right,
    #[token("inner")]
    Inner,
    #[token("full")]
    Full,
    #[token("on")]
    On,
    #[token("is")]
    Is,
    #[token("null")]
    Null,
    #[token("fn")]
    Fn,
    #[token("test")]
    Test,
    #[token("and")]
    And,
    #[token("or")]
    Or,
    #[token("not")]
    Not,
    #[token("true")]
    True,
    #[token("false")]
    False,
    #[token("match")]
    Match,
    #[token("let")]
    Let,
    #[token("union")]
    Union,
    #[token("assert")]
    Assert,
    #[token("given")]
    Given,
    #[token("expect")]
    Expect,

    // Identifiers
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", |lex| lex.slice().to_string())]
    Ident(String),

    // String literals
    #[regex(r#""([^"\\]|\\.)*""#, |lex| {
        let s = lex.slice();
        s[1..s.len()-1].to_string()
    })]
    String(String),

    // Numeric literals
    #[regex(r"[0-9]+", |lex| lex.slice().parse().ok(), priority = 2)]
    Int(i64),

    #[regex(r"[0-9]+\.[0-9]+", |lex| lex.slice().parse().ok())]
    Float(f64),

    // Operators
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Star,
    #[token("/")]
    Slash,
    #[token("%")]
    Percent,
    #[token("==")]
    Eq,
    #[token("!=")]
    Ne,
    #[token("<")]
    Lt,
    #[token("<=")]
    Le,
    #[token(">")]
    Gt,
    #[token(">=")]
    Ge,
    #[token(".")]
    Dot,
    #[token(",")]
    Comma,
    #[token(":")]
    Colon,
    #[token("->")]
    Arrow,
    #[token("=>")]
    FatArrow,
    #[token("...")]
    Spread,
    #[token("??")]
    NullCoalesce,
    #[token("++")]
    Concat,
    #[token("!")]
    Bang,
    #[token("=")]
    Assign,

    // Delimiters
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
}

impl std::fmt::Display for TokenKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenKind::Schema => write!(f, "schema"),
            TokenKind::Struct => write!(f, "struct"),
            TokenKind::Source => write!(f, "source"),
            TokenKind::Sink => write!(f, "sink"),
            TokenKind::Pipeline => write!(f, "pipeline"),
            TokenKind::From => write!(f, "from"),
            TokenKind::Where => write!(f, "where"),
            TokenKind::Select => write!(f, "select"),
            TokenKind::Group => write!(f, "group"),
            TokenKind::By => write!(f, "by"),
            TokenKind::Order => write!(f, "order"),
            TokenKind::Limit => write!(f, "limit"),
            TokenKind::Join => write!(f, "join"),
            TokenKind::Left => write!(f, "left"),
            TokenKind::Right => write!(f, "right"),
            TokenKind::Inner => write!(f, "inner"),
            TokenKind::Full => write!(f, "full"),
            TokenKind::On => write!(f, "on"),
            TokenKind::Is => write!(f, "is"),
            TokenKind::Null => write!(f, "null"),
            TokenKind::Fn => write!(f, "fn"),
            TokenKind::Test => write!(f, "test"),
            TokenKind::And => write!(f, "and"),
            TokenKind::Or => write!(f, "or"),
            TokenKind::Not => write!(f, "not"),
            TokenKind::True => write!(f, "true"),
            TokenKind::False => write!(f, "false"),
            TokenKind::Match => write!(f, "match"),
            TokenKind::Let => write!(f, "let"),
            TokenKind::Union => write!(f, "union"),
            TokenKind::Assert => write!(f, "assert"),
            TokenKind::Given => write!(f, "given"),
            TokenKind::Expect => write!(f, "expect"),
            TokenKind::Ident(s) => write!(f, "{}", s),
            TokenKind::String(s) => write!(f, "\"{}\"", s),
            TokenKind::Int(n) => write!(f, "{}", n),
            TokenKind::Float(n) => write!(f, "{}", n),
            TokenKind::Plus => write!(f, "+"),
            TokenKind::Minus => write!(f, "-"),
            TokenKind::Star => write!(f, "*"),
            TokenKind::Slash => write!(f, "/"),
            TokenKind::Percent => write!(f, "%"),
            TokenKind::Eq => write!(f, "=="),
            TokenKind::Ne => write!(f, "!="),
            TokenKind::Lt => write!(f, "<"),
            TokenKind::Le => write!(f, "<="),
            TokenKind::Gt => write!(f, ">"),
            TokenKind::Ge => write!(f, ">="),
            TokenKind::Dot => write!(f, "."),
            TokenKind::Comma => write!(f, ","),
            TokenKind::Colon => write!(f, ":"),
            TokenKind::Arrow => write!(f, "->"),
            TokenKind::FatArrow => write!(f, "=>"),
            TokenKind::Spread => write!(f, "..."),
            TokenKind::NullCoalesce => write!(f, "??"),
            TokenKind::Concat => write!(f, "++"),
            TokenKind::Bang => write!(f, "!"),
            TokenKind::Assign => write!(f, "="),
            TokenKind::LParen => write!(f, "("),
            TokenKind::RParen => write!(f, ")"),
            TokenKind::LBrace => write!(f, "{{"),
            TokenKind::RBrace => write!(f, "}}"),
            TokenKind::LBracket => write!(f, "["),
            TokenKind::RBracket => write!(f, "]"),
        }
    }
}
