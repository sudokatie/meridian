//! Parser for the Meridian language.

use crate::ast::*;
use crate::error::ParseError;
use crate::lexer::{lex, Token};
use crate::span::Span;
use crate::token::TokenKind;

/// Parse source code into a program.
pub fn parse(source: &str) -> Result<Program, Vec<ParseError>> {
    let tokens = lex(source);
    let mut parser = Parser::new(tokens, source);
    parser.parse_program()
}

/// The parser state.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    errors: Vec<ParseError>,
    source: String,
}

impl Parser {
    fn new(tokens: Vec<Token>, source: &str) -> Self {
        Self {
            tokens,
            pos: 0,
            errors: Vec::new(),
            source: source.to_string(),
        }
    }

    fn parse_program(&mut self) -> Result<Program, Vec<ParseError>> {
        let mut items = Vec::new();

        while !self.is_at_end() {
            match self.parse_item() {
                Ok(item) => items.push(item),
                Err(e) => {
                    self.errors.push(e);
                    self.synchronize();
                }
            }
        }

        if self.errors.is_empty() {
            Ok(Program { items })
        } else {
            // Attach source code to errors for better reporting
            let errors_with_source: Vec<ParseError> = self.errors
                .iter()
                .map(|e| e.clone().with_source(&self.source))
                .collect();
            Err(errors_with_source)
        }
    }

    fn parse_item(&mut self) -> Result<Item, ParseError> {
        match self.peek_kind() {
            Some(TokenKind::Schema) => self.parse_schema().map(Item::Schema),
            Some(TokenKind::Source) => self.parse_source().map(Item::Source),
            Some(TokenKind::Sink) => self.parse_sink().map(Item::Sink),
            Some(TokenKind::Pipeline) => self.parse_pipeline().map(Item::Pipeline),
            Some(TokenKind::Fn) => self.parse_function().map(Item::Function),
            Some(TokenKind::Test) => self.parse_test().map(Item::Test),
            Some(_) => {
                let token = self.peek().unwrap();
                Err(ParseError::unexpected_token(
                    token.span,
                    "schema, source, sink, pipeline, fn, or test",
                    format!("{}", token.kind),
                ))
            }
            None => Err(ParseError::unexpected_eof("item")),
        }
    }

    fn parse_schema(&mut self) -> Result<Schema, ParseError> {
        let start = self.expect(TokenKind::Schema)?.span;
        let name = self.parse_ident()?;
        self.expect(TokenKind::LBrace)?;

        let mut fields = Vec::new();
        while !self.check(TokenKind::RBrace) {
            fields.push(self.parse_field_def()?);
        }

        let end = self.expect(TokenKind::RBrace)?.span;

        Ok(Schema {
            name,
            fields,
            span: start.merge(end),
        })
    }

    fn parse_field_def(&mut self) -> Result<FieldDef, ParseError> {
        let name = self.parse_ident()?;
        self.expect(TokenKind::Colon)?;
        let ty = self.parse_type()?;

        Ok(FieldDef {
            span: name.span.merge(ty.span()),
            name,
            ty,
        })
    }

    fn parse_type(&mut self) -> Result<TypeExpr, ParseError> {
        let name = self.parse_ident()?;
        let start = name.span;

        // Check for generic types: list<T>, map<K, V>, nullable<T>
        if self.check(TokenKind::Lt) {
            self.advance(); // consume <
            match name.name.as_str() {
                "list" => {
                    let inner = self.parse_type()?;
                    self.expect(TokenKind::Gt)?;
                    let end = self.prev_span();
                    return Ok(TypeExpr::List(Box::new(inner), start.merge(end)));
                }
                "map" => {
                    let key = self.parse_type()?;
                    self.expect(TokenKind::Comma)?;
                    let value = self.parse_type()?;
                    self.expect(TokenKind::Gt)?;
                    let end = self.prev_span();
                    return Ok(TypeExpr::Map(Box::new(key), Box::new(value), start.merge(end)));
                }
                "nullable" => {
                    let inner = self.parse_type()?;
                    self.expect(TokenKind::Gt)?;
                    let end = self.prev_span();
                    return Ok(TypeExpr::Nullable(Box::new(inner), start.merge(end)));
                }
                _ => {
                    return Err(ParseError::unexpected_token(
                        start,
                        "list, map, or nullable",
                        format!("{}<...>", name.name),
                    ));
                }
            }
        }

        // Check for parameterized types: decimal(p, s), enum(values...)
        if self.check(TokenKind::LParen) {
            self.advance(); // consume (
            match name.name.as_str() {
                "decimal" => {
                    let precision = self.expect_int()? as u8;
                    self.expect(TokenKind::Comma)?;
                    let scale = self.expect_int()? as u8;
                    self.expect(TokenKind::RParen)?;
                    let end = self.prev_span();
                    return Ok(TypeExpr::Decimal {
                        precision,
                        scale,
                        span: start.merge(end),
                    });
                }
                "enum" => {
                    let mut values = Vec::new();
                    while !self.check(TokenKind::RParen) {
                        let value = self.expect_string()?;
                        values.push(value);
                        if !self.check(TokenKind::RParen) {
                            self.expect(TokenKind::Comma)?;
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                    let end = self.prev_span();
                    return Ok(TypeExpr::Enum(values, start.merge(end)));
                }
                _ => {
                    return Err(ParseError::unexpected_token(
                        start,
                        "decimal or enum",
                        format!("{}(...)", name.name),
                    ));
                }
            }
        }

        // Simple named type (string, int, float, interval, etc.)
        Ok(TypeExpr::Named(name))
    }

    fn parse_source(&mut self) -> Result<Source, ParseError> {
        let start = self.expect(TokenKind::Source)?.span;
        let name = self.parse_ident()?;
        // Note: 'from' is a keyword, so we expect the token directly
        self.expect(TokenKind::From)?;
        let source_type = self.parse_ident()?.name;
        self.expect(TokenKind::LParen)?;
        let path = self.expect_string()?;
        self.expect(TokenKind::RParen)?;

        let config = if self.check(TokenKind::LBrace) {
            self.advance();
            let mut cfg = Vec::new();
            while !self.check(TokenKind::RBrace) {
                // Config keys can be keywords like 'schema'
                let key = self.parse_ident_or_keyword()?;
                self.expect(TokenKind::Colon)?;
                let value = self.parse_expr()?;
                cfg.push((key, value));
            }
            self.expect(TokenKind::RBrace)?;
            cfg
        } else {
            Vec::new()
        };

        let end = self.prev_span();

        Ok(Source {
            name,
            source_type,
            path,
            config,
            span: start.merge(end),
        })
    }

    fn parse_sink(&mut self) -> Result<Sink, ParseError> {
        let start = self.expect(TokenKind::Sink)?.span;
        let pipeline = self.parse_ident()?;
        self.expect_ident("to")?;
        let sink_type = self.parse_ident()?.name;
        self.expect(TokenKind::LParen)?;
        let path = self.expect_string()?;
        self.expect(TokenKind::RParen)?;

        let config = if self.check(TokenKind::LBrace) {
            self.advance();
            let mut cfg = Vec::new();
            while !self.check(TokenKind::RBrace) {
                // Config keys can be keywords
                let key = self.parse_ident_or_keyword()?;
                self.expect(TokenKind::Colon)?;
                let value = self.parse_expr()?;
                cfg.push((key, value));
            }
            self.expect(TokenKind::RBrace)?;
            cfg
        } else {
            Vec::new()
        };

        let end = self.prev_span();

        Ok(Sink {
            pipeline,
            sink_type,
            path,
            config,
            span: start.merge(end),
        })
    }

    fn parse_pipeline(&mut self) -> Result<Pipeline, ParseError> {
        let start = self.expect(TokenKind::Pipeline)?.span;
        let name = self.parse_ident()?;
        self.expect(TokenKind::LBrace)?;

        let mut statements = Vec::new();
        while !self.check(TokenKind::RBrace) {
            statements.push(self.parse_statement()?);
        }

        let end = self.expect(TokenKind::RBrace)?.span;

        Ok(Pipeline {
            name,
            statements,
            span: start.merge(end),
        })
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek_kind() {
            Some(TokenKind::From) => self.parse_from().map(Statement::From),
            Some(TokenKind::Where) => self.parse_where().map(Statement::Where),
            Some(TokenKind::Select) => self.parse_select().map(Statement::Select),
            Some(TokenKind::Group) => self.parse_group_by().map(Statement::GroupBy),
            Some(TokenKind::Order) => self.parse_order_by().map(Statement::OrderBy),
            Some(TokenKind::Limit) => self.parse_limit().map(Statement::Limit),
            Some(TokenKind::Join | TokenKind::Left | TokenKind::Right | TokenKind::Full | TokenKind::Inner) => {
                self.parse_join().map(Statement::Join)
            }
            Some(TokenKind::Union) => self.parse_union().map(Statement::Union),
            Some(_) => {
                let token = self.peek().unwrap();
                Err(ParseError::unexpected_token(
                    token.span,
                    "statement",
                    format!("{}", token.kind),
                ))
            }
            None => Err(ParseError::unexpected_eof("statement")),
        }
    }

    fn parse_from(&mut self) -> Result<FromStmt, ParseError> {
        let start = self.expect(TokenKind::From)?.span;
        let source = self.parse_ident()?;
        Ok(FromStmt {
            span: start.merge(source.span),
            source,
        })
    }

    fn parse_where(&mut self) -> Result<WhereStmt, ParseError> {
        let start = self.expect(TokenKind::Where)?.span;
        let condition = self.parse_expr()?;
        Ok(WhereStmt {
            span: start.merge(condition.span()),
            condition,
        })
    }

    fn parse_select(&mut self) -> Result<SelectStmt, ParseError> {
        let start = self.expect(TokenKind::Select)?.span;
        self.expect(TokenKind::LBrace)?;

        let mut fields = Vec::new();
        while !self.check(TokenKind::RBrace) {
            fields.push(self.parse_select_field()?);
            if !self.check(TokenKind::RBrace) {
                // Comma is optional
                let _ = self.check_and_advance(TokenKind::Comma);
            }
        }

        let end = self.expect(TokenKind::RBrace)?.span;

        Ok(SelectStmt {
            fields,
            span: start.merge(end),
        })
    }

    fn parse_select_field(&mut self) -> Result<SelectField, ParseError> {
        // Check for spread
        if self.check(TokenKind::Spread) {
            let start = self.advance().span;
            let source = self.parse_ident()?;
            return Ok(SelectField::Spread {
                span: start.merge(source.span),
                source,
            });
        }

        // Parse name/expr
        let name = self.parse_ident()?;

        if self.check(TokenKind::Colon) {
            self.advance();
            let expr = self.parse_expr()?;
            Ok(SelectField::Named {
                span: name.span.merge(expr.span()),
                name,
                expr,
            })
        } else {
            Ok(SelectField::Expr {
                span: name.span,
                expr: Expr::Ident(name),
            })
        }
    }

    fn parse_group_by(&mut self) -> Result<GroupByStmt, ParseError> {
        let start = self.expect(TokenKind::Group)?.span;
        self.expect(TokenKind::By)?;

        let mut keys = vec![self.parse_expr()?];
        while self.check(TokenKind::Comma) {
            self.advance();
            keys.push(self.parse_expr()?);
        }

        let end = keys.last().unwrap().span();

        Ok(GroupByStmt {
            keys,
            span: start.merge(end),
        })
    }

    fn parse_order_by(&mut self) -> Result<OrderByStmt, ParseError> {
        let start = self.expect(TokenKind::Order)?.span;
        self.expect(TokenKind::By)?;

        let mut keys = vec![self.parse_order_key()?];
        while self.check(TokenKind::Comma) {
            self.advance();
            keys.push(self.parse_order_key()?);
        }

        let end = keys.last().unwrap().0.span();

        Ok(OrderByStmt {
            keys,
            span: start.merge(end),
        })
    }

    fn parse_order_key(&mut self) -> Result<(Expr, SortOrder), ParseError> {
        let expr = self.parse_expr()?;
        let order = if self.check_ident("desc") {
            self.advance();
            SortOrder::Desc
        } else {
            if self.check_ident("asc") {
                self.advance();
            }
            SortOrder::Asc
        };
        Ok((expr, order))
    }

    fn parse_limit(&mut self) -> Result<LimitStmt, ParseError> {
        let start = self.expect(TokenKind::Limit)?.span;
        let count = self.expect_int()? as u64;
        Ok(LimitStmt {
            count,
            span: start.merge(self.prev_span()),
        })
    }

    fn parse_join(&mut self) -> Result<JoinStmt, ParseError> {
        let start = self.peek().map(|t| t.span).unwrap_or_default();

        let kind = if self.check(TokenKind::Left) {
            self.advance();
            JoinKind::Left
        } else if self.check(TokenKind::Right) {
            self.advance();
            JoinKind::Right
        } else if self.check(TokenKind::Full) {
            self.advance();
            JoinKind::Full
        } else if self.check(TokenKind::Inner) {
            self.advance();
            JoinKind::Inner
        } else {
            JoinKind::Inner
        };

        self.expect(TokenKind::Join)?;
        let source = self.parse_ident()?;
        self.expect(TokenKind::On)?;
        let condition = self.parse_expr()?;

        Ok(JoinStmt {
            kind,
            source,
            span: start.merge(condition.span()),
            condition,
        })
    }

    fn parse_union(&mut self) -> Result<UnionStmt, ParseError> {
        let start = self.expect(TokenKind::Union)?.span;
        let pipeline = self.parse_ident()?;
        Ok(UnionStmt {
            span: start.merge(pipeline.span),
            pipeline,
        })
    }

    fn parse_function(&mut self) -> Result<Function, ParseError> {
        let start = self.expect(TokenKind::Fn)?.span;
        let name = self.parse_ident()?;
        self.expect(TokenKind::LParen)?;

        let mut params = Vec::new();
        while !self.check(TokenKind::RParen) {
            params.push(self.parse_param()?);
            if !self.check(TokenKind::RParen) {
                self.expect(TokenKind::Comma)?;
            }
        }

        self.expect(TokenKind::RParen)?;
        self.expect(TokenKind::Arrow)?;
        let return_type = self.parse_type()?;
        self.expect(TokenKind::LBrace)?;
        let body = self.parse_expr()?;
        let end = self.expect(TokenKind::RBrace)?.span;

        Ok(Function {
            name,
            params,
            return_type,
            body,
            span: start.merge(end),
        })
    }

    fn parse_param(&mut self) -> Result<Param, ParseError> {
        let name = self.parse_ident()?;
        self.expect(TokenKind::Colon)?;
        let ty = self.parse_type()?;
        Ok(Param {
            span: name.span.merge(ty.span()),
            name,
            ty,
        })
    }

    fn parse_test(&mut self) -> Result<Test, ParseError> {
        let start = self.expect(TokenKind::Test)?.span;
        let name = self.expect_string()?;
        self.expect(TokenKind::LBrace)?;

        let mut body = Vec::new();
        while !self.check(TokenKind::RBrace) {
            body.push(self.parse_test_statement()?);
        }

        let end = self.expect(TokenKind::RBrace)?.span;

        Ok(Test {
            name,
            body,
            span: start.merge(end),
        })
    }

    fn parse_test_statement(&mut self) -> Result<TestStatement, ParseError> {
        match self.peek_kind() {
            Some(TokenKind::Assert) => {
                let start = self.advance().span;
                let expr = self.parse_expr()?;
                Ok(TestStatement::Assert(expr.clone(), start.merge(expr.span())))
            }
            Some(TokenKind::Given) => {
                let start = self.advance().span;
                let name = self.parse_ident()?;
                self.expect(TokenKind::Assign)?;
                let value = self.parse_expr()?;
                Ok(TestStatement::Given {
                    span: start.merge(value.span()),
                    name,
                    value,
                })
            }
            Some(TokenKind::Expect) => {
                let start = self.advance().span;
                let pipeline = self.parse_ident()?;
                self.expect(TokenKind::Eq)?;
                let value = self.parse_expr()?;
                Ok(TestStatement::Expect {
                    span: start.merge(value.span()),
                    pipeline,
                    value,
                })
            }
            Some(_) => {
                let token = self.peek().unwrap();
                Err(ParseError::unexpected_token(
                    token.span,
                    "assert, given, or expect",
                    format!("{}", token.kind),
                ))
            }
            None => Err(ParseError::unexpected_eof("test statement")),
        }
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and()?;
        while self.check(TokenKind::Or) {
            self.advance();
            let right = self.parse_and()?;
            let span = left.span().merge(right.span());
            left = Expr::Binary(Box::new(left), BinOp::Or, Box::new(right), span);
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_equality()?;
        while self.check(TokenKind::And) {
            self.advance();
            let right = self.parse_equality()?;
            let span = left.span().merge(right.span());
            left = Expr::Binary(Box::new(left), BinOp::And, Box::new(right), span);
        }
        Ok(left)
    }

    fn parse_equality(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_comparison()?;
        while let Some(op) = self.match_token(&[TokenKind::Eq, TokenKind::Ne]) {
            let op = match op {
                TokenKind::Eq => BinOp::Eq,
                TokenKind::Ne => BinOp::Ne,
                _ => unreachable!(),
            };
            let right = self.parse_comparison()?;
            let span = left.span().merge(right.span());
            left = Expr::Binary(Box::new(left), op, Box::new(right), span);
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_term()?;
        while let Some(op) = self.match_token(&[TokenKind::Lt, TokenKind::Le, TokenKind::Gt, TokenKind::Ge]) {
            let op = match op {
                TokenKind::Lt => BinOp::Lt,
                TokenKind::Le => BinOp::Le,
                TokenKind::Gt => BinOp::Gt,
                TokenKind::Ge => BinOp::Ge,
                _ => unreachable!(),
            };
            let right = self.parse_term()?;
            let span = left.span().merge(right.span());
            left = Expr::Binary(Box::new(left), op, Box::new(right), span);
        }
        Ok(left)
    }

    fn parse_term(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_factor()?;
        while let Some(op) = self.match_token(&[TokenKind::Plus, TokenKind::Minus, TokenKind::Concat]) {
            let op = match op {
                TokenKind::Plus => BinOp::Add,
                TokenKind::Minus => BinOp::Sub,
                TokenKind::Concat => BinOp::Concat,
                _ => unreachable!(),
            };
            let right = self.parse_factor()?;
            let span = left.span().merge(right.span());
            left = Expr::Binary(Box::new(left), op, Box::new(right), span);
        }
        Ok(left)
    }

    fn parse_factor(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_unary()?;
        while let Some(op) = self.match_token(&[TokenKind::Star, TokenKind::Slash, TokenKind::Percent]) {
            let op = match op {
                TokenKind::Star => BinOp::Mul,
                TokenKind::Slash => BinOp::Div,
                TokenKind::Percent => BinOp::Mod,
                _ => unreachable!(),
            };
            let right = self.parse_unary()?;
            let span = left.span().merge(right.span());
            left = Expr::Binary(Box::new(left), op, Box::new(right), span);
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        if self.check(TokenKind::Minus) {
            let start = self.advance().span;
            let expr = self.parse_unary()?;
            let span = start.merge(expr.span());
            return Ok(Expr::Unary(UnaryOp::Neg, Box::new(expr), span));
        }
        if self.check(TokenKind::Not) {
            let start = self.advance().span;
            let expr = self.parse_unary()?;
            let span = start.merge(expr.span());
            return Ok(Expr::Unary(UnaryOp::Not, Box::new(expr), span));
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.check(TokenKind::Dot) {
                self.advance();
                let field = self.parse_ident()?;
                let span = expr.span().merge(field.span);
                expr = Expr::Field(Box::new(expr), field, span);
            } else if self.check(TokenKind::NullCoalesce) {
                self.advance();
                let right = self.parse_unary()?;
                let span = expr.span().merge(right.span());
                expr = Expr::NullCoalesce(Box::new(expr), Box::new(right), span);
            } else if self.check(TokenKind::Bang) {
                let end = self.advance().span;
                let span = expr.span().merge(end);
                expr = Expr::NonNullAssert(Box::new(expr), span);
            } else if self.check(TokenKind::Is) {
                self.advance(); // consume 'is'
                let is_not = if self.check(TokenKind::Not) {
                    self.advance();
                    true
                } else {
                    false
                };
                let end = self.expect(TokenKind::Null)?.span;
                let span = expr.span().merge(end);
                let op = if is_not { UnaryOp::IsNotNull } else { UnaryOp::IsNull };
                expr = Expr::Unary(op, Box::new(expr), span);
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.peek_kind() {
            Some(TokenKind::Int(n)) => {
                let span = self.advance().span;
                Ok(Expr::Int(n, span))
            }
            Some(TokenKind::Float(n)) => {
                let span = self.advance().span;
                Ok(Expr::Float(n, span))
            }
            Some(TokenKind::String(s)) => {
                let span = self.advance().span;
                Ok(Expr::String(s, span))
            }
            Some(TokenKind::True) => {
                let span = self.advance().span;
                Ok(Expr::Bool(true, span))
            }
            Some(TokenKind::False) => {
                let span = self.advance().span;
                Ok(Expr::Bool(false, span))
            }
            Some(TokenKind::LBracket) => self.parse_list(),
            Some(TokenKind::Match) => self.parse_match(),
            Some(TokenKind::LParen) => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(TokenKind::RParen)?;
                Ok(expr)
            }
            Some(TokenKind::Ident(_)) => {
                let ident = self.parse_ident()?;
                if self.check(TokenKind::LParen) {
                    self.advance();
                    let mut args = Vec::new();
                    while !self.check(TokenKind::RParen) {
                        args.push(self.parse_expr()?);
                        if !self.check(TokenKind::RParen) {
                            self.expect(TokenKind::Comma)?;
                        }
                    }
                    let end = self.expect(TokenKind::RParen)?.span;
                    Ok(Expr::Call(ident.clone(), args, ident.span.merge(end)))
                } else {
                    Ok(Expr::Ident(ident))
                }
            }
            Some(_) => {
                let token = self.peek().unwrap();
                Err(ParseError::unexpected_token(
                    token.span,
                    "expression",
                    format!("{}", token.kind),
                ))
            }
            None => Err(ParseError::unexpected_eof("expression")),
        }
    }

    fn parse_list(&mut self) -> Result<Expr, ParseError> {
        let start = self.expect(TokenKind::LBracket)?.span;
        let mut elements = Vec::new();
        while !self.check(TokenKind::RBracket) {
            elements.push(self.parse_expr()?);
            if !self.check(TokenKind::RBracket) {
                self.expect(TokenKind::Comma)?;
            }
        }
        let end = self.expect(TokenKind::RBracket)?.span;
        Ok(Expr::List(elements, start.merge(end)))
    }

    fn parse_match(&mut self) -> Result<Expr, ParseError> {
        let start = self.expect(TokenKind::Match)?.span;
        self.expect(TokenKind::LBrace)?;

        let mut arms = Vec::new();
        let mut default = None;

        while !self.check(TokenKind::RBrace) {
            if self.check_ident("_") {
                self.advance();
                self.expect(TokenKind::Arrow)?;
                default = Some(Box::new(self.parse_expr()?));
            } else {
                let pattern = self.parse_expr()?;
                self.expect(TokenKind::Arrow)?;
                let body = self.parse_expr()?;
                arms.push((pattern, body));
            }
            // Optional comma
            let _ = self.check_and_advance(TokenKind::Comma);
        }

        let end = self.expect(TokenKind::RBrace)?.span;
        Ok(Expr::Match(arms, default, start.merge(end)))
    }

    fn parse_ident(&mut self) -> Result<Ident, ParseError> {
        match self.peek() {
            Some(Token {
                kind: TokenKind::Ident(name),
                span,
            }) => {
                let name = name.clone();
                let span = *span;
                self.advance();
                Ok(Ident::new(name, span))
            }
            Some(token) => Err(ParseError::unexpected_token(
                token.span,
                "identifier",
                format!("{}", token.kind),
            )),
            None => Err(ParseError::unexpected_eof("identifier")),
        }
    }

    /// Parse an identifier, also accepting keywords as identifiers.
    /// Used in contexts where keywords can be used as names (e.g., config keys).
    fn parse_ident_or_keyword(&mut self) -> Result<Ident, ParseError> {
        match self.peek() {
            Some(Token {
                kind: TokenKind::Ident(name),
                span,
            }) => {
                let name = name.clone();
                let span = *span;
                self.advance();
                Ok(Ident::new(name, span))
            }
            Some(token) => {
                // Accept keywords as identifiers in this context
                let name = format!("{}", token.kind);
                let span = token.span;
                // Only accept actual keywords, not operators
                if matches!(
                    token.kind,
                    TokenKind::Schema
                        | TokenKind::Source
                        | TokenKind::Sink
                        | TokenKind::Pipeline
                        | TokenKind::From
                        | TokenKind::Where
                        | TokenKind::Select
                        | TokenKind::Group
                        | TokenKind::By
                        | TokenKind::Order
                        | TokenKind::Limit
                        | TokenKind::Join
                        | TokenKind::Left
                        | TokenKind::Right
                        | TokenKind::Full
                        | TokenKind::Inner
                        | TokenKind::On
                        | TokenKind::Is
                        | TokenKind::Null
                        | TokenKind::Fn
                        | TokenKind::Test
                        | TokenKind::And
                        | TokenKind::Or
                        | TokenKind::Not
                        | TokenKind::True
                        | TokenKind::False
                        | TokenKind::Match
                        | TokenKind::Let
                        | TokenKind::Union
                        | TokenKind::Assert
                        | TokenKind::Given
                        | TokenKind::Expect
                ) {
                    self.advance();
                    Ok(Ident::new(name, span))
                } else {
                    Err(ParseError::unexpected_token(
                        token.span,
                        "identifier",
                        format!("{}", token.kind),
                    ))
                }
            }
            None => Err(ParseError::unexpected_eof("identifier")),
        }
    }

    // Helper methods

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_kind(&self) -> Option<TokenKind> {
        self.peek().map(|t| t.kind.clone())
    }

    fn advance(&mut self) -> Token {
        let token = self.tokens[self.pos].clone();
        self.pos += 1;
        token
    }

    fn is_at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn check(&self, kind: TokenKind) -> bool {
        self.peek().map(|t| std::mem::discriminant(&t.kind) == std::mem::discriminant(&kind)).unwrap_or(false)
    }

    fn check_ident(&self, name: &str) -> bool {
        matches!(self.peek(), Some(Token { kind: TokenKind::Ident(n), .. }) if n == name)
    }

    fn check_and_advance(&mut self, kind: TokenKind) -> bool {
        if self.check(kind) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<Token, ParseError> {
        if self.check(kind.clone()) {
            Ok(self.advance())
        } else {
            match self.peek() {
                Some(token) => Err(ParseError::unexpected_token(
                    token.span,
                    format!("{}", kind),
                    format!("{}", token.kind),
                )),
                None => Err(ParseError::unexpected_eof(format!("{}", kind))),
            }
        }
    }

    fn expect_ident(&mut self, name: &str) -> Result<Token, ParseError> {
        if self.check_ident(name) {
            Ok(self.advance())
        } else {
            match self.peek() {
                Some(token) => Err(ParseError::unexpected_token(
                    token.span,
                    name,
                    format!("{}", token.kind),
                )),
                None => Err(ParseError::unexpected_eof(name)),
            }
        }
    }

    fn expect_string(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Some(Token {
                kind: TokenKind::String(s),
                ..
            }) => {
                let s = s.clone();
                self.advance();
                Ok(s)
            }
            Some(token) => Err(ParseError::unexpected_token(
                token.span,
                "string",
                format!("{}", token.kind),
            )),
            None => Err(ParseError::unexpected_eof("string")),
        }
    }

    fn expect_int(&mut self) -> Result<i64, ParseError> {
        match self.peek() {
            Some(Token {
                kind: TokenKind::Int(n),
                ..
            }) => {
                let n = *n;
                self.advance();
                Ok(n)
            }
            Some(token) => Err(ParseError::unexpected_token(
                token.span,
                "integer",
                format!("{}", token.kind),
            )),
            None => Err(ParseError::unexpected_eof("integer")),
        }
    }

    fn match_token(&mut self, kinds: &[TokenKind]) -> Option<TokenKind> {
        for kind in kinds {
            if self.check(kind.clone()) {
                return Some(self.advance().kind);
            }
        }
        None
    }

    fn prev_span(&self) -> Span {
        self.tokens.get(self.pos.saturating_sub(1)).map(|t| t.span).unwrap_or_default()
    }

    fn synchronize(&mut self) {
        while !self.is_at_end() {
            match self.peek_kind() {
                Some(
                    TokenKind::Schema
                    | TokenKind::Source
                    | TokenKind::Sink
                    | TokenKind::Pipeline
                    | TokenKind::Fn
                    | TokenKind::Test,
                ) => return,
                _ => {
                    self.advance();
                }
            }
        }
    }
}

impl TypeExpr {
    fn span(&self) -> Span {
        match self {
            TypeExpr::Named(i) => i.span,
            TypeExpr::Decimal { span, .. } => *span,
            TypeExpr::List(_, span) => *span,
            TypeExpr::Map(_, _, span) => *span,
            TypeExpr::Enum(_, span) => *span,
            TypeExpr::Nullable(_, span) => *span,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty() {
        let result = parse("");
        assert!(result.is_ok());
        assert!(result.unwrap().items.is_empty());
    }

    #[test]
    fn test_parse_schema() {
        let source = r#"
            schema Order {
                id: string
                amount: decimal
            }
        "#;
        let result = parse(source);
        assert!(result.is_ok());
        let program = result.unwrap();
        assert_eq!(program.items.len(), 1);
        assert!(matches!(program.items[0], Item::Schema(_)));
    }

    #[test]
    fn test_parse_pipeline() {
        let source = r#"
            pipeline doubled {
                from orders
                where amount > 100
                select {
                    id,
                    total: amount * 2
                }
            }
        "#;
        let result = parse(source);
        assert!(result.is_ok());
        let program = result.unwrap();
        assert_eq!(program.items.len(), 1);
        assert!(matches!(program.items[0], Item::Pipeline(_)));
    }

    #[test]
    fn test_parse_list_type() {
        let source = r#"
            schema Order {
                items: list<string>
            }
        "#;
        let result = parse(source);
        assert!(result.is_ok());
        let program = result.unwrap();
        if let Item::Schema(schema) = &program.items[0] {
            assert!(matches!(schema.fields[0].ty, TypeExpr::List(_, _)));
        } else {
            panic!("Expected schema");
        }
    }

    #[test]
    fn test_parse_map_type() {
        let source = r#"
            schema Config {
                settings: map<string, int>
            }
        "#;
        let result = parse(source);
        assert!(result.is_ok());
        let program = result.unwrap();
        if let Item::Schema(schema) = &program.items[0] {
            assert!(matches!(schema.fields[0].ty, TypeExpr::Map(_, _, _)));
        } else {
            panic!("Expected schema");
        }
    }

    #[test]
    fn test_parse_decimal_type() {
        let source = r#"
            schema Order {
                amount: decimal(10, 2)
            }
        "#;
        let result = parse(source);
        assert!(result.is_ok());
        let program = result.unwrap();
        if let Item::Schema(schema) = &program.items[0] {
            if let TypeExpr::Decimal { precision, scale, .. } = &schema.fields[0].ty {
                assert_eq!(*precision, 10);
                assert_eq!(*scale, 2);
            } else {
                panic!("Expected decimal type");
            }
        } else {
            panic!("Expected schema");
        }
    }

    #[test]
    fn test_parse_nullable_type() {
        let source = r#"
            schema User {
                email: nullable<string>
            }
        "#;
        let result = parse(source);
        assert!(result.is_ok());
        let program = result.unwrap();
        if let Item::Schema(schema) = &program.items[0] {
            assert!(matches!(schema.fields[0].ty, TypeExpr::Nullable(_, _)));
        } else {
            panic!("Expected schema");
        }
    }

    #[test]
    fn test_parse_enum_type() {
        let source = r#"
            schema Order {
                status: enum("pending", "completed", "cancelled")
            }
        "#;
        let result = parse(source);
        assert!(result.is_ok());
        let program = result.unwrap();
        if let Item::Schema(schema) = &program.items[0] {
            if let TypeExpr::Enum(values, _) = &schema.fields[0].ty {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], "pending");
            } else {
                panic!("Expected enum type");
            }
        } else {
            panic!("Expected schema");
        }
    }

    #[test]
    fn test_parse_interval_type() {
        let source = r#"
            schema Duration {
                window: interval
            }
        "#;
        let result = parse(source);
        assert!(result.is_ok());
        let program = result.unwrap();
        if let Item::Schema(schema) = &program.items[0] {
            if let TypeExpr::Named(ident) = &schema.fields[0].ty {
                assert_eq!(ident.name, "interval");
            } else {
                panic!("Expected named type");
            }
        } else {
            panic!("Expected schema");
        }
    }
}
