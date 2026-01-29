//! Lexer for the Meridian language.

use logos::Logos;

use crate::span::Span;
use crate::token::TokenKind;

/// A token with its source span.
#[derive(Debug, Clone)]
pub struct Token {
    /// The kind of token.
    pub kind: TokenKind,
    /// The source span.
    pub span: Span,
}

impl Token {
    /// Create a new token.
    pub fn new(kind: TokenKind, span: Span) -> Self {
        Self { kind, span }
    }
}

/// Tokenize source code into a list of tokens.
pub fn lex(source: &str) -> Vec<Token> {
    let mut lexer = TokenKind::lexer(source);
    let mut tokens = Vec::new();

    while let Some(result) = lexer.next() {
        let span = lexer.span();
        match result {
            Ok(kind) => {
                tokens.push(Token {
                    kind,
                    span: Span::new(span.start, span.end),
                });
            }
            Err(_) => {
                // Skip error tokens for now, parser will handle missing tokens
            }
        }
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lex_empty() {
        let tokens = lex("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_lex_keywords() {
        let tokens = lex("schema source sink pipeline from where select");
        assert_eq!(tokens.len(), 7);
        assert!(matches!(tokens[0].kind, TokenKind::Schema));
        assert!(matches!(tokens[1].kind, TokenKind::Source));
        assert!(matches!(tokens[2].kind, TokenKind::Sink));
        assert!(matches!(tokens[3].kind, TokenKind::Pipeline));
        assert!(matches!(tokens[4].kind, TokenKind::From));
        assert!(matches!(tokens[5].kind, TokenKind::Where));
        assert!(matches!(tokens[6].kind, TokenKind::Select));
    }

    #[test]
    fn test_lex_identifier() {
        let tokens = lex("my_variable");
        assert_eq!(tokens.len(), 1);
        assert!(matches!(&tokens[0].kind, TokenKind::Ident(s) if s == "my_variable"));
    }

    #[test]
    fn test_lex_string() {
        let tokens = lex(r#""hello world""#);
        assert_eq!(tokens.len(), 1);
        assert!(matches!(&tokens[0].kind, TokenKind::String(s) if s == "hello world"));
    }

    #[test]
    fn test_lex_numbers() {
        let tokens = lex("42 3.14");
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0].kind, TokenKind::Int(42)));
        assert!(matches!(tokens[1].kind, TokenKind::Float(f) if (f - 3.14).abs() < 0.001));
    }

    #[test]
    fn test_lex_operators() {
        let tokens = lex("+ - * / == != < <= > >=");
        assert_eq!(tokens.len(), 10);
        assert!(matches!(tokens[0].kind, TokenKind::Plus));
        assert!(matches!(tokens[1].kind, TokenKind::Minus));
        assert!(matches!(tokens[4].kind, TokenKind::Eq));
        assert!(matches!(tokens[5].kind, TokenKind::Ne));
    }

    #[test]
    fn test_lex_comments_skipped() {
        let tokens = lex("schema -- this is a comment\nsource");
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0].kind, TokenKind::Schema));
        assert!(matches!(tokens[1].kind, TokenKind::Source));
    }
}
