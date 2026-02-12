//! Meridian Language Server Protocol Implementation
//!
//! Provides IDE support for the Meridian data transformation language.

use dashmap::DashMap;
use meridian_parser::parse;
use meridian_types::check_program;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};

/// Document state stored for each open file
#[derive(Debug)]
struct Document {
    /// The document content
    content: String,
    /// The document version
    version: i32,
}

/// The Meridian Language Server
pub struct MeridianLanguageServer {
    /// LSP client for sending notifications
    client: Client,
    /// Open documents
    documents: DashMap<Url, Document>,
}

impl MeridianLanguageServer {
    /// Create a new language server instance
    pub fn new(client: Client) -> Self {
        Self {
            client,
            documents: DashMap::new(),
        }
    }

    /// Analyze a document and publish diagnostics
    async fn analyze_document(&self, uri: &Url) {
        let Some(doc) = self.documents.get(uri) else {
            return;
        };

        let content = doc.content.clone();
        drop(doc); // Release the lock

        let diagnostics = self.get_diagnostics(&content);

        self.client
            .publish_diagnostics(uri.clone(), diagnostics, None)
            .await;
    }

    /// Get diagnostics for the given source code
    fn get_diagnostics(&self, source: &str) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        // Parse the source
        match parse(source) {
            Ok(ast) => {
                // Type check
                if let Err(errors) = check_program(&ast) {
                    for error in errors {
                        let span = error.span();
                        let range = span_to_range((span.start, span.end), source);
                        diagnostics.push(Diagnostic {
                            range,
                            severity: Some(DiagnosticSeverity::ERROR),
                            code: None,
                            code_description: None,
                            source: Some("meridian".to_string()),
                            message: error.to_string(),
                            related_information: None,
                            tags: None,
                            data: None,
                        });
                    }
                }
            }
            Err(errors) => {
                for error in errors {
                    // ParseError uses miette's SourceSpan internally
                    // For now, use line 0 position 0 - precise locations coming in future
                    let range = Range {
                        start: Position { line: 0, character: 0 },
                        end: Position { line: 0, character: 1 },
                    };
                    diagnostics.push(Diagnostic {
                        range,
                        severity: Some(DiagnosticSeverity::ERROR),
                        code: None,
                        code_description: None,
                        source: Some("meridian".to_string()),
                        message: error.to_string(),
                        related_information: None,
                        tags: None,
                        data: None,
                    });
                }
            }
        }

        diagnostics
    }
}

/// Convert a byte span to an LSP Range
fn span_to_range(span: (usize, usize), source: &str) -> Range {
    let start = offset_to_position(span.0, source);
    let end = offset_to_position(span.1, source);
    Range { start, end }
}

/// Convert a byte offset to an LSP Position
fn offset_to_position(offset: usize, source: &str) -> Position {
    let mut line = 0u32;
    let mut col = 0u32;
    
    for (i, c) in source.char_indices() {
        if i >= offset {
            break;
        }
        if c == '\n' {
            line += 1;
            col = 0;
        } else {
            col += 1;
        }
    }
    
    Position { line, character: col }
}

#[tower_lsp::async_trait]
impl LanguageServer for MeridianLanguageServer {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                completion_provider: Some(CompletionOptions {
                    trigger_characters: Some(vec![".".to_string()]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            server_info: Some(ServerInfo {
                name: "meridian-lsp".to_string(),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            }),
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "Meridian language server initialized")
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let content = params.text_document.text;
        let version = params.text_document.version;

        self.documents.insert(
            uri.clone(),
            Document { content, version },
        );

        self.analyze_document(&uri).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        let version = params.text_document.version;

        // We use full sync, so there's only one change with the full content
        if let Some(change) = params.content_changes.into_iter().next() {
            self.documents.insert(
                uri.clone(),
                Document {
                    content: change.text,
                    version,
                },
            );

            self.analyze_document(&uri).await;
        }
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        self.documents.remove(&params.text_document.uri);
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = &params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let Some(doc) = self.documents.get(uri) else {
            return Ok(None);
        };

        // Get the word at the position
        let offset = position_to_offset(position, &doc.content);
        let word = get_word_at_offset(&doc.content, offset);

        // Check if it's a keyword
        let hover_text = match word.as_str() {
            "from" => Some("**from** - Read data from a source table or file"),
            "where" => Some("**where** - Filter rows based on a condition"),
            "select" => Some("**select** - Choose which columns to include"),
            "group" => Some("**group by** - Group rows for aggregation"),
            "order" => Some("**order by** - Sort the result set"),
            "limit" => Some("**limit** - Limit the number of rows returned"),
            "join" => Some("**join** - Combine rows from multiple sources"),
            "union" => Some("**union** - Combine results from multiple queries"),
            "stream" => Some("**stream** - Define a streaming data source"),
            "window" => Some("**window** - Apply windowing to streaming data"),
            "emit" => Some("**emit** - Configure streaming output behavior"),
            "let" => Some("**let** - Define a reusable expression"),
            "fn" => Some("**fn** - Define a custom function"),
            _ => None,
        };

        Ok(hover_text.map(|text| Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::Markdown,
                value: text.to_string(),
            }),
            range: None,
        }))
    }

    async fn completion(&self, _params: CompletionParams) -> Result<Option<CompletionResponse>> {
        // Provide keyword completions
        let keywords = vec![
            ("from", "Read from a data source"),
            ("where", "Filter rows"),
            ("select", "Choose columns"),
            ("group by", "Group for aggregation"),
            ("order by", "Sort results"),
            ("limit", "Limit rows"),
            ("join", "Join sources"),
            ("left join", "Left outer join"),
            ("right join", "Right outer join"),
            ("full join", "Full outer join"),
            ("union", "Union results"),
            ("stream", "Streaming source"),
            ("window", "Window function"),
            ("emit", "Emit config"),
            ("tumbling", "Tumbling window"),
            ("sliding", "Sliding window"),
            ("session", "Session window"),
            ("let", "Define expression"),
            ("fn", "Define function"),
            ("if", "Conditional"),
            ("then", "Then branch"),
            ("else", "Else branch"),
            ("and", "Logical AND"),
            ("or", "Logical OR"),
            ("not", "Logical NOT"),
            ("true", "Boolean true"),
            ("false", "Boolean false"),
            ("null", "Null value"),
        ];

        let items: Vec<CompletionItem> = keywords
            .into_iter()
            .map(|(label, detail)| CompletionItem {
                label: label.to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some(detail.to_string()),
                ..Default::default()
            })
            .collect();

        Ok(Some(CompletionResponse::Array(items)))
    }
}

/// Convert an LSP Position to a byte offset
fn position_to_offset(position: Position, source: &str) -> usize {
    let mut line = 0u32;
    let mut col = 0u32;
    
    for (i, c) in source.char_indices() {
        if line == position.line && col == position.character {
            return i;
        }
        if c == '\n' {
            if line == position.line {
                return i;
            }
            line += 1;
            col = 0;
        } else {
            col += 1;
        }
    }
    
    source.len()
}

/// Get the word at a given byte offset
fn get_word_at_offset(source: &str, offset: usize) -> String {
    let bytes = source.as_bytes();
    
    // Find start of word
    let mut start = offset;
    while start > 0 && is_word_char(bytes[start - 1]) {
        start -= 1;
    }
    
    // Find end of word
    let mut end = offset;
    while end < bytes.len() && is_word_char(bytes[end]) {
        end += 1;
    }
    
    source[start..end].to_string()
}

fn is_word_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// Run the language server
pub async fn run_server() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(|client| MeridianLanguageServer::new(client));
    Server::new(stdin, stdout, socket).serve(service).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_to_position_single_line() {
        let source = "hello world";
        let pos = offset_to_position(6, source);
        assert_eq!(pos.line, 0);
        assert_eq!(pos.character, 6);
    }

    #[test]
    fn test_offset_to_position_multi_line() {
        let source = "line1\nline2\nline3";
        
        // Start of line 2
        let pos = offset_to_position(6, source);
        assert_eq!(pos.line, 1);
        assert_eq!(pos.character, 0);
        
        // Middle of line 2
        let pos = offset_to_position(8, source);
        assert_eq!(pos.line, 1);
        assert_eq!(pos.character, 2);
    }

    #[test]
    fn test_position_to_offset() {
        let source = "line1\nline2\nline3";
        
        // Start of file
        assert_eq!(position_to_offset(Position { line: 0, character: 0 }, source), 0);
        
        // Start of line 2
        assert_eq!(position_to_offset(Position { line: 1, character: 0 }, source), 6);
        
        // Middle of line 2
        assert_eq!(position_to_offset(Position { line: 1, character: 2 }, source), 8);
    }

    #[test]
    fn test_get_word_at_offset() {
        let source = "from users where name";
        
        assert_eq!(get_word_at_offset(source, 2), "from");
        assert_eq!(get_word_at_offset(source, 7), "users");
        assert_eq!(get_word_at_offset(source, 14), "where");
    }

    #[test]
    fn test_span_to_range() {
        let source = "line1\nline2";
        let range = span_to_range((6, 11), source);
        
        assert_eq!(range.start.line, 1);
        assert_eq!(range.start.character, 0);
        assert_eq!(range.end.line, 1);
        assert_eq!(range.end.character, 5);
    }
}
