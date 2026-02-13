# Meridian Language Support for VS Code

Syntax highlighting and language server support for the Meridian data transformation DSL.

## Features

- Syntax highlighting for .mer and .meridian files
- Real-time diagnostics (syntax and type errors)
- Hover information for keywords
- Go to definition for schemas, functions, sources, and pipelines
- Autocomplete for keywords and identifiers

## Requirements

You need the Meridian language server installed:

```bash
# Build and install from source
cd /path/to/meridian
cargo install --path crates/meridian-lsp
```

Or set the path in settings:

```json
{
  "meridian.server.path": "/path/to/meridian-lsp"
}
```

## Installation

### From VSIX (recommended)

1. Build the extension:
   ```bash
   cd editors/vscode
   npm install
   npm run compile
   npx vsce package
   ```

2. Install in VS Code:
   - Open VS Code
   - Go to Extensions (Cmd+Shift+X)
   - Click the "..." menu
   - Select "Install from VSIX..."
   - Choose the generated .vsix file

### Development

1. Clone and build:
   ```bash
   cd editors/vscode
   npm install
   npm run compile
   ```

2. Open in VS Code and press F5 to launch Extension Development Host

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `meridian.server.path` | `meridian-lsp` | Path to the language server executable |
| `meridian.trace.server` | `off` | Trace communication with the server |

## Language Features

### Supported Keywords

- Declarations: `schema`, `source`, `stream`, `sink`, `pipeline`, `fn`, `test`
- Pipeline operations: `from`, `where`, `select`, `group by`, `order by`, `limit`, `join`
- Streaming: `window`, `tumbling`, `sliding`, `session`, `emit`, `within`
- Expressions: `match`, `let`, `and`, `or`, `not`

### File Extensions

- `.mer` (recommended)
- `.meridian`

## License

MIT
