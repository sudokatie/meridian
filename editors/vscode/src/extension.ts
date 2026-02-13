import * as path from 'path';
import { workspace, ExtensionContext, window } from 'vscode';
import {
    LanguageClient,
    LanguageClientOptions,
    ServerOptions,
    TransportKind,
} from 'vscode-languageclient/node';

let client: LanguageClient | undefined;

export function activate(context: ExtensionContext): void {
    const config = workspace.getConfiguration('meridian');
    const serverPath = config.get<string>('server.path', 'meridian-lsp');
    
    // Server options - run the LSP executable
    const serverOptions: ServerOptions = {
        run: {
            command: serverPath,
            transport: TransportKind.stdio,
        },
        debug: {
            command: serverPath,
            transport: TransportKind.stdio,
        },
    };

    // Client options - register for .mer files
    const clientOptions: LanguageClientOptions = {
        documentSelector: [{ scheme: 'file', language: 'meridian' }],
        synchronize: {
            fileEvents: workspace.createFileSystemWatcher('**/*.mer'),
        },
        outputChannel: window.createOutputChannel('Meridian Language Server'),
    };

    // Create and start the client
    client = new LanguageClient(
        'meridian',
        'Meridian Language Server',
        serverOptions,
        clientOptions
    );

    client.start().catch((err) => {
        window.showErrorMessage(
            `Failed to start Meridian language server: ${err.message}. ` +
            `Make sure meridian-lsp is installed and in your PATH, ` +
            `or set the meridian.server.path setting.`
        );
    });
}

export function deactivate(): Thenable<void> | undefined {
    if (!client) {
        return undefined;
    }
    return client.stop();
}
