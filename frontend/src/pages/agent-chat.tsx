/**
 * Agent Chat page — Vercel AI SDK useChat, message bubbles, result cards.
 * T059 — Connected to POST /api/agent/chat with streaming responses.
 */

import React, { useRef, useEffect } from 'react';
import { useChat } from 'ai/react';

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function AgentChatPage() {
  const { messages, input, handleInputChange, handleSubmit, isLoading, error } = useChat({
    api: '/api/agent/chat',
  });

  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)] max-w-4xl mx-auto p-6">
      <h1 className="text-2xl font-bold mb-4">Agent Chat</h1>

      {/* Messages area */}
      <div className="flex-1 overflow-y-auto rounded-md border bg-muted/20 p-4 space-y-4 mb-4">
        {messages.length === 0 && (
          <div className="text-center py-16 text-muted-foreground">
            <p className="text-lg mb-2">Duval County Property Intelligence</p>
            <p className="text-sm">
              Ask questions about properties, roof ages, ownership history,
              water proximity, transit access, and more.
            </p>
          </div>
        )}

        {messages.map((message) => (
          <MessageBubble key={message.id} message={message} />
        ))}

        {isLoading && (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <span className="inline-block w-2 h-2 rounded-full bg-primary animate-pulse" />
            Agent is thinking...
          </div>
        )}

        {error && (
          <div className="rounded-md border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
            Error: {error.message}
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <form onSubmit={handleSubmit} className="flex gap-2">
        <input
          value={input}
          onChange={handleInputChange}
          placeholder="Ask about Duval County properties..."
          className="flex-1 rounded-md border border-input bg-background px-4 py-2.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          disabled={isLoading}
        />
        <button
          type="submit"
          disabled={isLoading || !input.trim()}
          className="rounded-md bg-blue-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          Send
        </button>
      </form>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Message bubble
// ---------------------------------------------------------------------------

interface ChatMessage {
  id: string;
  role: 'user' | 'assistant' | 'system' | 'data';
  content: string;
  toolInvocations?: ToolInvocation[];
}

interface ToolInvocation {
  toolCallId: string;
  toolName: string;
  args: Record<string, unknown>;
  state: string;
  result?: unknown;
}

function MessageBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === 'user';

  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-[85%] rounded-lg px-4 py-3 ${
          isUser
            ? 'bg-primary text-primary-foreground'
            : 'bg-card border text-card-foreground'
        }`}
      >
        {/* Role label */}
        <div className={`text-xs font-medium mb-1 ${isUser ? 'text-primary-foreground/70' : 'text-muted-foreground'}`}>
          {isUser ? 'You' : 'Agent'}
        </div>

        {/* Message text */}
        {message.content && (
          <div className="text-sm whitespace-pre-wrap">{message.content}</div>
        )}

        {/* Tool invocations / result cards */}
        {message.toolInvocations && message.toolInvocations.length > 0 && (
          <div className="mt-3 space-y-2">
            {message.toolInvocations.map((invocation) => (
              <ToolResultCard key={invocation.toolCallId} invocation={invocation} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tool result card
// ---------------------------------------------------------------------------

function ToolResultCard({ invocation }: { invocation: ToolInvocation }) {
  const result = invocation.result as Record<string, unknown> | undefined;

  if (invocation.state === 'call') {
    return (
      <div className="rounded-md border border-muted bg-muted/50 px-3 py-2 text-xs text-muted-foreground">
        Calling {invocation.toolName}...
      </div>
    );
  }

  if (!result) return null;

  // Query results
  if (invocation.toolName === 'queryProperties' && result.results) {
    const rows = result.results as Record<string, unknown>[];
    const queryExecuted = result.query_executed as string | undefined;
    const dataSource = result.data_source as string | undefined;

    return (
      <div className="rounded-md border bg-background p-3 space-y-2">
        {/* Result cards */}
        {rows.slice(0, 5).map((row, i) => (
          <div key={i} className="rounded border px-3 py-2 text-xs space-y-1">
            <div className="font-medium">
              {String(row.parcel_id ?? '')} — {String(row.address ?? 'N/A')}
            </div>
            <div className="text-muted-foreground flex flex-wrap gap-x-3">
              {row.roof_age_years != null && <span>Roof: {String(row.roof_age_years)} yrs</span>}
              {row.ownership_tenure_years != null && (
                <span>Ownership: {String(row.ownership_tenure_years)} yrs</span>
              )}
              {row.assessed_value != null && (
                <span>Value: ${(Number(row.assessed_value) / 1000).toFixed(0)}k</span>
              )}
              {row.contributing_sources != null ? (
                <span>Source: {String(row.contributing_sources)}</span>
              ) : null}
              {row.last_pipeline_run != null ? (
                <span>Run: {String(row.last_pipeline_run)}</span>
              ) : null}
            </div>
          </div>
        ))}
        {rows.length > 5 && (
          <div className="text-xs text-muted-foreground">
            ...and {rows.length - 5} more results
          </div>
        )}

        {/* Query transparency */}
        {queryExecuted && (
          <div className="text-xs text-muted-foreground border-t pt-2 mt-2">
            <div className="font-medium mb-1">Query executed:</div>
            <code className="block bg-muted rounded px-2 py-1 text-xs break-all">
              {queryExecuted}
            </code>
          </div>
        )}
        {dataSource && (
          <div className="text-xs text-muted-foreground">
            Data source: {dataSource}
          </div>
        )}
      </div>
    );
  }

  // Property detail result
  if (invocation.toolName === 'getPropertyDetail' && result.property) {
    const prop = result.property as Record<string, unknown>;
    return (
      <div className="rounded-md border bg-background px-3 py-2 text-xs space-y-1">
        <div className="font-medium">
          {String(prop.parcel_id ?? '')} — {String(prop.address ?? 'N/A')}
        </div>
        <div className="text-muted-foreground grid grid-cols-2 gap-1">
          {prop.assessed_value != null && <span>Value: ${(Number(prop.assessed_value) / 1000).toFixed(0)}k</span>}
          {prop.roof_age_years != null && <span>Roof: {String(prop.roof_age_years)} yrs</span>}
          {prop.current_owner_name != null ? <span>Owner: {String(prop.current_owner_name)}</span> : null}
          {prop.water_proximity_ft != null && <span>Water: {String(prop.water_proximity_ft)} ft</span>}
        </div>
      </div>
    );
  }

  // Run history result
  if (invocation.toolName === 'getRunHistory' && result.runs) {
    const runs = result.runs as Record<string, unknown>[];
    return (
      <div className="rounded-md border bg-background px-3 py-2 text-xs space-y-1">
        <div className="font-medium">Recent Pipeline Runs</div>
        {runs.map((run, i) => (
          <div key={i} className="text-muted-foreground">
            Run {String(run.run_id ?? '').slice(0, 8)} — {String(run.status ?? '')} —{' '}
            {String(run.record_count ?? 0)} records
          </div>
        ))}
      </div>
    );
  }

  // Error
  if (result.error) {
    return (
      <div className="rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive">
        {String(result.error)}
      </div>
    );
  }

  // Generic result
  return (
    <div className="rounded-md border bg-background px-3 py-2 text-xs text-muted-foreground">
      <pre className="whitespace-pre-wrap break-all">{JSON.stringify(result, null, 2)}</pre>
    </div>
  );
}

export default AgentChatPage;
