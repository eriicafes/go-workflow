import { useChat } from '@ai-sdk/react';
import { WorkflowChatTransport } from '@ai-sdk/workflow/client';
import {
  lastAssistantMessageIsCompleteWithApprovalResponses,
  type UIMessage,
} from 'ai';
import { cn } from 'cn';
import { useEffect, useRef, useState, type FormEvent } from 'react';
import { Button } from './components/button';
import { MessagePart } from './parts';
import { load, MESSAGES_KEY, RUN_KEY, save } from './utils';

const suggestions = [
  'Find flights from SFO to JFK on 2026-11-20',
  'I need to get from Lagos to London next Friday',
];

export function App() {
  // The chat and the run it is streaming survive a reload, so the page
  // reconnects to a run that is still going.
  const [initialMessages] = useState(() => load<UIMessage[]>(MESSAGES_KEY, []));
  const [runId, setRunId] = useState(() => load<string | null>(RUN_KEY, null));
  const [resume] = useState(runId != null);

  // The transport lives as long as the component.
  const [transport] = useState(
    () =>
      new WorkflowChatTransport<UIMessage>({
        api: '/api/chat',
        maxConsecutiveErrors: 5,
        onChatSendMessage: response => setRunId(response.headers.get('x-workflow-run-id')),
        onChatEnd: () => setRunId(null),
        // Resuming after a reload reconnects to the saved run.
        prepareReconnectToStreamRequest: ({ api }) => {
          const id = load<string | null>(RUN_KEY, null);
          return { api: id ? `/api/chat/${encodeURIComponent(id)}/stream` : api };
        },
      }),
  );

  const { messages, sendMessage, addToolApprovalResponse, setMessages, status, error } = useChat({
    id: 'flight-chat',
    transport,
    messages: initialMessages,
    resume,
    // Once the user answers every approval request, send the answers back so
    // a new run executes or denies the tool calls.
    sendAutomaticallyWhen: lastAssistantMessageIsCompleteWithApprovalResponses,
  });

  useEffect(() => {
    save(RUN_KEY, runId);
  }, [runId]);

  // Save the chat between turns. While a run is active the messages saved
  // when its turn started are kept, even if the connection fails: a reload
  // replays the run's whole stream onto them.
  useEffect(() => {
    if (runId == null) save(MESSAGES_KEY, messages);
  }, [messages, runId]);

  const bottom = useRef<HTMLDivElement>(null);
  useEffect(() => {
    bottom.current?.scrollIntoView({ block: 'end' });
  }, [messages]);

  const [input, setInput] = useState('');
  const busy = status === 'submitted' || status === 'streaming';

  function submit(event: FormEvent) {
    event.preventDefault();
    const text = input.trim();
    if (!text || busy) return;
    sendMessage({ text });
    setInput('');
  }

  function newChat() {
    setRunId(null);
    setMessages([]);
  }

  return (
    <div className="mx-auto flex min-h-screen max-w-3xl flex-col px-4">
      <header className="flex items-start justify-between gap-4 border-b border-stone-200 pt-5 pb-3 dark:border-stone-800">
        <div>
          <h1 className="text-xl font-semibold">Flight agent</h1>
          <p className="mt-0.5 text-sm text-stone-500 dark:text-stone-400">
            A durable Go WorkflowAgent. Reload or restart the server mid-answer and it picks up where it left off.
          </p>
        </div>
        <div className="flex shrink-0 items-center gap-3">
          <span
            className={cn(
              'text-xs tabular-nums',
              busy ? 'text-blue-600 dark:text-blue-400' : status === 'error' ? 'text-red-600' : 'text-stone-500',
            )}
          >
            {busy ? (runId ? `run ${runId.slice(-8)}` : 'starting') : status}
          </span>
          <Button variant="secondary" onClick={newChat} disabled={busy || messages.length === 0}>
            New chat
          </Button>
        </div>
      </header>

      <main className="flex flex-1 flex-col gap-3 py-5">
        {messages.length === 0 && (
          <div className="mt-[15vh] text-center text-stone-500 dark:text-stone-400">
            <p className="mb-4">Ask me to find and book a flight. Bookings wait for your approval.</p>
            <div className="flex flex-wrap justify-center gap-2">
              {suggestions.map(suggestion => (
                <Button key={suggestion} variant="secondary" onClick={() => sendMessage({ text: suggestion })}>
                  {suggestion}
                </Button>
              ))}
            </div>
          </div>
        )}
        {messages.map(message => (
          <div
            key={message.id}
            className={cn('flex max-w-[85%] flex-col gap-2', message.role === 'user' ? 'self-end' : 'self-start')}
          >
            {message.parts.map((part, index) => (
              <MessagePart
                key={index}
                role={message.role}
                part={part}
                onApproval={(id, approved) =>
                  addToolApprovalResponse({ id, approved, reason: approved ? undefined : 'The user declined the booking.' })
                }
              />
            ))}
          </div>
        ))}
        {status === 'submitted' && <div className="text-stone-500">Thinking…</div>}
        {error && <div className="text-red-600 dark:text-red-400">Something went wrong: {error.message}</div>}
        <div ref={bottom} />
      </main>

      <form onSubmit={submit} className="sticky bottom-0 flex gap-2 bg-stone-50 pt-3 pb-5 dark:bg-stone-950">
        <input
          value={input}
          onChange={event => setInput(event.target.value)}
          placeholder="Where would you like to fly?"
          aria-label="Message"
          className="flex-1 rounded-lg border border-stone-300 bg-white px-3 py-2 outline-blue-600 focus:outline-2 dark:border-stone-700 dark:bg-stone-900"
        />
        <Button type="submit" disabled={busy || !input.trim()}>
          Send
        </Button>
      </form>
    </div>
  );
}
