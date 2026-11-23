'use client';

import { type CSSProperties, useEffect, useState } from 'react';
import { cn } from '@/lib/cn';
import type { Token } from '@/lib/highlight-lines';

type Frame = { event: string; detail?: string; line: number; status: string };

const frames: Frame[] = [
  { event: 'run_created', line: 0, status: 'workflow.Start records the input' },
  { event: 'run_started', line: 0, status: 'a flow replays the runner from the top' },
  { event: 'step_created', detail: 'findSources', line: 1, status: 'the step is queued for a step delivery' },
  { event: 'step_started', detail: 'findSources', line: 1, status: 'a worker runs findSources' },
  { event: 'step_completed', detail: 'findSources', line: 1, status: 'its result is recorded' },
  { event: 'wait_created', detail: '24h', line: 7, status: 'the run suspends; nothing is held in memory' },
  { event: 'wait_completed', detail: '24h', line: 7, status: 'a day later, the timer fires' },
  { event: 'step_created', detail: 'summarize', line: 11, status: 'replay: findSources returns its recorded result' },
  { event: 'step_started', detail: 'summarize', line: 11, status: 'a worker runs summarize' },
  { event: 'step_completed', detail: 'summarize', line: 11, status: 'its result is recorded' },
  { event: 'run_completed', line: 12, status: 'the runner returns and the run finishes' },
];

export function ReplayDemo({ lines }: { lines: Token[][] }) {
  const [index, setIndex] = useState(frames.length - 1);

  useEffect(() => {
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    setIndex(0);
    const id = window.setInterval(() => {
      setIndex((i) => (i + 1) % (frames.length + 3));
    }, 1100);
    return () => window.clearInterval(id);
  }, []);

  const current = Math.min(index, frames.length - 1);
  const frame = frames[current];

  return (
    <div className="grid overflow-hidden rounded-2xl border bg-fd-card text-[13px] lg:grid-cols-[1.5fr_1fr]">
      <div className="min-w-0 border-b lg:border-r lg:border-b-0">
        <PanelHeader title="flows/generate_report.go" />
        <pre className="overflow-x-auto py-4 font-mono leading-6 [tab-size:4]">
          {lines.map((line, i) => (
            <div
              key={i}
              className={cn(
                'flex gap-4 px-4 transition-colors duration-300',
                i === frame.line ? 'bg-fd-foreground/[0.07]' : '',
              )}
            >
              <span className="w-4 shrink-0 select-none text-right text-fd-muted-foreground/60">
                {i + 1}
              </span>
              <span className="whitespace-pre">
                {line.length === 0
                  ? ' '
                  : line.map((t, j) => (
                      <span
                        key={j}
                        className="demo-token"
                        style={{ '--shiki-light': t.light, '--shiki-dark': t.dark } as CSSProperties}
                      >
                        {t.content}
                      </span>
                    ))}
              </span>
            </div>
          ))}
        </pre>
        <div className="flex items-center gap-2 border-t px-4 py-3 font-mono text-xs text-fd-muted-foreground">
          <span className="size-1.5 shrink-0 animate-pulse rounded-full bg-fd-foreground" />
          <span key={current} className="animate-fd-fade-in truncate">
            {frame.status}
          </span>
        </div>
      </div>
      <div className="min-w-0">
        <PanelHeader title="event log" />
        <ol className="py-3 font-mono leading-6">
          {frames.map((f, i) => (
            <li
              key={i}
              className={cn(
                'flex items-baseline gap-3 px-4 transition-all duration-300',
                i > current && 'opacity-0',
                i === current ? 'text-fd-foreground' : 'text-fd-muted-foreground',
              )}
            >
              <span className="w-5 shrink-0 text-right text-fd-muted-foreground/60">{i + 1}</span>
              <span className={cn(i === current && 'font-semibold')}>{f.event}</span>
              {f.detail && <span className="truncate text-fd-muted-foreground">{f.detail}</span>}
            </li>
          ))}
        </ol>
      </div>
    </div>
  );
}

function PanelHeader({ title }: { title: string }) {
  return (
    <div className="flex items-center gap-2 border-b px-4 py-2.5 font-mono text-xs text-fd-muted-foreground">
      <span className="flex gap-1.5">
        <span className="size-2.5 rounded-full border" />
        <span className="size-2.5 rounded-full border" />
        <span className="size-2.5 rounded-full border" />
      </span>
      <span className="ml-2">{title}</span>
    </div>
  );
}
