'use client';

import { Check, Copy } from 'lucide-react';
import { useState } from 'react';

export function CopyCommand({ command }: { command: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      onClick={() => {
        void navigator.clipboard.writeText(command).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1500);
        });
      }}
      className="inline-flex max-w-full items-center gap-3 rounded-full border bg-fd-background px-4 py-2.5 font-mono text-xs whitespace-nowrap transition-colors hover:bg-fd-accent sm:px-5 sm:text-sm"
    >
      <span className="text-fd-muted-foreground">$</span>
      <span className="truncate">{command}</span>
      {copied ? <Check className="size-4 shrink-0" /> : <Copy className="size-4 shrink-0 text-fd-muted-foreground" />}
    </button>
  );
}
