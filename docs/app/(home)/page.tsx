import Link from 'next/link';
import { ArrowRight, ArrowUpRight } from 'lucide-react';
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock';
import { Logo } from '@/components/logo';
import { GopherCredit } from '@/components/gopher-credit';
import { ReplayDemo } from '@/components/replay-demo';
import { CopyCommand } from '@/components/copy-command';
import { highlightLines } from '@/lib/highlight-lines';
import { replayDemoCode } from '@/lib/replay-demo-code';

const worlds = [
  { name: 'Vercel', href: '/docs/worlds/vercel', body: 'Vercel Functions on Vercel Queues.' },
  { name: 'AWS', href: '/docs/worlds/aws', body: 'Lambda on DynamoDB, SQS, and EventBridge.' },
  { name: 'Postgres', href: '/docs/worlds/postgres', body: 'Servers and containers. LISTEN/NOTIFY workers.' },
  { name: 'Local', href: '/docs/worlds/local', body: 'One process and a file on disk.' },
  { name: 'Test', href: '/docs/worlds/testworld', body: 'In memory, with a fake clock.' },
  { name: 'Yours', href: '/docs/worlds/custom', body: 'Implement World, pass the conformance suite.' },
];

const features = [
  {
    title: 'Steps',
    body: 'Side effects run in steps that retry on failure. Once a step completes, replays return its recorded result instead of running it again.',
    href: '/docs/steps',
  },
  {
    title: 'Durable waits',
    body: 'Sleep for a week, wait on a hook, or race a webhook against a timer, without holding a process.',
    href: '/docs/sleep',
  },
  {
    title: 'Errors that survive',
    body: 'errors.Is and errors.As work on a failed step, even after a restart, because the whole chain is recorded.',
    href: '/docs/errors',
  },
  {
    title: 'Concurrency',
    body: 'Start steps in parallel, wait for a group, or take the first to finish. Replay stays deterministic.',
    href: '/docs/concurrency',
  },
  {
    title: 'Streams',
    body: 'Write bytes from a step and read them from a browser or another run while the run is still going.',
    href: '/docs/streams',
  },
  {
    title: 'Caught at vet time',
    body: 'workflowcheck flags goroutines, select, map iteration, time.Now, and I/O in runners before they ship.',
    href: '/docs/tooling/analyzer',
  },
];

export default async function HomePage() {
  const demoLines = await highlightLines(replayDemoCode, 'go');
  return (
    <main className="flex flex-1 flex-col">
      <section className="relative overflow-hidden border-b">
        <GridBackdrop />
        <div className="relative mx-auto flex w-full max-w-6xl flex-col items-center px-4 pt-20 pb-16 text-center md:pt-28">
          <Logo className="mb-8 h-28 w-auto md:h-32" />
          <h1 className="max-w-3xl text-4xl font-medium tracking-tighter text-balance md:text-6xl">
            Make Any Go Function Durable
          </h1>
          <p className="mt-6 max-w-2xl text-base text-balance text-fd-muted-foreground md:text-lg">
            go-workflow brings the durability, reliability, and observability of the{' '}
            <a
              href="https://workflow-sdk.dev"
              className="underline underline-offset-4 decoration-fd-muted-foreground/50 hover:decoration-fd-muted-foreground"
            >
              Workflow SDK
            </a>{' '}
            to Go. Build apps and AI agents that can suspend, resume, and maintain state with ease.
          </p>
          <div className="mt-8 flex flex-wrap items-center justify-center gap-3">
            <Link
              href="/docs/quickstart"
              className="inline-flex items-center gap-2 rounded-full bg-fd-primary px-5 py-2.5 text-sm font-medium text-fd-primary-foreground transition-opacity hover:opacity-85"
            >
              Quickstart <ArrowRight className="size-4" />
            </Link>
            <CopyCommand command="go get github.com/eriicafes/go-workflow" />
          </div>
        </div>
        <div className="relative mx-auto w-full max-w-6xl px-4 pb-20">
          <ReplayDemo lines={demoLines} />
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-4 py-20">
        <SectionHeading
          eyebrow="Write"
          title="Plain Go, made durable"
          body="A runner is an ordinary function. The runtime turns each call into an event and replays it on the next delivery."
        />
        <div className="mt-10 grid gap-px overflow-hidden rounded-2xl border bg-fd-border sm:grid-cols-2 lg:grid-cols-3">
          {features.map((f) => (
            <Link key={f.title} href={f.href} className="group bg-fd-background p-6 transition-colors hover:bg-fd-card">
              <h3 className="flex items-center justify-between font-medium">
                {f.title}
                <ArrowRight className="size-4 -translate-x-1 opacity-0 transition-all group-hover:translate-x-0 group-hover:opacity-100" />
              </h3>
              <p className="mt-2 text-sm text-fd-muted-foreground">{f.body}</p>
            </Link>
          ))}
        </div>
      </section>

      <section className="border-y bg-fd-card">
        <div className="mx-auto grid w-full max-w-6xl gap-10 px-4 py-20 lg:grid-cols-[1fr_1.4fr]">
          <SectionHeading
            eyebrow="Run"
            title="One codebase, any World"
            body="A World stores runs and delivers them. Swap the World and your workflows stay the same."
            action={{ text: 'Compare Worlds', href: '/docs/worlds' }}
          />
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
            {worlds.map((w) => (
              <Link
                key={w.name}
                href={w.href}
                className="flex flex-col rounded-xl border bg-fd-background p-4 transition-colors hover:border-fd-foreground/40"
              >
                <span className="font-mono text-sm font-medium">{w.name}</span>
                <span className="mt-1 text-xs text-fd-muted-foreground">{w.body}</span>
              </Link>
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-4 py-20">
        <div className="grid gap-10 lg:grid-cols-[1fr_1.4fr]">
          <SectionHeading
            eyebrow="Inspect"
            title="The Workflow SDK web UI, for Go runs"
            body="Runs follow the Workflow SDK's event model, so its web UI lists every Go run, step, hook, and stream."
            action={{ text: 'Open the web UI', href: '/docs/tooling/web-ui' }}
          />
          <div className="min-w-0 [&_figure]:my-0">
            <DynamicCodeBlock
              lang="sh"
              code={`# mount your World's HTTP handler, then
export WORKFLOW_TARGET_WORLD=go-workflow/world-http
export WORKFLOW_HTTP_BASE_URL=http://localhost:8080/v1/workflow
npx workflow web`}
            />
          </div>
        </div>
      </section>

      <footer className="border-t">
        <div className="mx-auto flex w-full max-w-6xl flex-wrap items-center justify-between gap-4 px-4 py-8 text-sm text-fd-muted-foreground">
          <span className="flex flex-col gap-1">
            <span className="flex items-center gap-2">
              <Logo className="h-6 w-auto" /> go-workflow · MIT License
            </span>
            <GopherCredit className="text-xs" />
          </span>
          <span className="flex gap-5">
            <Link href="/docs" className="hover:text-fd-foreground">
              Docs
            </Link>
            <a href="https://github.com/eriicafes/go-workflow" className="inline-flex items-center gap-1 hover:text-fd-foreground">
              GitHub <ArrowUpRight className="size-3" />
            </a>
            <a href="https://pkg.go.dev/github.com/eriicafes/go-workflow" className="inline-flex items-center gap-1 hover:text-fd-foreground">
              pkg.go.dev <ArrowUpRight className="size-3" />
            </a>
          </span>
        </div>
      </footer>
    </main>
  );
}

function SectionHeading({
  eyebrow,
  title,
  body,
  action,
}: {
  eyebrow: string;
  title: string;
  body: string;
  action?: { text: string; href: string };
}) {
  return (
    <div className="flex flex-col items-start">
      <span className="font-mono text-xs tracking-widest text-fd-muted-foreground uppercase">{eyebrow}</span>
      <h2 className="mt-3 text-2xl font-semibold tracking-tight md:text-3xl">{title}</h2>
      <p className="mt-3 max-w-md text-fd-muted-foreground">{body}</p>
      {action && (
        <Link href={action.href} className="mt-6 inline-flex items-center gap-1.5 text-sm font-medium hover:underline">
          {action.text} <ArrowRight className="size-4" />
        </Link>
      )}
    </div>
  );
}

function GridBackdrop() {
  return (
    <div
      aria-hidden="true"
      className="pointer-events-none absolute inset-0 [background-image:linear-gradient(to_right,var(--color-fd-border)_1px,transparent_1px),linear-gradient(to_bottom,var(--color-fd-border)_1px,transparent_1px)] [background-size:48px_48px] [mask-image:radial-gradient(ellipse_at_top,black_20%,transparent_70%)]"
    />
  );
}
