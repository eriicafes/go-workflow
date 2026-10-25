import {
  getToolName,
  isReasoningUIPart,
  isTextUIPart,
  isToolUIPart,
  type UIMessage,
} from 'ai';
import { cn } from 'cn';
import type { ReactNode } from 'react';
import { Button } from './components/button';
import { clockTime } from './utils';

type Part = UIMessage['parts'][number];

type Flight = {
  flightId: string;
  airline: string;
  departure: string;
  arrival: string;
  priceUsd: number;
};

type Booking = { flightId?: string; passengerName?: string };

type Confirmation = { confirmationCode: string; flightId: string; passengerName: string };

export function MessagePart({
  role,
  part,
  onApproval,
}: {
  role: UIMessage['role'];
  part: Part;
  onApproval: (approvalId: string, approved: boolean) => void;
}) {
  if (isTextUIPart(part)) {
    return (
      <p
        className={cn(
          'whitespace-pre-wrap',
          role === 'user' && 'rounded-2xl rounded-br-sm bg-blue-600 px-3 py-2 text-white',
        )}
      >
        {part.text}
      </p>
    );
  }
  if (isReasoningUIPart(part)) {
    return (
      <details className="text-sm text-stone-500">
        <summary className="cursor-pointer">Reasoning</summary>
        <p className="whitespace-pre-wrap">{part.text}</p>
      </details>
    );
  }
  if (!isToolUIPart(part)) {
    return null;
  }

  const name = getToolName(part);
  if (part.state === 'output-error') {
    return (
      <Card tone="danger">
        {name} failed: {part.errorText}
      </Card>
    );
  }

  if (name === 'searchFlights') {
    const search = (part.input ?? {}) as { origin?: string; destination?: string; date?: string };
    const route = `${search.origin} → ${search.destination} on ${search.date}`;
    if (part.state !== 'output-available') {
      return <Card tone="pending">Searching flights {route}…</Card>;
    }
    const flights = part.output as Flight[];
    return (
      <Card>
        <div className="mb-1.5 font-semibold">
          {flights.length} flights {route}
        </div>
        <table className="w-full border-collapse">
          <tbody>
            {flights.map(flight => (
              <tr key={flight.flightId} className="border-t border-stone-200 dark:border-stone-800">
                <td className="py-1 pr-2 font-mono text-[13px]">{flight.flightId}</td>
                <td className="py-1 pr-2">{flight.airline}</td>
                <td className="py-1 pr-2">
                  {clockTime(flight.departure)} – {clockTime(flight.arrival)}
                </td>
                <td className="py-1 text-right tabular-nums">${flight.priceUsd}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    );
  }

  if (name === 'bookFlight') {
    const booking = (part.input ?? {}) as Booking;
    const summary = (
      <>
        <span className="font-mono text-[13px]">{booking.flightId}</span> for {booking.passengerName}
      </>
    );
    switch (part.state) {
      case 'input-streaming':
      case 'input-available':
        return <Card tone="pending">Preparing booking {summary}…</Card>;
      case 'approval-requested':
        return (
          <Card tone="warning">
            <div className="mb-2 font-semibold">Book {summary}?</div>
            <div className="flex gap-2">
              <Button onClick={() => onApproval(part.approval.id, true)}>Approve</Button>
              <Button variant="secondary" onClick={() => onApproval(part.approval.id, false)}>
                Deny
              </Button>
            </div>
          </Card>
        );
      case 'approval-responded':
        return (
          <Card tone="pending">
            {part.approval.approved ? 'Approved, booking' : 'Declined'} {summary}…
          </Card>
        );
      case 'output-denied':
        return <Card tone="danger">Booking of {summary} declined.</Card>;
      case 'output-available': {
        const confirmation = part.output as Confirmation;
        return (
          <Card tone="success">
            Booked {summary}. Confirmation{' '}
            <span className="font-mono text-[13px]">{confirmation.confirmationCode}</span>
          </Card>
        );
      }
    }
  }

  return <Card>{name}</Card>;
}

function Card({
  tone,
  children,
}: {
  tone?: 'pending' | 'warning' | 'success' | 'danger';
  children: ReactNode;
}) {
  return (
    <div
      className={cn(
        'rounded-xl border bg-white px-3 py-2.5 text-sm dark:bg-stone-900',
        tone === 'warning'
          ? 'border-amber-500'
          : tone === 'success'
            ? 'border-green-600'
            : 'border-stone-200 dark:border-stone-800',
        tone === 'pending' && 'text-stone-500',
        tone === 'danger' && 'text-red-600 dark:text-red-400',
      )}
    >
      {children}
    </div>
  );
}
