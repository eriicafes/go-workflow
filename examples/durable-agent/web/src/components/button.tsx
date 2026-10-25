import type { ComponentProps } from 'react';
import { cn } from 'cn';

export function Button({
  variant = 'primary',
  className,
  ...props
}: ComponentProps<'button'> & { variant?: 'primary' | 'secondary' }) {
  return (
    <button
      type="button"
      {...props}
      className={cn(
        'cursor-pointer rounded-lg border px-3.5 py-1.5 disabled:cursor-default disabled:opacity-50',
        variant === 'primary'
          ? 'border-blue-600 bg-blue-600 text-white'
          : 'border-stone-300 bg-transparent dark:border-stone-700',
        className,
      )}
    />
  );
}
