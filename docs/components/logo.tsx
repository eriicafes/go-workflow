import type { ImgHTMLAttributes } from 'react';

/**
 * The go-workflow mark: the Go gopher with the Workflow SDK step mark on its
 * belly. The gopher was designed by Renee French and is licensed under
 * CC BY 4.0, so pages that show it credit her (see the home page footer).
 */
export function Logo({ className, ...props }: ImgHTMLAttributes<HTMLImageElement>) {
  return (
    // eslint-disable-next-line @next/next/no-img-element
    <img
      src="/logo-gopher.svg"
      alt=""
      aria-hidden="true"
      className={`object-contain ${className ?? ''}`}
      {...props}
    />
  );
}
