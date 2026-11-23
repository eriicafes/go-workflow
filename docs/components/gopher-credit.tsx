/** Attribution the Go gopher's CC BY 4.0 license requires wherever the logo is shown. */
export function GopherCredit({ className }: { className?: string }) {
  return (
    <span className={className}>
      Go gopher by{' '}
      <a href="https://reneefrench.blogspot.com" className="underline-offset-2 hover:underline">
        Renee French
      </a>
      ,{' '}
      <a href="https://creativecommons.org/licenses/by/4.0/" className="underline-offset-2 hover:underline">
        CC BY 4.0
      </a>
    </span>
  );
}
