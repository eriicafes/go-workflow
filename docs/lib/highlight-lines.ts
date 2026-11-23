import { getHighlighter } from 'fumadocs-core/highlight';

export type Token = { content: string; light?: string; dark?: string };

/** Tokenizes code per line with the docs' light and dark Shiki themes. */
export async function highlightLines(code: string, lang: 'go'): Promise<Token[][]> {
  const highlighter = await getHighlighter('js', {
    langs: [lang],
    themes: ['github-light', 'github-dark'],
  });
  return highlighter
    .codeToTokens(code, {
      lang,
      themes: { light: 'github-light', dark: 'github-dark' },
      defaultColor: false,
    })
    .tokens.map((line) =>
      line.map((t) => ({
        content: t.content,
        light: t.htmlStyle?.['--shiki-light'],
        dark: t.htmlStyle?.['--shiki-dark'],
      })),
    );
}
