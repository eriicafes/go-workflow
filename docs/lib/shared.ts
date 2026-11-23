import { createGetUrl } from 'fumadocs-core/source';

export const appName = 'go-workflow';
export const appDescription = 'Durable workflows for Go.';
export const docsRoute = '/docs';
export const docsImageRoute = '/og/docs';
export const docsContentPath = 'docs/content/docs';

export const gitConfig = {
  user: 'eriicafes',
  repo: 'go-workflow',
  branch: 'main',
};

const getImageUrl = createGetUrl(docsImageRoute);

export function getPageImageUrl(page: { slugs: string[]; locale?: string }) {
  const segments = [...page.slugs, 'image.png'];

  return { segments, url: getImageUrl(segments, page.locale) };
}
