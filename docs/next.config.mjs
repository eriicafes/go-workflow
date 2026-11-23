import { createMDX } from 'fumadocs-mdx/next';

const withMDX = createMDX();

/** @type {import('next').NextConfig} */
const config = {
  reactStrictMode: true,
  async redirects() {
    // Section roots without an index page.
    const sections = [
      { source: '/docs/tooling', destination: '/docs/tooling/dev' },
      { source: '/docs/reference', destination: '/docs/reference/workflow' },
    ];
    // URLs of the previous VitePress site. Guide pages moved to the docs root.
    const renamed = {
      introduction: '/docs',
      'getting-started': '/docs/quickstart',
      encryption: '/docs/data',
    };
    const legacy = [
      ...Object.entries(renamed).map(([from, to]) => ({ source: `/guide/${from}`, destination: to })),
      { source: '/guide', destination: '/docs' },
      { source: '/guide/:path*', destination: '/docs/:path*' },
      { source: '/:section(worlds|tooling|reference)', destination: '/docs/:section' },
      { source: '/:section(worlds|tooling|reference)/:path*', destination: '/docs/:section/:path*' },
    ];
    return [
      ...sections.map((r) => ({ ...r, permanent: false })),
      ...legacy.map((r) => ({ ...r, permanent: true })),
    ];
  },
  async rewrites() {
    return [
      {
        source: '/docs/:path*.md',
        destination: '/llms.mdx/docs/:path*.md',
      },
      {
        source: '/docs/:path*.txt',
        destination: '/llms.mdx/docs/:path*.txt',
      },
    ];
  },
};

export default withMDX(config);
