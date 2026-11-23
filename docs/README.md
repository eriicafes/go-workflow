# go-workflow docs

The documentation site, built with [Fumadocs](https://fumadocs.dev) on Next.js. Pages live in `content/docs` as MDX:

- `(guide)` is served at the docs root (`/docs/quickstart`, `/docs/steps`, ...).
- `worlds`, `tooling`, and `reference` are their own tabs.

Each folder's `meta.json` sets the sidebar order. A page's `sidebarTitle` frontmatter shortens its sidebar label.

```sh
pnpm install
pnpm dev
```

URLs of the previous VitePress site, such as `/guide/steps`, redirect to their new pages.

The logo (`public/logo-gopher.svg`, `app/icon.svg`) is the Go gopher from `go.dev/images/gophers/front.svg`, recolored to black and white with the Workflow SDK step mark added. The gopher was designed by Renee French and is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), so keep the credit (`components/gopher-credit.tsx`) wherever the logo appears.
