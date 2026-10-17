import { defineConfig, type DefaultTheme } from 'vitepress'

const guideSidebar: DefaultTheme.SidebarItem[] = [
  {
    text: 'Introduction',
    items: [
      { text: 'What is go-workflow?', link: '/guide/introduction' },
      { text: 'Getting Started', link: '/guide/getting-started' },
      { text: 'How Replay Works', link: '/guide/replay' },
    ],
  },
  {
    text: 'Writing Workflows',
    items: [
      { text: 'Workflows and Runners', link: '/guide/workflows' },
      { text: 'Steps', link: '/guide/steps' },
      { text: 'Inline Steps', link: '/guide/inline-steps' },
      { text: 'Errors and Retries', link: '/guide/errors' },
      { text: 'Concurrency', link: '/guide/concurrency' },
      { text: 'Cancellation', link: '/guide/cancellation' },
      { text: 'Sleep and Timers', link: '/guide/sleep' },
      { text: 'Rollbacks', link: '/guide/rollbacks' },
      { text: 'Hooks', link: '/guide/hooks' },
      { text: 'Webhooks', link: '/guide/webhooks' },
      { text: 'Streams', link: '/guide/streams' },
      { text: 'Composing Workflows', link: '/guide/composition' },
    ],
  },
  {
    text: 'Running Workflows',
    items: [
      { text: 'Starting and Reading Runs', link: '/guide/runs' },
      { text: 'Application and Worker', link: '/guide/deployment' },
      { text: 'Testing', link: '/guide/testing' },
    ],
  },
  {
    text: 'Development',
    items: [
      { text: 'Development Loop', link: '/guide/development' },
      { text: 'go-dev Integration', link: '/tooling/dev' },
      { text: 'Workflow SDK Web UI', link: '/tooling/web-ui' },
      { text: 'world-http Adapter', link: '/tooling/world-http' },
      { text: 'Workflow Analyzer', link: '/tooling/analyzer' },
    ],
  },
]

export default defineConfig({
  title: 'go-workflow',
  description: 'Durable workflows for Go. Runners are ordinary Go functions whose steps are recorded and replayed.',
  cleanUrls: true,
  lastUpdated: true,
  head: [['link', { rel: 'icon', type: 'image/svg+xml', href: '/logo.svg' }]],
  markdown: {
    theme: { light: 'github-light', dark: 'github-dark' },
  },
  themeConfig: {
    logo: '/logo.svg',
    nav: [
      { text: 'Guide', link: '/guide/introduction', activeMatch: '/guide/' },
      { text: 'Worlds', link: '/worlds/', activeMatch: '/worlds/' },
      { text: 'Tooling', link: '/tooling/dev', activeMatch: '/tooling/' },
      { text: 'Reference', link: '/reference/workflow', activeMatch: '/reference/' },
      { text: 'pkg.go.dev', link: 'https://pkg.go.dev/github.com/eriicafes/go-workflow' },
    ],
    sidebar: {
      '/guide/': guideSidebar,
      '/worlds/': [
        {
          text: 'Worlds',
          items: [
            { text: 'Overview', link: '/worlds/' },
            { text: 'Local', link: '/worlds/local' },
            { text: 'Vercel', link: '/worlds/vercel' },
            { text: 'AWS', link: '/worlds/aws' },
            { text: 'Postgres', link: '/worlds/postgres' },
            { text: 'Test World', link: '/worlds/testworld' },
            { text: 'Custom Worlds', link: '/worlds/custom' },
          ],
        },
      ],
      '/tooling/': guideSidebar,
      '/reference/': [
        {
          text: 'Reference',
          items: [
            { text: 'workflow', link: '/reference/workflow' },
            { text: 'worlds', link: '/reference/worlds' },
            { text: 'Event Log', link: '/reference/events' },
            { text: 'HTTP Endpoints', link: '/reference/http' },
          ],
        },
      ],
    },
    socialLinks: [{ icon: 'github', link: 'https://github.com/eriicafes/go-workflow' }],
    editLink: {
      pattern: 'https://github.com/eriicafes/go-workflow/edit/main/docs/:path',
    },
    search: { provider: 'local' },
    outline: { level: [2, 3] },
    footer: {
      message: 'Released under the MIT License.',
    },
  },
})
