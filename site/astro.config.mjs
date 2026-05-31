import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightClientMermaid from '@pasqal-io/starlight-client-mermaid';

export default defineConfig({
  site: 'https://smithclay.github.io',
  base: '/canardstack',
  redirects: {
    '/get-started': '/tutorials/local-observability-stack',
    '/quickstart/serve': '/how-to/serve-ducklake',
    '/guides/lakehouse-ingest': '/how-to/write-with-duckdb-otlp',
    '/guides/query-with-grafana': '/how-to/connect-grafana',
    '/guides/query-with-duckdb': '/how-to/query-ducklake-with-sql',
    '/reference/api': '/reference/http-api',
    '/reference/schema': '/reference/storage-schema',
    '/reference/server': '/reference/runtime',
    '/reference/operational-limits': '/reference/runtime',
    '/architecture': '/explanation/query-only-architecture',
  },
  integrations: [
    starlight({
      title: 'canardstack',
      description: 'Query OpenTelemetry-shaped data stored in DuckLake.',
      logo: {
        src: './src/assets/canardstack.png',
        alt: 'canardstack logo',
      },
      customCss: ['./src/styles/brand.css'],
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/smithclay/canardstack',
        },
      ],
      editLink: {
        baseUrl: 'https://github.com/smithclay/canardstack/edit/main/site/',
      },
      plugins: [starlightClientMermaid()],
      sidebar: [
        {
          label: 'Start',
          items: [
            { label: 'Overview', link: '/' },
            {
              label: 'Tutorial: local observability stack',
              link: '/tutorials/local-observability-stack/',
            },
          ],
        },
        {
          label: 'How-to Guides',
          items: [
            { label: 'Serve an existing DuckLake catalog', link: '/how-to/serve-ducklake/' },
            { label: 'Write telemetry with duckdb-otlp', link: '/how-to/write-with-duckdb-otlp/' },
            { label: 'Connect Grafana', link: '/how-to/connect-grafana/' },
            { label: 'Query DuckLake with SQL', link: '/how-to/query-ducklake-with-sql/' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'HTTP API', link: '/reference/http-api/' },
            { label: 'Storage schema', link: '/reference/storage-schema/' },
            { label: 'Runtime contract', link: '/reference/runtime/' },
          ],
        },
        {
          label: 'Explanation',
          items: [
            { label: 'Query-only architecture', link: '/explanation/query-only-architecture/' },
          ],
        },
        {
          label: 'Project Docs',
          items: [
            {
              label: 'Repository README',
              link: 'https://github.com/smithclay/canardstack#readme',
            },
            {
              label: 'Architecture Docs',
              link: 'https://github.com/smithclay/canardstack/tree/main/docs/architecture',
            },
          ],
        },
      ],
    }),
  ],
});
