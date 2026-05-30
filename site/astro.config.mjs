import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightClientMermaid from '@pasqal-io/starlight-client-mermaid';

export default defineConfig({
  site: 'https://smithclay.github.io',
  base: '/canardstack',
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
          label: 'Tutorials',
          items: [
            { label: 'Overview', link: '/' },
            { label: 'Get Started', link: '/get-started/' },
            { label: 'Serve DuckLake', link: '/quickstart/serve/' },
          ],
        },
        {
          label: 'How-to Guides',
          items: [
            { label: 'Write with duckdb-otlp', link: '/guides/lakehouse-ingest/' },
            { label: 'Query with Grafana', link: '/guides/query-with-grafana/' },
            { label: 'Query with DuckDB SQL', link: '/guides/query-with-duckdb/' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'API', link: '/reference/api/' },
            { label: 'Schema', link: '/reference/schema/' },
            { label: 'Server Contract', link: '/reference/server/' },
            { label: 'Operational Limits', link: '/reference/operational-limits/' },
          ],
        },
        {
          label: 'Explanation',
          items: [
            { label: 'Architecture', link: '/architecture/' },
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
