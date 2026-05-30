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
          label: 'Start Here',
          items: [
            { label: 'Overview', link: '/' },
            { label: 'Architecture', link: '/architecture/' },
            {
              label: 'Local E2E Smoke',
              link: 'https://github.com/smithclay/canardstack/blob/main/docs/e2e-duckdb-otlp.md',
            },
          ],
        },
        {
          label: 'Operations',
          items: [{ label: 'Overview', link: '/operations/' }],
        },
        {
          label: 'Query Data',
          items: [
            { label: 'Grafana Datasources', link: '/query-data/grafana/' },
            { label: 'DuckDB SQL', link: '/query-data/duckdb/' },
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
