import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightClientMermaid from '@pasqal-io/starlight-client-mermaid';

export default defineConfig({
  site: 'https://smithclay.github.io',
  base: '/canardstack',
  integrations: [
    starlight({
      title: 'canardstack',
      description: 'OpenTelemetry logs, traces, and metrics stored in DuckLake.',
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
            { label: 'Demo', link: '/demo/' },
            { label: 'Architecture', link: '/architecture/' },
          ],
        },
        {
          label: 'Deployment',
          items: [
            { label: 'Overview', link: '/deployment/' },
            { label: 'Send Telemetry', link: '/deployment/send-telemetry/' },
            { label: 'MotherDuck', link: '/deployment/motherduck/' },
            { label: 'GCP Cloud Run', link: '/deployment/gcp-cloud-run/' },
            { label: 'AWS ECS/Fargate', link: '/deployment/aws-ecs-fargate/' },
          ],
        },
        {
          label: 'Operations',
          items: [
            { label: 'Overview', link: '/operations/' },
            { label: 'Failure Runbooks', link: '/operations/failure-runbooks/' },
          ],
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
