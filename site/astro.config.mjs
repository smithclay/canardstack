import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

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
      sidebar: [
        {
          label: 'Start Here',
          items: [
            { label: 'Overview', link: '/' },
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
