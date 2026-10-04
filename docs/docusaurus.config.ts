import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import type * as OpenApiPlugin from 'docusaurus-plugin-openapi-docs';

const config: Config = {
  title: 'WES — Work Planning & Release',
  tagline:
    'The conductor of the distribution centre: turns a shift’s charge into a plan, releases work continuously, and flow-balances off live buffer telemetry.',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://iqvo.github.io',
  baseUrl: '/wes-work-planning/',

  organizationName: 'IQVO',
  projectName: 'wes-work-planning',
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  themes: ['@docusaurus/theme-mermaid', 'docusaurus-theme-openapi-docs'],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: 'docs',
          editUrl:
            'https://github.com/IQVO/wes-work-planning/tree/main/docs/',
          docItemComponent: '@theme/ApiItem',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    'docusaurus-plugin-sass',
    [
      'docusaurus-plugin-openapi-docs',
      {
        id: 'openapi',
        docsPluginId: 'classic',
        config: {
          wes: {
            specPath: '../apis/openapi.yaml',
            outputDir: 'docs/api/rest',
            downloadUrl:
              'https://raw.githubusercontent.com/IQVO/wes-work-planning/main/apis/openapi.yaml',
            sidebarOptions: {
              groupPathsBy: 'tag',
              categoryLinkSource: 'tag',
            },
          } satisfies OpenApiPlugin.Options,
        },
      },
    ],
  ],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'WES Work Planning & Release',
      logo: {
        alt: 'WES Work Planning & Release',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/api/rest/wes-work-planning-release',
          label: 'API Reference',
          position: 'left',
        },
        {
          to: '/docs/adr/',
          label: 'ADRs',
          position: 'left',
        },
        {
          href: 'https://github.com/IQVO/wes-work-planning',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Overview', to: '/docs/overview/'},
            {label: 'Business Context', to: '/docs/business-context/domain-vision'},
            {label: 'Domain-Driven Design', to: '/docs/ddd/subdomain-classification'},
            {label: 'Architecture Decision Records', to: '/docs/adr/'},
          ],
        },
        {
          title: 'Contracts',
          items: [
            {label: 'REST API Reference', to: '/docs/api/rest/wes-work-planning-release'},
            {label: 'Events (AsyncAPI)', to: '/docs/api/events'},
            {
              label: 'openapi.yaml',
              href: 'https://github.com/IQVO/wes-work-planning/blob/main/apis/openapi.yaml',
            },
            {
              label: 'asyncapi.yaml',
              href: 'https://github.com/IQVO/wes-work-planning/blob/main/apis/asyncapi.yaml',
            },
          ],
        },
        {
          title: 'Ecosystem',
          items: [
            {label: 'Context Map', to: '/docs/ecosystem/context-map'},
            {
              label: 'inventory-storage',
              href: 'https://github.com/IQVO/inventory-storage',
            },
            {
              label: 'workforce-management',
              href: 'https://github.com/IQVO/workforce-management',
            },
            {
              label: 'fulfillment-execution',
              href: 'https://github.com/IQVO/fulfillment-execution',
            },
            {
              label: 'facility-layout',
              href: 'https://github.com/IQVO/facility-layout',
            },
          ],
        },
      ],
      copyright: `warehouse-systems · WES Work Planning &amp; Release · ${new Date().getFullYear()}`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'json', 'go', 'yaml', 'sql'],
    },
    mermaid: {
      theme: {light: 'neutral', dark: 'dark'},
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
