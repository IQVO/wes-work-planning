import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api/rest-reports/wes-work-planning-reports-api",
    },
    {
      type: "category",
      label: "Reports",
      link: {
        type: "doc",
        id: "api/rest-reports/reports",
      },
      items: [
        {
          type: "doc",
          id: "api/rest-reports/get-release-throughput-report",
          label: "Release throughput and backlog health per process path and UTC hour",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rest-reports/get-release-throughput-report-freshness",
          label: "How far the read model trails the event stream",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Health",
      link: {
        type: "doc",
        id: "api/rest-reports/health",
      },
      items: [
        {
          type: "doc",
          id: "api/rest-reports/get-reports-healthz",
          label: "Liveness probe",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
