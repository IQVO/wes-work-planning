import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api/rest/wes-work-planning-release",
    },
    {
      type: "category",
      label: "Charge Forecast",
      link: {
        type: "doc",
        id: "api/rest/charge-forecast",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/receive-charge-forecast",
          label: "Receive a charge forecast for a process path",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Shift Plan",
      link: {
        type: "doc",
        id: "api/rest/shift-plan",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/commit-shift-plan",
          label: "Commit this service's shift plan for a process path",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Work Units",
      link: {
        type: "doc",
        id: "api/rest/work-units",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/enqueue-work-unit",
          label: "Enqueue a work unit onto a process path's work pool",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api/rest/get-work-units-by-reference",
          label: "Look up work units by their external order-line reference",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rest/get-work-unit",
          label: "Get one work unit by its own identity",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rest/record-completion",
          label: "Record completion of a released work unit",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Release",
      link: {
        type: "doc",
        id: "api/rest/release",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/release-next-work",
          label: "Release the next highest-priority work unit on a process path",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Pool Configuration",
      link: {
        type: "doc",
        id: "api/rest/pool-configuration",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/configure-pool",
          label: "Configure a process path's work pool (feed mode and WIP limit)",
          className: "api-method put",
        },
      ],
    },
    {
      type: "category",
      label: "Telemetry",
      link: {
        type: "doc",
        id: "api/rest/telemetry",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/sample-backlog",
          label: "Sample live backlog telemetry for a process path",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Rebalance",
      link: {
        type: "doc",
        id: "api/rest/rebalance",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/rebalance-decision",
          label: "Get the flow-balancing recommendation for a process path",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Labor Plan View",
      link: {
        type: "doc",
        id: "api/rest/labor-plan-view",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/get-labor-plan-view",
          label: "Get the latest labor plan Workforce Management reported for a path",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Inventory View",
      link: {
        type: "doc",
        id: "api/rest/inventory-view",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/get-inventory-view",
          label: "Get the latest observed usable inventory for a SKU",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Health",
      link: {
        type: "doc",
        id: "api/rest/health",
      },
      items: [
        {
          type: "doc",
          id: "api/rest/health-check",
          label: "Health check",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rest/readiness-check",
          label: "Readiness check",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
