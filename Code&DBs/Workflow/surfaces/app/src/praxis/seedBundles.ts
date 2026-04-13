import type { PraxisSurfaceBundleV4 } from './manifest';

function cloneBundle(bundle: PraxisSurfaceBundleV4): PraxisSurfaceBundleV4 {
  return JSON.parse(JSON.stringify(bundle)) as PraxisSurfaceBundleV4;
}

const blankWorkspace: PraxisSurfaceBundleV4 = {
  version: 4,
  kind: 'praxis_surface_bundle',
  title: 'Blank Workspace',
  default_tab_id: 'main',
  tabs: [
    {
      id: 'main',
      label: 'Workspace',
      surface_id: 'main',
      source_option_ids: ['workspace_notes', 'web_search'],
    },
  ],
  surfaces: {
    main: {
      id: 'main',
      title: 'Workspace',
      kind: 'quadrant_manifest',
      manifest: {
        version: 2,
        grid: '4x4',
        title: 'Blank Workspace',
        quadrants: {
          A1: {
            module: 'markdown',
            span: '2x1',
            config: {
              content: 'Start with a source pill or add blocks to shape this workspace.',
            },
          },
          A3: {
            module: 'search-panel',
            span: '2x1',
            config: {
              placeholder: 'Search across attached sources',
            },
          },
        },
      },
    },
  },
  source_options: {
    workspace_notes: {
      id: 'workspace_notes',
      label: 'Workspace Notes',
      family: 'workspace',
      kind: 'document',
      availability: 'ready',
      activation: 'open',
      description: 'Open the local notes and scratch context for this workspace.',
    },
    web_search: {
      id: 'web_search',
      label: 'Web Search',
      family: 'external',
      kind: 'web_search',
      availability: 'ready',
      activation: 'open',
      description: 'Look up current public information when local state is not enough.',
    },
  },
  description: 'Minimal workspace seed with lightweight source pills.',
};

const entityWorkspace: PraxisSurfaceBundleV4 = {
  version: 4,
  kind: 'praxis_surface_bundle',
  title: 'Entity Workspace',
  default_tab_id: 'main',
  tabs: [
    {
      id: 'main',
      label: 'Entities',
      surface_id: 'main',
      source_option_ids: ['workspace_records', 'connected_crm', 'external_api'],
    },
  ],
  surfaces: {
    main: {
      id: 'main',
      title: 'Entities',
      kind: 'quadrant_manifest',
      manifest: {
        version: 2,
        grid: '4x4',
        title: 'Entity Workspace',
        quadrants: {
          A1: {
            module: 'search-panel',
            span: '2x1',
            config: {
              placeholder: 'Find records, entities, or artifacts',
            },
          },
          A3: {
            module: 'data-table',
            span: '2x2',
            config: {
              objectType: 'task',
              columns: [],
            },
          },
          C1: {
            module: 'key-value',
            span: '2x2',
            config: {
              title: 'Selection Details',
              subscribeSelection: 'task',
            },
          },
        },
      },
    },
  },
  source_options: {
    workspace_records: {
      id: 'workspace_records',
      label: 'Workspace Records',
      family: 'workspace',
      kind: 'object',
      availability: 'ready',
      activation: 'attach',
      description: 'Attach local records from the workspace object store.',
    },
    connected_crm: {
      id: 'connected_crm',
      label: 'Connected CRM',
      family: 'connected',
      kind: 'integration',
      availability: 'preview',
      activation: 'open',
      description: 'Preview connected CRM entities before wiring them into the surface.',
    },
    external_api: {
      id: 'external_api',
      label: 'External API',
      family: 'external',
      kind: 'api',
      availability: 'setup_required',
      activation: 'configure',
      setup_intent: 'Set up an external entity API source for this workspace.',
      description: 'Configure a third-party entity API before using it here.',
    },
  },
  description: 'Workspace seed for browsing and inspecting records with compact source pills.',
};

const workflowReview: PraxisSurfaceBundleV4 = {
  version: 4,
  kind: 'praxis_surface_bundle',
  title: 'Workflow Review',
  default_tab_id: 'main',
  tabs: [
    {
      id: 'main',
      label: 'Review',
      surface_id: 'main',
      source_option_ids: ['run_outputs', 'approval_notes', 'web_search'],
    },
  ],
  surfaces: {
    main: {
      id: 'main',
      title: 'Review',
      kind: 'quadrant_manifest',
      manifest: {
        version: 2,
        grid: '4x4',
        title: 'Workflow Review',
        quadrants: {
          A1: {
            module: 'metric',
            span: '1x1',
            config: {
              label: 'Open Checks',
              value: '0',
            },
          },
          B1: {
            module: 'metric',
            span: '1x1',
            config: {
              label: 'Outputs',
              value: '0',
            },
          },
          A2: {
            module: 'activity-feed',
            span: '2x2',
            config: {
              title: 'Recent Activity',
            },
          },
          C1: {
            module: 'key-value',
            span: '2x3',
            config: {
              title: 'Selected Output',
              subscribeSelection: 'workflow_output',
            },
          },
        },
      },
    },
  },
  source_options: {
    run_outputs: {
      id: 'run_outputs',
      label: 'Run Outputs',
      family: 'workspace',
      kind: 'manifest',
      availability: 'ready',
      activation: 'open',
      description: 'Open saved run outputs and execution artifacts for this workflow.',
    },
    approval_notes: {
      id: 'approval_notes',
      label: 'Approval Notes',
      family: 'reference',
      kind: 'document',
      availability: 'preview',
      activation: 'open',
      description: 'Review notes, checkpoints, and context before approving changes.',
    },
    web_search: {
      id: 'web_search',
      label: 'Web Search',
      family: 'external',
      kind: 'web_search',
      availability: 'ready',
      activation: 'open',
      description: 'Look up current public information when local state is not enough.',
    },
  },
  description: 'Workflow review surface with output, activity, and approval context.',
};

export const seedBundles = [
  {
    id: 'blank_workspace',
    label: 'Blank Workspace',
    description: 'Minimal workspace plus compact local/reference pills.',
    bundle: blankWorkspace,
  },
  {
    id: 'entity_workspace',
    label: 'Entity Workspace',
    description: 'Records + detail pane with connected and external source pills.',
    bundle: entityWorkspace,
  },
  {
    id: 'workflow_review',
    label: 'Workflow Review',
    description: 'Run outputs, checkpoints, and review context in one tab.',
    bundle: workflowReview,
  },
] as const;

export function getSeedBundle(seedId: string): PraxisSurfaceBundleV4 | null {
  const entry = seedBundles.find((candidate) => candidate.id === seedId);
  return entry ? cloneBundle(entry.bundle) : null;
}
