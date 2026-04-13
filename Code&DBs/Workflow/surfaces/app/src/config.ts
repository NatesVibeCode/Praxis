/**
 * App configuration — single source of truth for product identity.
 * Change the name here and it updates everywhere in the UI.
 */
export const APP_CONFIG = {
  name: 'Praxis',
  suiteName: 'Praxis',
  engineName: 'Praxis Engine',
  databaseName: 'Praxis.db',
  tagline: 'Build, inspect, and run workflows with explicit control.',
  vocabulary: {
    run: 'Run',
    workflow: 'Workflow',
    agent: 'Agent',
    builder: 'Builder',
    step: 'Step',
  },
} as const;
