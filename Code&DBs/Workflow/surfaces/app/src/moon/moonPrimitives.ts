import type {
  BuildNode,
  BuildNodeIntegrationArgs,
  HttpRequestIntegrationArgs,
  HttpRequestPreset,
  NotificationIntegrationArgs,
  WorkflowInvokeIntegrationArgs,
} from '../shared/types';

export interface PrimitiveSurfacePolicy {
  tier: 'primary' | 'advanced' | 'hidden';
  badge: string;
  detail: string;
  hardChoice?: string;
}

export interface MoonPrimitiveSpec {
  route: string;
  surface?: PrimitiveSurfacePolicy;
  scaffold?: (
    node: BuildNode,
    options: {
      title: string;
      summary: string;
    },
  ) => Partial<BuildNode>;
}

export const HTTP_REQUEST_PRESETS = [
  {
    value: 'fetch_json',
    label: 'Fetch JSON',
    description: 'GET a JSON endpoint and capture the response.',
    method: 'GET',
    headers: { Accept: 'application/json' } as Record<string, string>,
    body: undefined,
    urlPlaceholder: 'https://api.example.com/items/42',
    bodyPlaceholder: '',
  },
  {
    value: 'post_json',
    label: 'Send JSON',
    description: 'POST a structured JSON payload to an external API.',
    method: 'POST',
    headers: { Accept: 'application/json', 'Content-Type': 'application/json' } as Record<string, string>,
    body: {},
    urlPlaceholder: 'https://api.example.com/events',
    bodyPlaceholder: '{\n  "status": "ready"\n}',
  },
  {
    value: 'webhook_callback',
    label: 'Webhook callback',
    description: 'POST an event and status payload to another system.',
    method: 'POST',
    headers: { Accept: 'application/json', 'Content-Type': 'application/json' } as Record<string, string>,
    body: { event: 'workflow.completed', status: 'ok' },
    urlPlaceholder: 'https://example.com/webhooks/workflow-complete',
    bodyPlaceholder: '{\n  "event": "workflow.completed",\n  "status": "ok"\n}',
  },
  {
    value: 'custom',
    label: 'Custom',
    description: 'Keep manual control of method, headers, and body.',
    method: 'POST',
    headers: {},
    body: '',
    urlPlaceholder: 'https://example.com/endpoint',
    bodyPlaceholder: 'Body JSON or text\n{\n  "status": "ready"\n}',
  },
] as const;

export const DEFAULT_HTTP_REQUEST_PRESET: HttpRequestPreset = 'post_json';
const HTTP_REQUEST_PRESET_VALUES = new Set<HttpRequestPreset>(HTTP_REQUEST_PRESETS.map((preset) => preset.value));

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string');
}

export function normalizeIntegrationArgs(value: unknown): BuildNodeIntegrationArgs {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? { ...(value as Record<string, unknown>) }
    : {};
}

export function normalizeHttpHeaders(value: unknown): Record<string, string> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).flatMap(([key, headerValue]) => {
      const normalizedKey = String(key || '').trim();
      if (!normalizedKey) return [];
      return [[normalizedKey, String(headerValue ?? '').trim()]];
    }),
  );
}

export function requestMethodSupportsBody(method: string): boolean {
  const normalizedMethod = method.trim().toUpperCase();
  return normalizedMethod !== 'GET' && normalizedMethod !== 'DELETE';
}

export function getHttpRequestPreset(value: unknown): HttpRequestPreset | null {
  if (typeof value !== 'string') return null;
  const preset = value.trim() as HttpRequestPreset;
  return HTTP_REQUEST_PRESET_VALUES.has(preset) ? preset : null;
}

export function httpRequestPresetDefinition(preset: HttpRequestPreset) {
  return HTTP_REQUEST_PRESETS.find((candidate) => candidate.value === preset) || HTTP_REQUEST_PRESETS[1];
}

export function inferHttpRequestPreset(args: Record<string, unknown>): HttpRequestPreset {
  const explicitPreset = getHttpRequestPreset(args.request_preset);
  if (explicitPreset) return explicitPreset;

  const method = typeof args.method === 'string' ? args.method.trim().toUpperCase() : '';
  const headers = normalizeHttpHeaders(args.headers);
  const accept = (headers.Accept || headers.accept || '').toLowerCase();
  const contentType = (headers['Content-Type'] || headers['content-type'] || '').toLowerCase();
  const body = args.body ?? args.body_template;

  if (method === 'GET' && accept.includes('application/json')) {
    return 'fetch_json';
  }
  if (
    method === 'POST'
    && contentType.includes('application/json')
    && body
    && typeof body === 'object'
    && !Array.isArray(body)
    && typeof (body as Record<string, unknown>).event === 'string'
  ) {
    return 'webhook_callback';
  }
  if (method === 'POST' && contentType.includes('application/json')) {
    return 'post_json';
  }
  return 'custom';
}

export function buildNotificationIntegrationArgs(
  existing: BuildNodeIntegrationArgs,
  draft: {
    title: string;
    message: string;
    status: string;
    fallbackTitle: string;
    fallbackMessage: string;
  },
): NotificationIntegrationArgs {
  return {
    ...(existing as Record<string, unknown>),
    title: draft.title.trim() || draft.fallbackTitle || 'Notification',
    message: draft.message.trim() || draft.fallbackMessage,
    status: draft.status,
  };
}

export function buildHttpRequestIntegrationArgs(
  existing: BuildNodeIntegrationArgs,
  draft: {
    preset: HttpRequestPreset;
    url: string;
    method: string;
    headers: Record<string, unknown>;
    body: unknown;
    timeoutText: string;
  },
): HttpRequestIntegrationArgs {
  const presetDefinition = httpRequestPresetDefinition(draft.preset);
  const next: HttpRequestIntegrationArgs = {
    ...(existing as Record<string, unknown>),
    request_preset: draft.preset,
    url: draft.url,
    endpoint: draft.url,
    method: draft.method.trim() || presetDefinition.method,
    headers: Object.keys(draft.headers).length > 0 ? draft.headers : presetDefinition.headers,
  };

  delete next.body;
  delete next.body_template;
  if (requestMethodSupportsBody(next.method || '')) {
    if (draft.body !== undefined && draft.body !== '') {
      next.body = draft.body;
    } else if (presetDefinition.body !== undefined && draft.preset !== 'custom') {
      next.body = presetDefinition.body;
    }
  }

  delete next.timeout;
  if (draft.timeoutText.trim()) {
    next.timeout = Number(draft.timeoutText.trim());
  }
  return next;
}

export function buildWorkflowInvokeIntegrationArgs(
  existing: BuildNodeIntegrationArgs,
  draft: {
    workflowId: string;
    payload: unknown;
  },
): WorkflowInvokeIntegrationArgs {
  const next: WorkflowInvokeIntegrationArgs = {
    ...(existing as Record<string, unknown>),
    workflow_id: draft.workflowId,
    target_workflow_id: draft.workflowId,
  };

  delete next.payload;
  delete next.input;
  delete next.inputs;
  if (draft.payload !== undefined && draft.payload !== '') {
    next.payload = draft.payload;
  }
  return next;
}

const MOON_PRIMITIVES: Record<string, MoonPrimitiveSpec> = {
  'auto/research': {
    route: 'auto/research',
    surface: {
      tier: 'primary',
      badge: 'Core now',
      detail: 'Primary Moon step primitive with a real planned runtime route.',
    },
    scaffold: (node) => ({
      prompt: node.prompt || 'Research the request using the attached context and return grounded findings.',
      outputs: isStringArray(node.outputs) && node.outputs.length > 0 ? node.outputs : ['research_findings'],
    }),
  },
  'auto/classify': {
    route: 'auto/classify',
    surface: {
      tier: 'primary',
      badge: 'Core now',
      detail: 'Primary Moon step primitive with a real planned runtime route.',
    },
    scaffold: (node) => ({
      prompt: node.prompt || 'Classify the input, return the selected label, and explain the reasoning briefly.',
      outputs: isStringArray(node.outputs) && node.outputs.length > 0 ? node.outputs : ['classification', 'classification_reason'],
    }),
  },
  'auto/draft': {
    route: 'auto/draft',
    surface: {
      tier: 'primary',
      badge: 'Core now',
      detail: 'Primary Moon step primitive with a real planned runtime route.',
    },
    scaffold: (node) => ({
      prompt: node.prompt || 'Draft the requested output using the available context and preserve the requested tone.',
      outputs: isStringArray(node.outputs) && node.outputs.length > 0 ? node.outputs : ['draft_output'],
    }),
  },
  '@notifications/send': {
    route: '@notifications/send',
    surface: {
      tier: 'primary',
      badge: 'Core now',
      detail: 'Real action primitive with a stable property surface in the node inspector.',
    },
    scaffold: (node, options) => {
      const integrationArgs = normalizeIntegrationArgs(node.integration_args);
      return {
        integration_args: buildNotificationIntegrationArgs(integrationArgs, {
          title: typeof integrationArgs.title === 'string' ? integrationArgs.title : '',
          message: typeof integrationArgs.message === 'string' ? integrationArgs.message : '',
          status: typeof integrationArgs.status === 'string' ? integrationArgs.status : 'info',
          fallbackTitle: options.title,
          fallbackMessage: options.summary || `Notify when ${options.title.toLowerCase()} completes.`,
        }),
        outputs: isStringArray(node.outputs) && node.outputs.length > 0 ? node.outputs : ['notification_delivery'],
      };
    },
  },
  '@webhook/post': {
    route: '@webhook/post',
    surface: {
      tier: 'primary',
      badge: 'Core now',
      detail: 'Visible now that Moon offers opinionated request presets instead of a blank transport form.',
    },
    scaffold: (node) => {
      const integrationArgs = normalizeIntegrationArgs(node.integration_args);
      const requestPreset = getHttpRequestPreset(integrationArgs.request_preset) || DEFAULT_HTTP_REQUEST_PRESET;
      const presetDefinition = httpRequestPresetDefinition(requestPreset);
      return {
        integration_args: buildHttpRequestIntegrationArgs(integrationArgs, {
          preset: requestPreset,
          url: typeof integrationArgs.url === 'string'
            ? integrationArgs.url
            : typeof integrationArgs.endpoint === 'string'
              ? integrationArgs.endpoint
              : '',
          method: typeof integrationArgs.method === 'string' ? integrationArgs.method : presetDefinition.method,
          headers: normalizeHttpHeaders(integrationArgs.headers),
          body: integrationArgs.body ?? integrationArgs.body_template,
          timeoutText: integrationArgs.timeout == null ? '' : String(integrationArgs.timeout),
        }),
        outputs: isStringArray(node.outputs) && node.outputs.length > 0 ? node.outputs : ['http_response'],
      };
    },
  },
  '@workflow/invoke': {
    route: '@workflow/invoke',
    surface: {
      tier: 'primary',
      badge: 'Core now',
      detail: 'Visible now that Moon can pick saved child workflows by name from the inspector.',
    },
    scaffold: (node) => {
      const integrationArgs = normalizeIntegrationArgs(node.integration_args);
      return {
        integration_args: buildWorkflowInvokeIntegrationArgs(integrationArgs, {
          workflowId:
            typeof integrationArgs.workflow_id === 'string'
              ? integrationArgs.workflow_id
              : typeof integrationArgs.target_workflow_id === 'string'
                ? integrationArgs.target_workflow_id
                : '',
          payload: integrationArgs.payload ?? integrationArgs.input ?? integrationArgs.inputs,
        }),
        outputs: isStringArray(node.outputs) && node.outputs.length > 0 ? node.outputs : ['invoked_run'],
      };
    },
  },
  'workflow.fanout': {
    route: 'workflow.fanout',
    surface: {
      tier: 'primary',
      badge: 'API only',
      detail: 'Count-based burst. Use when you want N parallel SLM workers against the same prompt template — e.g. 40 Haiku workers for broad research or architecture sweeps. CLI adapters are rejected.',
    },
    scaffold: (node) => ({
      prompt: node.prompt || 'Burst N parallel workers over the same prompt template and return merged results.',
      outputs: isStringArray(node.outputs) && node.outputs.length > 0 ? node.outputs : ['fanout_results'],
    }),
  },
  'workflow.loop': {
    route: 'workflow.loop',
    surface: {
      tier: 'primary',
      badge: 'Any provider',
      detail: 'Item-based map. Use when you have a list of distinct inputs and want to run the same step over each — e.g. per-lead research, per-URL scrape.',
    },
    scaffold: (node) => ({
      prompt: node.prompt || 'For each item in the list, run this step and return per-item results.',
      outputs: isStringArray(node.outputs) && node.outputs.length > 0 ? node.outputs : ['loop_results'],
    }),
  },
};

export function getMoonPrimitive(route?: string | null): MoonPrimitiveSpec | null {
  if (!route) return null;
  return MOON_PRIMITIVES[route] || null;
}

export function scaffoldMoonPrimitiveNode(
  node: BuildNode,
  options: {
    actionValue: string;
    title: string;
    summary: string;
  },
): Partial<BuildNode> {
  const primitive = getMoonPrimitive(options.actionValue);
  return primitive?.scaffold ? primitive.scaffold(node, options) : {};
}
