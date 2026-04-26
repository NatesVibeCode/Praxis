/**
 * Single helper that dispatches shell-navigation commands through the
 * operation_catalog_gateway via POST /api/operate.
 *
 * Each call produces an authority_operation_receipts row + an authority_events
 * row (event_required=TRUE on the registered command). The reducer for
 * ui_shell_state.live folds the events keyed by session_aggregate_ref so the
 * server-side projection stays in sync with the client's optimistic state.
 *
 * Anchored to decision.shell_navigation_cqrs.20260426.
 */

export type ShellNavigationOperation =
  | 'shell.surface.opened'
  | 'shell.tab.closed'
  | 'shell.draft.guard.consulted'
  | 'shell.history.popped'
  | 'shell.session.bootstrapped';

export interface OperationReceipt {
  receipt_id: string;
  operation_ref: string;
  operation_name: string;
  operation_kind: string;
  authority_domain_ref: string;
  execution_status: string;
  result_status: string | null;
  event_ids?: string[];
}

export interface DispatchResult {
  ok: boolean;
  operation_receipt?: OperationReceipt;
  error?: string;
  reason_code?: string;
  [key: string]: unknown;
}

export interface DispatchArgs {
  operation: ShellNavigationOperation;
  input: Record<string, unknown>;
}

export async function dispatchShellNavigation(args: DispatchArgs): Promise<DispatchResult> {
  const body = {
    operation_name: args.operation,
    input: args.input,
    mode: 'command',
  };

  let res: Response;
  try {
    res = await fetch('/api/operate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
      reason_code: 'shell.dispatch.network_error',
    };
  }

  let payload: DispatchResult;
  try {
    payload = (await res.json()) as DispatchResult;
  } catch (err) {
    return {
      ok: false,
      error: `shell.dispatch.parse_error: ${err instanceof Error ? err.message : String(err)}`,
      reason_code: 'shell.dispatch.parse_error',
    };
  }

  if (!res.ok && payload && payload.ok !== false) {
    payload.ok = false;
  }
  return payload;
}
