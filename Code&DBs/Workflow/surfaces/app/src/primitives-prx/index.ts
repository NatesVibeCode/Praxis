/**
 * primitives-prx — stateful React adapters for the prx-* CSS primitives.
 *
 * Per the round-2 review (operator_decisions:design-system-single-react-primitive-library):
 *   - Presentational primitives live in `src/primitives/`. This package is
 *     only for stateful adapters and typed data-attr helpers.
 *   - Existing primitives in `src/primitives/` (DataTable, MetricCard,
 *     StatsRow, ActivityFeed, LoadingSkeleton) were refactored in place
 *     to render the prx-* CSS structure — those are the canonical entry
 *     points for those shapes.
 *   - Only stateful behaviors that don't fit in pure CSS get adapters here.
 */
export { PromptInput } from './PromptInput';
export type { PromptInputProps, PromptRef } from './PromptInput';

export { Tabstrip } from './Tabstrip';
export type { TabstripProps, Tab } from './Tabstrip';

export { DispatchButton } from './DispatchButton';
export type { DispatchButtonProps, DispatchEvent, IdempotencyPolicy } from './DispatchButton';

// Typed-data-attr helpers + const-asserted enums
export {
  SOURCE_KINDS, TONES, STATUS_TONES, GATE_STATES, RECEIPT_STATES,
  FLOW_NODE_STATES, LED_TONES, SPINNER_SETS, ICON_SIZES, ICON_TONES,
  OPERATION_KINDS, IDEMPOTENCY_POLICIES,
  chipProps, gateProps, receiptProps, flowNodeProps, ledProps,
  statusCapProps, iconTileProps,
} from './types';
export type {
  SourceKind, ChipTone, StatusTone, GateState, ReceiptState,
  FlowNodeState, LedTone, SpinnerSet, IconSize, IconTone,
  OperationKind,
} from './types';

// Telemetry bridge — frontend prx:* events → gateway operation
export {
  installTelemetry, uninstallTelemetry, setForwarder,
} from './telemetry';
export type {
  TrackedEventName, PrimitiveTelemetryPayload,
} from './telemetry';

// Lazy mode-stylesheet loader for the React app
export {
  applyMode, currentMode, nextMode,
} from './lazyMode';
export type { Mode } from './lazyMode';

// Live telemetry inspector
export { PrimitiveUsagePanel } from './PrimitiveUsagePanel';
export { PrimitiveUsageOverlay } from './PrimitiveUsageOverlay';
