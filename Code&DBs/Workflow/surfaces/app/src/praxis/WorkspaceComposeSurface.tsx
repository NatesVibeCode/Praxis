import React, { useEffect, useMemo, useState } from 'react';
import { materializePlan, triggerWorkflow } from '../shared/buildController';
import type { BuildPayload } from '../shared/types';
import type { ComposeDraftSpec, ComposeSurfaceSpec, PraxisSurfaceBundleV4 } from './manifest';
import {
  WorkspaceClauseEditor,
  WorkspaceCompiledReceiptGrid,
  WorkspaceContractList,
  WorkspacePathScopePicker,
  WorkspaceVerifierCard,
  deriveRequirements,
  lineCountLabel,
  normalizeLineDraft,
  verifierDisplayName,
  verifierHelpText,
  type WorkspaceVerifierRef,
} from './WorkspaceComposePrimitives';

interface WorkspaceComposeSurfaceProps {
  manifestId: string;
  bundle: PraxisSurfaceBundleV4;
  surface: ComposeSurfaceSpec;
  workspaceTitle: string;
  onSaveBundle: (nextBundle: PraxisSurfaceBundleV4) => Promise<PraxisSurfaceBundleV4>;
}

interface GenerateResult {
  manifest_id?: string;
  manifest?: Record<string, unknown>;
  confidence?: number;
  explanation?: string;
  typed_gap?: Record<string, unknown>;
}

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function stringValue(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

const STANDARD_READ_SCOPE = [
  'Code&DBs/Workflow/',
  'config/',
  'docs/',
];

const STANDARD_WRITE_SCOPE = [
  'Code&DBs/Workflow/surfaces/app/',
  'Code&DBs/Workflow/runtime/',
  'Code&DBs/Workflow/tests/',
];

export function WorkspaceComposeSurface({
  manifestId,
  bundle,
  surface,
  workspaceTitle,
  onSaveBundle,
}: WorkspaceComposeSurfaceProps) {
  const draft = surface.draft ?? {};
  const [intent, setIntent] = useState(draft.intent ?? '');
  const [readScopeDraft, setReadScopeDraft] = useState<string[]>(draft.read_scope?.length ? draft.read_scope : []);
  const [writeScopeDraft, setWriteScopeDraft] = useState<string[]>(draft.write_scope?.length ? draft.write_scope : []);
  const [requirementDraft, setRequirementDraft] = useState<string[]>(draft.requirements?.length ? draft.requirements : []);
  const [antiRequirementDraft, setAntiRequirementDraft] = useState<string[]>(draft.anti_requirements?.length ? draft.anti_requirements : []);
  const [verifierRef, setVerifierRef] = useState(draft.verifier_ref ?? '');
  const [verifiers, setVerifiers] = useState<WorkspaceVerifierRef[]>([]);
  const [generateResult, setGenerateResult] = useState<GenerateResult | null>(null);
  const [buildPayload, setBuildPayload] = useState<BuildPayload | null>(null);
  const [compiledFingerprint, setCompiledFingerprint] = useState<string | null>(null);
  const [runId, setRunId] = useState(draft.run_id ?? '');
  const [busy, setBusy] = useState<'compile' | 'dispatch' | 'save' | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const loadVerifiers = async () => {
      try {
        const response = await fetch('/api/verifiers?limit=100');
        const payload = await response.json().catch(() => null);
        if (!response.ok) throw new Error(payload?.detail || payload?.error || 'Verifier catalog unavailable');
        if (!cancelled) {
          const items = Array.isArray(payload?.items) ? payload.items as WorkspaceVerifierRef[] : [];
          setVerifiers(items);
          setVerifierRef((current) => current || (items.length === 1 ? items[0].verifier_ref : current));
        }
      } catch {
        if (!cancelled) setVerifiers([]);
      }
    };
    void loadVerifiers();
    return () => {
      cancelled = true;
    };
  }, []);

  const readScopeLines = useMemo(() => normalizeLineDraft(readScopeDraft), [readScopeDraft]);
  const writeScopeLines = useMemo(() => normalizeLineDraft(writeScopeDraft), [writeScopeDraft]);
  const requirementLines = useMemo(() => normalizeLineDraft(requirementDraft), [requirementDraft]);
  const antiRequirementLines = useMemo(() => normalizeLineDraft(antiRequirementDraft), [antiRequirementDraft]);
  const derivedRequirements = useMemo(() => deriveRequirements(intent), [intent]);
  const activeVerifier = useMemo(
    () => verifiers.find((verifier) => verifier.verifier_ref === verifierRef) ?? null,
    [verifierRef, verifiers],
  );
  const fingerprint = useMemo(() => JSON.stringify({
    intent: intent.trim(),
    read_scope: readScopeLines,
    write_scope: writeScopeLines,
    requirements: requirementLines,
    anti_requirements: antiRequirementLines,
    verifier_ref: verifierRef || null,
  }), [antiRequirementLines, intent, readScopeLines, requirementLines, verifierRef, writeScopeLines]);
  const previewIsFresh = Boolean(compiledFingerprint && compiledFingerprint === fingerprint);
  const workflowId = stringValue(buildPayload?.workflow?.id);
  const operationReceiptId = stringValue(record(buildPayload?.operation_receipt).receipt_id);
  const verifierMissing = verifiers.length > 0 && !verifierRef;
  const canDispatch = Boolean(workflowId && previewIsFresh && !verifierMissing && busy !== 'dispatch');
  const verifierLabel = activeVerifier
    ? verifierDisplayName(activeVerifier, verifierRef)
    : verifierRef
      ? verifierDisplayName(null, verifierRef)
      : verifiers.length
        ? 'unselected proof gate'
        : 'no proof gates';
  const verifierDescription = activeVerifier
    ? verifierHelpText(activeVerifier)
    : verifiers.length
      ? 'A proof gate is the check this run must satisfy before it can be sealed.'
      : 'No proof gates are registered yet. You can still draft and compile, but dispatch needs a registered proof gate.';
  const scopeLabel = readScopeLines.length || writeScopeLines.length ? 'selected' : 'not selected';
  const contractState = busy === 'compile'
    ? 'compiling'
    : buildPayload
      ? previewIsFresh
        ? 'compiled'
        : 'out of date'
      : 'draft';
  const readyLabel = canDispatch ? 'ready to dispatch' : buildPayload ? 'compile changed' : 'unsealed';

  const contractText = useMemo(() => {
    const sections = [
      intent.trim(),
      requirementLines.length ? `Requirements:\n${requirementLines.map((line) => `- ${line}`).join('\n')}` : '',
      readScopeLines.length ? `Read scope:\n${readScopeLines.map((line) => `- ${line}`).join('\n')}` : '',
      writeScopeLines.length ? `Write scope:\n${writeScopeLines.map((line) => `- ${line}`).join('\n')}` : '',
      antiRequirementLines.length ? `Anti-requirements:\n${antiRequirementLines.map((line) => `- ${line}`).join('\n')}` : '',
      verifierRef ? `Proof gate:\n- ${verifierRef}` : '',
    ].filter(Boolean);
    return sections.join('\n\n');
  }, [antiRequirementLines, intent, readScopeLines, requirementLines, verifierRef, writeScopeLines]);

  const draftBundle = (updates?: Partial<ComposeDraftSpec>): PraxisSurfaceBundleV4 => {
    const nextBundle = structuredClone(bundle) as PraxisSurfaceBundleV4;
    nextBundle.surfaces[surface.id] = {
      ...surface,
      draft: {
        intent,
        read_scope: readScopeLines,
        write_scope: writeScopeLines,
        requirements: requirementLines,
        anti_requirements: antiRequirementLines,
        verifier_ref: verifierRef || undefined,
        generated_manifest_id: generateResult?.manifest_id ?? draft.generated_manifest_id,
        workflow_id: workflowId || draft.workflow_id,
        run_id: runId || draft.run_id,
        last_compiled_at: previewIsFresh ? new Date().toISOString() : draft.last_compiled_at,
        ...updates,
      },
    };
    return nextBundle;
  };

  const saveDraft = async (updates?: Partial<ComposeDraftSpec>) => {
    setBusy('save');
    setError(null);
    try {
      await onSaveBundle(draftBundle(updates));
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : 'Draft could not save');
    } finally {
      setBusy(null);
    }
  };

  const compileContract = async () => {
    if (!intent.trim()) return;
    setBusy('compile');
    setError(null);
    setRunId('');
    try {
      const generateResponse = await fetch('/api/manifests/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          intent: contractText,
          read_scope: readScopeLines,
          write_scope: writeScopeLines,
          requirements: requirementLines,
          anti_requirements: antiRequirementLines,
          verifier_ref: verifierRef || undefined,
        }),
      });
      const generated = await generateResponse.json().catch(() => null) as GenerateResult | null;
      if (!generateResponse.ok) throw new Error(stringValue(record(generated).error) || 'Contract could not compile');
      setGenerateResult(generated);
      const materialized = await materializePlan(contractText, {
        title: workspaceTitle,
        fullCompose: true,
      });
      setBuildPayload(materialized);
      setCompiledFingerprint(fingerprint);
      await onSaveBundle(draftBundle({
        generated_manifest_id: generated?.manifest_id,
        workflow_id: materialized.workflow?.id,
        run_id: undefined,
        last_compiled_at: new Date().toISOString(),
      }));
    } catch (compileError) {
      setError(compileError instanceof Error ? compileError.message : 'Contract could not compile');
    } finally {
      setBusy(null);
    }
  };

  const dispatch = async () => {
    if (!workflowId) return;
    setBusy('dispatch');
    setError(null);
    try {
      const result = await triggerWorkflow(workflowId, {
        manifestId,
        operationReceiptId,
        dispatchedBy: 'workspace.compose',
        metadata: {
          generated_manifest_id: generateResult?.manifest_id,
          verifier_ref: verifierRef || null,
        },
      });
      setRunId(result.run_id);
      await onSaveBundle(draftBundle({ workflow_id: workflowId, run_id: result.run_id }));
    } catch (dispatchError) {
      setError(dispatchError instanceof Error ? dispatchError.message : 'Dispatch failed');
    } finally {
      setBusy(null);
    }
  };

  const generatedManifest = record(generateResult?.manifest);
  const generatedTitle = stringValue(generatedManifest.title) || stringValue(generatedManifest.name) || workspaceTitle;
  const intentWordCount = intent.trim() ? intent.trim().split(/\s+/).length : 0;

  return (
    <div className="workspace-compose" data-contract-state={contractState}>
      <div className="workspace-compose__main">
        <section className="workspace-compose__draft" aria-label="Compose contract">
          <div className="workspace-compose__intro">
            <div className="workspace-compose__eyebrow">compose · feature-build-and-review</div>
            <h1>Materialize something.</h1>
            <p>
              Write it like you would brief a colleague. Praxis compiles your prose into a sealed
              contract on the right: scope, requirements, anti-requirements, proof gate, before any code is dispatched.
            </p>
          </div>

          <label className="workspace-compose__field workspace-compose__field--intent">
            <span className="workspace-compose__field-label">
              <span className="workspace-compose__ord">1</span>
              <span>task</span>
              <span className="workspace-compose__field-hint">{intentWordCount ? lineCountLabel(intentWordCount, 'word') : 'empty'}</span>
            </span>
            <textarea
              value={intent}
              onChange={(event) => setIntent(event.target.value)}
              placeholder="Describe the outcome this workspace should produce."
              spellCheck
            />
          </label>

          <div className="workspace-compose__scope-row">
            <WorkspacePathScopePicker
              ordinal="2"
              label="read scope"
              hint={readScopeLines.length ? lineCountLabel(readScopeLines.length, 'path') : 'suggested'}
              explanation="What is your workflow allowed to read? Pick real repo paths so the contract cannot silently reach outside its lane."
              lines={readScopeDraft}
              placeholder="Search files or folders..."
              standardPaths={STANDARD_READ_SCOPE}
              standardDescription="Allows the workflow to inspect the main runtime, config, and docs areas. Narrow it later when the task is specific."
              emptyDescription="No read scope means the contract has no explicit read boundary yet. That is allowed for drafting, but weak before dispatch."
              onChange={setReadScopeDraft}
            />
            <WorkspacePathScopePicker
              ordinal="3"
              label="write scope"
              hint={writeScopeLines.length ? lineCountLabel(writeScopeLines.length, 'path') : 'suggested'}
              explanation="What is this workflow allowed to write or edit? Pick the smallest real paths that should be changed."
              lines={writeScopeDraft}
              placeholder="Search editable files or folders..."
              standardPaths={STANDARD_WRITE_SCOPE}
              standardDescription="Allows edits in the app surface, runtime layer, and tests. Use this only when the task spans those areas."
              emptyDescription="No write scope means the contract has no explicit edit boundary yet. That is allowed for drafting, but risky before dispatch."
              onChange={setWriteScopeDraft}
            />
          </div>

          <WorkspaceClauseEditor
            ordinal="4"
            label="requirements"
            hint={requirementLines.length ? lineCountLabel(requirementLines.length, 'clause') : 'derived until explicit'}
            explanation="What must be true when this workflow is done? Use / to tag real fields, objects, or flows inside the clause."
            clauses={requirementDraft}
            placeholder="{invoice.total_amount} must equal {ocr.calculated_total_amount}"
            addLabel="+ add requirement"
            tone="requirement"
            onChange={setRequirementDraft}
          />

          <WorkspaceClauseEditor
            ordinal="5"
            label="anti-requirements"
            hint={antiRequirementLines.length ? lineCountLabel(antiRequirementLines.length, 'clause') : 'recommended'}
            explanation="What must not happen? Use / for field locks, ceilings, mismatch rules, or reconciliation flow references."
            clauses={antiRequirementDraft}
            placeholder="cannot touch {invoice.vendor_id} or {purchase_order.item_total} must not exceed {bom.item_total}"
            addLabel="+ add anti-requirement"
            tone="anti"
            onChange={setAntiRequirementDraft}
          />

          <label className="workspace-compose__field workspace-compose__field--select">
            <span className="workspace-compose__field-label">
              <span className="workspace-compose__ord">6</span>
              <span>proof gate</span>
              <span className="workspace-compose__field-hint">{verifierLabel}</span>
            </span>
            <p className="workspace-compose__field-help">
              A proof gate is the check this dispatch must pass before it can be treated as sealed.
            </p>
            <select
              value={verifierRef}
              onChange={(event) => setVerifierRef(event.target.value)}
              title={verifierDescription}
            >
              <option value="">{verifiers.length ? 'Select proof gate' : 'No proof gates registered'}</option>
              {verifiers.map((verifier) => (
                <option key={verifier.verifier_ref} value={verifier.verifier_ref}>
                  {verifierDisplayName(verifier)}
                </option>
              ))}
            </select>
            <div className="workspace-compose__verifier-help" title={verifierDescription}>
              <strong>{verifierLabel}</strong>
              <span>{verifierDescription}</span>
            </div>
          </label>

          {error ? <div className="workspace-compose__error">{error}</div> : null}

          <div className="workspace-compose__actionbar">
            <span className="workspace-compose__status">
              scope <b>{scopeLabel}</b> / proof gate <b>{verifierLabel}</b> / clauses <b>{requirementLines.length + antiRequirementLines.length}</b> / {contractState}
            </span>
            <button
              type="button"
              className="workspace-compose__ghost"
              disabled={busy === 'save'}
              onClick={() => void saveDraft()}
            >
              {busy === 'save' ? 'Saving' : 'Save draft'}
            </button>
            <button
              type="button"
              className="workspace-compose__primary"
              disabled={!intent.trim() || busy === 'compile'}
              onClick={() => void compileContract()}
            >
              {busy === 'compile' ? 'Compiling' : 'Compile contract'}
            </button>
            <button
              type="button"
              className="workspace-compose__primary workspace-compose__primary--dispatch"
              disabled={!canDispatch}
              onClick={() => void dispatch()}
            >
              {busy === 'dispatch' ? 'Dispatching' : 'Seal & dispatch'}
            </button>
          </div>
        </section>

        <aside className="workspace-compose__manifest" aria-label="Compiled contract preview">
          <div className="workspace-compose__contract-kicker">contract · {contractState}</div>
          <div className="workspace-compose__manifest-header">
            <div className="workspace-compose__seal" aria-hidden="true" />
            <div>
              <div className="workspace-compose__manifest-name">{generatedTitle || surface.title}</div>
              <div className="workspace-compose__manifest-meta">
                draft · <b>{readyLabel}</b>
                {previewIsFresh ? ' · fresh' : ''}
              </div>
            </div>
          </div>

          {generateResult?.typed_gap ? (
            <div className="workspace-compose__gap">
              <strong>Typed gap</strong>
              <span>{stringValue(generateResult.typed_gap.reason) || 'No legal template matched this intent.'}</span>
            </div>
          ) : null}

          <WorkspaceContractList title="read scope" items={readScopeLines} empty="No read scope locked yet." locked />
          <WorkspaceContractList title="write scope" items={writeScopeLines} empty="No write scope locked yet." locked />
          <WorkspaceContractList
            title="requirements"
            items={requirementLines.length ? requirementLines : derivedRequirements}
            empty="Write a task brief to derive requirements."
            derived={!requirementLines.length}
          />
          <WorkspaceContractList title="anti-requirements" items={antiRequirementLines} empty="No anti-requirements yet." />

          <div className="workspace-compose__mblock">
            <h4>proof gate</h4>
            <WorkspaceVerifierCard
              verifierLabel={verifierLabel}
              verifierCount={verifiers.length}
              verifierMissing={verifierMissing}
              operationReceiptId={operationReceiptId}
              description={verifierDescription}
            />
          </div>

          {buildPayload ? (
            <WorkspaceCompiledReceiptGrid
              generatedManifestId={generateResult?.manifest_id}
              workflowId={workflowId}
              runId={runId}
              compiledSpec={buildPayload.compiled_spec ?? buildPayload.definition ?? {}}
            />
          ) : null}
        </aside>
      </div>

      <div className="workspace-compose__meta-rail">
        <span>STATUS · <b>{contractState}</b></span>
        <span>SCOPE · <b>{scopeLabel}</b></span>
        <span>PROOF GATE · <b>{verifierLabel}</b></span>
        <span>WORKSPACE · <b>{workspaceTitle}</b></span>
      </div>
    </div>
  );
}
