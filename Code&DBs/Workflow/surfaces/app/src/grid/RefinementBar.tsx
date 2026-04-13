import React, { FormEvent, useEffect, useRef, useState } from 'react';
import './RefinementBar.css';

interface RefinementBarProps {
  manifestId: string;
  onRefined: (newManifest: any) => void;
}

type ManifestSnapshot = Record<string, unknown> & {
  id?: string;
  name?: string;
  title?: string;
};

interface RefinementResult {
  previousManifest: ManifestSnapshot;
  refinedManifest: Record<string, unknown>;
  changelog: string;
}

function stripManifestMetadata(manifest: ManifestSnapshot): Record<string, unknown> {
  const { id, name, ...rest } = manifest;
  return rest;
}

async function parseJsonSafe(response: Response): Promise<any> {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

export function RefinementBar({ manifestId, onRefined }: RefinementBarProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [instruction, setInstruction] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isUndoing, setIsUndoing] = useState(false);
  const [isDismissed, setIsDismissed] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<RefinementResult | null>(null);

  useEffect(() => {
    setInstruction('');
    setIsLoading(false);
    setIsUndoing(false);
    setIsDismissed(false);
    setError(null);
    setResult(null);
  }, [manifestId]);

  useEffect(() => {
    const focusInput = () => {
      requestAnimationFrame(() => {
        inputRef.current?.focus();
        inputRef.current?.select();
      });
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      const isFocusShortcut = (event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k';

      if (isFocusShortcut) {
        event.preventDefault();
        setIsDismissed(false);
        focusInput();
        return;
      }

      if (event.key === 'Escape') {
        setIsDismissed(true);
      }
    };

    const handleFillRefinement = (event: Event) => {
      const customEvent = event as CustomEvent<string>;
      setIsDismissed(false);
      setInstruction(customEvent.detail);
      focusInput();
    };

    window.addEventListener('keydown', handleKeyDown);
    window.addEventListener('fill-refinement', handleFillRefinement);
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
      window.removeEventListener('fill-refinement', handleFillRefinement);
    };
  }, []);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmedInstruction = instruction.trim();

    if (!trimmedInstruction || isLoading || result) {
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const currentManifestResponse = await fetch(`/api/manifests/${manifestId}`);
      const currentManifestPayload = await parseJsonSafe(currentManifestResponse);
      if (!currentManifestResponse.ok) {
        throw new Error(currentManifestPayload?.error || `Failed to load manifest ${manifestId}`);
      }

      const previousManifest = currentManifestPayload as ManifestSnapshot;

      const refineResponse = await fetch('/api/manifests/refine', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          manifest_id: manifestId,
          instruction: trimmedInstruction,
        }),
      });
      const refinePayload = await parseJsonSafe(refineResponse);
      if (!refineResponse.ok) {
        throw new Error(refinePayload?.error || `Failed to refine manifest ${manifestId}`);
      }

      setInstruction('');
      setResult({
        previousManifest,
        refinedManifest: (refinePayload?.manifest ?? {}) as Record<string, unknown>,
        changelog: typeof refinePayload?.changelog === 'string' && refinePayload.changelog.trim()
          ? refinePayload.changelog
          : 'Manifest refined successfully.',
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Refinement failed');
    } finally {
      setIsLoading(false);
    }
  };

  const handleAccept = () => {
    if (!result) {
      return;
    }
    setIsDismissed(true);
    onRefined(result.refinedManifest);
  };

  const handleUndo = async () => {
    if (!result || isUndoing) {
      return;
    }

    setIsUndoing(true);
    setError(null);

    try {
      const manifestName = typeof result.previousManifest.name === 'string' && result.previousManifest.name.trim()
        ? result.previousManifest.name
        : manifestId;

      const restoreResponse = await fetch('/api/manifests/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: manifestId,
          name: manifestName,
          manifest: stripManifestMetadata(result.previousManifest),
        }),
      });
      const restorePayload = await parseJsonSafe(restoreResponse);
      if (!restoreResponse.ok) {
        throw new Error(restorePayload?.error || `Failed to restore manifest ${manifestId}`);
      }

      const restoredManifest = stripManifestMetadata(result.previousManifest);
      setResult(null);
      setInstruction('');
      setIsDismissed(true);
      onRefined(restoredManifest);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Undo failed');
    } finally {
      setIsUndoing(false);
    }
  };

  if (isDismissed) {
    return null;
  }

  return (
    <div className="refinement-bar" role="region" aria-label="Manifest refinement bar">
      {!result ? (
        <form className="refinement-bar__form" onSubmit={handleSubmit}>
          <input
            ref={inputRef}
            className="refinement-bar__input"
            type="text"
            placeholder="Tell me what to change..."
            value={instruction}
            onChange={(event) => setInstruction(event.target.value)}
            disabled={isLoading}
          />
          <div className="refinement-bar__actions">
            <span className="refinement-bar__shortcut">Cmd/Ctrl+K</span>
            <button
              className="refinement-bar__button refinement-bar__button--accent"
              type="submit"
              disabled={isLoading || instruction.trim().length === 0}
            >
              {isLoading ? 'Refining...' : 'Refine'}
            </button>
          </div>
        </form>
      ) : (
        <div className="refinement-bar__result">
          <div className="refinement-bar__status">
            <span className="refinement-bar__label">Changelog</span>
            <span className="refinement-bar__changelog">{result.changelog}</span>
          </div>
          <div className="refinement-bar__actions">
            <button
              className="refinement-bar__button refinement-bar__button--ghost"
              type="button"
              onClick={handleUndo}
              disabled={isUndoing}
            >
              {isUndoing ? 'Undoing...' : 'Undo'}
            </button>
            <button
              className="refinement-bar__button refinement-bar__button--accent"
              type="button"
              onClick={handleAccept}
              disabled={isUndoing}
            >
              Accept
            </button>
          </div>
        </div>
      )}
      {error && <div className="refinement-bar__error">{error}</div>}
    </div>
  );
}
