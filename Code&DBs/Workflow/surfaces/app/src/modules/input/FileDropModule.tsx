import React, { useRef, useState, DragEvent } from 'react';
import { QuadrantProps } from '../types';
import { world } from '../../world';

interface FileDropConfig {
  accept?: string;
  worldPath?: string;
  label?: string;
  uploadEndpoint?: string;
  scope?: 'instance' | 'step' | 'workflow';
  workflowId?: string;
  stepId?: string;
  description?: string;
}

interface UploadedFileRecord {
  id: string;
  filename: string;
  content_type: string;
  size_bytes: number;
  scope: string;
  storage_path: string;
}

export const FileDropModule: React.FC<QuadrantProps> = ({ config: rawConfig }) => {
  const config = (rawConfig || {}) as FileDropConfig;
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [isHovered, setIsHovered] = useState(false);
  const [filename, setFilename] = useState<string | null>(null);
  const [status, setStatus] = useState<'idle' | 'uploading' | 'uploaded' | 'error'>('idle');
  const [message, setMessage] = useState<string | null>(null);

  const {
    accept = '.txt',
    worldPath,
    label = 'Drag and drop a file here',
    uploadEndpoint = '/api/files',
    scope = 'instance',
    workflowId,
    stepId,
    description = '',
  } = config;

  const fileToBase64 = (file: File): Promise<string> => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = typeof reader.result === 'string' ? reader.result : '';
      const base64 = result.includes(',') ? result.split(',')[1] : result;
      if (base64) resolve(base64);
      else reject(new Error('Failed to encode file'));
    };
    reader.onerror = () => reject(reader.error ?? new Error('Failed to encode file'));
    reader.readAsDataURL(file);
  });

  const uploadFile = async (file: File) => {
    setFilename(file.name);
    setStatus('uploading');
    setMessage(null);

    try {
      const content = await fileToBase64(file);
      const response = await fetch(uploadEndpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filename: file.name,
          content,
          content_type: file.type || 'application/octet-stream',
          scope,
          workflow_id: workflowId,
          step_id: stepId,
          description,
        }),
      });

      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload?.error || `Upload failed (${response.status})`);
      }

      const uploadedFile = payload?.file as UploadedFileRecord | undefined;
      if (!uploadedFile?.id) {
        throw new Error('Upload succeeded but no file record was returned');
      }

      if (worldPath) {
        world.set(worldPath, {
          ...uploadedFile,
          original_filename: file.name,
        });
      }

      setStatus('uploaded');
      setMessage(`Uploaded as ${uploadedFile.filename}`);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      setStatus('error');
      setMessage(errorMessage);
    } finally {
      if (inputRef.current) {
        inputRef.current.value = '';
      }
    }
  };

  const handleFiles = async (files: FileList | null | undefined) => {
    if (!files || files.length === 0) return;
    await uploadFile(files[0]);
  };

  const handleDragOver = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsHovered(true);
  };

  const handleDragLeave = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsHovered(false);
  };

  const handleDrop = async (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsHovered(false);
    await handleFiles(e.dataTransfer.files);
  };

  return (
    <div style={{ padding: 'var(--space-md, 16px)', width: '100%', height: '100%', boxSizing: 'border-box', display: 'flex', flexDirection: 'column' }}>
      <input
        ref={inputRef}
        type="file"
        accept={accept}
        onChange={(event) => {
          void handleFiles(event.target.files);
        }}
        style={{ display: 'none' }}
      />
      <div
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        onClick={() => inputRef.current?.click()}
        style={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          border: `2px dashed ${isHovered ? 'var(--accent, #58a6ff)' : 'var(--border, #30363d)'}`,
          borderRadius: 'var(--radius, 8px)',
          backgroundColor: isHovered ? 'rgba(88, 166, 255, 0.1)' : 'var(--bg-card, #161b22)',
          color: 'var(--text-muted, #8b949e)',
          cursor: 'pointer',
          transition: 'all 0.2s ease',
          fontFamily: 'var(--font-sans, sans-serif)',
          padding: '24px'
        }}
      >
        <span style={{ color: 'var(--text, #c9d1d9)', fontWeight: 600 }}>{filename || label}</span>
        <span style={{ fontSize: '12px', marginTop: '8px' }}>
          {status === 'idle' && `Accepts ${accept}`}
          {status === 'uploading' && 'Uploading to file storage...'}
          {status === 'uploaded' && message}
          {status === 'error' && message}
        </span>
      </div>
    </div>
  );
};
