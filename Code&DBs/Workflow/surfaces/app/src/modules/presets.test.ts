import { describe, expect, it } from 'vitest';
import { getPreset } from './presets';

describe('bug tracking presets', () => {
  it('opens the human ticket table on active work by default', () => {
    const preset = getPreset('open-bugs-table');

    expect(preset?.name).toBe('Open Tickets');
    expect(preset?.config.endpoint).toBe('bugs?open_only=1&include_replay_state=1&limit=50');
    expect(preset?.config.columns).toEqual([
      { key: 'bug_id', label: 'Ticket' },
      { key: 'title', label: 'Bug' },
      { key: 'severity', label: 'Severity' },
      { key: 'status', label: 'Status' },
      { key: 'replay_ready', label: 'Replay' },
    ]);
  });

  it('offers a ticket workbench with replay-aware open ticket data', () => {
    const preset = getPreset('open-ticket-workbench');

    expect(preset?.moduleId).toBe('bug-card');
    expect(preset?.config).toMatchObject({
      endpoint: 'bugs?open_only=1&include_replay_state=1&limit=20',
      title: 'Open Tickets',
    });
  });
});
