import { fireEvent, render, screen } from '@testing-library/react';
import React, { useState } from 'react';
import { describe, expect, test } from 'vitest';

import { MoonOutcomeContract } from './MoonOutcomeContract';

function Harness() {
  const [open, setOpen] = useState(false);
  const [success, setSuccess] = useState('');
  const [failure, setFailure] = useState('');
  return (
    <MoonOutcomeContract
      open={open}
      successCriteria={success}
      failureCriteria={failure}
      suggestions={[{ value: 'customer.status', detail: 'Field · Customer' }]}
      onOpenChange={setOpen}
      onSuccessChange={setSuccess}
      onFailureChange={setFailure}
    />
  );
}

describe('MoonOutcomeContract', () => {
  test('stays optional until opened and supports slash data pills', () => {
    render(<Harness />);

    expect(screen.queryByLabelText('This run succeeds if')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /run outcome contract, pattern and anti-pattern, optional/i }));

    const success = screen.getByLabelText('This run succeeds if');
    fireEvent.focus(success);
    fireEvent.change(success, { target: { value: '/cust' } });
    fireEvent.mouseDown(screen.getByRole('option', { name: /customer.status/i }));

    expect(screen.getByLabelText('This run succeeds if')).toHaveValue('{customer.status}');
  });
});
