// Canary.jsx — In Practice: refund batch demo.
// Plays a short transcript: agent processes 46 refunds within policy,
// then the 47th trips the threshold and is blocked. Verifier closes the
// run by proving Σ(written) = Σ(approved). Receipts stream on the right.

function Canary({ runKey }) {
  const [step, setStep] = React.useState(0);
  const [receipts, setReceipts] = React.useState([]);

  React.useEffect(() => {
    setStep(0);
    setReceipts([]);
  }, [runKey]);

  const script = [
    { t: 380, line: <><span className="prompt">$ </span><span className="you">praxis run refund_batch --queue support.refunds</span></> },
    { t: 520, line: <><span className="dim">› materializing tec_refund_batch_8c41ae · 47 tickets in scope</span></> },
    { t: 460, line: <><span className="dim">› hydrating shard · payments.refund · stripe.refund tool bound</span></> },

    { t: 760, line: <><span className="ai">agent</span><span className="dim"> · processing 46 refunds within policy ($12 – $487)</span></> },
    { t: 920, line: <><span className="ok">✓ 46 refunds written</span><span className="dim"> · $11,408.22 disbursed · receipts rcp_a3f7…rcp_a424</span></>,
              receipt: { kind: 'allow', rows: [
                ['action',   'payments.refund · ×46'],
                ['range',    '$12.00 – $487.00'],
                ['total',    '$11,408.22'],
                ['policy',   'within write_scope · within thresholds'],
                ['result',   ['ok','accepted · sealed']],
                ['ids',      'rcp_a3f7 → rcp_a424'],
              ]}},

    { t: 700, line: <><span className="ai">agent</span><span className="dim"> · proposing: payments.refund.create · ticket #t_8821 · $1,840.00</span></> },
    { t: 640, line: <><span className="err">! refused</span><span className="dim"> · refund.amount &gt; $500 · approval gate engaged</span></>,
              receipt: { kind: 'blocked', rows: [
                ['action',   'payments.refund.create'],
                ['ticket',   't_8821 · enterprise · order ord_44c1'],
                ['amount',   '$1,840.00'],
                ['policy',   ['warn','refund.amount > $500 · locked']],
                ['result',   ['warn','refused · routed for human approval']],
                ['route',    'finance-ops · slack #refunds-approval'],
                ['id',       'rcp_b1c2'],
              ]}},

    { t: 540, line: <><span className="dim">› running verifier · Σ(refunds.written) vs Σ(refunds.approved)</span></> },
    { t: 480, line: <><span className="ok">✓ verified</span><span className="dim"> · 46 written · 1 routed · 0 drift · $11,408.22 reconciled</span></>,
              receipt: { kind: 'verify', rows: [
                ['verifier', 'tec_refund_batch_8c41ae.complete'],
                ['written',  ['ok','46 · $11,408.22']],
                ['routed',   '1 · awaiting human'],
                ['locked',   ['ok','0 unauthorized writes']],
                ['drift',    ['ok','none']],
                ['next',     'residue → memory'],
              ]}},
    { t: 600, line: <><span className="prompt">$ </span><span className="dim">_</span></>, cursor: true },
  ];

  React.useEffect(() => {
    if (step >= script.length) return;
    const s = script[step];
    const id = setTimeout(() => {
      if (s.receipt) setReceipts(prev => [...prev, s.receipt]);
      setStep(step + 1);
    }, s.t);
    return () => clearTimeout(id);
  }, [step, runKey]);

  const visible = script.slice(0, step);
  const cursorOn = step > 0 && script[step - 1] && script[step - 1].cursor;

  return (
    <div className="card canary">
      <div className="canary-pane left">
        <div className="pane-label">
          <span>sandbox · workflow_5f4c22 · plan→execute→verify</span>
        </div>
        {visible.map((s, i) => (
          <span key={i} className={'line' + (i === visible.length - 1 && s.cursor ? ' cursor' : '')}>
            {s.line}
          </span>
        ))}
        {step < script.length && !cursorOn && <span className="line">&nbsp;</span>}
      </div>
      <div className="canary-pane right">
        <div className="pane-label">
          <span>receipts</span>
          <span style={{color:'var(--fg3)'}}>{receipts.length} sealed</span>
        </div>
        {receipts.length === 0 && (
          <div style={{color:'var(--fg3)', fontFamily:'var(--font-mono)', fontSize:'var(--fs-xs)'}}>
            · awaiting first action
          </div>
        )}
        {receipts.map((r, i) => (
          <ReceiptRow key={i} kind={r.kind} rows={r.rows} delay={i * 60} />
        ))}
      </div>
    </div>
  );
}

function ReceiptRow({ kind, rows, delay }) {
  const [shown, setShown] = React.useState(false);
  React.useEffect(() => {
    const id = setTimeout(() => setShown(true), delay);
    return () => clearTimeout(id);
  }, [delay]);
  return (
    <div className={'receipt ' + kind + (shown ? ' in' : '')}>
      {rows.map(([k, v], i) => {
        let cls = '', text = v;
        if (Array.isArray(v)) { cls = v[0]; text = v[1]; }
        return (
          <div className="row" key={i}>
            <div className="k">{k}</div>
            <div className={'v ' + cls}>{text}</div>
          </div>
        );
      })}
    </div>
  );
}

window.Canary = Canary;
