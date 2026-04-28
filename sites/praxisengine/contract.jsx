// Contract.jsx — interactive Task Environment Contract card.
// Scenario: refund batch from the support queue → payments ledger.
// One row per concept. Hovering a row dims the others; clicking
// the "locked" row triggers the In Practice demo.

function ContractCard({ onAttackLocked }) {
  const [hover, setHover] = React.useState(null);
  const rows = [
    { k: 'task',        v: <span className="muted">Process refund batch · support queue → payments ledger</span> },
    { k: 'read scope',  v: (
      <>
        <span className="field read">support.tickets.refund_*</span>
        <span className="field read">customers.tier</span>
        <span className="field read">orders.{'{id}'}</span>
        <span className="field read">payments.history</span>
      </>
    )},
    { k: 'write scope', v: (
      <>
        <span className="field write">payments.refund.create</span>
        <span className="field write">support.tickets.note</span>
        <span className="field write">support.tickets.status</span>
      </>
    )},
    { k: 'locked', highlight: true, v: (
      <>
        <span className="field locked">refund.amount &gt; $500</span>
        <span className="field locked">refund.cross_currency</span>
        <span className="field locked">customer.tier = enterprise</span>
      </>
    )},
    { k: 'tools',       v: (
      <>
        <span className="field">stripe.refund</span>
        <span className="field">commands.propose</span>
        <span className="field">verifier.run</span>
      </>
    )},
    { k: 'approval',    v: <span className="gate">human · finance ops · for any locked.* match</span> },
    { k: 'verifier',    v: <span className="muted">Σ(refunds.written) = Σ(refunds.approved) · 0 locked.* writes · all tickets resolved</span> },
    { k: 'retry',       v: <span className="muted">requires <code>previous_failure</code> + <code>retry_delta</code></span> },
  ];

  return (
    <div className="card contract">
      <div className="contract-head">
        <div className="title">
          <span className="glyph">{'❍'}</span>
          <span>task_environment_contract · <span style={{color:'var(--fg2)'}}>tec_refund_batch_8c41ae</span></span>
        </div>
        <div className="meta">scope · sealed</div>
      </div>
      <div className="contract-body">
        {rows.map((r, i) => (
          <div
            key={r.k}
            className={'contract-row' + (r.highlight ? ' is-highlight' : '')}
            onMouseEnter={() => setHover(i)}
            onMouseLeave={() => setHover(null)}
            onClick={r.k === 'locked' ? onAttackLocked : undefined}
            style={{
              opacity: hover == null || hover === i ? 1 : 0.45,
              transition: 'opacity 160ms ease',
              cursor: r.k === 'locked' ? 'pointer' : 'default',
            }}
          >
            <div className="key">{r.k}</div>
            <div className="val">{r.v}</div>
          </div>
        ))}
      </div>
      <div className="contract-foot">
        <span>materialized · {new Date().toISOString().slice(0,10)}</span>
        <span className="ok">✓ environment ready</span>
      </div>
    </div>
  );
}

window.ContractCard = ContractCard;
