// Loop.jsx — anti-patterns are part of the patterns.
// First agent fails on a real ambiguity. Failure becomes typed residue.
// A new agent is dispatched with that residue loaded, and completes.

function Loop() {
  return (
    <div className="card loop">
      <div className="loop-col first">
        <div className="label">
          <span>first dispatch · agent_a · failed</span>
          <span style={{color:'var(--danger-soft)'}}>verifier red</span>
        </div>
        <h4>Process refund batch · attempt 1</h4>

        <div className="loop-step">
          <span className="num">01</span>
          <span className="what">Materialize contract · early version, threshold inferred from prior batch <em>· agent_a</em></span>
          <span className="t">0m 04s</span>
        </div>
        <div className="loop-step">
          <span className="num">02</span>
          <span className="what">Process 38 refunds within inferred threshold</span>
          <span className="t">0m 31s</span>
        </div>
        <div className="loop-step">
          <span className="num">03</span>
          <span className="what">Refused · ticket t_8821 · enterprise tier not in scope</span>
          <span className="t">0m 02s</span>
        </div>
        <div className="loop-step">
          <span className="num">04</span>
          <span className="what">Retry without <code style={{padding:'0 4px'}}>retry_delta</code> · refused by retry policy</span>
          <span className="t">0m 01s</span>
        </div>
        <div className="loop-step">
          <span className="num">05</span>
          <span className="what">Verifier red · enterprise customer routing not handled</span>
          <span className="t">0m 03s</span>
        </div>
        <div className="loop-step">
          <span className="num">06</span>
          <span className="what">Agent_a halts · failure record sealed</span>
          <span className="t">0m 01s</span>
        </div>

        <div className="loop-residue">
          residue captured · <strong>anti-pattern: enterprise tier not in initial contract</strong>{' '}
          · failure record fr_91d2 · seven other agents have hit this same boundary{' '}
          · the contract template gets a new field: <code style={{padding:'0 4px'}}>customer.tier</code>.
        </div>
      </div>

      <div className="loop-arrow">→</div>

      <div className="loop-col second">
        <div className="label">
          <span>fresh dispatch · agent_b · informed</span>
          <span className="delta">aware of fr_91d2</span>
        </div>
        <h4>Process refund batch · attempt 2</h4>

        <div className="loop-step reused">
          <span className="num">01</span>
          <span className="what">Materialize contract · enterprise tier locked from the start <em>· template fr_91d2 applied</em></span>
          <span className="t">0m 03s</span>
        </div>
        <div className="loop-step">
          <span className="num">02</span>
          <span className="what">Process 46 refunds within policy</span>
          <span className="t">0m 38s</span>
        </div>
        <div className="loop-step reused">
          <span className="num">03</span>
          <span className="what">Recognize ticket t_8821 shape · route to finance-ops <em>· no retry, no thrash</em></span>
          <span className="t">0m 02s</span>
        </div>
        <div className="loop-step">
          <span className="num">04</span>
          <span className="what">Verifier green · Σ(written) = Σ(approved)</span>
          <span className="t">0m 03s</span>
        </div>
        <div className="loop-step">
          <span className="num">05</span>
          <span className="what">Seal run · receipts sealed · ticket queue cleared</span>
          <span className="t">0m 02s</span>
        </div>

        <div className="loop-residue">
          new residue · the <strong>refund_batch contract template</strong> now ships
          with the enterprise lock by default. Every future dispatch inherits this
          knowledge. The next ambiguity will fail differently — and teach
          something new.
        </div>
      </div>
    </div>
  );
}

window.Loop = Loop;
