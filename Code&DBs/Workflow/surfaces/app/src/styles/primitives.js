// =============================================================
// Praxis primitives — behavior layer.
// Vanilla JS, autoinit by class, custom-event bus.
// Pairs with primitives.css. Drop with: <script src="primitives.js" defer></script>
// =============================================================
(function () {
  const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
  const on = (el, ev, fn) => el.addEventListener(ev, fn);
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  // ── Catalog table ─────────────────────────────────────────
  function initTable(el) {
    const cols = JSON.parse(el.getAttribute('data-cols') || '[]');
    const rows = JSON.parse(el.getAttribute('data-rows') || '[]');
    let sortKey = null, sortDir = 1, filter = '';
    const body = el.querySelector('.body') || el.appendChild(Object.assign(document.createElement('div'), { className: 'body' }));
    const meta = el.querySelector('.prx-table-meta');
    const filterInput = el.querySelector('.prx-table-filter');

    function render() {
      let view = rows.slice();
      if (filter) {
        const f = filter.toLowerCase();
        view = view.filter(r => Object.values(r).join(' ').toLowerCase().includes(f));
      }
      if (sortKey) view.sort((a, b) => (String(a[sortKey] ?? '') < String(b[sortKey] ?? '') ? -1 : 1) * sortDir);
      if (meta) meta.textContent = `${view.length} of ${rows.length}`;

      const head = `<tr>${cols.map(c => {
        const cls = sortKey === c.key ? `sort-${sortDir > 0 ? 'asc' : 'desc'}` : '';
        return `<th data-key="${esc(c.key)}" class="${cls}">${esc(c.label)}<span class="arrow"></span></th>`;
      }).join('')}</tr>`;

      if (view.length === 0) {
        body.innerHTML = `<table><thead>${head}</thead></table><div class="empty">no matches · ${esc(filter || '')}</div>`;
        return;
      }

      const trs = view.map(r => {
        const tds = cols.map(c => {
          const v = r[c.key];
          if (c.kind === 'stat') return `<td><span class="stat-cap" data-tone="${esc(v?.tone || 'dim')}">${esc(v?.label || v || '')}</span></td>`;
          if (c.kind === 'chip') return `<td><span class="prx-chip" data-tone="${esc(c.tone || '')}">${esc(v ?? '')}</span></td>`;
          if (c.kind === 'mono') return `<td style="color:var(--text-muted)">${esc(v ?? '')}</td>`;
          if (c.kind === 'bool') return `<td>${v ? '<span class="stat-cap" data-tone="ok">yes</span>' : '<span class="stat-cap" data-tone="dim">no</span>'}</td>`;
          return `<td>${esc(v ?? '')}</td>`;
        }).join('');
        return `<tr data-row='${esc(JSON.stringify(r))}'>${tds}</tr>`;
      }).join('');

      body.innerHTML = `<table><thead>${head}</thead><tbody>${trs}</tbody></table>`;
    }

    on(el, 'click', (e) => {
      const th = e.target.closest('th[data-key]');
      if (th) {
        const k = th.dataset.key;
        if (sortKey === k) sortDir *= -1; else { sortKey = k; sortDir = 1; }
        render();
        return;
      }
      const tr = e.target.closest('tr[data-row]');
      if (tr) {
        body.querySelectorAll('tr.selected').forEach(x => x.classList.remove('selected'));
        tr.classList.add('selected');
        let data;
        try { data = JSON.parse(tr.dataset.row.replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')); } catch { data = {}; }
        el.dispatchEvent(new CustomEvent('prx:row-select', { detail: data, bubbles: true }));
      }
    });

    if (filterInput) on(filterInput, 'input', (e) => { filter = e.target.value; render(); });

    render();
    el.__prx = { render, get rows() { return rows; } };
  }

  // ── Drawer ─────────────────────────────────────────────────
  function initDrawer(el) {
    const close = () => {
      el.classList.remove('open');
      document.documentElement.classList.remove('prx-drawer-open');
    };
    el.querySelectorAll('[data-close]').forEach(b => on(b, 'click', close));
    const bd = el.querySelector('.prx-drawer-backdrop');
    if (bd) on(bd, 'click', close);
    on(document, 'keydown', (e) => { if (e.key === 'Escape' && el.classList.contains('open')) close(); });
  }

  function openDrawer(idOrEl, data) {
    const el = typeof idOrEl === 'string' ? document.getElementById(idOrEl) : idOrEl;
    if (!el) return;
    if (data) el.dispatchEvent(new CustomEvent('prx:drawer-fill', { detail: data }));
    el.classList.add('open');
    document.documentElement.classList.add('prx-drawer-open');
  }

  // ── Command palette ───────────────────────────────────────
  function initPalette(el) {
    const input = el.querySelector('.prx-palette-input');
    const list = el.querySelector('.prx-palette-list');
    const items = JSON.parse(el.getAttribute('data-items') || '[]');
    let filtered = items.slice();
    let cursor = 0;

    function render() {
      if (filtered.length === 0) {
        list.innerHTML = `<div class="prx-palette-empty">no tools match · "${esc(input.value)}"</div>`;
        return;
      }
      list.innerHTML = filtered.map((it, i) => `
        <div class="prx-palette-row${i === cursor ? ' sel' : ''}" data-i="${i}">
          <span class="glyph">${esc(it.glyph || '›')}</span>
          <span class="cmd">${esc(it.name)}</span>
          <span class="desc">${esc(it.desc || '')}</span>
          <span class="kind">${esc(it.kind || '')}</span>
        </div>`).join('');
    }
    function open() {
      el.classList.add('open');
      input.value = '';
      filtered = items.slice();
      cursor = 0;
      render();
      setTimeout(() => input.focus(), 0);
    }
    function close() { el.classList.remove('open'); }
    function run(it) {
      if (!it) return;
      el.dispatchEvent(new CustomEvent('prx:tool-run', { detail: it, bubbles: true }));
      close();
    }

    on(input, 'input', () => {
      const q = input.value.toLowerCase();
      filtered = items.filter(it => (it.name + ' ' + (it.desc || '') + ' ' + (it.kind || '')).toLowerCase().includes(q));
      cursor = 0;
      render();
    });
    on(input, 'keydown', (e) => {
      if (e.key === 'ArrowDown') { cursor = Math.min(cursor + 1, filtered.length - 1); render(); e.preventDefault(); list.querySelector('.sel')?.scrollIntoView({ block: 'nearest' }); }
      else if (e.key === 'ArrowUp') { cursor = Math.max(cursor - 1, 0); render(); e.preventDefault(); list.querySelector('.sel')?.scrollIntoView({ block: 'nearest' }); }
      else if (e.key === 'Enter') { run(filtered[cursor]); }
      else if (e.key === 'Escape') { close(); }
    });
    on(list, 'click', (e) => {
      const r = e.target.closest('.prx-palette-row');
      if (r) run(filtered[+r.dataset.i]);
    });
    const bd = el.querySelector('.prx-palette-backdrop');
    if (bd) on(bd, 'click', close);

    on(document, 'keydown', (e) => {
      const k = e.key?.toLowerCase();
      if ((e.metaKey || e.ctrlKey) && k === 'k') {
        e.preventDefault();
        if (el.classList.contains('open')) close(); else open();
      }
    });

    el.__prx = { open, close };
  }

  // ── Wizard ────────────────────────────────────────────────
  function initWizard(el) {
    const cfg = JSON.parse(el.getAttribute('data-config') || '{"steps":[]}');
    const previewFnName = el.getAttribute('data-preview');
    const stepsEl = el.querySelector('.prx-wizard-steps') || el.appendChild(Object.assign(document.createElement('div'), { className: 'prx-wizard-steps' }));
    const bodyEl = el.querySelector('.prx-wizard-body') || el.appendChild(Object.assign(document.createElement('div'), { className: 'prx-wizard-body' }));
    let formEl = bodyEl.querySelector('.prx-wizard-form');
    let previewEl = bodyEl.querySelector('.prx-wizard-preview');
    if (!formEl) { formEl = document.createElement('div'); formEl.className = 'prx-wizard-form'; bodyEl.appendChild(formEl); }
    if (!previewEl) { previewEl = document.createElement('div'); previewEl.className = 'prx-wizard-preview'; bodyEl.appendChild(previewEl); }
    const footEl = el.querySelector('.prx-wizard-foot') || el.appendChild(Object.assign(document.createElement('div'), { className: 'prx-wizard-foot' }));

    const state = { step: 0, data: cfg.initial || {} };

    function renderSteps() {
      stepsEl.innerHTML = cfg.steps.map((s, i) => {
        const cls = i === state.step ? 'active' : i < state.step ? 'done' : '';
        const sep = i < cfg.steps.length - 1 ? '<span class="sep">·</span>' : '';
        const num = String(i + 1).padStart(2, '0');
        return `<span class="prx-wizard-step ${cls}"><span class="num"><span>${num}</span></span><span>${esc(s.label)}</span></span>${sep}`;
      }).join('');
    }

    function renderField(f) {
      const v = state.data[f.key] ?? '';
      const reqGlyph = f.required ? '<span class="req">*</span>' : '';
      const hintCls = f.hintTone === 'warn' ? 'prx-field-hint warn' : 'prx-field-hint';
      const hint = f.hint ? `<div class="${hintCls}">${esc(f.hint)}</div>` : '';
      const lbl = `<label class="prx-field-label">${esc(f.label)}${reqGlyph}</label>`;

      if (f.type === 'radio') {
        const pills = f.options.map(o => `<span class="prx-radio-pill ${v === o.value ? 'checked' : ''}" data-field="${esc(f.key)}" data-value="${esc(o.value)}">${esc(o.label)}</span>`).join('');
        return `<div class="prx-field">${lbl}<div class="prx-radio-group">${pills}</div>${hint}</div>`;
      }
      const placeholder = f.placeholder ? ` placeholder="${esc(f.placeholder)}"` : '';
      return `<div class="prx-field">${lbl}<input class="prx-field-input" data-field="${esc(f.key)}" type="${esc(f.type || 'text')}" value="${esc(v)}"${placeholder} />${hint}</div>`;
    }

    function renderForm() {
      const cur = cfg.steps[state.step];
      const fields = (cur.fields || []).map(renderField).join('');
      formEl.innerHTML = `<h3>${esc(cur.title || cur.label)}</h3><p class="step-desc">${esc(cur.description || '')}</p>${fields}`;

      formEl.querySelectorAll('input.prx-field-input').forEach(input => {
        on(input, 'input', e => { state.data[input.dataset.field] = e.target.value; renderPreview(); renderFoot(); });
      });
      formEl.querySelectorAll('.prx-radio-pill').forEach(pill => {
        on(pill, 'click', () => { state.data[pill.dataset.field] = pill.dataset.value; renderForm(); renderPreview(); renderFoot(); });
      });
    }

    function renderPreview() {
      const fn = previewFnName ? window[previewFnName] : null;
      if (typeof fn === 'function') {
        previewEl.innerHTML = fn(state.data, state.step);
      } else if (cfg.previewLabel) {
        previewEl.innerHTML = `<div class="preview-cap"><span>${esc(cfg.previewLabel)}</span><span>${esc(JSON.stringify(state.data).length)} chars</span></div><pre>${esc(JSON.stringify(state.data, null, 2))}</pre>`;
      } else {
        previewEl.innerHTML = `<div class="preview-cap"><span>state</span></div><pre>${esc(JSON.stringify(state.data, null, 2))}</pre>`;
      }
    }

    function validateStep() {
      const cur = cfg.steps[state.step];
      for (const f of (cur.fields || [])) {
        if (f.required && !state.data[f.key]) return false;
      }
      return true;
    }

    function renderFoot() {
      const last = state.step === cfg.steps.length - 1;
      footEl.innerHTML = `
        <button class="back" ${state.step === 0 ? 'disabled' : ''}>‹ back</button>
        <span>step ${state.step + 1} of ${cfg.steps.length}</span>
        <button class="next primary" ${validateStep() ? '' : 'disabled'}>${last ? '↵ submit' : 'next ›'}</button>
      `;
      on(footEl.querySelector('.back'), 'click', () => { state.step = Math.max(0, state.step - 1); renderAll(); });
      on(footEl.querySelector('.next'), 'click', () => {
        if (!validateStep()) return;
        if (state.step === cfg.steps.length - 1) {
          el.dispatchEvent(new CustomEvent('prx:wizard-submit', { detail: state.data, bubbles: true }));
        } else { state.step += 1; renderAll(); }
      });
    }

    function renderAll() { renderSteps(); renderForm(); renderPreview(); renderFoot(); }
    renderAll();
    el.__prx = { state, render: renderAll };
  }

  // ── boot ──
  function init() {
    $$('.prx-table').forEach(initTable);
    $$('.prx-drawer').forEach(initDrawer);
    $$('.prx-palette').forEach(initPalette);
    $$('.prx-wizard').forEach(initWizard);
  }
  if (document.readyState === 'loading') on(document, 'DOMContentLoaded', init); else init();

  window.Prx = {
    drawer: { open: openDrawer },
    palette: { open: () => document.querySelector('.prx-palette')?.__prx?.open?.() },
  };
})();
