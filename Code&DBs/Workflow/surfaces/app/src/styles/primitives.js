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
    let rows = JSON.parse(el.getAttribute('data-rows') || '[]');
    let sortKey = null, sortDir = 1, filter = '';
    const body = el.querySelector('.body') || el.appendChild(Object.assign(document.createElement('div'), { className: 'body' }));
    if (!body.hasAttribute('tabindex')) body.setAttribute('tabindex', '0');
    if (!body.hasAttribute('role')) body.setAttribute('role', 'group');
    if (!body.hasAttribute('aria-label')) {
      const ownerId = el.id || 'catalog';
      body.setAttribute('aria-label', `Catalog rows · ${ownerId}`);
    }
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
    el.__prx = {
      render,
      get rows() { return rows; },
      setRows(newRows) { rows = Array.isArray(newRows) ? newRows : []; render(); },
    };
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
      const inputId = 'prx-wzfield-' + esc(f.key) + '-' + Math.random().toString(36).slice(2, 7);
      const lbl = `<label class="prx-field-label" for="${inputId}">${esc(f.label)}${reqGlyph}</label>`;

      if (f.type === 'radio') {
        const groupLbl = `<label class="prx-field-label" id="${inputId}-lbl">${esc(f.label)}${reqGlyph}</label>`;
        const pills = f.options.map(o => `<span class="prx-radio-pill ${v === o.value ? 'checked' : ''}" role="radio" aria-checked="${v === o.value}" tabindex="0" data-field="${esc(f.key)}" data-value="${esc(o.value)}">${esc(o.label)}</span>`).join('');
        return `<div class="prx-field">${groupLbl}<div class="prx-radio-group" role="radiogroup" aria-labelledby="${inputId}-lbl">${pills}</div>${hint}</div>`;
      }
      const placeholder = f.placeholder ? ` placeholder="${esc(f.placeholder)}"` : '';
      if (f.type === 'textarea') {
        const rows = f.rows || 4;
        return `<div class="prx-field">${lbl}<textarea id="${inputId}" class="prx-field-input prx-field-textarea" data-field="${esc(f.key)}" rows="${rows}"${placeholder}>${esc(v)}</textarea>${hint}</div>`;
      }
      return `<div class="prx-field">${lbl}<input id="${inputId}" class="prx-field-input" data-field="${esc(f.key)}" type="${esc(f.type || 'text')}" value="${esc(v)}"${placeholder} />${hint}</div>`;
    }

    function renderForm() {
      const cur = cfg.steps[state.step];
      const fields = (cur.fields || []).map(renderField).join('');
      formEl.innerHTML = `<h3>${esc(cur.title || cur.label)}</h3><p class="step-desc">${esc(cur.description || '')}</p>${fields}`;

      formEl.querySelectorAll('input.prx-field-input, textarea.prx-field-input').forEach(input => {
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

  // ── Schema-driven form ────────────────────────────────────
  function initForm(el) {
    const schema = JSON.parse(el.getAttribute('data-schema') || '{}');
    const tones = JSON.parse(el.getAttribute('data-tones') || '{}');
    const initial = JSON.parse(el.getAttribute('data-initial') || '{}');
    const body = el.querySelector('.prx-form-body') || el.appendChild(Object.assign(document.createElement('div'), { className: 'prx-form-body' }));
    const data = JSON.parse(JSON.stringify(initial));

    function fieldHtml(key, sub, path) {
      const tone = tones[path] || '';
      const v = path.split('.').reduce((o, k) => (o == null ? undefined : o[k]), data) ?? '';
      const reqGlyph = (schema.required || []).includes(key) ? '<span class="req">*</span>' : '';
      const safePath = esc(path).replace(/\./g, '-');
      const inputId = 'prx-form-' + safePath + '-' + Math.random().toString(36).slice(2, 7);
      const lbl = `<label class="prx-field-label" for="${inputId}">${esc(key)}${reqGlyph} <span style="color:var(--text-muted);font-size:9px;margin-left:6px">${esc(sub.type || '')}</span></label>`;
      const hint = sub.description ? `<div class="prx-field-hint">${esc(sub.description)}</div>` : '';
      const dataField = `data-path="${esc(path)}"`;
      const dataTone = tone ? `data-tone="${esc(tone)}"` : '';

      if (sub.enum) {
        const groupLbl = `<label class="prx-field-label" id="${inputId}-lbl">${esc(key)}${reqGlyph}</label>`;
        const pills = sub.enum.map(o => `<span class="prx-radio-pill ${v === o ? 'checked' : ''}" role="radio" aria-checked="${v === o}" tabindex="0" ${dataField} data-value="${esc(o)}">${esc(o)}</span>`).join('');
        return `<div class="prx-field" ${dataTone}>${groupLbl}<div class="prx-radio-group" role="radiogroup" aria-labelledby="${inputId}-lbl">${pills}</div>${hint}</div>`;
      }
      if (sub.type === 'boolean') {
        const groupLbl = `<label class="prx-field-label" id="${inputId}-lbl">${esc(key)}${reqGlyph}</label>`;
        const pills = ['true', 'false'].map(o => `<span class="prx-radio-pill ${String(v) === o ? 'checked' : ''}" role="radio" aria-checked="${String(v) === o}" tabindex="0" ${dataField} data-value="${o}" data-bool="1">${o}</span>`).join('');
        return `<div class="prx-field" ${dataTone}>${groupLbl}<div class="prx-radio-group" role="radiogroup" aria-labelledby="${inputId}-lbl">${pills}</div>${hint}</div>`;
      }
      if (sub.type === 'object' && sub.properties) {
        const inner = Object.entries(sub.properties).map(([k, p]) => fieldHtml(k, p, path + '.' + k)).join('');
        return `<div class="prx-form-group" role="group" aria-label="${esc(key)}"><div class="prx-form-group-title"><span>${esc(key)}</span><span class="path">${esc(path)}</span></div>${inner}</div>`;
      }
      const inputType = sub.type === 'number' || sub.type === 'integer' ? 'number' : 'text';
      const placeholder = sub.examples?.[0] ? ` placeholder="${esc(sub.examples[0])}"` : '';
      return `<div class="prx-field" ${dataTone}>${lbl}<input id="${inputId}" class="prx-field-input" ${dataField} type="${inputType}" value="${esc(v)}"${placeholder} />${hint}</div>`;
    }

    function render() {
      const fields = Object.entries(schema.properties || {}).map(([k, sub]) => fieldHtml(k, sub, k)).join('');
      body.innerHTML = fields;
      body.querySelectorAll('input.prx-field-input').forEach(input => {
        on(input, 'input', e => {
          setPath(data, input.dataset.path, input.type === 'number' ? Number(e.target.value) : e.target.value);
          el.dispatchEvent(new CustomEvent('prx:form-change', { detail: { ...data }, bubbles: true }));
        });
      });
      body.querySelectorAll('.prx-radio-pill').forEach(pill => {
        on(pill, 'click', () => {
          const val = pill.dataset.bool ? pill.dataset.value === 'true' : pill.dataset.value;
          setPath(data, pill.dataset.path, val);
          render();
          el.dispatchEvent(new CustomEvent('prx:form-change', { detail: { ...data }, bubbles: true }));
        });
      });
    }
    function setPath(obj, path, val) {
      const keys = path.split('.');
      let cur = obj;
      while (keys.length > 1) {
        const k = keys.shift();
        if (cur[k] == null || typeof cur[k] !== 'object') cur[k] = {};
        cur = cur[k];
      }
      cur[keys[0]] = val;
    }
    render();
    el.__prx = { get data() { return data; }, render };
  }

  // ── Dispatch bar ──────────────────────────────────────────
  async function sha256(s) {
    if (!window.crypto?.subtle) return 'sha256:unavailable';
    const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
    return 'sha256:' + Array.from(new Uint8Array(buf)).slice(0, 8).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  function initDispatch(el) {
    const opName = el.getAttribute('data-op') || 'unknown';
    const idemPolicy = el.getAttribute('data-idempotency') || 'none';
    let payload = JSON.parse(el.getAttribute('data-payload') || '{}');
    let dry = false;
    const seenHashes = new Set();

    el.innerHTML = `
      <span class="op">${esc(opName)}</span>
      <span class="hash">payload <span class="h">${esc('sha256:—')}</span></span>
      <span class="replay" data-state="miss">cache miss</span>
      <label class="dry"><input type="checkbox" /> dry-run</label>
      <button class="run">${idemPolicy === 'read_only' ? '↻ read' : '› dispatch'}</button>
    `;

    const hashEl = el.querySelector('.hash .h');
    const replayEl = el.querySelector('.replay');
    const dryInput = el.querySelector('.dry input');
    const runBtn = el.querySelector('.run');

    async function refreshHash() {
      const h = await sha256(JSON.stringify(payload));
      hashEl.textContent = h;
      const isHit = (idemPolicy === 'read_only' || idemPolicy === 'idempotent') && seenHashes.has(h);
      replayEl.setAttribute('data-state', isHit ? 'hit' : 'miss');
      replayEl.textContent = isHit ? 'cache hit' : 'cache miss';
    }

    on(dryInput, 'change', e => { dry = e.target.checked; runBtn.classList.toggle('dry', dry); runBtn.textContent = dry ? '› dry-run' : (idemPolicy === 'read_only' ? '↻ read' : '› dispatch'); });
    on(runBtn, 'click', async () => {
      const h = await sha256(JSON.stringify(payload));
      seenHashes.add(h);
      refreshHash();
      el.dispatchEvent(new CustomEvent('prx:dispatch', { detail: { op: opName, payload: { ...payload }, hash: h, dry, idemPolicy }, bubbles: true }));
    });

    refreshHash();
    el.__prx = {
      setPayload(p) { payload = p; refreshHash(); },
      get payload() { return payload; },
    };
  }

  // ── Catalog command bar ───────────────────────────────────
  function initCmdBar(el) {
    const items = JSON.parse(el.getAttribute('data-items') || '[]');
    const input = el.querySelector('input.q');
    const results = el.querySelector('.results');
    const formArea = el.querySelector('.selected-form');
    let selected = null;

    function renderResults(filter) {
      const f = (filter || '').toLowerCase();
      const matches = items.filter(it => (it.name + ' ' + (it.desc || '')).toLowerCase().includes(f)).slice(0, 6);
      results.innerHTML = matches.map((it, i) => `
        <div class="prx-palette-row" data-name="${esc(it.name)}">
          <span class="glyph">${esc(it.glyph || '›')}</span>
          <span class="cmd">${esc(it.name)}</span>
          <span class="desc">${esc(it.desc || '')}</span>
          <span class="kind">${esc(it.kind || '')}</span>
        </div>`).join('');
      results.querySelectorAll('.prx-palette-row').forEach(row => {
        on(row, 'click', () => {
          selected = items.find(x => x.name === row.dataset.name);
          renderForm();
        });
      });
    }
    function renderForm() {
      if (!selected) { formArea.innerHTML = ''; return; }
      formArea.innerHTML = `
        <div class="op-cap"><span><span class="op">${esc(selected.name)}</span> · ${esc(selected.kind || '')}</span><span>${esc(selected.idempotency_policy || 'none')}</span></div>
        <div class="prx-form" data-schema='${esc(JSON.stringify(selected.schema || { type: 'object', properties: {} }))}' data-initial='${esc(JSON.stringify(selected.initial || {}))}'>
          <div class="prx-form-body"></div>
        </div>
        <div style="margin-top:14px">
          <div class="prx-dispatch" data-op="${esc(selected.name)}" data-idempotency="${esc(selected.idempotency_policy || 'none')}" data-payload='${esc(JSON.stringify(selected.initial || {}))}'></div>
        </div>
      `;
      const formEl = formArea.querySelector('.prx-form');
      const dispEl = formArea.querySelector('.prx-dispatch');
      initForm(formEl);
      initDispatch(dispEl);
      on(formEl, 'prx:form-change', (e) => { dispEl.__prx?.setPayload(e.detail); });
      on(dispEl, 'prx:dispatch', (e) => {
        el.dispatchEvent(new CustomEvent('prx:cmd-run', { detail: e.detail, bubbles: true }));
      });
    }

    on(input, 'input', e => renderResults(e.target.value));
    renderResults('');
  }

  // ── Flow canvas ───────────────────────────────────────────
  function initFlow(el) {
    on(el, 'click', (e) => {
      const node = e.target.closest('.prx-flow-node');
      if (node) {
        let data;
        try { data = JSON.parse(node.getAttribute('data-node') || '{}'); } catch { data = {}; }
        el.dispatchEvent(new CustomEvent('prx:flow-node-select', { detail: { ...data, name: node.querySelector('.name')?.textContent }, bubbles: true }));
      }
    });
  }

  // ── Step builder ──────────────────────────────────────────
  function initStepBuilder(el) {
    const accumType = el.getAttribute('data-accumulator') || '';
    const allItems = JSON.parse(el.getAttribute('data-items') || '[]');
    // narrow by `consumes` matching accumulator type
    const legal = allItems.filter(it => !accumType || (it.consumes || []).includes(accumType));
    const narrowEl = el.querySelector('.narrow-bar .ops') || el.appendChild(document.createElement('div'));
    const composeEl = el.querySelector('.compose');
    const countEl = el.querySelector('.narrow-bar .cap .count');
    let selected = null;

    if (countEl) countEl.textContent = `${legal.length} legal next`;
    narrowEl.innerHTML = legal.map(it => `
      <div class="op-row" data-name="${esc(it.name)}">
        <span class="glyph">${esc(it.glyph || '›')}</span>
        <span class="name">${esc(it.name)}</span>
        <span class="desc">${esc(it.produces ? '→ ' + it.produces : (it.desc || ''))}</span>
        <span class="kind">${esc(it.kind || '')}</span>
      </div>`).join('');

    function selectOp(name) {
      selected = legal.find(x => x.name === name);
      narrowEl.querySelectorAll('.op-row').forEach(r => r.classList.toggle('sel', r.dataset.name === name));
      if (!composeEl) return;
      if (!selected) { composeEl.innerHTML = '<div class="empty">pick an op above to materialize its form</div>'; return; }
      composeEl.innerHTML = `
        <div class="prx-form" data-schema='${esc(JSON.stringify(selected.schema || { type: 'object', properties: {} }))}' data-initial='${esc(JSON.stringify(selected.initial || {}))}'>
          <div class="prx-form-body"></div>
        </div>
        <div style="margin-top:14px">
          <div class="prx-dispatch" data-op="${esc(selected.name)}" data-idempotency="${esc(selected.idempotency_policy || 'none')}" data-payload='${esc(JSON.stringify(selected.initial || {}))}'></div>
        </div>
      `;
      const formEl = composeEl.querySelector('.prx-form');
      const dispEl = composeEl.querySelector('.prx-dispatch');
      initForm(formEl);
      initDispatch(dispEl);
      on(formEl, 'prx:form-change', (e) => { dispEl.__prx?.setPayload(e.detail); });
      on(dispEl, 'prx:dispatch', (e) => {
        el.dispatchEvent(new CustomEvent('prx:step-run', {
          detail: { ...e.detail, op: selected.name, produces: selected.produces },
          bubbles: true,
        }));
      });
    }

    on(narrowEl, 'click', (e) => {
      const row = e.target.closest('.op-row');
      if (row) selectOp(row.dataset.name);
    });
    if (legal.length === 1) selectOp(legal[0].name); // auto-pick if only one option
    else if (composeEl) composeEl.innerHTML = '<div class="empty">pick an op above to materialize its form</div>';

    el.__prx = { get selected() { return selected; }, selectOp };
  }

  // ── Workflow bar ──────────────────────────────────────────
  function initWorkflowBar(el) {
    el.querySelectorAll('.controls button').forEach(btn => {
      on(btn, 'click', () => {
        el.dispatchEvent(new CustomEvent('prx:workflow-control', {
          detail: { action: btn.dataset.action || btn.textContent.trim() },
          bubbles: true,
        }));
      });
    });
  }

  // ── Glyph spinner ─────────────────────────────────────────
  const SPINNER_FRAMES = {
    braille:  ['⡷','⡯','⡟','⢿','⣻','⣽','⣾','⣷'],
    quadrant: ['▖','▘','▝','▗'],
    dot:      ['·','•','●','•'],
    bar:      ['▁','▂','▃','▄','▅','▆','▇','█','▇','▆','▅','▄','▃','▂'],
  };
  function initSpinner(el) {
    const set = el.getAttribute('data-set') || 'braille';
    const frames = SPINNER_FRAMES[set] || SPINNER_FRAMES.braille;
    const intervalMs = Number(el.getAttribute('data-interval-ms') || (el.getAttribute('data-tone') === 'dim' ? 520 : 80));
    let i = 0;
    el.textContent = frames[0];
    const interval = setInterval(() => {
      i = (i + 1) % frames.length;
      el.textContent = frames[i];
    }, Number.isFinite(intervalMs) && intervalMs > 0 ? intervalMs : 80);
    el.__prx = { stop: () => clearInterval(interval) };
  }

  // ── Live numeral counter ──────────────────────────────────
  function initNumeral(el) {
    const initial = el.getAttribute('data-value') || '0';
    let prev = '';
    function render(value) {
      const s = String(value);
      const padded = s.padStart(prev.length || 0, ' ');
      const oldDigits = prev.split('');
      const newDigits = padded.split('');
      el.innerHTML = newDigits.map((d, i) => {
        const isSep = d === ',' || d === '.';
        const cls = isSep ? 'sep' : 'digit';
        return `<span class="${cls}">${esc(d)}</span>`;
      }).join('');
      // flash digits that changed
      newDigits.forEach((d, i) => {
        if (oldDigits[i] !== d && d !== ',' && d !== '.') {
          const span = el.children[i];
          if (!span) return;
          span.classList.add('flip');
          setTimeout(() => span.classList.remove('flip'), 240);
        }
      });
      prev = padded;
    }
    render(initial);
    // watch for data-value attribute changes
    new MutationObserver((muts) => {
      for (const m of muts) {
        if (m.attributeName === 'data-value') render(el.getAttribute('data-value'));
      }
    }).observe(el, { attributes: true });
    el.__prx = { set: (v) => { el.setAttribute('data-value', v); } };
  }

  // ── Diagnostic readout ────────────────────────────────────
  function initDiag(el) {
    el.querySelectorAll('.row .v').forEach(v => {
      new MutationObserver(() => {
        v.classList.remove('flash');
        // force reflow so the animation restarts
        // eslint-disable-next-line no-unused-expressions
        void v.offsetWidth;
        v.classList.add('flash');
        setTimeout(() => v.classList.remove('flash'), 400);
      }).observe(v, { childList: true, characterData: true, subtree: true });
    });
  }

  // ── Tape transport ────────────────────────────────────────
  function initTransport(el) {
    el.querySelectorAll('button').forEach(btn => {
      on(btn, 'click', () => {
        el.querySelectorAll('button').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        el.dispatchEvent(new CustomEvent('prx:transport', {
          detail: { action: btn.dataset.action || btn.textContent.trim() },
          bubbles: true,
        }));
      });
    });
  }

  // ── Evidence stack ───────────────────────────────────────
  function initEvidenceStack(el) {
    const reader = el.closest('.demo')?.querySelector('.prx-evidence-reader');
    const select = (item) => {
      el.querySelectorAll('.item').forEach(x => x.classList.remove('selected'));
      item.classList.add('selected');
      let detail = {};
      try { detail = JSON.parse(item.getAttribute('data-evidence') || '{}'); } catch { detail = {}; }
      detail.kind = detail.kind || item.getAttribute('data-kind') || 'evidence';
      detail.title = detail.title || item.querySelector('.main')?.textContent?.trim() || 'evidence';
      detail.body = detail.body || item.querySelector('.meta')?.textContent?.trim() || '';
      if (reader) {
        const title = reader.querySelector('h3');
        const body = reader.querySelector('p');
        const cap = reader.querySelector('.cap');
        if (title) title.textContent = detail.title;
        if (body) body.textContent = detail.body;
        if (cap) cap.textContent = `${detail.kind} evidence`;
      }
      el.dispatchEvent(new CustomEvent('prx:evidence-select', { detail, bubbles: true }));
    };
    el.querySelectorAll('.item').forEach(item => {
      on(item, 'click', () => select(item));
    });
  }

  // ── Next legal moves rail ────────────────────────────────
  function initLegalRail(el) {
    const reader = el.closest('.demo')?.querySelector('.prx-legal-reader');
    const select = (move) => {
      el.querySelectorAll('.move').forEach(x => x.classList.remove('selected'));
      move.classList.add('selected');
      const detail = {
        context: el.getAttribute('data-context') || '',
        action: move.getAttribute('data-action') || move.querySelector('.label')?.textContent?.trim() || '',
        description: move.getAttribute('data-detail') || move.querySelector('.why')?.textContent?.trim() || '',
        denied: move.classList.contains('denied'),
      };
      if (reader) {
        const title = reader.querySelector('h3');
        const body = reader.querySelector('p');
        const hint = reader.querySelector('.hint span');
        if (title) title.textContent = detail.action;
        if (body) body.textContent = detail.description;
        if (hint) hint.textContent = `prx:legal-move · ${detail.denied ? 'denied' : 'allowed'}`;
      }
      el.dispatchEvent(new CustomEvent('prx:legal-move', { detail, bubbles: true }));
    };
    el.querySelectorAll('.move').forEach(move => {
      on(move, 'click', () => select(move));
    });
  }

  // ── Action consequence preview ───────────────────────────
  function collectRows(el) {
    const rows = {};
    el.querySelectorAll('.row').forEach(row => {
      const key = row.querySelector('.k')?.textContent?.trim().toLowerCase().replace(/\s+/g, '_') || '';
      const value = row.querySelector('.v')?.textContent?.trim() || '';
      if (key) rows[key] = value;
    });
    return rows;
  }

  function initActionPreview(el) {
    if (el.__prxActionPreview) return;
    el.__prxActionPreview = true;
    const output = el.closest('.demo')?.querySelector('.prx-action-preview-output');

    on(el, 'click', (event) => {
      const button = event.target.closest('[data-action]');
      if (!button || !el.contains(button)) return;

      const consequences = collectRows(el);
      const detail = {
        operation: el.getAttribute('data-operation') || el.querySelector('.preview-head span')?.textContent?.trim() || 'operation',
        action: button.getAttribute('data-action') || button.textContent.trim(),
        consequences,
      };

      if (output) {
        const emitted = consequences.will_emit || 'no event';
        output.textContent = `selected preview · ${detail.operation} · emits ${emitted}`;
        output.setAttribute('data-state', 'selected');
      }

      el.dispatchEvent(new CustomEvent('prx:action-preview-select', { detail, bubbles: true }));
    });
  }

  // ── Empty state explainer ─────────────────────────────────
  function initEmptyExplainer(el) {
    if (el.__prxEmptyExplainer) return;
    el.__prxEmptyExplainer = true;
    const output = el.closest('.demo')?.querySelector('.prx-empty-output');

    on(el, 'click', (event) => {
      const move = event.target.closest('[data-action]');
      if (!move || !el.contains(move)) return;

      const detail = {
        emptyKind: el.getAttribute('data-empty-kind') || 'empty',
        action: move.getAttribute('data-action') || move.textContent.trim(),
        title: el.querySelector('.title')?.textContent?.trim() || '',
        reason: el.querySelector('.why')?.textContent?.trim() || '',
      };

      if (output) {
        output.textContent = `next legal move · ${detail.action} · ${detail.emptyKind}`;
        output.setAttribute('data-state', 'selected');
      }

      el.dispatchEvent(new CustomEvent('prx:empty-next', { detail, bubbles: true }));
    });
  }

  // ── Tabstrip (kbd-prefix) ─────────────────────────────────
  function initTabstrip(el) {
    const tabs = Array.from(el.querySelectorAll('.tab'));
    if (!el.hasAttribute('role')) el.setAttribute('role', 'tablist');
    if (!el.hasAttribute('aria-label')) el.setAttribute('aria-label', 'Primary tabs');
    tabs.forEach((tab, idx) => {
      if (!tab.hasAttribute('role')) tab.setAttribute('role', 'tab');
      const isActive = tab.classList.contains('active');
      tab.setAttribute('aria-selected', isActive ? 'true' : 'false');
      tab.setAttribute('tabindex', isActive ? '0' : '-1');
      const kbd = tab.querySelector('.kbd');
      if (kbd && !kbd.hasAttribute('aria-hidden')) kbd.setAttribute('aria-hidden', 'true');
    });

    function activate(tab) {
      tabs.forEach(t => {
        t.classList.remove('active');
        t.setAttribute('aria-selected', 'false');
        t.setAttribute('tabindex', '-1');
      });
      tab.classList.add('active');
      tab.setAttribute('aria-selected', 'true');
      tab.setAttribute('tabindex', '0');
      tab.focus();
      el.dispatchEvent(new CustomEvent('prx:tab-select', {
        detail: { value: tab.dataset.value, kbd: tab.dataset.kbd, label: tab.textContent.trim() },
        bubbles: true,
      }));
    }
    function moveFocus(currentIndex, delta) {
      const next = (currentIndex + delta + tabs.length) % tabs.length;
      activate(tabs[next]);
    }
    tabs.forEach((t, i) => {
      on(t, 'click', () => activate(t));
      on(t, 'keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); activate(t); }
        else if (e.key === 'ArrowRight' || e.key === 'ArrowDown') { e.preventDefault(); moveFocus(i, +1); }
        else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') { e.preventDefault(); moveFocus(i, -1); }
        else if (e.key === 'Home') { e.preventDefault(); moveFocus(-1, +1); }
        else if (e.key === 'End') { e.preventDefault(); moveFocus(tabs.length, -1); }
      });
    });
    on(document, 'keydown', e => {
      if (!e.shiftKey || e.metaKey || e.ctrlKey || e.altKey) return;
      const tag = e.target?.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || e.target?.isContentEditable) return;
      const k = e.key?.toLowerCase();
      const match = el.querySelector(`.tab[data-kbd="${k}"]`);
      if (match) { e.preventDefault(); activate(match); }
    });
  }

  // ── Prompt input (autocomplete + classification) ──────────
  function initPromptInput(el) {
    const ta = el.querySelector('textarea');
    if (!ta) return; // tolerate stub markup without a textarea
    const refsEl = el.querySelector('.refs') || (() => {
      const d = document.createElement('div'); d.className = 'refs'; el.appendChild(d); return d;
    })();
    const summaryEl = el.querySelector('.summary');
    const refs = JSON.parse(el.getAttribute('data-refs') || '[]');
    let cursor = 0;
    let filtered = [];
    let triggerStart = -1;
    let triggerChar = '';

    function updateSummary() {
      if (!summaryEl) return;
      const text = ta.value;
      const referenced = refs.filter(r => text.includes('{' + r.name + '}') || new RegExp('@' + r.name + '\\b').test(text));
      const lower = text.toLowerCase();
      let trigger = null;
      if (/webhook|http|url|endpoint/.test(lower)) trigger = 'Webhook trigger';
      else if (/event|fires|on plan|on workflow/.test(lower)) trigger = 'Event trigger';
      else if (/schedule|every|cron|daily|hourly/.test(lower)) trigger = 'Schedule trigger';
      else if (text.length > 8) trigger = 'Manual trigger';
      const stepHints = (text.match(/\bthen\b|\bnext\b|\bafter\b|→|->|;\s/gi) || []).length;
      const stepCount = text.length > 8 ? Math.max(1, stepHints + 1) : 0;
      const parts = [];
      if (trigger) parts.push(`<strong>${esc(trigger)}</strong>`);
      if (stepCount > 0) parts.push(`<strong>${stepCount} step${stepCount === 1 ? '' : 's'}</strong>`);
      if (referenced.length) parts.push(`<strong>${referenced.length}</strong> upstream ref${referenced.length === 1 ? '' : 's'}`);
      summaryEl.innerHTML = parts.length ? 'Looks like: ' + parts.join(' → ') : '<em>no inferences yet · keep typing</em>';
    }

    function showRefs(char, query) {
      triggerChar = char;
      filtered = refs.filter(r => r.name.toLowerCase().includes(query.toLowerCase())).slice(0, 8);
      cursor = 0;
      if (filtered.length === 0) { refsEl.classList.remove('open'); return; }
      refsEl.innerHTML = filtered.map((r, i) => `
        <div class="row${i === cursor ? ' sel' : ''}" data-i="${i}">
          <span class="glyph">${char === '{' ? '{' : '@'}</span>
          <span class="name">${esc(r.name)}</span>
          <span class="type">${esc(r.type || '')}</span>
        </div>`).join('');
      refsEl.classList.add('open');
      refsEl.style.left = '8px';
      refsEl.style.top = (ta.offsetHeight + 4) + 'px';
    }

    function insertRef(item) {
      if (!item) return;
      const before = ta.value.slice(0, triggerStart);
      const after = ta.value.slice(ta.selectionStart);
      const close = triggerChar === '{' ? '}' : '';
      const insert = triggerChar + item.name + close;
      ta.value = before + insert + after;
      refsEl.classList.remove('open');
      triggerStart = -1;
      updateSummary();
      ta.focus();
      const newPos = (before + insert).length;
      ta.setSelectionRange(newPos, newPos);
      el.dispatchEvent(new CustomEvent('prx:prompt-ref-insert', { detail: { ref: item, char: triggerChar }, bubbles: true }));
    }

    on(ta, 'input', () => {
      const pos = ta.selectionStart;
      let i = pos - 1;
      while (i >= 0 && /[a-zA-Z0-9_.]/.test(ta.value[i])) i--;
      if (i >= 0 && (ta.value[i] === '{' || ta.value[i] === '@')) {
        triggerStart = i;
        const query = ta.value.slice(i + 1, pos);
        showRefs(ta.value[i], query);
      } else {
        refsEl.classList.remove('open');
      }
      updateSummary();
      el.dispatchEvent(new CustomEvent('prx:prompt-change', { detail: { value: ta.value }, bubbles: true }));
    });
    on(ta, 'keydown', e => {
      if (!refsEl.classList.contains('open')) return;
      if (e.key === 'ArrowDown') {
        cursor = Math.min(cursor + 1, filtered.length - 1);
        refsEl.querySelectorAll('.row').forEach((r, i) => r.classList.toggle('sel', i === cursor));
        e.preventDefault();
      } else if (e.key === 'ArrowUp') {
        cursor = Math.max(cursor - 1, 0);
        refsEl.querySelectorAll('.row').forEach((r, i) => r.classList.toggle('sel', i === cursor));
        e.preventDefault();
      } else if (e.key === 'Enter' || e.key === 'Tab') {
        insertRef(filtered[cursor]); e.preventDefault();
      } else if (e.key === 'Escape') {
        refsEl.classList.remove('open');
      }
    });
    on(refsEl, 'click', e => {
      const row = e.target.closest('.row');
      if (row) insertRef(filtered[+row.dataset.i]);
    });
    on(document, 'click', e => {
      if (!el.contains(e.target)) refsEl.classList.remove('open');
    });
    updateSummary();
  }

  // ── A11y sweep ────────────────────────────────────────────
  // Single pass at boot to fix the WCAG findings axe-core flagged:
  //   - scrollable-region-focusable: any vertical/horizontal scroller
  //     gets tabindex=0 + role=region + aria-label
  //   - select-name: <select> elements without an accessible name
  //     get aria-label inferred from a sibling label or the id
  //   - region: floating modals (palette, drawer) get role=dialog
  //   - label: stray <input> not inside <label> + no for-id linkage
  //     gets aria-label from its placeholder if present
  function initA11y(root) {
    const ctx = root || document;
    const SCROLL_TARGETS = [
      '.prx-table .body',
      '.prx-diff',
      '.prx-wizard-preview',
      '.prx-drawer-panel > .body',
      '.prx-flow',
      '.prx-palette-list',
    ];
    SCROLL_TARGETS.forEach(sel => {
      ctx.querySelectorAll(sel).forEach(el => {
        if (!el.hasAttribute('tabindex')) el.setAttribute('tabindex', '0');
        // Use group instead of region so each one doesn't need a unique landmark name
        if (!el.hasAttribute('role')) el.setAttribute('role', 'group');
        if (!el.hasAttribute('aria-label')) {
          // Try to disambiguate via the nearest id'd ancestor
          const named = el.closest('[id]');
          const ownerId = named?.id;
          const baseLabel = sel.replace('.prx-', '').replace(' .body', ' rows').replace(' > ', ' ');
          el.setAttribute('aria-label', ownerId ? `${baseLabel} · ${ownerId}` : baseLabel);
        }
      });
    });
    // Selects without an accessible name
    ctx.querySelectorAll('select:not([aria-label]):not([aria-labelledby])').forEach(sel => {
      const id = sel.id || sel.name;
      if (id) sel.setAttribute('aria-label', String(id).replace(/[-_]/g, ' '));
    });
    // Modals (palette + drawer) → role=dialog when not already set
    ctx.querySelectorAll('.prx-palette').forEach(el => {
      if (!el.hasAttribute('role')) el.setAttribute('role', 'dialog');
      if (!el.hasAttribute('aria-modal')) el.setAttribute('aria-modal', 'true');
      if (!el.hasAttribute('aria-label')) el.setAttribute('aria-label', 'Command palette');
    });
    ctx.querySelectorAll('.prx-drawer').forEach(el => {
      if (!el.hasAttribute('role')) el.setAttribute('role', 'dialog');
      if (!el.hasAttribute('aria-modal')) el.setAttribute('aria-modal', 'true');
      if (!el.hasAttribute('aria-label')) el.setAttribute('aria-label', 'Receipt drawer');
    });
    // Stray inputs without label-for linkage: infer aria-label from placeholder
    ctx.querySelectorAll('input:not([aria-label]):not([aria-labelledby])').forEach(input => {
      if (input.id && ctx.querySelector(`label[for="${input.id}"]`)) return;
      if (input.closest('label')) return;
      const placeholder = input.getAttribute('placeholder');
      const dataPath = input.getAttribute('data-path') || input.getAttribute('data-field');
      if (placeholder) input.setAttribute('aria-label', placeholder);
      else if (dataPath) input.setAttribute('aria-label', dataPath);
    });
    // Status caps surface their tone via aria-label so screenreaders speak it
    ctx.querySelectorAll('.stat-cap[data-tone]:not([aria-label])').forEach(s => {
      const tone = s.getAttribute('data-tone');
      const txt = s.textContent.trim();
      s.setAttribute('aria-label', `${txt} (${tone})`);
    });
  }

  // ── boot ──
  function init() {
    $$('.prx-table').forEach(initTable);
    $$('.prx-drawer').forEach(initDrawer);
    $$('.prx-palette').forEach(initPalette);
    $$('.prx-wizard').forEach(initWizard);
    $$('.prx-form').forEach(initForm);
    $$('.prx-dispatch').forEach(initDispatch);
    $$('.prx-cmd-bar').forEach(initCmdBar);
    $$('.prx-flow').forEach(initFlow);
    $$('.prx-step-builder').forEach(initStepBuilder);
    $$('.prx-workflow-bar').forEach(initWorkflowBar);
    $$('.prx-spinner').forEach(initSpinner);
    $$('.prx-numeral').forEach(initNumeral);
    $$('.prx-diag').forEach(initDiag);
    $$('.prx-transport').forEach(initTransport);
    $$('.prx-evidence-stack').forEach(initEvidenceStack);
    $$('.prx-legal-rail').forEach(initLegalRail);
    $$('.prx-action-preview').forEach(initActionPreview);
    $$('.prx-empty-explainer').forEach(initEmptyExplainer);
    $$('.prx-tabstrip').forEach(initTabstrip);
    $$('.prx-prompt-input').forEach(initPromptInput);
    initA11y();
  }
  if (document.readyState === 'loading') on(document, 'DOMContentLoaded', init); else init();

  // ── Schema → Wizard config ────────────────────────────────
  // Walk a JSON Schema (Pydantic-shape) and produce a wizard config.
  // Optional schema extensions:
  //   x-praxis-steps: [{ label, title, description, fields: [...names] }]
  //     → controls step grouping. If omitted, fields auto-group by 3 per step.
  //   x-step (per property): step label this field belongs to.
  function schemaToWizardConfig(schema) {
    if (!schema || schema.type !== 'object' || !schema.properties) {
      throw new Error('schemaToWizardConfig: expected an object schema with properties');
    }
    const propNames = Object.keys(schema.properties);
    const required = new Set(schema.required || []);

    function fieldFromProp(name, prop) {
      const f = {
        key: name,
        label: prop.title || name,
        required: required.has(name),
        hint: prop.description,
      };
      const example = Array.isArray(prop.examples) && prop.examples.length ? prop.examples[0] : undefined;
      if (example !== undefined) f.placeholder = String(example).split('\n')[0].slice(0, 80);

      if (Array.isArray(prop.enum)) {
        f.type = 'radio';
        f.options = prop.enum.map(v => ({ label: String(v), value: String(v) }));
      } else if (prop.type === 'boolean') {
        f.type = 'radio';
        f.options = [{ label: 'true', value: 'true' }, { label: 'false', value: 'false' }];
      } else if (prop.format === 'textarea' || prop.format === 'multi-line' || (typeof prop.maxLength === 'number' && prop.maxLength > 200)) {
        f.type = 'textarea';
        f.rows = prop['x-rows'] || 4;
      } else if (prop.type === 'integer' || prop.type === 'number') {
        f.type = 'number';
      } else {
        f.type = 'text';
      }
      return f;
    }

    let steps;
    const hint = schema['x-praxis-steps'];
    if (hint && hint.length) {
      steps = hint.map(s => ({
        label: s.label,
        title: s.title || s.label,
        description: s.description || '',
        fields: (s.fields || []).map(name => {
          if (!schema.properties[name]) throw new Error(`x-praxis-steps references unknown field: ${name}`);
          return fieldFromProp(name, schema.properties[name]);
        }),
      }));
    } else {
      const chunks = [];
      const PER = 3;
      for (let i = 0; i < propNames.length; i += PER) chunks.push(propNames.slice(i, i + PER));
      steps = chunks.map((chunk, i) => ({
        label: `step ${i + 1}`,
        title: i === 0 ? (schema.title || `Step ${i + 1}`) : `Step ${i + 1}`,
        description: i === 0 ? (schema.description || '') : '',
        fields: chunk.map(name => fieldFromProp(name, schema.properties[name])),
      }));
    }
    steps.push({
      label: 'review',
      title: 'Review',
      description: 'Generated from JSON Schema. Submit to dispatch the payload through the gateway.',
      fields: [],
    });

    const initial = {};
    for (const [name, prop] of Object.entries(schema.properties)) {
      if (prop.default !== undefined) initial[name] = String(prop.default);
    }

    return { initial, steps, _schema: schema };
  }

  function buildWizardFromSchema(targetEl, schema, opts) {
    targetEl.innerHTML = '';
    const w = document.createElement('div');
    w.className = 'prx-wizard';
    w.setAttribute('data-config', JSON.stringify(schemaToWizardConfig(schema)));
    if (opts && opts.preview) w.setAttribute('data-preview', opts.preview);
    if (opts && opts.id) w.id = opts.id;
    w.innerHTML = `
      <div class="prx-wizard-steps"></div>
      <div class="prx-wizard-body">
        <div class="prx-wizard-form"></div>
        <div class="prx-wizard-preview"></div>
      </div>
      <div class="prx-wizard-foot"></div>`;
    targetEl.appendChild(w);
    initWizard(w);
    return w;
  }

  // ── Gateway shim ──────────────────────────────────────────
  // Simulates execute_operation_from_subsystems locally so the
  // showcase can demo the full dispatch contract without a backend:
  //   - input hashed → idempotency_key
  //   - read_only / idempotent ops replay from cache on hash match
  //   - completed receipts emit a synthetic event_id when event_required
  //   - all dispatches accumulate in `gateway.receipts`
  // Any code that wants to listen subscribes via on(target, 'prx:gateway-dispatch', ...)
  const _gatewayBus = document.createElement('div');
  const _gateway = {
    receipts: [],
    cache: new Map(),
    bus: _gatewayBus,
    _ts() { return new Date().toISOString().replace('T', ' ').slice(0, 19); },
    _rid() { return 'r_' + Math.random().toString(16).slice(2, 8); },
    _hash(op, payload) {
      const s = JSON.stringify({ op, payload });
      let h = 0;
      for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
      const a = Math.abs(h).toString(16).padStart(8, '0');
      const b = Math.abs(h * 31).toString(16).padStart(8, '0');
      return 'sha256:' + a + b;
    },
    dispatch(op, kind, idempotency, eventRequired, payload) {
      const idem = this._hash(op, payload);
      const cacheable = idempotency === 'read_only' || idempotency === 'idempotent';

      if (cacheable && this.cache.has(idem)) {
        const origId = this.cache.get(idem);
        const orig = this.receipts.find(r => r.receipt_id === origId);
        if (orig) {
          const replay = {
            ...orig,
            receipt_id: this._rid(),
            execution_status: 'replayed',
            replay_of: origId,
            duration_ms: 1 + Math.floor(Math.random() * 8),
            ts: this._ts(),
          };
          this.receipts.push(replay);
          _gatewayBus.dispatchEvent(new CustomEvent('prx:gateway-dispatch', { detail: replay, bubbles: true }));
          return replay;
        }
      }

      const status = Math.random() < 0.92 ? 'completed' : 'failed';
      const r = {
        receipt_id: this._rid(),
        operation: op,
        kind,
        idempotency_policy: idempotency,
        execution_status: status,
        duration_ms: 80 + Math.floor(Math.random() * 220),
        payload,
        idempotency_key: idem,
        event_id: eventRequired && status === 'completed' ? 'e_' + Math.random().toString(16).slice(2, 8) : null,
        event_type: eventRequired && status === 'completed' ? `${op}.completed` : null,
        ts: this._ts(),
        replay_of: null,
      };
      this.receipts.push(r);
      if (cacheable && status === 'completed') this.cache.set(idem, r.receipt_id);
      _gatewayBus.dispatchEvent(new CustomEvent('prx:gateway-dispatch', { detail: r, bubbles: true }));
      return r;
    },
    replay(receiptId) {
      const orig = this.receipts.find(r => r.receipt_id === receiptId && !r.replay_of);
      if (!orig) return null;
      return this.dispatch(orig.operation, orig.kind, orig.idempotency_policy, !!orig.event_type, orig.payload);
    },
    find(receiptId) { return this.receipts.find(r => r.receipt_id === receiptId); },
    on(ev, fn) { _gatewayBus.addEventListener(ev, fn); },
  };

  // ── Receipt diff renderer ─────────────────────────────────
  // Renders two receipts as a prx-receipt-diff with delta strip.
  function renderReceiptDiff(targetEl, original, replay) {
    const fmt = (r, label, ts) => {
      const stateAttr = r.execution_status === 'failed' ? 'refused' : 'ok';
      const dur = (r.duration_ms ?? 0) + 'ms';
      return `<div class="col-${label}">
        <div class="prx-receipt" data-state="${stateAttr}">
          <div class="hd"><span>receipt ${label.toUpperCase()} · ${r.replay_of ? 'replay' : 'original'}</span><span>${esc(r.ts || ts || '')}</span></div>
          <div class="row"><span class="k">action</span><span class="v">${esc(r.operation || '—')}</span></div>
          <div class="row"><span class="k">status</span><span class="v">${esc(r.execution_status || '—')}</span></div>
          <div class="row"><span class="k">duration</span><span class="v">${esc(dur)}</span></div>
          <div class="row"><span class="k">idempotency</span><span class="v">${esc(r.idempotency_policy || '—')}</span></div>
          <div class="row"><span class="k">event</span><span class="v">${esc(r.event_type || r.event_id || '—')}</span></div>
          <div class="ft"><span class="hash">${esc(r.receipt_id || '—')}</span><span></span></div>
        </div>
      </div>`;
    };
    const samePayload = JSON.stringify(original.payload) === JSON.stringify(replay.payload);
    const isCacheHit = !!replay.replay_of;
    // A cache-served replay is semantically identical (output came from the same row).
    // Otherwise we compare the surface fields.
    const sameStatus = original.execution_status === replay.execution_status;
    const identical = samePayload && (isCacheHit || sameStatus);
    const deltaState = identical ? 'same' : 'diff';
    const deltaText = isCacheHit
      ? `payload-hash match · cached replay served in ${replay.duration_ms}ms (vs ${original.duration_ms}ms first run)`
      : sameStatus
        ? `status ${original.execution_status} held · duration ${original.duration_ms}ms → ${replay.duration_ms}ms`
        : `status ${original.execution_status} → ${replay.execution_status} · duration ${original.duration_ms}ms → ${replay.duration_ms}ms`;
    targetEl.innerHTML = `
      <div class="prx-receipt-diff">
        ${fmt(original, 'a', original.ts)}
        <div class="arrow">⇄</div>
        ${fmt(replay, 'b', replay.ts)}
      </div>
      <div class="prx-receipt-diff" style="grid-template-columns:1fr">
        <div class="delta" data-state="${deltaState}">${esc(deltaText)}</div>
      </div>`;
  }

  window.Prx = {
    drawer: { open: openDrawer },
    palette: { open: () => document.querySelector('.prx-palette')?.__prx?.open?.() },
    wizard: { init: initWizard, fromSchema: buildWizardFromSchema },
    table: { setRows: (el, rows) => el.__prx?.setRows?.(rows) },
    gateway: _gateway,
    receiptDiff: renderReceiptDiff,
    schemaToWizardConfig,
  };
})();
