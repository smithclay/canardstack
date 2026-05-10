pub fn index_html() -> String {
    r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Canardstack Local Investigate</title>
  <style>
    :root { color-scheme: light; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f6f7f9; color: #202735; }
    * { box-sizing: border-box; }
    body { margin: 0; }
    header { height: 52px; display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 0 18px; border-bottom: 1px solid #d8dee8; background: #fff; }
    h1 { font-size: 17px; margin: 0; font-weight: 650; }
    main { display: grid; grid-template-columns: 320px minmax(0, 1fr); min-height: calc(100vh - 52px); }
    aside { border-right: 1px solid #d8dee8; background: #fff; padding: 12px; display: grid; gap: 10px; align-content: start; }
    section { padding: 14px; display: grid; grid-template-rows: auto minmax(0, 1fr); gap: 12px; min-width: 0; }
    button, textarea, input, select { font: inherit; }
    button { border: 1px solid #bfc8d6; background: #fff; color: #202735; border-radius: 6px; padding: 8px 10px; cursor: pointer; }
    button.primary { border-color: #19615a; background: #19615a; color: #fff; }
    label { display: grid; gap: 4px; font-size: 12px; color: #566175; }
    input, select, textarea { border: 1px solid #c7d0dc; border-radius: 6px; padding: 7px 8px; background: #fff; min-width: 0; }
    textarea { min-height: 76px; resize: vertical; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    .actions { display: flex; gap: 8px; flex-wrap: wrap; }
    .meta { display: flex; gap: 12px; color: #566175; font-size: 12px; flex-wrap: wrap; }
    .panel { display: grid; grid-template-rows: auto minmax(0, 1fr); gap: 8px; min-width: 0; min-height: 0; }
    table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d8dee8; }
    th, td { border-bottom: 1px solid #e6eaf0; padding: 7px 8px; font-size: 12px; text-align: left; vertical-align: top; overflow-wrap: anywhere; }
    th { background: #eef2f6; font-weight: 650; }
    pre { white-space: pre-wrap; overflow-wrap: anywhere; margin: 0; background: #fff; border: 1px solid #d8dee8; border-radius: 6px; padding: 10px; font-size: 12px; overflow: auto; }
    .split { display: grid; grid-template-columns: minmax(0, 1fr) 360px; gap: 12px; min-height: 0; }
    @media (max-width: 860px) { main { grid-template-columns: 1fr; } aside { border-right: 0; border-bottom: 1px solid #d8dee8; } .split, .row { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
<header><h1>Canardstack Local Investigate</h1><span id="status">local compatibility APIs</span></header>
<main>
  <aside>
    <label>Surface<select id="surface">
      <option value="prom">Prometheus metrics</option>
      <option value="loki">Loki logs</option>
      <option value="tempo-search">Tempo search</option>
      <option value="tempo-trace">Tempo trace</option>
    </select></label>
    <label>Query<input id="query"></label>
    <div class="row">
      <label>Start<input id="start"></label>
      <label>End<input id="end"></label>
    </div>
    <div class="row">
      <label>Step<input id="step" value="60"></label>
      <label>Limit<input id="limit" type="number" value="50"></label>
    </div>
    <div class="actions">
      <button class="primary" onclick="runQuery()">Run</button>
      <button onclick="loadLabels()">Labels</button>
      <button onclick="loadSeries()">Series</button>
    </div>
    <textarea id="params"></textarea>
  </aside>
  <section>
    <div class="meta" id="meta"></div>
    <div class="split">
      <div class="panel"><div id="table"></div></div>
      <pre id="raw"></pre>
    </div>
  </section>
</main>
<script>
const apiKey = localStorage.canardstackApiKey || prompt('API key', 'dev-canardstack-key') || 'dev-canardstack-key';
localStorage.canardstackApiKey = apiKey;
const now = new Date(); const hourAgo = new Date(Date.now() - 3600_000);
q('start').value = hourAgo.toISOString(); q('end').value = now.toISOString();
q('query').value = 'avg(smoke.gauge{service_name="checkout"})';
q('params').value = JSON.stringify({"service.name":"checkout"}, null, 2);
q('surface').onchange = () => {
  const s = q('surface').value;
  q('query').value = s === 'prom' ? 'avg(smoke.gauge{service_name="checkout"})' : s === 'loki' ? '{service_name="checkout"} |= "smoke"' : s === 'tempo-trace' ? '11111111111111111111111111111111' : '';
};
async function runQuery() {
  const s = q('surface').value;
  let path = '';
  if (s === 'prom') path = '/api/v1/query_range?' + qs({query:q('query').value,start:q('start').value,end:q('end').value,step:q('step').value});
  if (s === 'loki') path = '/loki/api/v1/query_range?' + qs({query:q('query').value,start:q('start').value,end:q('end').value,limit:q('limit').value});
  if (s === 'tempo-search') path = '/api/search?' + qs(Object.assign(JSON.parse(q('params').value || '{}'), {start:q('start').value,end:q('end').value,limit:q('limit').value}));
  if (s === 'tempo-trace') path = '/api/v2/traces/' + encodeURIComponent(q('query').value.trim());
  await fetchAndRender(path);
}
async function loadLabels() {
  const s = q('surface').value;
  const base = s === 'loki' ? '/loki/api/v1/labels' : '/api/v1/labels';
  await fetchAndRender(base + '?' + qs({start:q('start').value,end:q('end').value}));
}
async function loadSeries() {
  const s = q('surface').value;
  const base = s === 'loki' ? '/loki/api/v1/series' : '/api/v1/series';
  await fetchAndRender(base + '?' + qs({start:q('start').value,end:q('end').value}));
}
async function fetchAndRender(path) {
  const r = await fetch(path, {headers:{authorization:'Bearer '+apiKey, accept:'application/json'}});
  const result = await r.json();
  q('raw').textContent = JSON.stringify(result, null, 2);
  q('meta').textContent = `HTTP ${r.status} | ${path}`;
  render(result);
}
function render(result) {
  const rows = flatten(result);
  if (!rows.length) { q('table').innerHTML = '<pre>'+escapeHtml(JSON.stringify(result.data ?? result, null, 2))+'</pre>'; return; }
  const keys = Object.keys(rows[0] || {});
  q('table').innerHTML = '<table><thead><tr>'+keys.map(k=>`<th>${k}</th>`).join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+keys.map(k=>`<td>${escapeHtml(format(r[k]))}</td>`).join('')+'</tr>').join('')+'</tbody></table>';
}
function flatten(result) {
  const data = result.data ?? result;
  if (Array.isArray(data)) return data.map(v => typeof v === 'object' ? v : {value:v});
  if (Array.isArray(data.result)) return data.result.map(v => ({...v, values: JSON.stringify(v.values ?? v.value ?? '')}));
  if (Array.isArray(data.traces)) return data.traces;
  const spans = data.batches?.[0]?.instrumentationLibrarySpans?.[0]?.spans;
  if (Array.isArray(spans)) return spans;
  return [];
}
function qs(obj){ return new URLSearchParams(Object.entries(obj).filter(([,v]) => v !== undefined && v !== '')).toString(); }
function format(v){ return typeof v === 'object' && v !== null ? JSON.stringify(v) : (v ?? ''); }
function escapeHtml(s){ return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function q(id){ return document.getElementById(id); }
</script>
</body>
</html>"#.to_string()
}
