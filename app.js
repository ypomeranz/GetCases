'use strict';

const BASE_URL = 'https://www.courtlistener.com/api/rest/v4/';
const STORAGE_BASE = 'https://storage.courtlistener.com/';
const LOC_CUTOFF = 542;
const GOVINFO_MAX = 582;

const TOKEN_KEY = 'courtlistener.api_token';

const COURT_BLUEBOOK = {
  ca1: '1st Cir.', ca2: '2d Cir.', ca3: '3d Cir.', ca4: '4th Cir.',
  ca5: '5th Cir.', ca6: '6th Cir.', ca7: '7th Cir.', ca8: '8th Cir.',
  ca9: '9th Cir.', ca10: '10th Cir.', ca11: '11th Cir.',
  cadc: 'D.C. Cir.', cafc: 'Fed. Cir.', cavet: 'Vet. App.', caaf: 'C.A.A.F.',
  akd: 'D. Alaska', almd: 'M.D. Ala.', alnd: 'N.D. Ala.', alsd: 'S.D. Ala.',
  ared: 'E.D. Ark.', arwd: 'W.D. Ark.', azd: 'D. Ariz.',
  cacd: 'C.D. Cal.', caed: 'E.D. Cal.', cand: 'N.D. Cal.', casd: 'S.D. Cal.',
  cod: 'D. Colo.', ctd: 'D. Conn.', ded: 'D. Del.', dcd: 'D.D.C.',
  flmd: 'M.D. Fla.', flnd: 'N.D. Fla.', flsd: 'S.D. Fla.',
  gamd: 'M.D. Ga.', gand: 'N.D. Ga.', gasd: 'S.D. Ga.',
  gud: 'D. Guam', hid: 'D. Haw.', idd: 'D. Idaho',
  ilcd: 'C.D. Ill.', ilnd: 'N.D. Ill.', ilsd: 'S.D. Ill.',
  innd: 'N.D. Ind.', insd: 'S.D. Ind.',
  iand: 'N.D. Iowa', iasd: 'S.D. Iowa', ksd: 'D. Kan.',
  kyed: 'E.D. Ky.', kywd: 'W.D. Ky.',
  laed: 'E.D. La.', lamd: 'M.D. La.', lawd: 'W.D. La.',
  med: 'D. Me.', mdd: 'D. Md.', mad: 'D. Mass.',
  mied: 'E.D. Mich.', miwd: 'W.D. Mich.', mnd: 'D. Minn.',
  msnd: 'N.D. Miss.', mssd: 'S.D. Miss.',
  moed: 'E.D. Mo.', mowd: 'W.D. Mo.', mtd: 'D. Mont.',
  ned: 'D. Neb.', nvd: 'D. Nev.', nhd: 'D.N.H.', njd: 'D.N.J.',
  nmd: 'D.N.M.', nmid: 'D.N. Mar. I.',
  nyed: 'E.D.N.Y.', nynd: 'N.D.N.Y.', nysd: 'S.D.N.Y.', nywd: 'W.D.N.Y.',
  nced: 'E.D.N.C.', ncmd: 'M.D.N.C.', ncwd: 'W.D.N.C.', ndd: 'D.N.D.',
  ohnd: 'N.D. Ohio', ohsd: 'S.D. Ohio',
  oked: 'E.D. Okla.', oknd: 'N.D. Okla.', okwd: 'W.D. Okla.',
  ord: 'D. Or.', paed: 'E.D. Pa.', pamd: 'M.D. Pa.', pawd: 'W.D. Pa.',
  prd: 'D.P.R.', rid: 'D.R.I.', scd: 'D.S.C.', sdd: 'D.S.D.',
  tned: 'E.D. Tenn.', tnmd: 'M.D. Tenn.', tnwd: 'W.D. Tenn.',
  txed: 'E.D. Tex.', txnd: 'N.D. Tex.', txsd: 'S.D. Tex.', txwd: 'W.D. Tex.',
  utd: 'D. Utah', vtd: 'D. Vt.', vaed: 'E.D. Va.', vawd: 'W.D. Va.',
  vid: 'D.V.I.', waed: 'E.D. Wash.', wawd: 'W.D. Wash.',
  wvnd: 'N.D. W. Va.', wvsd: 'S.D. W. Va.',
  wied: 'E.D. Wis.', wiwd: 'W.D. Wis.', wyd: 'D. Wyo.',
  cit: "Ct. Int'l Trade", uscfc: 'Fed. Cl.', tax: 'T.C.',
  ala: 'Ala.', alaska: 'Alaska', ariz: 'Ariz.', ark: 'Ark.',
  cal: 'Cal.', colo: 'Colo.', conn: 'Conn.', del: 'Del.', dc: 'D.C.',
  fla: 'Fla.', ga: 'Ga.', haw: 'Haw.', idaho: 'Idaho', ill: 'Ill.',
  ind: 'Ind.', iowa: 'Iowa', kan: 'Kan.', ky: 'Ky.', la: 'La.',
  me: 'Me.', md: 'Md.', mass: 'Mass.', mich: 'Mich.', minn: 'Minn.',
  miss: 'Miss.', mo: 'Mo.', mont: 'Mont.', neb: 'Neb.', nev: 'Nev.',
  nh: 'N.H.', nj: 'N.J.', nm: 'N.M.', ny: 'N.Y.', nc: 'N.C.', nd: 'N.D.',
  ohio: 'Ohio', okla: 'Okla.', or: 'Or.', pa: 'Pa.', ri: 'R.I.',
  sc: 'S.C.', sd: 'S.D.', tenn: 'Tenn.', tex: 'Tex.', utah: 'Utah',
  vt: 'Vt.', va: 'Va.', wash: 'Wash.', wva: 'W. Va.', wis: 'Wis.', wyo: 'Wyo.',
};

const OPINION_TYPE_LABELS = {
  '010combined': 'Opinion',
  '015unamimous': 'Unanimous Opinion',
  '020lead': 'Lead Opinion',
  '025plurality': 'Plurality Opinion',
  '030concurrence': 'Concurrence',
  '035concurrenceinpart': 'Concurrence in Part',
  '040dissent': 'Dissent',
  '050addendum': 'Addendum',
  '060remittitur': 'Remittitur',
  '070rehearing': 'Rehearing',
  '080onthemerits': 'On the Merits',
  '090onmotiontoamend': 'On Motion to Amend',
};

const CITE_PRIORITY = [
  / U\.S\. /, / S\. Ct\. /,
  / F\.4th /, / F\.3d /, / F\.2d /, / F\. \d/,
  / F\. Supp\. 3d /, / F\. Supp\. 2d /, / F\. Supp\. /,
  / B\.R\. /,
];

const NOISE_CITE_RE = /lexis|westlaw|\bwl\b/i;
const US_CITE_RE = /(\d+)\s+U\.S\.\s+(\d+)/;
const CITE_PARSE_RE = /^(\d+)\s+(.+?)\s+(\d+)\s*$/;
const CLUSTER_ID_RE = /\/clusters\/(\d+)\/?/;
const OPINION_ID_RE = /\/opinions\/(\d+)\/?/;
const COURT_ID_RE = /\/courts\/([^/]+)\/?/;

function stripHtmlTags(s) {
  return String(s || '').replace(/<[^>]+>/g, '').trim();
}

function stripHtmlBlock(html) {
  let t = String(html || '').replace(
    /<(br|\/p|\/div|\/h[1-6]|\/li|\/tr|\/blockquote)\b[^>]*>/gi, '\n');
  t = t.replace(/<[^>]+>/g, '');
  t = t.replace(/\n{3,}/g, '\n\n');
  return t.trim();
}

function pickCitation(citations) {
  if (!citations) return '';
  const list = Array.isArray(citations) ? citations : [String(citations)];
  const clean = list.map(stripHtmlTags).filter(Boolean);
  const nonNoise = clean.filter((c) => !NOISE_CITE_RE.test(c));
  const pool = nonNoise.length ? nonNoise : clean;
  for (const re of CITE_PRIORITY) {
    const hit = pool.find((c) => re.test(c));
    if (hit) return hit;
  }
  return pool[0] || '';
}

function clusterCitationsToStrings(citations) {
  const out = [];
  for (const c of citations || []) {
    if (c && typeof c === 'object') {
      const { volume, reporter, page } = c;
      if (volume && reporter && page) out.push(`${volume} ${reporter} ${page}`);
    } else if (typeof c === 'string' && c.trim()) {
      out.push(c.trim());
    }
  }
  return out;
}

function extractClusterId(url) {
  const m = String(url || '').match(CLUSTER_ID_RE);
  return m ? parseInt(m[1], 10) : null;
}

function extractOpinionId(url) {
  const m = String(url || '').match(OPINION_ID_RE);
  return m ? parseInt(m[1], 10) : null;
}

function extractCourtId(url) {
  const m = String(url || '').match(COURT_ID_RE);
  return m ? m[1] : '';
}

function pad3(n) { return String(n).padStart(3, '0'); }
function pad4(n) { return String(n).padStart(4, '0'); }

function usReportsLocUrl(citation) {
  const m = String(citation).match(US_CITE_RE);
  if (!m) return null;
  const vol = parseInt(m[1], 10);
  const page = parseInt(m[2], 10);
  if (vol > LOC_CUTOFF) return null;
  return `https://cdn.loc.gov/service/ll/usrep/usrep${pad3(vol)}/usrep${pad3(vol)}${pad3(page)}/usrep${pad3(vol)}${pad3(page)}.pdf`;
}

function usReportsGovInfoUrls(citation) {
  const m = String(citation).match(US_CITE_RE);
  if (!m) return null;
  const vol = parseInt(m[1], 10);
  const page = parseInt(m[2], 10);
  if (vol > GOVINFO_MAX) return null;
  return {
    link: `https://www.govinfo.gov/link/usreports/${vol}/${page}`,
    direct: `https://www.govinfo.gov/content/pkg/USREPORTS-${vol}/pdf/USREPORTS-${vol}-${page}.pdf`,
  };
}

function slugifyReporter(reporter) {
  return String(reporter)
    .toLowerCase()
    .replace(/ /g, '-')
    .replace(/[^a-z0-9-]/g, '')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function staticCaseLawUrl(citation) {
  const cite = stripHtmlTags(citation);
  const m = cite.match(CITE_PARSE_RE);
  if (!m) return null;
  const vol = m[1];
  const reporter = m[2].trim();
  const page = parseInt(m[3], 10);
  const slug = slugifyReporter(reporter);
  if (!slug) return null;
  return `https://static.case.law/${slug}/${vol}/case-pdfs/${pad4(page)}-01.pdf`;
}

function buildDefaultFilename(item) {
  const caseName = stripHtmlTags(item.caseName || item.case_name || 'opinion');
  const cite = pickCitation(item.citation || []);
  const date = String(item.dateFiled || item.date_filed || '');
  const year = date.length >= 4 ? date.slice(0, 4) : '';
  const courtId = String(item.court_id || item.court || '').toLowerCase().trim();
  const isScotus = courtId.includes('scotus');
  let courtAbbr = '';
  if (!isScotus) {
    courtAbbr = COURT_BLUEBOOK[courtId] || String(item.court || courtId).trim();
  }
  let paren = '';
  if (courtAbbr && year) paren = `(${courtAbbr} ${year})`;
  else if (year) paren = `(${year})`;
  else if (courtAbbr) paren = `(${courtAbbr})`;

  const parts = [caseName, cite].filter(Boolean);
  let raw = parts.join(', ');
  if (paren) raw = raw ? `${raw} ${paren}` : paren;

  const safe = Array.from(raw)
    .map((ch) => /[A-Za-z0-9 .,()\-_']/.test(ch) ? ch : '_')
    .join('')
    .slice(0, 120)
    .trim();
  return safe;
}

class CourtListenerError extends Error {
  constructor(status, message) {
    super(`HTTP ${status}: ${message}`);
    this.status = status;
  }
}

class CourtListenerClient {
  constructor(token) {
    this.token = token;
  }

  _headers() {
    return { 'Authorization': `Token ${this.token}`, 'Accept': 'application/json' };
  }

  async _request(url) {
    let resp;
    try {
      resp = await fetch(url, { headers: this._headers() });
    } catch (e) {
      throw new CourtListenerError(0, `Network error: ${e.message}`);
    }
    if (!resp.ok) {
      let detail;
      try { detail = (await resp.json()).detail; } catch { detail = await resp.text(); }
      throw new CourtListenerError(resp.status, detail || resp.statusText);
    }
    return resp.json();
  }

  _buildUrl(endpoint, params) {
    const url = new URL(endpoint.replace(/^\//, ''), BASE_URL);
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        if (v !== null && v !== undefined && v !== '') url.searchParams.set(k, v);
      }
    }
    return url.toString();
  }

  get(endpoint, params) { return this._request(this._buildUrl(endpoint, params)); }
  getUrl(url) { return this._request(url); }

  search(query, opts = {}) {
    const params = {
      q: query,
      type: opts.type || 'o',
      court: opts.court || null,
      filed_after: opts.dateFrom || null,
      filed_before: opts.dateTo || null,
      highlight: opts.highlight ? 'on' : null,
      cursor: opts.cursor || null,
      page_size: opts.pageSize || 20,
    };
    return this.get('search/', params);
  }

  getCluster(id, fields) {
    return this.get(`clusters/${id}/`, fields ? { fields } : null);
  }

  getOpinion(id, fields) {
    return this.get(`opinions/${id}/`, fields ? { fields } : null);
  }

  listCitingOpinions(citedOpinionId) {
    return this.get('opinions-cited/', { cited_opinion: citedOpinionId });
  }
}

const $ = (id) => document.getElementById(id);

const ui = {
  query: $('query'),
  court: $('court'),
  dateFrom: $('date-from'),
  dateTo: $('date-to'),
  pageSize: $('page-size'),
  searchBtn: $('search-btn'),
  status: $('status'),
  actionStatus: $('action-status'),

  results: $('results-table').querySelector('tbody'),
  orders: $('orders-table').querySelector('tbody'),
  preview: $('preview'),

  pdfBtn: $('pdf-btn'),
  textBtn: $('text-btn'),
  citeBtn: $('cite-btn'),
  scholarBtn: $('scholar-btn'),

  settingsBtn: $('settings-btn'),
  settingsDialog: $('settings-dialog'),
  tokenInput: $('token-input'),
  tokenShow: $('token-show'),
  tokenSave: $('token-save'),
  tokenCancel: $('token-cancel'),

  pdfDialog: $('pdf-dialog'),
  pdfCandidates: $('pdf-candidates'),
  pdfFallbackNote: $('pdf-fallback-note'),
  pdfClose: $('pdf-close'),

  textDialog: $('text-dialog'),
  textTitle: $('text-dialog-title'),
  textMeta: $('text-meta'),
  textBody: $('text-body'),
  textSave: $('text-save'),
  textClose: $('text-close'),

  citingDialog: $('citing-dialog'),
  citingTitle: $('citing-title'),
  citingStatus: $('citing-status'),
  citingTable: $('citing-table').querySelector('tbody'),
  citingPdf: $('citing-pdf'),
  citingText: $('citing-text'),
  citingScholar: $('citing-scholar'),
  citingClose: $('citing-close'),
};

const state = {
  results: [],
  selectedIdx: null,
  selectedSource: null,
  client: null,
  sortState: new Map(),
  citingItem: null,
  citingResults: [],
  citingSelectedIdx: null,
};

function getClient() {
  const token = (localStorage.getItem(TOKEN_KEY) || '').trim();
  if (!token) {
    openSettings(true);
    return null;
  }
  if (!state.client || state.client.token !== token) {
    state.client = new CourtListenerClient(token);
  }
  return state.client;
}

function openSettings(showWarning = false) {
  ui.tokenInput.value = localStorage.getItem(TOKEN_KEY) || '';
  ui.tokenInput.type = 'password';
  ui.tokenShow.checked = false;
  ui.settingsDialog.showModal();
  if (showWarning) {
    ui.actionStatus.textContent = 'Enter your CourtListener API token to continue.';
  }
}

ui.settingsBtn.addEventListener('click', () => openSettings());
ui.tokenShow.addEventListener('change', () => {
  ui.tokenInput.type = ui.tokenShow.checked ? 'text' : 'password';
});
ui.tokenSave.addEventListener('click', () => {
  const v = ui.tokenInput.value.trim();
  if (v) localStorage.setItem(TOKEN_KEY, v);
  ui.settingsDialog.close();
});
ui.tokenCancel.addEventListener('click', () => ui.settingsDialog.close());

function formatRow(item) {
  const caseName = stripHtmlTags(item.caseName || item.case_name || '(unknown)');
  const court = item.court || item.court_id || '';
  const date = item.dateFiled || item.date_filed || '';
  const cite = pickCitation(item.citation || []);
  const status = item.status || item.precedentialStatus || '';
  return { case_name: caseName, court, date_filed: date, citation: cite, status };
}

function makeRow(idx, item, source) {
  const tr = document.createElement('tr');
  tr.dataset.idx = idx;
  tr.dataset.source = source;
  const r = formatRow(item);
  tr.innerHTML = `
    <td class="case-name"></td>
    <td class="court"></td>
    <td class="date"></td>
    <td class="cite"></td>
    <td></td>`;
  tr.children[0].textContent = r.case_name;
  tr.children[1].textContent = r.court;
  tr.children[2].textContent = r.date_filed;
  tr.children[3].textContent = r.citation;
  tr.children[4].textContent = r.status;
  tr.addEventListener('click', () => selectRow(idx, source));
  tr.addEventListener('dblclick', () => { selectRow(idx, source); onDownloadPdf(); });
  tr.addEventListener('contextmenu', (e) => {
    e.preventDefault();
    selectRow(idx, source);
    onCitingOpinions();
  });
  return tr;
}

function selectRow(idx, source) {
  state.selectedIdx = idx;
  state.selectedSource = source;
  for (const t of [ui.results, ui.orders]) {
    for (const tr of t.children) tr.classList.remove('selected');
  }
  const target = source === 'orders' ? ui.orders : ui.results;
  for (const tr of target.children) {
    if (parseInt(tr.dataset.idx, 10) === idx) tr.classList.add('selected');
  }
  const item = state.results[idx];
  showPreview(item);
  ui.pdfBtn.disabled = false;
  ui.textBtn.disabled = false;
  ui.citeBtn.disabled = false;
  ui.scholarBtn.disabled = false;
}

function showPreview(item) {
  const opinions = item.opinions || [];
  let mainOp = null;
  let maxCites = -1;
  for (const op of opinions) {
    const n = (op.cites || []).length;
    if (n > maxCites) { maxCites = n; mainOp = op; }
  }
  const snippet = mainOp ? stripHtmlTags(mainOp.snippet || '') : '';
  ui.preview.textContent = snippet || '(No preview available — view text or download PDF for full opinion.)';
}

function clearResults() {
  ui.results.innerHTML = '';
  ui.orders.innerHTML = '';
  state.results = [];
  state.selectedIdx = null;
  ui.preview.textContent = 'Select a result to preview.';
  ui.pdfBtn.disabled = true;
  ui.textBtn.disabled = true;
  ui.citeBtn.disabled = true;
  ui.scholarBtn.disabled = true;
}

async function onSearch() {
  const client = getClient();
  if (!client) return;
  const q = ui.query.value.trim();
  if (!q) { ui.status.textContent = 'Please enter a search query.'; return; }

  clearResults();
  ui.searchBtn.disabled = true;
  ui.status.innerHTML = '<span class="spinner"></span>Searching…';

  try {
    const data = await client.search(q, {
      court: ui.court.value || null,
      dateFrom: ui.dateFrom.value || null,
      dateTo: ui.dateTo.value || null,
      pageSize: parseInt(ui.pageSize.value, 10) || 20,
      highlight: true,
    });
    onResults(data);
  } catch (e) {
    ui.status.textContent = `Error: ${e.message}`;
    if (e.status === 401 || e.status === 403) openSettings(true);
  } finally {
    ui.searchBtn.disabled = false;
  }
}

function onResults(data) {
  const results = (data.results || []).map((item) => {
    if (Array.isArray(item.citation)) {
      item.citation = item.citation.map(stripHtmlTags);
    } else if (item.citation) {
      item.citation = stripHtmlTags(item.citation);
    }
    return item;
  });
  state.results = results;

  for (let i = 0; i < results.length; i++) {
    const item = results[i];
    const opinions = item.opinions || [];
    let mainOp = null;
    let maxCites = -1;
    for (const op of opinions) {
      const n = (op.cites || []).length;
      if (n > maxCites) { maxCites = n; mainOp = op; }
    }
    const courtVal = String(item.court_id || '');
    const cites = mainOp ? (mainOp.cites || []).length : null;
    const isOrders = courtVal.includes('scotus') && cites !== null && cites <= 2;
    const target = isOrders ? ui.orders : ui.results;
    target.appendChild(makeRow(i, item, isOrders ? 'orders' : 'main'));
  }

  const count = data.count || results.length;
  ui.status.textContent = results.length
    ? `Showing ${results.length} of ${count.toLocaleString()} results.`
    : 'No results found.';
}

function setupSorting(table, source) {
  const ths = table.querySelectorAll('th[data-sort]');
  ths.forEach((th) => {
    th.addEventListener('click', () => {
      const col = th.dataset.sort;
      const tableEl = th.closest('table');
      const tbody = tableEl.querySelector('tbody');
      const prev = state.sortState.get(source);
      const reverse = prev && prev.col === col ? !prev.reverse : false;
      state.sortState.set(source, { col, reverse });

      const colIdx = ['case_name', 'court', 'date_filed', 'citation', 'status'].indexOf(col);
      const rows = Array.from(tbody.children);
      rows.sort((a, b) => {
        const av = a.children[colIdx].textContent.toLowerCase();
        const bv = b.children[colIdx].textContent.toLowerCase();
        if (av < bv) return reverse ? 1 : -1;
        if (av > bv) return reverse ? -1 : 1;
        return 0;
      });
      tbody.replaceChildren(...rows);

      ths.forEach((other) => other.classList.remove('sort-asc', 'sort-desc'));
      th.classList.add(reverse ? 'sort-desc' : 'sort-asc');
    });
  });
}

setupSorting(document.getElementById('results-table'), 'main');
setupSorting(document.getElementById('orders-table'), 'orders');

function buildPdfCandidates(item) {
  const out = [];
  const courtVal = String(item.court_id || '');
  const isScotus = courtVal.includes('scotus');

  const cites = Array.isArray(item.citation) ? item.citation
    : item.citation ? [String(item.citation)] : [];
  const usCite = cites.find((c) => / U\.S\. /.test(c));

  if (usCite) {
    const loc = usReportsLocUrl(usCite);
    if (loc) out.push({ source: 'LOC US Reports (official)', url: loc });
    const gov = usReportsGovInfoUrls(usCite);
    if (gov) {
      out.push({ source: 'GovInfo link', url: gov.link });
      out.push({ source: 'GovInfo direct PDF', url: gov.direct });
    }
  }

  if (!isScotus) {
    for (const c of cites) {
      if (NOISE_CITE_RE.test(c)) continue;
      const u = staticCaseLawUrl(c);
      if (u) out.push({ source: `static.case.law (${c})`, url: u });
    }
  }

  const local = item.local_path || item.localPath;
  if (local) out.push({ source: 'CourtListener storage', url: STORAGE_BASE + String(local).replace(/^\//, '') });

  if (item.download_url) out.push({ source: 'Original court source', url: item.download_url });

  const seen = new Set();
  return out.filter((c) => {
    if (seen.has(c.url)) return false;
    seen.add(c.url);
    return true;
  });
}

async function gatherPdfCandidatesAsync(client, item) {
  const candidates = buildPdfCandidates(item);

  const opinionId = item.id;
  if (opinionId) {
    try {
      const op = await client.getOpinion(opinionId, 'local_path,download_url');
      if (op.local_path) {
        const u = STORAGE_BASE + String(op.local_path).replace(/^\//, '');
        if (!candidates.find((c) => c.url === u)) {
          candidates.push({ source: 'CourtListener storage (opinion record)', url: u });
        }
      }
      if (op.download_url && !candidates.find((c) => c.url === op.download_url)) {
        candidates.push({ source: 'Original court source (opinion record)', url: op.download_url });
      }
    } catch { /* ignore */ }
  }

  const clusterId = item.cluster_id || item.id;
  if (clusterId) {
    try {
      const cluster = await client.getCluster(parseInt(clusterId, 10), 'sub_opinions,citations');
      if (!candidates.some((c) => c.url.includes('static.case.law'))) {
        const altCites = clusterCitationsToStrings(cluster.citations || []);
        for (const c of altCites) {
          if (NOISE_CITE_RE.test(c)) continue;
          const u = staticCaseLawUrl(c);
          if (u && !candidates.find((x) => x.url === u)) {
            candidates.push({ source: `static.case.law (${c})`, url: u });
          }
        }
      }
      for (const opUrl of cluster.sub_opinions || []) {
        try {
          const op = await client.getUrl(`${opUrl}?fields=local_path,download_url`);
          if (op.local_path) {
            const u = STORAGE_BASE + String(op.local_path).replace(/^\//, '');
            if (!candidates.find((c) => c.url === u)) {
              candidates.push({ source: 'CourtListener storage (sub-opinion)', url: u });
            }
          }
          if (op.download_url && !candidates.find((c) => c.url === op.download_url)) {
            candidates.push({ source: 'Original court source (sub-opinion)', url: op.download_url });
          }
        } catch { /* ignore */ }
      }
    } catch { /* ignore */ }
  }

  return candidates;
}

async function onDownloadPdf() {
  const item = currentItem();
  if (!item) return;
  const client = getClient();
  if (!client) return;

  ui.actionStatus.innerHTML = '<span class="spinner"></span>Resolving PDF sources…';
  let candidates;
  try {
    candidates = await gatherPdfCandidatesAsync(client, item);
  } catch (e) {
    ui.actionStatus.textContent = `Error: ${e.message}`;
    return;
  }

  showPdfDialog(candidates, item);
  ui.actionStatus.textContent = candidates.length
    ? `${candidates.length} PDF source(s) found.`
    : 'No PDF source found — try View Text instead.';
}

function showPdfDialog(candidates, item) {
  ui.pdfCandidates.innerHTML = '';
  const filename = `${buildDefaultFilename(item)}.pdf`;
  for (const c of candidates) {
    const li = document.createElement('li');
    const src = document.createElement('div');
    src.className = 'source';
    src.textContent = c.source;
    const a = document.createElement('a');
    a.href = c.url;
    a.target = '_blank';
    a.rel = 'noopener';
    a.textContent = 'Open PDF in new tab';
    a.download = filename;
    const url = document.createElement('div');
    url.className = 'url';
    url.textContent = c.url;
    li.append(src, a, url);
    ui.pdfCandidates.appendChild(li);
  }
  ui.pdfFallbackNote.hidden = candidates.length > 0;
  ui.pdfDialog.showModal();
}

ui.pdfClose.addEventListener('click', () => ui.pdfDialog.close());

async function assembleCaseText(client, item) {
  const clusterId = item.cluster_id || item.id;
  if (!clusterId) return '';

  const cluster = await client.getCluster(
    parseInt(clusterId, 10),
    'case_name,citations,judges,attorneys,syllabus,headnotes,sub_opinions');

  const lines = [];
  const caseName = stripHtmlTags(cluster.case_name || item.caseName || item.case_name || '');
  lines.push(caseName);

  const citeStrs = clusterCitationsToStrings(cluster.citations || []);
  if (citeStrs.length) lines.push(citeStrs.join(', '));
  lines.push('');

  for (const [field, label] of [
    ['judges', 'Judges'], ['attorneys', 'Attorneys'],
    ['syllabus', 'Syllabus'], ['headnotes', 'Headnotes'],
  ]) {
    const v = stripHtmlBlock(cluster[field] || '');
    if (v) {
      lines.push(`${label}: ${v}`);
      lines.push('');
    }
  }

  const subUrls = cluster.sub_opinions || [];
  const opinions = [];
  for (const url of subUrls) {
    try {
      const op = await client.getUrl(`${url}?fields=ordering_key,type,html_with_citations,html,plain_text`);
      opinions.push(op);
    } catch { /* skip failed sub-opinions */ }
  }
  opinions.sort((a, b) => {
    const ak = a.ordering_key, bk = b.ordering_key;
    if (ak == null && bk == null) return 0;
    if (ak == null) return 1;
    if (bk == null) return -1;
    return ak - bk;
  });

  for (const op of opinions) {
    const label = OPINION_TYPE_LABELS[op.type] || op.type || 'Opinion';
    lines.push(`--- ${label} ---`);
    lines.push('');
    const txt = op.html_with_citations || op.html || op.plain_text || '';
    if (txt) lines.push(stripHtmlBlock(txt));
    lines.push('');
  }

  return lines.join('\n');
}

async function onViewText() {
  const item = currentItem();
  if (!item) return;
  const client = getClient();
  if (!client) return;

  ui.actionStatus.innerHTML = '<span class="spinner"></span>Assembling opinion text…';
  try {
    const text = await assembleCaseText(client, item);
    if (!text.trim()) {
      ui.actionStatus.textContent = 'No text available for this opinion.';
      return;
    }
    showTextDialog(item, text);
    ui.actionStatus.textContent = `Loaded ${text.length.toLocaleString()} characters.`;
  } catch (e) {
    ui.actionStatus.textContent = `Error: ${e.message}`;
  }
}

function showTextDialog(item, text) {
  const caseName = stripHtmlTags(item.caseName || item.case_name || 'Opinion');
  ui.textTitle.textContent = caseName;
  ui.textMeta.textContent = `${pickCitation(item.citation || [])}  ·  ${text.length.toLocaleString()} characters`;
  ui.textBody.textContent = text;
  ui.textSave.onclick = () => {
    const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `${buildDefaultFilename(item)}.txt`;
    a.click();
    URL.revokeObjectURL(a.href);
  };
  ui.textDialog.showModal();
}

ui.textClose.addEventListener('click', () => ui.textDialog.close());

function buildScholarUrl(item) {
  const cite = pickCitation(item.citation || []);
  const caseName = stripHtmlTags(item.caseName || item.case_name || '');
  const date = String(item.dateFiled || item.date_filed || '');
  const year = date.length >= 4 ? date.slice(0, 4) : '';
  const q = cite ? `"${cite}"` : (year ? `${caseName} ${year}` : caseName);
  return `https://scholar.google.com/scholar?q=${encodeURIComponent(q)}&as_sdt=4`;
}

function onOpenScholar() {
  const item = currentItem();
  if (!item) return;
  window.open(buildScholarUrl(item), '_blank', 'noopener');
  ui.actionStatus.textContent = 'Opened Google Scholar in a new tab.';
}

function currentItem() {
  if (state.selectedIdx == null) return null;
  return state.results[state.selectedIdx] || null;
}

ui.pdfBtn.addEventListener('click', onDownloadPdf);
ui.textBtn.addEventListener('click', onViewText);
ui.scholarBtn.addEventListener('click', onOpenScholar);
ui.citeBtn.addEventListener('click', onCitingOpinions);

ui.searchBtn.addEventListener('click', onSearch);
ui.query.addEventListener('keydown', (e) => { if (e.key === 'Enter') onSearch(); });

async function pMapWithLimit(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  const workers = Array(Math.min(limit, items.length)).fill(0).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      try { out[idx] = await fn(items[idx], idx); }
      catch { out[idx] = null; }
    }
  });
  await Promise.all(workers);
  return out;
}

async function onCitingOpinions() {
  const item = currentItem();
  if (!item) return;
  const client = getClient();
  if (!client) return;

  state.citingItem = item;
  state.citingResults = [];
  state.citingSelectedIdx = null;
  ui.citingTable.innerHTML = '';
  ui.citingPdf.disabled = true;
  ui.citingText.disabled = true;
  ui.citingScholar.disabled = true;

  const caseName = stripHtmlTags(item.caseName || item.case_name || '?');
  ui.citingTitle.textContent = `Citing: ${caseName}`;
  ui.citingStatus.innerHTML = '<span class="spinner"></span>Resolving opinion ID…';
  ui.citingDialog.showModal();

  const clusterId = item.cluster_id || item.id;
  if (!clusterId) {
    ui.citingStatus.textContent = 'Could not resolve cluster ID.';
    return;
  }

  try {
    const cluster = await client.getCluster(parseInt(clusterId, 10), 'sub_opinions');
    const ids = (cluster.sub_opinions || []).map(extractOpinionId).filter((x) => x != null);
    const opId = ids[0];
    if (opId == null) {
      await fallbackCitingSearch(client, clusterId);
      return;
    }

    ui.citingStatus.innerHTML = '<span class="spinner"></span>Fetching citing opinions…';
    const allEntries = [];
    let nextUrl = null;
    while (true) {
      const data = nextUrl ? await client.getUrl(nextUrl) : await client.listCitingOpinions(opId);
      allEntries.push(...(data.results || []));
      nextUrl = data.next;
      ui.citingStatus.innerHTML = `<span class="spinner"></span>Fetched ${allEntries.length} citing opinions…`;
      if (!nextUrl) break;
    }
    allEntries.sort((a, b) => (b.depth || 0) - (a.depth || 0));

    if (!allEntries.length) {
      ui.citingStatus.textContent = 'No opinions cite this case.';
      return;
    }

    const total = allEntries.length;
    const FIRST = 20;
    const firstBatch = allEntries.slice(0, FIRST);
    const rest = allEntries.slice(FIRST);

    ui.citingStatus.innerHTML = `<span class="spinner"></span>Resolving details for top ${firstBatch.length}…`;
    const resolved = (await pMapWithLimit(firstBatch, 8, (e) => fetchCitingCase(client, e))).filter(Boolean);
    appendCitingResults(resolved);

    if (rest.length) {
      ui.citingStatus.textContent = `Showing ${state.citingResults.length} of ${total.toLocaleString()} citing opinions · Loading more…`;
      for (let i = 0; i < rest.length; i += FIRST) {
        const chunk = rest.slice(i, i + FIRST);
        const batch = (await pMapWithLimit(chunk, 8, (e) => fetchCitingCase(client, e))).filter(Boolean);
        appendCitingResults(batch);
        const final = (i + FIRST) >= rest.length;
        ui.citingStatus.textContent = final
          ? `${state.citingResults.length} of ${total.toLocaleString()} citing opinions`
          : `Showing ${state.citingResults.length} of ${total.toLocaleString()} citing opinions · Loading more…`;
      }
    } else {
      ui.citingStatus.textContent = `${state.citingResults.length} of ${total.toLocaleString()} citing opinions`;
    }
  } catch (e) {
    ui.citingStatus.textContent = `Error: ${e.message}`;
  }
}

async function fetchCitingCase(client, entry) {
  const opUrl = String(entry.citing_opinion || '');
  const opId = extractOpinionId(opUrl);
  if (opId == null) return null;
  try {
    const opinion = await client.getOpinion(opId, 'cluster');
    const cid = extractClusterId(String(opinion.cluster || ''));
    if (cid == null) return null;
    const cluster = await client.getCluster(cid, 'case_name,citations,date_filed,docket');
    const cites = clusterCitationsToStrings(cluster.citations || []);
    let courtId = '';
    if (cluster.docket) {
      try {
        const docket = await client.getUrl(`${cluster.docket}?fields=court`);
        courtId = extractCourtId(String(docket.court || ''));
      } catch { /* ignore */ }
    }
    return {
      caseName: cluster.case_name || '',
      case_name: cluster.case_name || '',
      citation: cites,
      dateFiled: cluster.date_filed || '',
      date_filed: cluster.date_filed || '',
      cluster_id: cid,
      court: courtId, court_id: courtId,
      _depth: entry.depth || 0,
    };
  } catch { return null; }
}

async function fallbackCitingSearch(client, clusterId) {
  ui.citingStatus.innerHTML = '<span class="spinner"></span>Fetching (search fallback)…';
  try {
    const data = await client.search(`cites:(${clusterId})`, { type: 'o', pageSize: 20 });
    const results = (data.results || []).map((it) => {
      if (Array.isArray(it.citation)) it.citation = it.citation.map(stripHtmlTags);
      else if (it.citation) it.citation = stripHtmlTags(it.citation);
      it._depth = '–';
      return it;
    });
    appendCitingResults(results);
    ui.citingStatus.textContent = `${results.length} of ${(data.count || results.length).toLocaleString()} results (depth unavailable)`;
  } catch (e) {
    ui.citingStatus.textContent = `Error: ${e.message}`;
  }
}

function appendCitingResults(batch) {
  const offset = state.citingResults.length;
  state.citingResults.push(...batch);
  for (let i = 0; i < batch.length; i++) {
    const item = batch[i];
    const idx = offset + i;
    const r = formatRow(item);
    const tr = document.createElement('tr');
    tr.dataset.idx = idx;
    tr.innerHTML = `
      <td class="case-name"></td>
      <td class="court"></td>
      <td class="date"></td>
      <td class="cite"></td>
      <td class="depth"></td>`;
    tr.children[0].textContent = r.case_name;
    tr.children[1].textContent = r.court;
    tr.children[2].textContent = r.date_filed;
    tr.children[3].textContent = r.citation;
    tr.children[4].textContent = String(item._depth ?? '');
    tr.addEventListener('click', () => selectCitingRow(idx));
    tr.addEventListener('dblclick', () => { selectCitingRow(idx); onCitingPdf(); });
    ui.citingTable.appendChild(tr);
  }
}

function selectCitingRow(idx) {
  state.citingSelectedIdx = idx;
  for (const tr of ui.citingTable.children) tr.classList.remove('selected');
  for (const tr of ui.citingTable.children) {
    if (parseInt(tr.dataset.idx, 10) === idx) tr.classList.add('selected');
  }
  ui.citingPdf.disabled = false;
  ui.citingText.disabled = false;
  ui.citingScholar.disabled = false;
}

function currentCitingItem() {
  if (state.citingSelectedIdx == null) return null;
  return state.citingResults[state.citingSelectedIdx] || null;
}

async function onCitingPdf() {
  const item = currentCitingItem();
  if (!item) return;
  const client = getClient();
  if (!client) return;
  ui.citingStatus.innerHTML = '<span class="spinner"></span>Resolving PDF sources…';
  try {
    const candidates = await gatherPdfCandidatesAsync(client, item);
    showPdfDialog(candidates, item);
    ui.citingStatus.textContent = candidates.length
      ? `${candidates.length} PDF source(s) found.`
      : 'No PDF source found.';
  } catch (e) {
    ui.citingStatus.textContent = `Error: ${e.message}`;
  }
}

async function onCitingText() {
  const item = currentCitingItem();
  if (!item) return;
  const client = getClient();
  if (!client) return;
  ui.citingStatus.innerHTML = '<span class="spinner"></span>Assembling text…';
  try {
    const text = await assembleCaseText(client, item);
    if (!text.trim()) { ui.citingStatus.textContent = 'No text available.'; return; }
    showTextDialog(item, text);
    ui.citingStatus.textContent = `Loaded ${text.length.toLocaleString()} characters.`;
  } catch (e) {
    ui.citingStatus.textContent = `Error: ${e.message}`;
  }
}

function onCitingScholar() {
  const item = currentCitingItem();
  if (!item) return;
  window.open(buildScholarUrl(item), '_blank', 'noopener');
}

ui.citingPdf.addEventListener('click', onCitingPdf);
ui.citingText.addEventListener('click', onCitingText);
ui.citingScholar.addEventListener('click', onCitingScholar);
ui.citingClose.addEventListener('click', () => ui.citingDialog.close());

if (!localStorage.getItem(TOKEN_KEY)) {
  ui.actionStatus.textContent = 'Click Settings to enter your CourtListener API token.';
}
