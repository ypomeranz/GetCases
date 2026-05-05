# Web App — `index.html` / `app.js` / `styles.css`

A pure client-side port of the Tkinter / Shiny GUIs. No build step, no server.

## Run

Just open `index.html` in any modern browser, or serve the directory:

```sh
python3 -m http.server 8000      # then visit http://localhost:8000
```

Click **Settings** to paste your CourtListener API token (stored in
`localStorage`; the app never sends it anywhere except the CourtListener API).

## What works the same as the Python GUI

- **Search** with court / date / page-size filters
- **Sortable results** in the main table, with the SCOTUS-orders side-table
  populated when the lead opinion has ≤ 2 outbound citations
- **Snippet preview** on the right panel
- **Citing Opinions** modal — same depth-sorted strategy: resolve the cited
  opinion ID, fetch all pages from `/opinions-cited/`, then resolve each
  citing case via `/opinions/` → `/clusters/` (parallel, max 8 in flight).
- **Assembled case text** ("View Text") — pulls the cluster + every
  sub-opinion and stitches them together exactly like `_assemble_case_text()`
- **Save text as `.txt`** with the same Bluebook filename format as the
  Python `_build_default_filename()`
- **Right-click a result** to open Citing Opinions
- **Double-click a result** to start the PDF flow

## What works *differently* (browser limitations)

### PDF download — no HEAD validation
The Python code probes `LOC`, `GovInfo`, and `static.case.law` with `HEAD`
requests to pick the working URL. Browsers cannot read the status of
cross-origin responses, so I can't replicate that. Instead, **Download PDF**
now opens a dialog showing every candidate source in priority order:

1. LOC US Reports (vols 1-542)
2. GovInfo link service / direct PDF (vols 1-582)
3. `static.case.law` (Harvard CAP) for non-SCOTUS reporters
4. CourtListener storage (`local_path` from search result, opinion record,
   and every sub-opinion)
5. Original court source (`download_url`)

Each is a link that opens in a new tab — the user clicks the first one that
loads. Cross-origin PDFs ignore the `download="..."` filename hint, so the
user saves via the browser's PDF viewer.

### Google Scholar — open in new tab only
Direct scraping (`google_scholar.py`) is impossible from a browser:
Scholar sends no `Access-Control-Allow-Origin` header, so `fetch()` calls
are blocked. Google also actively blocks JS-driven requests.

The **Open in Scholar** button instead navigates to the Scholar search URL
in a new tab so the user can read the opinion themselves.

If you want the original "scrape and cache" behaviour back, you would need a
small backend proxy (e.g. a Python Flask endpoint that forwards to
`scholar.google.com`).

### Cache
The Python version persists the Scholar text cache in SQLite. The web
version drops that cache entirely, since the Scholar fetch path no longer
runs in-browser.

### CORS — CourtListener itself
The CourtListener API sends `Access-Control-Allow-Origin: *`, so direct
calls from the browser work. If your network blocks them, host the page
behind a CORS-friendly proxy.
