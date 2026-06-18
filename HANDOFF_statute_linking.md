# Handoff — linking statutes, regulations & federal rules from opinions

**Audience:** a fresh Claude Code session picking up this work in a new
container. Read this top to bottom before touching code.

**One-line goal (from the user, a practicing lawyer):** when GetCases shows a
court opinion, detect citations to **state statutes, state regulations, and the
federal rules** (Civ. P., Crim. P., Evid., App. P., etc.) — tolerant of
Bluebook spacing/period/capitalization quirks — turn them into clickable links,
and show the cited provision *inside* the app's statute viewer (the same window
that already serves U.S. Code and C.F.R.). Source of choice: **Cornell LII**
(`www.law.cornell.edu`).

---

## 1. Decisions the user already made (do not re-litigate)

From an `AskUserQuestion` round:

- **State statutes:** "Full in-app for select states." → Build full in-app
  statute parsing for a chosen set of priority states; for the rest, detect the
  citation and open the source in the browser (link-out). The user has **not
  yet named the select states** — ask which jurisdictions they practice in
  before building per-state in-app parsers. (Detection of all states can proceed
  without that list.)
- **Jurisdiction coverage for detection:** Federal rules (all sets) **+ all 50
  states + specific states + DC/territories** — i.e. detect broadly.
- **Unblocking network:** "Add hosts to egress allowlist." (See §3 — still
  pending as of this writing.)

---

## 2. Status

| Piece | State |
|---|---|
| Federal rules — detection | ✅ Done, tested offline (`fed_rules.py`) |
| Federal rules — GUI wiring (links in opinions + viewer + Quick Look Up) | ✅ Done |
| Federal rules — Cornell HTML→paragraph parser | ⚠️ Written to the LII template, **NOT verified against a live page** (egress blocked here) |
| State regulations (Cornell, all 50, in-app) | ⬜ Not started — needs live recon of URL/HTML |
| State statutes — all-50 detection table | ⬜ Not started (HTML-independent; can build now) |
| State statutes — in-app for select states | ⬜ Not started — needs the user's state list + sources |

Commits live on branch **`claude/friendly-goodall-0faz93`** (confirm your task's
branch instructions still point here). The federal-rules work is one commit:
"Add federal rules (Cornell LII) as a citation source."

---

## 3. THE BLOCKER: network egress allowlist

This container's outbound network is governed by an **egress allowlist proxy**.
Requests to the legal sources return HTTP 403 with body:

```
Host not in allowlist: www.law.cornell.edu. Add this host to your network egress settings to allow access.
```

This blocks **`www.law.cornell.edu`**, and also the app's *existing* sources
**`uscode.house.gov`** and **`www.ecfr.gov`**. Only `pypi.org` was reachable.

**First thing to do in the new session — re-test egress:**

```python
import urllib.request
H={"User-Agent":"Mozilla/5.0 ... Chrome/124.0 Safari/537.36","Accept":"text/html"}
for url in ["https://www.law.cornell.edu/rules/fre/rule_404",
            "https://www.law.cornell.edu/regulations",
            "https://uscode.house.gov/", "https://www.ecfr.gov/api/versioner/v1/titles.json"]:
    try:
        r=urllib.request.urlopen(urllib.request.Request(url,headers=H),timeout=25)
        print("OK", r.status, url, len(r.read()))
    except Exception as e: print("ERR", getattr(e,'code',type(e).__name__), url)
```

**How the user fixes it** (NOT in their browser — that was a misunderstanding):
the environment's **network egress settings** in *Claude Code on the web*. Add
`www.law.cornell.edu`, `uscode.house.gov`, `www.ecfr.gov`. May require starting a
fresh session. Docs: https://code.claude.com/docs/en/claude-code-on-the-web

**Ways to inspect Cornell's real HTML (you need this to finish the parsers):**
1. Egress allowlist (above) → fetch directly with a browser User-Agent (this is
   exactly what `us_code.py`/`ecfr.py` do via `_BROWSER_HEADERS`).
2. **Fallback:** ask the user to paste the saved HTML of one rule page and one
   state-reg page. The `WebFetch` tool reaches the internet but Cornell **403s
   its User-Agent**, and `web.archive.org` is blocked by the tool — so WebFetch
   is not a path to Cornell's HTML.

**Important:** the user's actual GetCases desktop app runs on *their* machine,
where Cornell is reachable. The egress block only affects *this dev sandbox*.
So the user can test any shipped feature live by pulling the branch.

---

## 4. Architecture you must follow

Each legal source is a **module implementing one contract**, and the GUI is
source-agnostic via a registry. Copy this pattern for every new source.

### 4a. Source-module contract (see `us_code.py`, `ecfr.py`, `fed_rules.py`)

Module-level:
- `XXX_CITE_RE` — `re.Pattern` that finds citations in running opinion text.
- `cite_spec(m) -> str` — compact `"a:b:subs"` spec from a match
  (`"title:section:sub,sub"`; for rules `"set:rule:sub,sub"`).
- `spec_label(spec) -> str` — human label, e.g. `"42 U.S.C. § 1983(b)"`.
- `load_section(a, b) -> Doc` — fetch + parse; raise `RuntimeError(msg)` on
  failure (the GUI shows `str(exc)` in the status line).

`Doc` object (a `@dataclass`) must expose:
- `.paras: list[tuple[str, int, str]]` — `(kind, indent, text)` stream. `kind` ∈
  `{"sechead","head","body","credit","note-head","note-body"}`; `indent` 0–6.
- `.kind: str` — the registry key (`"usc"`, `"cfr"`, `"rule"`, …).
- `.label`, `.source_name`, `.source_note`, `.url` — strings for the title bar,
  "Source:" box, status line, and "Open in Browser".
- `.title`, `.section` — the two identifiers; `neighbors()` returns these.
  (Used by the U.S.C. cross-ref resolver via `self._doc.title`.)
- `.bluebook_cite(subs=()) -> str` — citation appended on Copy/Export.
- `.neighbors() -> (prev|None, next|None)` — each is a `(title, section)` tuple
  for prev/next buttons; return `(None, None)` on any failure.

Indentation: reuse the shared engine `from us_code import infer_enum_level`
with a per-source hierarchy tuple (U.S.C. & rules: `("a","1","A","i","I")`;
C.F.R.: `("a","1","i","A")`).

Each module has an `if __name__ == "__main__":` block of **offline tests** (the
citation regex against real Bluebook strings + the parser against a synthetic
HTML/XML sample). Run `python3 <module>.py` — exit 0 = pass. Mirror this.

### 4b. GUI integration points (in `courtlistener_gui.py`)

Grep these symbols (line numbers drift):

- **Imports** — `import fed_rules` next to `import ecfr` / `import us_code`.
- **Registry** — `_STATUTE_SOURCES` (kind→module) and `_SOURCE_HOST`
  (kind→host string), defined just above `_fetch_statute_window`. **Add new
  sources here.** This replaced the old `us_code if kind=="usc" else ecfr`
  ternaries.
- **Opinion-text detection** — `_insert_plain_with_links`: runs each
  `XXX_CITE_RE.finditer`, tags matches with kind, then builds the link
  `action = (kind, cite_spec(m))`. Overlapping matches: first/longest wins.
- **In-viewer cross-refs** — `_StatuteWindow._insert_refs`: same idea for text
  shown *inside* the viewer, plus source-specific cross-reference regexes
  (`_USC_XREF_RE` for "section X of title Y"; `_CFR_SECREF_RE` for bare
  "§ a.b"). Guarded by `self._doc.kind`.
- **Link dispatch** — `_StatuteWindow._follow_link` (opinion reader): `if kind
  in _STATUTE_SOURCES: self._open_statute(kind, value)`. For a **link-out**
  (browser) action you'll add a new kind handled with `webbrowser.open(url)`
  (see the existing `webbrowser` import and the "Open in Browser" button).
- **Fetch+window** — `_fetch_statute_window(parent, kind, spec, status)`: looks
  up `mod = _STATUTE_SOURCES[kind]`, splits `spec` into `title:section:subs`,
  threads `mod.load_section(title, section)`, opens `_StatuteWindow`.
- **Prev/next** — `_StatuteWindow._go_neighbor`: `mod =
  _STATUTE_SOURCES[self._doc.kind]`.
- **Hand-typed lookup** — `_parse_statute_query` (used by Quick Look Up dialog
  `_show_statute_lookup` and Spotlight): returns `(kind, spec)`. Federal-rule
  queries are tried first via `fed_rules.parse_query`.

### 4c. kind / spec scheme

- `usc`  → spec `"42:1983:b,1"`        (title:section:subs)
- `cfr`  → spec `"29:1614.105:a,1"`    (title:section:subs)
- `rule` → spec `"fre:404:b,1"`        (set:rule:subs); set ∈ frcp/frcrmp/fre/frap/frbp
- **(planned)** `statereg` → spec like `"<state>:<cite>"`, in-app from Cornell
- **(planned)** `statestat` → in-app for select states
- **(planned)** `browse` → action `("browse", url)`; `_follow_link` opens it in
  the browser (link-out for non-select state statutes)

---

## 5. What "federal rules" delivered (reference implementation)

`fed_rules.py` — read it; it's the template for everything below.

- `RULESETS`: key → (Bluebook abbr, full name, Cornell path). Keys = Cornell
  path segments: `frcp`, `frcrmp`, `fre`, `frap`, `frbp`.
- `RULE_CITE_RE`: matches three shapes, all case/space/period-insensitive:
  abbreviated (`Fed. R. Evid. 404(b)`, `Fed.R.Civ.P. 56`), initialism
  (`FRCP 56`, `F.R.E. 404`, `FRCrP 41`), spelled-out (`Rule 56 of the Federal
  Rules of Civil Procedure`, `Federal Rule of Evidence 403`). Deliberately does
  **not** match a bare `Rule 56` (ambiguous w/ local rules) and won't swallow a
  year `(2020)` as a subdivision.
- `cite_spec` / `spec_label` / `parse_query` / `rule_url` / `RuleDoc` /
  `load_section` / `parse_rule_html` (the unverified part) / `_set_order`
  (TOC scrape for prev-next).
- Cornell rule URL (CONFIRMED via search): `https://www.law.cornell.edu/rules/{set}/rule_{N}`
  e.g. `/rules/fre/rule_404`, `/rules/frcp/rule_56`, `/rules/frap/rule_32.1`.

**To finish federal rules once egress is open:**
1. `python3 -c "import fed_rules; d=fed_rules.load_section('fre','404'); [print(p) for p in d.paras]"`
2. Compare the paragraph stream to the live page. Fix `parse_rule_html` /
   `_content_region` / `_NOTE_HEAD_RE` to match real LII markup (container id,
   how subdivisions and "Notes of Advisory Committee" are tagged).
3. Add a couple of real-HTML assertions to `fed_rules.py`'s test block.

---

## 6. Plan for state law

### 6a. State regulations — Cornell hosts all 50 (in-app) — `state_regs.py`
- Cornell publishes state regs for all 50 states under `/regulations`
  (confirmed via search; **per-state URL scheme NOT yet verified** — needs
  egress). Recon: fetch `https://www.law.cornell.edu/regulations`, pick one
  state (e.g. California Code of Regulations), and learn the deep-link pattern +
  HTML structure. Then implement the standard contract (§4a). Likely very close
  to `fed_rules.py`.

### 6b. State statutes — detection table (all 50 + DC + territories)
Cornell does **not** host uniform full-text state statutes (it mostly links out
to official sources). So: **detect everywhere; full in-app parse only for the
user's select states; link-out (browser) for the rest.**

Build a data-driven table (Bluebook **Table T1**) mapping each jurisdiction's
citation signature(s) → (normalized spec, link target / parser). The formats
vary a lot — design for it. Representative forms (tolerate `Ann.`, spacing,
periods, case):

```
N.Y. Penal Law § 125.25            Cal. Penal Code § 187            (subject-matter codes)
Tex. Penal Code Ann. § 19.02       Md. Code Ann., Crim. Law § 2-201 (subject + comma)
Fla. Stat. § 776.012               Va. Code Ann. § 18.2-32          (title-section)
Ohio Rev. Code Ann. § 2903.01      Ga. Code Ann. § 16-5-1           (O.C.G.A.)
720 ILCS 5/9-1                      (chapter / act / section)
Mass. Gen. Laws ch. 265, § 1       (a.k.a. M.G.L. c. 265, § 1)
42 Pa. Cons. Stat. § 9711          (a.k.a. 42 Pa.C.S. § 9711)
N.J. Stat. Ann. § 2C:11-3          La. Rev. Stat. § 14:30            (colon)
Wash. Rev. Code § 9A.32.030 (RCW)  Mich. Comp. Laws § 750.316 (MCL)
La. Civ. Code art. 2315            (article-based, not §)
```

Approach: per-jurisdiction compiled regex fragments (like each source has its
own `CITE_RE`), assembled so `_insert_plain_with_links` can scan them. Keep the
state abbreviation/name table aligned with `court_catalog.py`'s `STATE_COURTS`.

### 6c. Link-out target (design decision for non-select states)
Default to opening the **official state source** or the Cornell `/states/<state>`
landing page in the browser (`("browse", url)`). Deep per-statute links need
per-state URL schemes (egress-dependent research). Recommend: ship link-out to a
reliable target first, deepen later. Confirm the preference with the user.

---

## 7. Testing & conventions

- Per-module offline tests: `python3 fed_rules.py` (and `us_code.py`,
  `ecfr.py`) — exit 0 = pass. Add the same for new modules.
- GUI syntax: `python3 -m py_compile courtlistener_gui.py`.
- **`tkinter` is NOT installed in the sandbox**, so you can't import
  `courtlistener_gui` here; rely on `py_compile` + isolated logic tests. Example
  integration check (no tkinter): run all `*_CITE_RE` over a mixed sentence and
  print specs/labels — see the session transcript.
- Style: match the repo — `from __future__ import annotations`, type hints,
  module docstring explaining the source + page structure, lazy `import
  requests` inside functions, threaded fetch + `parent.after(...)` to touch Tk,
  in-memory cache with a `threading.Lock`.
- Git: develop on the assigned branch; commit with clear messages; push with
  `git push -u origin <branch>` (retry w/ backoff on network errors). **Do not
  open a PR unless the user asks.**

---

## 8. Suggested order for the new session

1. Re-test egress (§3). If still blocked, tell the user exactly what to add and
   offer the paste-HTML fallback; you can still do step 4 meanwhile.
2. If open: verify & fix `fed_rules.parse_rule_html` against the live FRE 404
   page; add real-HTML assertions.
3. Recon + build `state_regs.py` (Cornell, all 50) to the §4a contract; wire
   into `_STATUTE_SOURCES` as `statereg`.
4. Build the state-statute **detection** table (all 50 + DC + territories) —
   HTML-independent, do anytime. Wire detection into both scan points.
5. Ask the user for the **select states**; build in-app statute parsing for
   those; add `("browse", url)` link-out for the rest (new `_follow_link` kind).
6. Keep every step covered by offline tests; commit per coherent slice.

---

## 9. File map

- `courtlistener_gui.py` — the Tk app (large). Citation detection + statute
  viewer live here; integration points in §4b.
- `us_code.py` — U.S. Code from OLRC (uscode.house.gov). Owns `infer_enum_level`.
- `ecfr.py` — C.F.R. from eCFR (ecfr.gov).
- `fed_rules.py` — **NEW** Federal Rules from Cornell LII (this work).
- `court_catalog.py` — court IDs + Bluebook abbrs + `STATE_COURTS` (reuse the
  state list for the statute table).
- `bluebook_names.py`, `courtlistener.py`, `google_scholar.py` — case-name
  abbreviation, CL API client, Scholar fetcher (not central to this task).
