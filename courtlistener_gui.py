"""
CourtListener GUI
=================
A Tkinter interface for searching US case law via the CourtListener API
and downloading opinion PDFs.

Requires:
    pip install requests

Usage:
    python courtlistener_gui.py

Token lookup order:
  1. COURTLISTENER_TOKEN environment variable
  2. ~/.config/courtlistener/config.json  (saved automatically after first use)
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import tkinter as tk
import urllib.parse
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed

from pathlib import Path
from tkinter import filedialog, font as tkfont, messagebox, ttk
from typing import Optional


def _ensure_dependencies() -> None:
    """
    Check for the third-party packages this GUI needs and offer to
    pip-install any that are missing before the imports below run.

    ``requests`` is required; the rest enable features and declining just
    disables them: ``beautifulsoup4`` (Google Scholar / opinion parsing),
    ``pynput`` (global hotkey), and ``pypdfium2`` + ``Pillow`` (the in-app
    PDF viewer).
    """
    import importlib
    import importlib.util

    def missing_packages() -> list[str]:
        return [
            pip_name
            for module, pip_name in (
                ("requests", "requests"),
                ("bs4", "beautifulsoup4"),
                ("pynput", "pynput"),
                ("pypdfium2", "pypdfium2"),  # in-app PDF viewer
                ("PIL", "Pillow"),           # in-app PDF viewer (imports as PIL)
            )
            if importlib.util.find_spec(module) is None
        ]

    missing = missing_packages()
    if not missing:
        return
    root = tk.Tk()
    root.withdraw()
    try:
        if messagebox.askyesno(
            "Missing Packages",
            "This application needs the following Python package(s), "
            "which are not installed:\n\n    " + ", ".join(missing)
            + "\n\nInstall them now with pip?",
        ):
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "install", *missing],
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                messagebox.showerror(
                    "Install Failed",
                    "pip install failed:\n\n" + (proc.stderr or proc.stdout)[-800:],
                )
            else:
                importlib.invalidate_caches()
                messagebox.showinfo(
                    "Packages Installed", "Installed: " + ", ".join(missing)
                )
        if importlib.util.find_spec("requests") is None:
            messagebox.showerror(
                "Missing Dependency",
                "The 'requests' package is required to run this application.\n\n"
                "Install it with:\n    pip install requests",
            )
            sys.exit(1)
    finally:
        root.destroy()


_ensure_dependencies()

import requests as _requests

from bluebook_names import abbreviate_case_name
from cl_parse import parse_cl_html as _parse_cl_html
from courtlistener import CourtListenerClient, CourtListenerError
import ecfr
import fed_rules
import state_statutes
import statutes_at_large
import us_code
from court_catalog import (
    CATALOG as _COURT_CATALOG,
    COURT_BLUEBOOK as _COURT_BLUEBOOK,
    STATE_COURTS as _STATE_COURTS,
    all_court_ids as _all_court_ids,
)

_CONFIG_PATH = Path.home() / ".config" / "courtlistener" / "config.json"


def _load_saved_token() -> str:
    """Return the token saved in the config file, or '' if none."""
    try:
        data = json.loads(_CONFIG_PATH.read_text())
        return data.get("api_token", "")
    except Exception:
        return ""


def _save_token(token: str) -> None:
    """Persist *token* to the config file."""
    try:
        _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps({"api_token": token}))
    except Exception:
        pass  # Non-fatal – token simply won't persist


# Persistent session for third-party hosts (LOC, GovInfo, static.case.law).
# Uses a full browser-like header set; government CDNs reset connections when
# they see Python's default User-Agent or missing Accept/Sec-Fetch headers.
_anon_session = _requests.Session()
_anon_session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "application/pdf,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
})

# URL routing for official US Reports PDFs:
#   vols 1-542  → LOC CDN per-opinion PDFs (volume and page both 3-digit zero-padded)
#              If LOC fails, fall back to GovInfo (available from vol 2 onward).
#   vols 543-582 → GovInfo link service only (redirects to per-opinion PDF)
#   vols 583+   → not available on GovInfo; skip
_LOC_CUTOFF = 542
_GOVINFO_MAX = 582
_US_CITE_RE = re.compile(r"(\d+)\s+U\.S\.\s+(\d+)")

# Regex to parse a standard legal citation: "volume reporter page"
# Examples: "410 F.2d 1234", "12 F. Supp. 2d 567", "100 Cal. 400"
_CITE_PARSE_RE = re.compile(r"^(\d+)\s+(.+)\s+(\d+)")

_CLUSTER_ID_RE = re.compile(r"/clusters/(\d+)/?")
_COURT_ID_RE = re.compile(r"/courts/([^/]+)/?")


def _extract_cluster_id(url: str) -> Optional[int]:
    """Parse a cluster ID out of a CourtListener clusters URL."""
    m = _CLUSTER_ID_RE.search(str(url))
    return int(m.group(1)) if m else None


def _extract_court_id(url: str) -> str:
    """Parse a court slug out of a CourtListener courts URL (e.g. 'scotus', 'ca9')."""
    m = _COURT_ID_RE.search(str(url))
    return m.group(1) if m else ""


def _cluster_citations_to_strings(citations) -> list[str]:
    """Convert cluster-endpoint citations (dicts or strings) to plain strings."""
    result: list[str] = []
    for c in (citations or []):
        if isinstance(c, dict):
            vol = c.get("volume", "")
            rep = c.get("reporter", "")
            page = c.get("page", "")
            if vol and rep and page:
                result.append(f"{vol} {rep} {page}")
        elif isinstance(c, str) and c.strip():
            result.append(c.strip())
    return result


# Bluebook rule 6.1(a): close up adjacent single capitals, but set a single
# capital off from a longer abbreviation with a space.  Ordinal series
# designators ("2d", "3d", "4th") count as single capitals for this purpose.
# Sources such as Google Scholar emit reporters closed up ("S.Ct.",
# "L.Ed.2d", "F.Supp.2d"); these helpers re-space them to the proper
# Bluebook form ("S. Ct.", "L. Ed. 2d", "F. Supp. 2d") while leaving cites
# that are already correct — including all-single-capital reporters like
# "U.S." and "N.Y.S.2d" — untouched.
_REPORTER_UNIT_RE = re.compile(
    r"[A-Z]\.|"                     # single capital + period: F. S. N. Y.
    r"\d+(?:st|nd|rd|th|d)|"        # ordinal series designator: 2d 3d 4th
    r"[A-Za-z][A-Za-z'’]*\.?"       # longer word, optional period: Supp. Ct. So. App'x
)
_SINGLE_CAP_RE = re.compile(r"[A-Z]\.")


def _reporter_unit_is_tight(unit: str) -> bool:
    """A single capital ("F.") or an ordinal ("2d") closes up with its
    neighbour; longer abbreviations ("Supp.", "Ct.") take a space."""
    return bool(_SINGLE_CAP_RE.fullmatch(unit)) or unit[:1].isdigit()


def _respace_reporter(reporter: str) -> str:
    """Re-space a reporter abbreviation per Bluebook rule 6.1(a)."""
    units = _REPORTER_UNIT_RE.findall(reporter)
    if not units:
        return reporter.strip()
    out = units[0]
    for prev, cur in zip(units, units[1:]):
        tight = _reporter_unit_is_tight(prev) and _reporter_unit_is_tight(cur)
        out += ("" if tight else " ") + cur
    return out


def _respace_reporter_in_cite(cite: str) -> str:
    """Re-space the reporter inside a "volume reporter page" citation,
    leaving the string unchanged if it isn't a standard reporter cite."""
    m = _CITE_PARSE_RE.match(cite or "")
    if not m:
        return cite
    vol, reporter, page = m.group(1), m.group(2).strip(), m.group(3)
    return f"{vol} {_respace_reporter(reporter)} {page}"


# Priority-ordered patterns for picking the best citation for display,
# filenames, and Google Scholar searches.
# Order: U.S. Reports > S.Ct. > Federal Reporter (newest first) >
#        Federal Supplement > state/other reporters > anything non-Lexis.
_CITE_PRIORITY = [
    re.compile(r" U\.S\. "),
    re.compile(r" S\. Ct\. "),
    re.compile(r" F\.4th "),
    re.compile(r" F\.3d "),
    re.compile(r" F\.2d "),
    re.compile(r" F\. \d"),          # "F. " immediately before a digit (not F. Supp.)
    re.compile(r" F\. Supp\. 3d "),
    re.compile(r" F\. Supp\. 2d "),
    re.compile(r" F\. Supp\. "),
    re.compile(r" B\.R\. "),
]

_NOISE_CITE_RE = re.compile(r"lexis|westlaw|\bwl\b", re.IGNORECASE)


def _pick_citation(citations) -> str:
    """
    Return the most useful citation from *citations* for display,
    filenames, and Google Scholar searches.

    Strips HTML tags, discards Lexis/Westlaw cites, then walks
    ``_CITE_PRIORITY`` to find the best reporter.  Falls back to the
    first non-noise cite, or the raw first entry if everything is noise.
    """
    if not citations:
        return ""
    if isinstance(citations, str):
        citations = [citations]

    clean = [re.sub(r"<[^>]+>", "", c).strip() for c in citations]
    non_noise = [c for c in clean if c and not _NOISE_CITE_RE.search(c)]

    pool = non_noise if non_noise else clean
    for pat in _CITE_PRIORITY:
        hit = next((c for c in pool if pat.search(c)), None)
        if hit:
            return hit

    return pool[0] if pool else ""



# Strip reporter series designators ("2d", "4th") and volume/page digits
# before comparing reporter words against a court abbreviation.
_REPORTER_SERIES_RE = re.compile(r"\b\d*(?:2d|3d|4th|5th|6th)\b\.?|\b\d+\b")

_SCOTUS_REPORTERS = {"U.S.", "S. Ct.", "S.Ct.", "L. Ed.", "L. Ed. 2d", "L.Ed.", "L.Ed.2d"}


def _court_for_paren(citation: str, court_id: str, fallback: str = "") -> str:
    """
    Court abbreviation for a Bluebook date parenthetical, omitting or
    trimming whatever the reporter title already conveys (rule 10.4):

      60 Fed. Cl. 600        → ()          reporter names the court
      306 Md. 556            → ()          official state reporter, highest court
      100 Cal. App. 4th 454  → ()          official reporter names the court
      75 Cal. Rptr. 2d 1     → (Ct. App.)  reporter conveys the state only
      12 N.Y.S.2d 345        → (App. Div.) reporter conveys the state only
      510 A.2d 562           → (Md.)       regional reporter conveys nothing
    """
    court_id = (court_id or "").strip().lower()
    m = _CITE_PARSE_RE.match(citation or "")
    reporter = m.group(2).strip() if m else ""
    if "scotus" in court_id or reporter in _SCOTUS_REPORTERS:
        return ""
    abbr = _COURT_BLUEBOOK.get(court_id, "") or (fallback or "").strip()
    if not abbr or not reporter:
        return abbr
    rep_tokens = [t for t in _REPORTER_SERIES_RE.sub(" ", reporter).split() if t]
    ct_tokens = abbr.split()
    meaningful = [t for t in ct_tokens if t != "Ct."]
    if meaningful and all(t in rep_tokens for t in meaningful):
        return ""
    if (
        rep_tokens
        and len(ct_tokens) > 1
        and rep_tokens[0].replace(".", "").lower().startswith(
            ct_tokens[0].replace(".", "").lower()
        )
    ):
        return " ".join(ct_tokens[1:])
    return abbr


def _build_default_filename(item: dict) -> str:
    """
    Return a sanitized default filename (without extension) for saving an opinion.

    Format: ``Case Name, Reporter Cite (Court YEAR)``
    The court abbreviation follows Bluebook rule 10.4 — omitted for SCOTUS
    and whenever the reporter already conveys it (e.g. ``60 Fed. Cl. 600``).
    Falls back gracefully when citation or date are missing.
    """
    # Case name, abbreviated per Bluebook rule 10.2.2 (table T6/T10)
    case_name = abbreviate_case_name(re.sub(
        r"<[^>]+>", "",
        item.get("caseName") or item.get("case_name") or "opinion"
    ).strip())

    # Best citation (U.S. Reports > S.Ct. > Federal Reporters > others)
    citation_str = _pick_citation(item.get("citation", []))

    # Year from date filed
    date_filed = item.get("dateFiled") or item.get("date_filed") or ""
    year = date_filed[:4] if len(date_filed) >= 4 else ""

    # Court abbreviation — omitted when SCOTUS or conveyed by the reporter
    court_id = str(item.get("court_id") or item.get("court") or "").strip().lower()
    court_abbr = _court_for_paren(
        citation_str, court_id, str(item.get("court") or court_id).strip()
    )

    # Build the parenthetical: (Court YEAR) or (YEAR) for SCOTUS
    if court_abbr and year:
        paren = f"({court_abbr} {year})"
    elif year:
        paren = f"({year})"
    elif court_abbr:
        paren = f"({court_abbr})"
    else:
        paren = ""

    # Assemble parts, skipping empty ones.
    # Join case name + citation with a comma, then append the parenthetical
    # with a space only (no comma before it).
    main_parts = [p for p in [case_name, citation_str] if p]
    raw_name = ", ".join(main_parts)
    if paren:
        raw_name = f"{raw_name} {paren}" if raw_name else paren

    # Sanitize: keep alphanumeric, spaces, and common filename-safe punctuation
    safe = "".join(
        c if c.isalnum() or c in " .,()-_'&" else "_"
        for c in raw_name
    )[:120].strip()
    return safe


def _us_reports_loc_url(citation: str) -> Optional[str]:
    """
    Return the LOC CDN PDF URL for a US Reports citation, or None if the
    volume falls outside the LOC collection (vols 1-542 only).
    """
    m = _US_CITE_RE.search(citation)
    if not m:
        return None
    vol, page = int(m.group(1)), int(m.group(2))
    if vol > _LOC_CUTOFF:
        return None
    return (
        f"https://cdn.loc.gov/service/ll/usrep/"
        f"usrep{vol:03d}/usrep{vol:03d}{page:03d}/usrep{vol:03d}{page:03d}.pdf"
    )


def _us_reports_govinfo_url(citation: str) -> Optional[tuple[str, str]]:
    """
    Return (link_url, direct_pdf_url) for a US Reports citation, or None if
    the volume is outside the GovInfo range (vols 2-582).

    GovInfo holds US Reports starting from vol 2, so this also serves as a
    fallback for vols 1-542 when the LOC CDN is unavailable.

    link_url:       https://www.govinfo.gov/link/usreports/{vol}/{page}
    direct_pdf_url: https://www.govinfo.gov/content/pkg/USREPORTS-{vol}/pdf/USREPORTS-{vol}-{page}.pdf
    """
    m = _US_CITE_RE.search(citation)
    if not m:
        return None
    vol, page = int(m.group(1)), int(m.group(2))
    if vol > _GOVINFO_MAX:
        return None
    link_url = f"https://www.govinfo.gov/link/usreports/{vol}/{page}"
    direct_url = f"https://www.govinfo.gov/content/pkg/USREPORTS-{vol}/pdf/USREPORTS-{vol}-{page}.pdf"
    return link_url, direct_url


def _slugify_reporter(reporter: str) -> str:
    """
    Convert a reporter abbreviation to the slug used by static.case.law.

    The Caselaw Access Project slugify rules:
      1. Lowercase
      2. Spaces → hyphens
      3. Remove all characters that are not alphanumeric or hyphens
      4. Collapse consecutive hyphens; strip leading/trailing hyphens

    Examples:
      "F.2d"        → "f2d"
      "F.3d"        → "f3d"
      "F. Supp."    → "f-supp"
      "F. Supp. 2d" → "f-supp-2d"
      "F. App'x"    → "f-appx"
      "Cal."        → "cal"
      "N.E.2d"      → "ne2d"
    """
    s = reporter.lower()
    s = s.replace(" ", "-")
    s = re.sub(r"[^a-z0-9-]", "", s)
    s = re.sub(r"-+", "-", s)
    s = s.strip("-")
    # Old reporter names → the slug static.case.law actually uses.  The Federal
    # Reporter is "F." today, so an old "Fed. Rep." cite must look there.
    return _CASE_LAW_REPORTER_ALIASES.get(s, s)


# Old/long reporter names → the slug static.case.law uses for the modern form.
_CASE_LAW_REPORTER_ALIASES = {
    "fed-rep": "f",        # Federal Reporter (old "Fed. Rep." → "F.")
    "fed-rep-2d": "f2d",
    "fed-rep-3d": "f3d",
}


def _static_case_law_url(citation: str) -> Optional[str]:
    """
    Return the PDF URL candidate on static.case.law for a citation string
    such as '410 F.2d 1234', or None if the citation cannot be parsed.

    URL pattern:
      https://static.case.law/{reporter-slug}/{volume}/case-pdfs/{page:04d}-01.pdf
    """
    citation = re.sub(r"<[^>]+>", "", citation).strip()
    m = _CITE_PARSE_RE.match(citation)
    if not m:
        return None
    vol, reporter, page = m.group(1), m.group(2).strip(), m.group(3)
    slug = _slugify_reporter(reporter)
    if not slug:
        return None
    return f"https://static.case.law/{slug}/{vol}/case-pdfs/{int(page):04d}-01.pdf"


def _gather_all_citations(client, item: dict) -> list[str]:
    """Every citation known for a case: the search-result cite(s) plus the
    cluster record's parallel cites (de-duplicated, HTML-stripped).

    Early Supreme Court results frequently carry only a nominative-reporter
    cite (e.g. "19 How. 393"); the parallel "U.S." cite that locates the
    official PDF lives on the cluster.  Likewise Federal Reporter cases may
    expose only one of several parallel cites.  Trying them all — rather than
    just the first — is what lets the PDF resolver succeed for these."""
    out: list[str] = []
    seen: set[str] = set()

    def add(c) -> None:
        c = re.sub(r"<[^>]+>", "", str(c)).strip()
        if c and c not in seen:
            seen.add(c)
            out.append(c)

    raw = item.get("citation", [])
    for c in (raw if isinstance(raw, list) else [raw] if raw else []):
        add(c)
    cluster_id = item.get("cluster_id") or item.get("id")
    if cluster_id:
        try:
            cr = client.get_cluster(int(cluster_id), fields="citations")
            for c in (cr.get("citations") or []):
                if isinstance(c, str):
                    add(c)
                elif isinstance(c, dict):
                    v, r, p = c.get("volume"), c.get("reporter"), c.get("page")
                    if v and r and p:
                        add(f"{v} {r} {p}")
        except Exception as exc:
            print(f"[resolve] cluster citation fetch failed: {exc}")
    return out


_OPINION_TYPE_LABELS: dict[str, str] = {
    "010combined": "Opinion",
    "015unamimous": "Unanimous Opinion",
    "020lead": "Lead Opinion",
    "025plurality": "Plurality Opinion",
    "030concurrence": "Concurrence",
    "035concurrenceinpart": "Concurrence in Part",
    "040dissent": "Dissent",
    "050addendum": "Addendum",
    "060remittitur": "Remittitur",
    "070rehearing": "Rehearing",
    "080onthemerits": "On the Merits",
    "090onmotiontoamend": "On Motion to Amend",
}


def _strip_html(html: str) -> str:
    """Strip HTML tags, converting block-level tags to newlines first."""
    text = re.sub(
        r"<(br|/p|/div|/h[1-6]|/li|/tr|/blockquote)\b[^>]*>",
        "\n", html, flags=re.IGNORECASE,
    )
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# CL opinion-type codes → OpinionPart kind
_CL_TYPE_KIND: dict[str, str] = {
    "010combined": "majority",
    "015unamimous": "majority",
    "020lead": "majority",
    "025plurality": "majority",
    "030concurrence": "concurrence",
    "035concurrenceinpart": "concurrence",
    "040dissent": "dissent",
    "050addendum": "majority",
    "060remittitur": "majority",
    "070rehearing": "majority",
    "080onthemerits": "majority",
    "090onmotiontoamend": "majority",
}


def _assemble_case_parts(
    client, item: dict
) -> "tuple[list[OpinionPart], list[Block], str, dict]":
    """Fetch a case from CourtListener and build structured OpinionParts.

    Returns (parts, all_blocks, plain_text, cluster_metadata).
    """
    try:
        from google_scholar import Block, OpinionPart, Span, blocks_to_text
    except ImportError:
        return [], [], "", {}

    cluster_id = item.get("cluster_id") or item.get("id")
    cluster = client.get_cluster(
        int(cluster_id),
        fields="case_name,citations,judges,attorneys,syllabus,headnotes,"
               "sub_opinions,date_filed,docket",
    )

    # --- Build header part from metadata ---
    header_blocks: list[Block] = []
    case_name = re.sub(
        r"<[^>]+>", "",
        cluster.get("case_name") or item.get("caseName")
        or item.get("case_name") or "",
    ).strip()
    if case_name:
        header_blocks.append(Block(kind="center", spans=[
            Span(text=case_name, bold=True),
        ]))

    citations = cluster.get("citations") or []
    cite_parts: list[str] = []
    for c in citations:
        if isinstance(c, dict):
            vol = c.get("volume", "")
            reporter = c.get("reporter", "")
            page = c.get("page", "")
            if vol and reporter and page:
                cite_parts.append(f"{vol} {reporter} {page}")
        elif isinstance(c, str) and c.strip():
            cite_parts.append(c.strip())
    if cite_parts:
        header_blocks.append(Block(kind="center", spans=[
            Span(text=", ".join(cite_parts)),
        ]))

    for field_name, label in [
        ("judges", "Judges"),
        ("attorneys", "Attorneys"),
    ]:
        val = (cluster.get(field_name) or "").strip()
        if val:
            val = _strip_html(val)
        if val:
            header_blocks.append(Block(kind="para", spans=[
                Span(text=f"{label}: ", bold=True),
                Span(text=val),
            ]))

    for field_name, label in [("syllabus", "Syllabus"), ("headnotes", "Headnotes")]:
        val = (cluster.get(field_name) or "").strip()
        if val:
            parsed, _fn = _parse_cl_html(val)  # syllabus/headnotes have no footnotes
            if parsed:
                header_blocks.append(Block(kind="heading", spans=[
                    Span(text=label, bold=True),
                ]))
                header_blocks.extend(parsed)

    parts: list[OpinionPart] = []
    if header_blocks:
        parts.append(OpinionPart(label="Header", kind="header", blocks=header_blocks))

    # --- Sub-opinions ---
    sub_urls = cluster.get("sub_opinions") or []
    opinions: list[dict] = []
    for url in sub_urls:
        try:
            op = client._get_url(
                url,
                {"fields": "ordering_key,type,author_str,per_curiam,"
                           "html_with_citations,html,plain_text"},
            )
            opinions.append(op)
        except Exception as exc:
            print(f"[cl-parts] failed to fetch sub-opinion {url}: {exc}")

    opinions.sort(key=lambda o: (
        o.get("ordering_key") is None, o.get("ordering_key") or 0,
    ))

    all_blocks: list[Block] = list(header_blocks)

    for idx, op in enumerate(opinions):
        type_code = op.get("type") or ""
        label = _OPINION_TYPE_LABELS.get(type_code, type_code or "Opinion")
        kind = _CL_TYPE_KIND.get(type_code, "majority")

        # Add author info to label
        author = (op.get("author_str") or "").strip()
        if op.get("per_curiam") and not author:
            author = "Per Curiam"
        if author:
            label = f"{label} ({author})"

        html_text = (
            op.get("html_with_citations")
            or op.get("html")
            or ""
        )
        op_footnotes: list[Block] = []
        if html_text:
            # Namespace footnote ids per opinion so a case's several opinions
            # (each numbering from 1) don't collide in the viewer.
            op_blocks, op_footnotes = _parse_cl_html(html_text, fn_prefix=f"op{idx}_")
        else:
            plain = (op.get("plain_text") or "").strip()
            if plain:
                try:
                    from google_scholar import educate_quotes
                    plain = educate_quotes(plain)
                except ImportError:
                    pass
                op_blocks = [
                    Block(kind="para", spans=[Span(text=para.strip())])
                    for para in re.split(r"\n{2,}", plain) if para.strip()
                ]
            else:
                op_blocks = []

        if op_blocks:
            parts.append(OpinionPart(label=label, kind=kind, blocks=op_blocks,
                                     footnotes=op_footnotes))
            all_blocks.extend(op_blocks)

    try:
        plain_text = blocks_to_text(all_blocks)
    except Exception:
        plain_text = ""

    return parts, all_blocks, plain_text, cluster


def _assemble_case_text(client, item: dict) -> str:
    """
    Build a plain-text representation of a case from CourtListener.

    Layout:
      Case name
      Citations
      (blank line)
      Judges: …        ← only if the cluster has data
      Attorneys: …     ← only if the cluster has data
      Syllabus: …      ← only if the cluster has data
      Headnotes: …     ← only if the cluster has data
      (blank line)
      --- Opinion type ---
      <opinion text>
      … repeated for each sub-opinion, sorted by ordering_key …
    """
    lines: list[str] = []

    cluster_id = item.get("cluster_id") or item.get("id")
    print(f"[text] fetching cluster {cluster_id}")
    cluster = client.get_cluster(
        int(cluster_id),
        fields="case_name,citations,judges,attorneys,syllabus,headnotes,sub_opinions",
    )

    # --- Header ---
    case_name = re.sub(
        r"<[^>]+>", "",
        cluster.get("case_name") or item.get("caseName") or item.get("case_name") or "",
    ).strip()
    lines.append(case_name)

    citations = cluster.get("citations") or []
    cite_parts: list[str] = []
    for c in citations:
        if isinstance(c, dict):
            vol = c.get("volume", "")
            reporter = c.get("reporter", "")
            page = c.get("page", "")
            if vol and reporter and page:
                cite_parts.append(f"{vol} {reporter} {page}")
        elif isinstance(c, str) and c.strip():
            cite_parts.append(c.strip())
    if cite_parts:
        lines.append(", ".join(cite_parts))
    lines.append("")

    # --- Metadata sections ---
    for field, label in [
        ("judges", "Judges"),
        ("attorneys", "Attorneys"),
        ("syllabus", "Syllabus"),
        ("headnotes", "Headnotes"),
    ]:
        val = (cluster.get(field) or "").strip()
        if val:
            val = _strip_html(val)
        if val:
            lines.append(f"{label}: {val}")
            lines.append("")

    # --- Sub-opinions ---
    sub_urls = cluster.get("sub_opinions") or []
    opinions: list[dict] = []
    for url in sub_urls:
        try:
            op = client._get_url(
                url,
                {"fields": "ordering_key,type,html_with_citations,html,plain_text"},
            )
            opinions.append(op)
        except Exception as exc:
            print(f"[text] failed to fetch sub-opinion {url}: {exc}")

    # Sort by ordering_key ascending; None sorts last
    opinions.sort(key=lambda o: (o.get("ordering_key") is None, o.get("ordering_key") or 0))

    for op in opinions:
        type_code = op.get("type") or ""
        label = _OPINION_TYPE_LABELS.get(type_code, type_code or "Opinion")
        lines.append(f"--- {label} ---")
        lines.append("")
        text = (
            op.get("html_with_citations")
            or op.get("html")
            or op.get("plain_text")
            or ""
        )
        if text:
            lines.append(_strip_html(text))
        lines.append("")

    return "\n".join(lines)


try:
    from google_scholar import (
        GoogleScholarFetcher,
        blocks_to_text,
        educate_quotes,
        parse_opinion_blocks,
        segment_blocks,
        text_similarity,
    )

    _SCHOLAR_AVAILABLE = True
except ImportError:
    _SCHOLAR_AVAILABLE = False

    def educate_quotes(text: str) -> str:  # graceful degradation
        return text

try:
    from pynput import keyboard as _pynput_keyboard

    _HOTKEY_AVAILABLE = True
except ImportError:
    _HOTKEY_AVAILABLE = False


def _stdin_is_tty() -> bool:
    """True when an interactive terminal is attached, so we can read the
    's' (show window) and 'q' (quit) commands the background process offers."""
    try:
        return bool(sys.stdin) and sys.stdin.isatty()
    except Exception:
        return False


class CourtListenerGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("CourtListener Case Law Search")
        self.root.geometry("1300x720")
        self.root.minsize(900, 500)

        self._client: Optional[CourtListenerClient] = None
        self._results: list[dict] = []
        self._scholar_results: list = []  # ScholarResult objects
        self._selected_courts: set[str] = set()  # empty = all courts
        self._search_thread: Optional[threading.Thread] = None
        self._scholar: Optional["GoogleScholarFetcher"] = None

        self._preview_cache: dict[int, str] = {}  # result index → snippet text
        self._sort_state: dict[int, tuple[str, bool]] = {}  # tree id → (col, reverse)

        # Initialize token from env or saved config
        initial_token = os.environ.get("COURTLISTENER_TOKEN") or _load_saved_token()
        self._token_var = tk.StringVar(value=initial_token)

        self._quick_popup: Optional[tk.Toplevel] = None
        self._hotkey_listener = None
        self._root_hidden = False

        self._build_ui()
        self._setup_global_hotkey()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close_window)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        style = ttk.Style()
        style.configure("Treeview", rowheight=28)

        # --- Menubar ---
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        settings_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Settings", menu=settings_menu)
        settings_menu.add_command(label="API Token…", command=self._show_settings_dialog)
        lookup_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Look Up", menu=lookup_menu)
        lookup_menu.add_command(
            label="U.S. Code / C.F.R. Section…", accelerator="Ctrl+L",
            command=self._show_statute_lookup,
        )
        lookup_menu.add_command(
            label="Open Citation List…",
            command=self._show_citation_list_dialog,
        )
        lookup_menu.add_command(
            label="Quick Look Up (case or statute)…", accelerator="Ctrl+S",
            command=self._show_quick_lookup,
        )
        self.root.bind("<Control-l>", lambda _e: self._show_statute_lookup())
        self.root.bind("<Control-s>", lambda _e: self._show_quick_lookup())

        # --- Search frame ---
        search_frame = ttk.LabelFrame(self.root, text="Search", padding=6)
        search_frame.pack(fill="x", padx=10, pady=(10, 4))

        # Row 1: query + button
        row1 = ttk.Frame(search_frame)
        row1.pack(fill="x", pady=(0, 4))
        ttk.Label(row1, text="Query:").pack(side="left")
        self._query_var = tk.StringVar()
        self._query_entry = ttk.Entry(row1, textvariable=self._query_var)
        self._query_entry.pack(side="left", padx=6, fill="x", expand=True)
        self._query_entry.bind("<Return>", lambda _e: self._do_search())
        self._search_btn = ttk.Button(row1, text="Search", command=self._do_search)
        self._search_btn.pack(side="left", padx=(0, 4))

        # Row 2: filters
        row2 = ttk.Frame(search_frame)
        row2.pack(fill="x")

        self._courts_btn_var = tk.StringVar(value="Courts: All ▾")
        ttk.Button(
            row2,
            textvariable=self._courts_btn_var,
            command=self._show_court_picker,
        ).pack(side="left", padx=(0, 12))

        ttk.Label(row2, text="Filed from:").pack(side="left")
        self._date_from_var = tk.StringVar()
        ttk.Entry(row2, textvariable=self._date_from_var, width=12).pack(
            side="left", padx=4
        )

        ttk.Label(row2, text="to:").pack(side="left")
        self._date_to_var = tk.StringVar()
        ttk.Entry(row2, textvariable=self._date_to_var, width=12).pack(
            side="left", padx=4
        )

        ttk.Label(row2, text="  Max results:").pack(side="left")
        self._page_size_var = tk.IntVar(value=20)
        ttk.Spinbox(
            row2, from_=5, to=20, textvariable=self._page_size_var, width=5
        ).pack(side="left", padx=4)

        # --- Results area: left trees + right preview ---
        results_frame = ttk.LabelFrame(self.root, text="Results", padding=6)
        results_frame.pack(fill="both", expand=True, padx=10, pady=4)

        # --- Status bar + action buttons — packed first so they are always
        #     visible regardless of window height.
        bottom = ttk.Frame(results_frame)
        bottom.pack(side="bottom", fill="x", pady=(4, 0))

        self._download_btn = ttk.Button(
            bottom,
            text="Download PDF",
            command=self._download_selected,
            state="disabled",
        )
        self._download_btn.pack(side="right", padx=4)

        scholar_tip = "" if _SCHOLAR_AVAILABLE else " (needs beautifulsoup4)"
        self._scholar_btn = ttk.Button(
            bottom,
            text=f"Scholar Text{scholar_tip}",
            command=self._fetch_scholar_text,
            state="disabled",
        )
        self._scholar_btn.pack(side="right", padx=4)

        self._status_var = tk.StringVar(value="Enter a query and click Search.")
        ttk.Label(bottom, textvariable=self._status_var, anchor="w").pack(
            side="left", fill="x", expand=True
        )

        # --- Compact preview strip, spans the full width above the status bar
        preview_frame = ttk.LabelFrame(results_frame, text="Preview", padding=2)
        preview_frame.pack(side="bottom", fill="x", pady=(4, 0))
        self._preview_text = tk.Text(
            preview_frame,
            wrap="word",
            height=4,
            state="disabled",
            font=("TkDefaultFont", 9),
            relief="flat",
            background="#f5f5f5",
        )
        self._preview_text.pack(fill="x", expand=True)

        paned = ttk.PanedWindow(results_frame, orient="horizontal")
        paned.pack(fill="both", expand=True)

        # -- Left pane: CourtListener results (main tree + orders tree) --
        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=3)
        ttk.Label(
            left_frame,
            text="CourtListener",
            foreground="gray",
            font=("TkDefaultFont", 9, "italic"),
        ).pack(anchor="w")

        cols = ("case_name", "court", "date_filed", "citation", "status")

        main_tree_frame = ttk.Frame(left_frame)
        main_tree_frame.pack(fill="both", expand=True)
        self._tree = ttk.Treeview(
            main_tree_frame, columns=cols, show="headings", selectmode="browse"
        )
        self._configure_tree_columns(self._tree)
        vsb = ttk.Scrollbar(main_tree_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        self._tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        self._tree.bind("<Double-1>", lambda _e: self._download_selected())
        self._tree.bind("<<TreeviewSelect>>", lambda _e: self._on_row_select(self._tree))
        self._tree.bind("<Button-3>", lambda e: self._on_right_click(e, self._tree))

        # Orders / short-opinion section
        orders_sep = ttk.Frame(left_frame)
        orders_sep.pack(fill="x", pady=(4, 0))
        ttk.Separator(orders_sep, orient="horizontal").pack(fill="x")
        ttk.Label(
            orders_sep,
            text="Orders  (≤ 2 citations)",
            foreground="gray",
            font=("TkDefaultFont", 9, "italic"),
        ).pack(anchor="w", pady=(2, 0))

        orders_tree_frame = ttk.Frame(left_frame)
        orders_tree_frame.pack(fill="x")
        self._orders_tree = ttk.Treeview(
            orders_tree_frame, columns=cols, show="headings", selectmode="browse", height=4
        )
        self._configure_tree_columns(self._orders_tree)
        vsb2 = ttk.Scrollbar(orders_tree_frame, orient="vertical", command=self._orders_tree.yview)
        self._orders_tree.configure(yscrollcommand=vsb2.set)
        self._orders_tree.pack(side="left", fill="x", expand=True)
        vsb2.pack(side="right", fill="y")
        self._orders_tree.bind("<Double-1>", lambda _e: self._download_selected())
        self._orders_tree.bind(
            "<<TreeviewSelect>>", lambda _e: self._on_row_select(self._orders_tree)
        )
        self._orders_tree.bind("<Button-3>", lambda e: self._on_right_click(e, self._orders_tree))

        # -- Right pane: Google Scholar results --
        scholar_pane = ttk.Frame(paned)
        paned.add(scholar_pane, weight=2)
        sch_header = ttk.Frame(scholar_pane)
        sch_header.pack(fill="x")
        ttk.Label(
            sch_header,
            text="Google Scholar",
            foreground="gray",
            font=("TkDefaultFont", 9, "italic"),
        ).pack(side="left")
        self._scholar_status_var = tk.StringVar(value="")
        ttk.Label(
            sch_header, textvariable=self._scholar_status_var, foreground="gray"
        ).pack(side="right")

        sch_tree_frame = ttk.Frame(scholar_pane)
        sch_tree_frame.pack(fill="both", expand=True)
        self._scholar_tree = ttk.Treeview(
            sch_tree_frame,
            columns=("case", "source"),
            show="headings",
            selectmode="browse",
        )
        self._scholar_tree.heading("case", text="Case")
        self._scholar_tree.heading("source", text="Court / Year")
        self._scholar_tree.column("case", width=250, minwidth=120)
        self._scholar_tree.column("source", width=140, minwidth=80)
        svsb = ttk.Scrollbar(
            sch_tree_frame, orient="vertical", command=self._scholar_tree.yview
        )
        self._scholar_tree.configure(yscrollcommand=svsb.set)
        self._scholar_tree.pack(side="left", fill="both", expand=True)
        svsb.pack(side="right", fill="y")
        self._scholar_tree.bind(
            "<<TreeviewSelect>>", lambda _e: self._on_scholar_row_select()
        )
        self._scholar_tree.bind(
            "<Double-1>", lambda _e: self._open_selected_scholar_result()
        )


    # ------------------------------------------------------------------
    # Window lifecycle — hide on close, keep hotkey alive
    # ------------------------------------------------------------------

    def _can_run_headless(self) -> bool:
        """True when the process can keep running without a visible window:
        either the global hotkey is live (Ctrl+Space opens search) or there's
        a terminal to read 's'/'q' from."""
        if _stdin_is_tty():
            return True
        return _HOTKEY_AVAILABLE and self._hotkey_listener is not None

    def _on_close_window(self) -> None:
        """Hide the main window instead of destroying it so the process keeps
        running in the background — the global hotkey stays live and the
        window can be reopened with 's' in the terminal.  Only quit outright
        when there's no way to bring it back."""
        if self._can_run_headless():
            self.root.withdraw()
            self._root_hidden = True
            self._print_background_help(closed=True)
        else:
            self.root.destroy()

    def _ensure_root_exists(self) -> None:
        """Make sure the root window is usable — show it if it was hidden."""
        if self._root_hidden:
            self.root.deiconify()
            self._root_hidden = False

    def _show_main_window(self) -> None:
        """Bring the full search window to the front (the 's' command and the
        quick-search 'open the main window' path both land here)."""
        try:
            self._ensure_root_exists()
            self.root.deiconify()
            self.root.lift()
            if sys.platform == "win32":
                self._win_force_foreground(self.root)
            self.root.focus_force()
            self._query_entry.focus_set()
        except tk.TclError:
            pass

    def _print_background_help(self, closed: bool = False) -> None:
        """Tell the user how to drive the background process — Ctrl+Space to
        search, 's' to open the full window, 'q' to quit."""
        hotkey = "Cmd+Space" if sys.platform == "darwin" else "Ctrl+Space"
        intro = (
            "Window closed — GetCases is still running in the background."
            if closed
            else "GetCases is running in the background."
        )
        tips = []
        if _HOTKEY_AVAILABLE and self._hotkey_listener is not None:
            tips.append(f"Press {hotkey} anywhere to search.")
        if _stdin_is_tty():
            tips.append(
                "Type 's' + Enter to open the full search window, "
                "'q' + Enter to quit."
            )
        if tips:
            print("\n" + intro + "\n  " + "\n  ".join(tips))

    # ------------------------------------------------------------------
    # Global hotkey (Ctrl+Space / Cmd+Space) → quick search popup
    # ------------------------------------------------------------------

    def _setup_global_hotkey(self) -> None:
        if not _HOTKEY_AVAILABLE:
            return
        hotkey = "<cmd>+<space>" if sys.platform == "darwin" else "<ctrl>+<space>"
        try:
            self._hotkey_listener = _pynput_keyboard.GlobalHotKeys(
                {hotkey: self._on_global_hotkey}
            )
            self._hotkey_listener.daemon = True
            self._hotkey_listener.start()
        except Exception:
            self._hotkey_listener = None

    def _on_global_hotkey(self) -> None:
        self.root.after(0, self._toggle_quick_search_popup)

    def _toggle_quick_search_popup(self) -> None:
        if self._quick_popup is not None:
            try:
                if self._quick_popup.winfo_exists():
                    self._quick_popup.destroy()
            except tk.TclError:
                pass
            self._quick_popup = None
            return

        popup = tk.Toplevel(self.root)
        self._quick_popup = popup
        # Reset the consecutive-empty-Return counter and dropdown generation
        # each time a fresh popup opens.
        self._spotlight_empty_returns = 0
        self._spotlight_generation = 0
        self._spotlight_results_frame = None
        popup.overrideredirect(True)
        popup.attributes("-topmost", True)

        pw, ph = 520, 48
        sx = popup.winfo_screenwidth()
        sy = popup.winfo_screenheight()
        popup.geometry(f"{pw}x{ph}+{(sx - pw) // 2}+{sy // 3}")

        border = tk.Frame(popup, bg="#888888")
        border.pack(fill="both", expand=True)
        inner = tk.Frame(border, bg="#ffffff", padx=10, pady=6)
        inner.pack(fill="both", expand=True, padx=2, pady=2)

        tk.Label(
            inner, text="Search CourtListener:", bg="#ffffff",
            fg="#555555", font=("TkDefaultFont", 11),
        ).pack(side="left", padx=(0, 6))

        entry_var = tk.StringVar()
        entry = tk.Entry(
            inner, textvariable=entry_var, font=("TkDefaultFont", 13),
            relief="flat", bg="#ffffff",
        )
        entry.pack(side="left", fill="x", expand=True)

        def _submit(_e=None) -> None:
            query = entry_var.get().strip()
            if not query:
                # Empty search bar: open the main window only on the second
                # consecutive Return (a deliberate "show me everything").
                self._spotlight_empty_returns += 1
                if self._spotlight_empty_returns >= 2:
                    self._open_main_from_spotlight(popup)
                return
            self._spotlight_empty_returns = 0

            # 1. Statute / regulation / federal rule: "42 USC 1983(b)",
            # "29 CFR 1614.105", "Fed. R. Civ. P. 56", "Cal. Penal Code 187".
            # The section sign is optional — it can't be typed on a keyboard.
            statute = _parse_statute_query(query)
            if statute:
                popup.destroy()
                self._quick_popup = None
                _open_statute_action(self.root, statute)
                return

            # 2. Case citation: "365 U.S. 167" or "Monroe v. Pape, 365 U.S. 167, 171"
            parsed = _parse_citation_line(query)
            if parsed:
                name, cite, pin = parsed
                fetcher = (
                    self._get_scholar() if _SCHOLAR_AVAILABLE else None
                )
                client = (
                    self._get_client()
                    if self._token_var.get().strip() else None
                )
                if fetcher is not None or client is not None:
                    popup.destroy()
                    self._quick_popup = None

                    def run() -> None:
                        self._try_open_citation(
                            name, cite, pin, fetcher, client,
                        )
                    threading.Thread(target=run, daemon=True).start()
                    return

            # 3. Fallback: show spotlight dropdown with search results
            # (keep the popup alive — it expands into the dropdown)
            self._show_spotlight_dropdown(popup, border, entry, query)

        def _dismiss(_e=None) -> None:
            popup.destroy()
            self._quick_popup = None

        entry.bind("<Return>", _submit)
        entry.bind("<Escape>", _dismiss)

        def _grab_focus(attempt: int = 0) -> None:
            try:
                popup.deiconify()
                popup.lift()
                if not popup.winfo_viewable():
                    if attempt < 25:
                        popup.after(20, lambda: _grab_focus(attempt + 1))
                    return
                # On Windows, Tk's focus_force cannot steal the foreground
                # from another process (the OS foreground-lock blocks it),
                # so go through the Win32 API on the real top-level HWND.
                if sys.platform == "win32":
                    self._win_force_foreground(popup)
                popup.focus_force()
                entry.focus_force()
                entry.icursor(tk.END)
                entry.selection_range(0, tk.END)
                # The entry may not hold keyboard focus on the first try;
                # retry until it does (or we run out of attempts).
                if popup.focus_get() is not entry and attempt < 25:
                    popup.after(20, lambda: _grab_focus(attempt + 1))
            except tk.TclError:
                pass

        popup.after(10, _grab_focus)

    def _open_main_from_spotlight(
        self, popup: tk.Toplevel, query: str = "",
    ) -> None:
        """Close the spotlight popup and bring up the full search window,
        optionally seeding and running *query*."""
        try:
            popup.destroy()
        except tk.TclError:
            pass
        self._quick_popup = None
        self._ensure_root_exists()
        self.root.deiconify()
        self.root.lift()
        if sys.platform == "win32":
            self._win_force_foreground(self.root)
        self.root.focus_force()
        self._query_entry.focus_set()
        if query:
            self._query_var.set(query)
            self._do_search()

    def _show_spotlight_dropdown(
        self, popup: tk.Toplevel, border: tk.Frame,
        entry: tk.Entry, query: str,
    ) -> None:
        """Expand the popup into a spotlight-style dropdown with streaming
        search results from Google Scholar and CourtListener."""

        # A fresh search retracts any dropdown still showing from the previous
        # query: bump the generation token so stale background callbacks are
        # ignored, and tear down the old results frame.
        self._spotlight_generation += 1
        my_gen = self._spotlight_generation
        old_frame = getattr(self, "_spotlight_results_frame", None)
        if old_frame is not None:
            try:
                old_frame.destroy()
            except tk.TclError:
                pass

        # Resize popup to accommodate results
        pw = 580
        row_h = 52
        max_rows = 6
        header_h = 48
        dropdown_h = row_h * max_rows + 28  # extra for status label
        sx = popup.winfo_screenwidth()
        sy = popup.winfo_screenheight()
        popup.geometry(
            f"{pw}x{header_h + dropdown_h}"
            f"+{(sx - pw) // 2}+{sy // 3}"
        )

        # Results frame below the search bar
        results_frame = tk.Frame(border, bg="#f0f0f0")
        results_frame.pack(fill="both", expand=True, padx=2, pady=(0, 2))
        self._spotlight_results_frame = results_frame

        # Tracking state
        result_rows: list[dict] = []
        selected_idx = [-1]  # mutable via closure

        def _add_result(court_id: str, name: str, cite: str, year: str,
                        source_label: str, open_fn) -> None:
            # Ignore results streaming in from a superseded search.
            if my_gen != self._spotlight_generation:
                return
            try:
                if not popup.winfo_exists():
                    return
            except tk.TclError:
                return

            idx = len(result_rows)
            if idx >= max_rows:
                return

            row = tk.Frame(results_frame, bg="#ffffff", padx=6, pady=4,
                           cursor="hand2")
            row.pack(fill="x", padx=4, pady=(2, 0))

            court_abbr = _COURT_BLUEBOOK.get(
                court_id, court_id.upper() if court_id else "?"
            )
            if court_id == "scotus":
                court_abbr = "SCOTUS"

            # Court badge on the left
            badge = tk.Label(
                row, text=court_abbr, bg="#3a5a8c", fg="#ffffff",
                font=("TkDefaultFont", 10, "bold"),
                padx=6, pady=2, anchor="center", width=8,
            )
            badge.pack(side="left", padx=(0, 8))

            # Text on the right
            text_frame = tk.Frame(row, bg="#ffffff")
            text_frame.pack(side="left", fill="x", expand=True)

            # Truncate name for display
            display_name = name[:80] + ("…" if len(name) > 80 else "")
            tk.Label(
                text_frame, text=display_name, bg="#ffffff", fg="#222222",
                font=("TkDefaultFont", 10), anchor="w",
            ).pack(fill="x")

            detail = f"{cite}" if cite else ""
            if year:
                detail = f"{detail} ({year})" if detail else f"({year})"
            detail_suffix = f"  — {source_label}" if source_label else ""
            tk.Label(
                text_frame, text=detail + detail_suffix, bg="#ffffff",
                fg="#888888", font=("TkDefaultFont", 8), anchor="w",
            ).pack(fill="x")

            entry_data = {
                "row": row, "open_fn": open_fn, "badge": badge,
                "text_frame": text_frame,
            }
            result_rows.append(entry_data)

            def on_click(_e=None) -> None:
                popup.destroy()
                self._quick_popup = None
                open_fn()

            for widget in (row, badge, text_frame):
                widget.bind("<Button-1>", on_click)
            for child in text_frame.winfo_children():
                child.bind("<Button-1>", on_click)

        def _highlight(idx: int) -> None:
            for i, r in enumerate(result_rows):
                bg = "#d0e0f0" if i == idx else "#ffffff"
                r["row"].config(bg=bg)
                r["text_frame"].config(bg=bg)
                for child in r["text_frame"].winfo_children():
                    try:
                        child.config(bg=bg)
                    except tk.TclError:
                        pass

        def _on_key(event) -> None:
            if not result_rows:
                return
            if event.keysym == "Down":
                selected_idx[0] = min(selected_idx[0] + 1,
                                      len(result_rows) - 1)
                _highlight(selected_idx[0])
            elif event.keysym == "Up":
                selected_idx[0] = max(selected_idx[0] - 1, 0)
                _highlight(selected_idx[0])
            elif event.keysym == "Return":
                if 0 <= selected_idx[0] < len(result_rows):
                    popup.destroy()
                    self._quick_popup = None
                    result_rows[selected_idx[0]]["open_fn"]()

        entry.bind("<Down>", _on_key)
        entry.bind("<Up>", _on_key)
        # Override Return to select from dropdown once results exist
        def _entry_return(_e=None) -> None:
            if result_rows and selected_idx[0] >= 0:
                # A result is highlighted — open it.
                self._spotlight_empty_returns = 0
                _on_key(type("E", (), {"keysym": "Return"})())
                return
            current = entry.get().strip()
            if current:
                # New (or re-typed) query with no selection: retract the
                # current dropdown and run a fresh search in the spotlight
                # interface rather than jumping to the main window.
                self._spotlight_empty_returns = 0
                self._show_spotlight_dropdown(popup, border, entry, current)
                return
            # Empty search bar: open the main window only on the second
            # consecutive Return.
            self._spotlight_empty_returns += 1
            if self._spotlight_empty_returns >= 2:
                self._open_main_from_spotlight(popup)
        entry.bind("<Return>", _entry_return)

        # Status label
        status_lbl = tk.Label(
            results_frame, text="Searching…", bg="#f0f0f0", fg="#999999",
            font=("TkDefaultFont", 8), anchor="w",
        )
        status_lbl.pack(fill="x", padx=8, pady=(4, 4))
        search_done = [0]  # track how many searches completed

        def _update_status() -> None:
            if my_gen != self._spotlight_generation:
                return
            try:
                if not popup.winfo_exists():
                    return
                n = len(result_rows)
                if search_done[0] >= 2:
                    status_lbl.config(
                        text=f"{n} results" if n else "No results found"
                    )
                else:
                    status_lbl.config(text=f"{n} results so far…")
            except tk.TclError:
                pass

        # Launch Scholar and CL searches in parallel
        def scholar_search() -> None:
            if not _SCHOLAR_AVAILABLE:
                search_done[0] += 1
                self.root.after(0, _update_status)
                return
            fetcher = self._get_scholar()
            if fetcher is None:
                search_done[0] += 1
                self.root.after(0, _update_status)
                return
            try:
                results = fetcher.search_cases(query, limit=3)
            except Exception:
                results = []
            for r in results[:3]:
                court_id = _scholar_source_to_court_id(r.source)
                year = _scholar_source_year(r.source)
                # The case's own reporter citation sits in the source byline.
                cite = _scholar_source_cite(r.source)
                if not cite:
                    m = _TEXT_CITE_RE.search(r.title + " " + r.snippet)
                    if m:
                        cite = re.sub(r"\s+", " ", m.group(0))

                def make_opener(sr=r):
                    def open_it():
                        f = self._get_scholar()
                        if f is None:
                            return
                        def run():
                            try:
                                res = f.fetch_by_url(sr.url)
                            except Exception as exc:
                                def fail(e=exc):
                                    messagebox.showerror(
                                        "Google Scholar Error",
                                        f'Could not load "{sr.title}".\n\n{e}',
                                    )
                                self._post_root(fail)
                                return
                            if res:
                                url, html = res
                                def show():
                                    _ScholarTextWindow(
                                        self.root, self, url, html,
                                        item=None,
                                    )
                                self._post_root(show)
                        threading.Thread(target=run, daemon=True).start()
                    return open_it

                self.root.after(
                    0, _add_result, court_id, r.title, cite, year,
                    "Scholar", make_opener(),
                )
            search_done[0] += 1
            self.root.after(0, _update_status)

        def cl_search() -> None:
            client = (
                self._get_client()
                if self._token_var.get().strip() else None
            )
            if client is None:
                search_done[0] += 1
                self.root.after(0, _update_status)
                return
            try:
                # Over-fetch so we can still fill 3 rows after dropping
                # SCOTUS "order" entries (≤ 2 outbound citations), the same
                # ones the main search routes out of the primary results.
                data = client.search(query, type="o", page_size=10)
                results = data.get("results") or []
            except Exception:
                results = []

            def _is_scotus_order(it: dict) -> bool:
                court_val = str(it.get("court_id") or it.get("court") or "")
                if "scotus" not in court_val.lower():
                    return False
                opinions = it.get("opinions") or []
                main_op = max(
                    opinions,
                    key=lambda o: len(o.get("cites") or []),
                    default=None,
                )
                cites_count = len(main_op.get("cites") or []) if main_op else 0
                return cites_count <= 2

            results = [it for it in results if not _is_scotus_order(it)]
            for item in results[:3]:
                case_name = re.sub(
                    r"<[^>]+>", "",
                    item.get("caseName") or item.get("case_name") or "",
                ).strip()
                court_id = str(
                    item.get("court_id") or item.get("court") or ""
                ).strip().lower()
                cite_str = _pick_citation(item.get("citation", []))
                date = item.get("dateFiled") or item.get("date_filed") or ""
                year = date[:4] if len(date) >= 4 else ""

                def make_opener(it=item, nm=case_name):
                    def open_it():
                        fetcher = (
                            self._get_scholar()
                            if _SCHOLAR_AVAILABLE else None
                        )
                        c = self._get_client()

                        def run():
                            # Try Google Scholar first, same as the main window
                            if fetcher is not None:
                                primary = _pick_citation(
                                    it.get("citation", [])
                                )
                                quick_result = None
                                if primary:
                                    try:
                                        quick_result = fetcher.fetch_by_citation(
                                            primary
                                        )
                                    except Exception:
                                        pass
                                if quick_result:
                                    url, html = quick_result
                                    def show_quick():
                                        win = _ScholarTextWindow(
                                            self.root, self, url, html,
                                            item=it,
                                            note="verifying against CourtListener…",
                                        )
                                        def verify():
                                            self._spotlight_verify_scholar(
                                                win, url, html, it,
                                                fetcher, c,
                                            )
                                        threading.Thread(
                                            target=verify, daemon=True,
                                        ).start()
                                    self._post_root(show_quick)
                                    return
                                # Quick fetch failed — try full search
                                try:
                                    result, cl_text, note = _find_scholar_for_item(
                                        c, fetcher, it, lambda m: None,
                                    )
                                except Exception:
                                    result, cl_text, note = None, None, ""
                                if result:
                                    s_url, s_html = result
                                    def show_full():
                                        _ScholarTextWindow(
                                            self.root, self, s_url, s_html,
                                            item=it, cl_text=cl_text,
                                            note=note,
                                        )
                                    self._post_root(show_full)
                                    return

                            # Scholar unavailable or failed — fall back to CL
                            if c is None:
                                def fail_no():
                                    messagebox.showerror(
                                        "Error",
                                        f'Could not load "{nm}".',
                                    )
                                self._post_root(fail_no)
                                return
                            try:
                                parts, blocks, plain, cluster = (
                                    _assemble_case_parts(c, it)
                                )
                            except Exception as exc:
                                def fail(e=exc):
                                    messagebox.showerror(
                                        "CourtListener Error",
                                        f'Could not load "{nm}".\n\n{e}',
                                    )
                                self._post_root(fail)
                                return
                            if parts or plain:
                                def show():
                                    _ScholarTextWindow(
                                        self.root, self, "", "",
                                        item=it, cl_text=plain,
                                        cl_parts=parts, cl_blocks=blocks,
                                    )
                                self._post_root(show)
                        threading.Thread(target=run, daemon=True).start()
                    return open_it

                self.root.after(
                    0, _add_result, court_id, case_name, cite_str, year,
                    "CourtListener", make_opener(),
                )
            search_done[0] += 1
            self.root.after(0, _update_status)

        threading.Thread(target=scholar_search, daemon=True).start()
        threading.Thread(target=cl_search, daemon=True).start()

    @staticmethod
    def _win_force_foreground(popup: tk.Misc) -> bool:
        """Force *popup* to the foreground on Windows, defeating the
        foreground-lock that stops a background process from stealing focus.

        Returns True if the window became the foreground window.  Several
        Win32 quirks are handled here that the naive approach gets wrong:

          * 64-bit window handles must be passed through ``wintypes.HWND``
            argtypes or ctypes truncates them to 32 bits, corrupting the
            handle so ``SetForegroundWindow`` silently fails.
          * the real OS top-level window is obtained with ``GetAncestor``
            from ``winfo_id`` (``wm_frame`` is unreliable on Windows).
          * the system foreground-lock timeout is temporarily set to 0 so
            the call is honored even though we're not the active app.
        """
        try:
            import ctypes
            from ctypes import wintypes
        except Exception:
            return False

        try:
            user32 = ctypes.windll.user32
            kernel32 = ctypes.windll.kernel32

            # Signatures — critical so 64-bit HWNDs survive the call.
            user32.GetForegroundWindow.restype = wintypes.HWND
            user32.GetWindowThreadProcessId.argtypes = [
                wintypes.HWND, ctypes.POINTER(wintypes.DWORD)
            ]
            user32.GetWindowThreadProcessId.restype = wintypes.DWORD
            user32.AttachThreadInput.argtypes = [
                wintypes.DWORD, wintypes.DWORD, wintypes.BOOL
            ]
            user32.AttachThreadInput.restype = wintypes.BOOL
            user32.BringWindowToTop.argtypes = [wintypes.HWND]
            user32.SetForegroundWindow.argtypes = [wintypes.HWND]
            user32.SetForegroundWindow.restype = wintypes.BOOL
            user32.SetActiveWindow.argtypes = [wintypes.HWND]
            user32.SetActiveWindow.restype = wintypes.HWND
            user32.SetFocus.argtypes = [wintypes.HWND]
            user32.SetFocus.restype = wintypes.HWND
            user32.GetAncestor.argtypes = [wintypes.HWND, wintypes.UINT]
            user32.GetAncestor.restype = wintypes.HWND
            user32.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
            user32.IsIconic.argtypes = [wintypes.HWND]
            user32.SystemParametersInfoW.argtypes = [
                wintypes.UINT, wintypes.UINT, ctypes.c_void_p, wintypes.UINT
            ]
            user32.SystemParametersInfoW.restype = wintypes.BOOL

            GA_ROOT = 2
            SW_SHOW, SW_RESTORE = 5, 9
            SPI_GETFGLOCK, SPI_SETFGLOCK = 0x2000, 0x2001
            SPIF_SENDCHANGE = 0x0002

            # winfo_id() can be a child wrapper; GetAncestor gives the
            # actual OS top-level window SetForegroundWindow expects.
            hwnd = user32.GetAncestor(wintypes.HWND(popup.winfo_id()), GA_ROOT)
            if not hwnd:
                hwnd = popup.winfo_id()

            user32.ShowWindow(
                hwnd, SW_RESTORE if user32.IsIconic(hwnd) else SW_SHOW
            )

            # Clear the foreground-lock timeout for the duration of the call.
            old_timeout = wintypes.DWORD(0)
            user32.SystemParametersInfoW(
                SPI_GETFGLOCK, 0, ctypes.byref(old_timeout), 0
            )
            user32.SystemParametersInfoW(
                SPI_SETFGLOCK, 0, ctypes.c_void_p(0), SPIF_SENDCHANGE
            )

            fg_win = user32.GetForegroundWindow()
            fg_thread = user32.GetWindowThreadProcessId(fg_win, None)
            our_thread = kernel32.GetCurrentThreadId()

            attached = False
            if fg_thread and fg_thread != our_thread:
                attached = bool(
                    user32.AttachThreadInput(fg_thread, our_thread, True)
                )
            user32.BringWindowToTop(hwnd)
            user32.SetForegroundWindow(hwnd)
            user32.SetActiveWindow(hwnd)
            user32.SetFocus(hwnd)
            if attached:
                user32.AttachThreadInput(fg_thread, our_thread, False)

            # Restore the user's original foreground-lock timeout.
            user32.SystemParametersInfoW(
                SPI_SETFGLOCK, 0,
                ctypes.c_void_p(old_timeout.value), SPIF_SENDCHANGE,
            )
            return bool(user32.GetForegroundWindow() == hwnd)
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Settings dialog
    # ------------------------------------------------------------------

    def _post_root(self, fn, *args) -> None:
        try:
            self.root.after(0, fn, *args)
        except tk.TclError:
            pass

    def _try_open_citation(self, name: str, cite: str, pin: str,
                           fetcher, client) -> bool:
        """Resolve one case citation and open its window (call from a
        worker thread).  Google Scholar by citation first — retrying as a
        name+citation search — with a pin-cite jump; then the
        CourtListener text.  Returns False when nothing was found."""
        if fetcher is not None:
            result = None
            try:
                result = fetcher.fetch_by_citation(cite)
                if not result and name:
                    hits = fetcher.search_cases(f"{name} {cite}", limit=1)
                    if hits:
                        result = fetcher.fetch_by_url(hits[0].url)
            except Exception as exc:
                print(f"[citelist] scholar {cite!r}: {exc}")
            if result:
                url, html = result

                def open_scholar() -> None:
                    try:
                        w = _ScholarTextWindow(self.root, self, url, html,
                                               item=None)
                        if pin:
                            w.jump_to_cite_page(cite, pin)
                    except tk.TclError:
                        pass

                self._post_root(open_scholar)
                return True
        if client is not None:
            try:
                data = client.search(f"citation:({cite})", type="o",
                                     page_size=1)
                results = data.get("results") or []
                if not results:
                    data = client.search(f'"{cite}"', type="o",
                                         page_size=1)
                    results = data.get("results") or []
                if results:
                    target = results[0]
                    parts, blocks, plain, cluster = _assemble_case_parts(
                        client, target,
                    )
                    if parts or plain:
                        def open_cl() -> None:
                            try:
                                _ScholarTextWindow(
                                    self.root, self, "", "",
                                    item=target, cl_text=plain,
                                    cl_parts=parts, cl_blocks=blocks,
                                )
                            except tk.TclError:
                                pass

                        self._post_root(open_cl)
                        return True
            except Exception as exc:
                print(f"[citelist] courtlistener {cite!r}: {exc}")
        return False

    def _show_citation_list_dialog(self) -> None:
        """Dialog that opens a batch of cases: one citation per line
        ("Monroe v. Pape, 365 U.S. 167, 171 (1961)").  Each line is
        resolved on Google Scholar first (jumping to the pin cite when
        the text is paginated by that reporter), falling back to the
        CourtListener text; the lines that resolved nowhere are listed."""
        dlg = tk.Toplevel(self.root)
        dlg.title("Open Citation List")
        dlg.geometry("560x420")
        dlg.minsize(440, 320)
        frame = ttk.Frame(dlg, padding=10)
        frame.pack(fill="both", expand=True)
        ttk.Label(
            frame,
            text="One citation per line — case name optional, pin cite "
                 "after the page number:",
        ).pack(anchor="w")
        ttk.Label(
            frame, foreground="gray",
            text="e.g.  Monroe v. Pape, 365 U.S. 167, 171 (1961)",
        ).pack(anchor="w", pady=(0, 4))
        box = tk.Text(frame, height=9, wrap="none", undo=True)
        box.pack(fill="both", expand=True)
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=(6, 0))
        open_btn = ttk.Button(row, text="Open All")
        open_btn.pack(side="left")
        status_var = tk.StringVar()
        ttk.Label(row, textvariable=status_var, foreground="gray").pack(
            side="left", padx=8, fill="x", expand=True
        )
        fail_box = tk.Text(frame, height=4, foreground="#a31515",
                           state="disabled")

        def post(fn, *args) -> None:
            try:
                self.root.after(0, fn, *args)
            except tk.TclError:
                pass

        def set_status(s: str) -> None:
            try:
                status_var.set(s)
            except tk.TclError:
                pass

        def show_failures(lines: list[str]) -> None:
            try:
                fail_box.config(state="normal")
                fail_box.delete("1.0", "end")
                fail_box.insert("1.0", "\n".join(lines))
                fail_box.config(state="disabled")
                fail_box.pack(fill="x", pady=(6, 0))
            except tk.TclError:
                pass

        def go() -> None:
            raw = [ln.strip() for ln in box.get("1.0", "end").splitlines()]
            lines = [ln for ln in raw if ln]
            if not lines:
                status_var.set("Nothing to open.")
                return
            entries, failures = [], []
            for ln in lines:
                parsed = _parse_citation_line(ln)
                if parsed:
                    entries.append((ln,) + parsed)
                else:
                    failures.append(f"{ln}   (no citation recognized)")
            fetcher = self._get_scholar() if _SCHOLAR_AVAILABLE else None
            client = (
                self._get_client()
                if self._token_var.get().strip() else None
            )
            if fetcher is None and client is None:
                status_var.set("Neither Google Scholar nor CourtListener "
                               "is available.")
                return
            open_btn.config(state="disabled")
            n, opened = len(entries), [0]

            def run() -> None:
                for i, (ln, name, cite, pin) in enumerate(entries, 1):
                    post(set_status, f"({i}/{n}) Searching {cite}…")
                    if self._try_open_citation(name, cite, pin,
                                               fetcher, client):
                        opened[0] += 1
                    else:
                        failures.append(ln)

                def finish() -> None:
                    try:
                        open_btn.config(state="normal")
                    except tk.TclError:
                        return
                    if failures:
                        set_status(
                            f"Opened {opened[0]} of {len(lines)}; "
                            f"{len(failures)} not found:"
                        )
                        show_failures(failures)
                    else:
                        set_status(f"Opened all {opened[0]} citation(s).")

                post(finish)

            threading.Thread(target=run, daemon=True).start()

        open_btn.config(command=go)

    def _show_quick_lookup(self) -> None:
        """Ctrl+S: one-line lookup that takes either a case citation
        (resolved exactly like a line of the citation-list dialog, pin
        cite included) or a statute/regulation citation."""
        dlg = tk.Toplevel(self.root)
        dlg.title("Quick Look Up")
        dlg.resizable(False, False)
        frame = ttk.Frame(dlg, padding=12)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text="Citation:").grid(row=0, column=0, sticky="w")
        query_var = tk.StringVar()
        entry = ttk.Entry(frame, textvariable=query_var, width=46)
        entry.grid(row=0, column=1, padx=6, sticky="we")
        entry.focus_set()
        status_var = tk.StringVar(
            value="e.g.  Monroe v. Pape, 365 U.S. 167, 171   ·   "
                  "42 USC 1983(b)   ·   29 CFR 1614.105(a)"
        )
        ttk.Label(frame, textvariable=status_var, foreground="gray").grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(8, 0)
        )
        open_btn = ttk.Button(frame, text="Open")
        open_btn.grid(row=0, column=2)
        frame.columnconfigure(1, weight=1)

        def set_status(s: str) -> None:
            try:
                status_var.set(s)
            except tk.TclError:
                pass

        def go(_e=None) -> None:
            q = query_var.get().strip()
            if not q:
                return
            # Statute/regulation first: "42 USC 1983" would otherwise
            # read as volume 42, reporter "USC", page 1983
            statute = _parse_statute_query(q)
            if statute:
                _open_statute_action(self.root, statute, set_status)
                return
            parsed = _parse_citation_line(q)
            if not parsed:
                set_status("Couldn't read that — try a reporter citation "
                           "or '42 USC 1983'.")
                return
            name, cite, pin = parsed
            fetcher = self._get_scholar() if _SCHOLAR_AVAILABLE else None
            client = (
                self._get_client()
                if self._token_var.get().strip() else None
            )
            if fetcher is None and client is None:
                set_status("Neither Google Scholar nor CourtListener "
                           "is available.")
                return
            open_btn.config(state="disabled")
            set_status(f"Searching {cite}…")

            def run() -> None:
                ok = self._try_open_citation(name, cite, pin,
                                             fetcher, client)

                def finish() -> None:
                    try:
                        open_btn.config(state="normal")
                    except tk.TclError:
                        return
                    set_status(f"Opened {cite}." if ok
                               else f"Not found: {cite}")

                self._post_root(finish)

            threading.Thread(target=run, daemon=True).start()

        open_btn.config(command=go)
        entry.bind("<Return>", go)

    def _show_statute_lookup(self) -> None:
        """Small dialog that opens a statute, regulation or federal rule by
        typed citation ("42 USC 1983(b)", "29 CFR 1614.105(a)",
        "Fed. R. Evid. 404(b)") in the statute viewer."""
        dlg = tk.Toplevel(self.root)
        dlg.title("Look Up Statute / Regulation / Rule")
        dlg.resizable(False, False)
        frame = ttk.Frame(dlg, padding=12)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text="Citation:").grid(row=0, column=0, sticky="w")
        query_var = tk.StringVar()
        entry = ttk.Entry(frame, textvariable=query_var, width=38)
        entry.grid(row=0, column=1, padx=6, sticky="we")
        entry.focus_set()
        status_var = tk.StringVar(
            value="e.g.  42 USC 1983(b)   ·   Fed. R. Evid. 404(b)   ·   "
                  "Cal. Penal Code 187   ·   Fla. Stat. 776.012"
        )
        ttk.Label(frame, textvariable=status_var, foreground="gray").grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(8, 0)
        )

        def go(_e=None) -> None:
            parsed = _parse_statute_query(query_var.get())
            if not parsed:
                status_var.set(
                    "Couldn't read that — try '42 USC 1983', "
                    "'29 CFR 1614.105(a)', 'Fed. R. Evid. 404(b)' or "
                    "'Cal. Penal Code 187'."
                )
                return
            # Parent on the root so the statute window outlives the dialog.
            # (A state we only link out to opens in the browser instead.)
            _open_statute_action(self.root, parsed, status_var.set)

        ttk.Button(frame, text="Look Up", command=go).grid(row=0, column=2)
        entry.bind("<Return>", go)
        frame.columnconfigure(1, weight=1)

    def _show_settings_dialog(self) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title("Settings")
        dlg.geometry("460x95")
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.transient(self.root)

        frame = ttk.LabelFrame(dlg, text="CourtListener API Token", padding=10)
        frame.pack(fill="both", expand=True, padx=10, pady=(10, 4))

        ttk.Label(frame, text="Token:").pack(side="left")
        entry = ttk.Entry(frame, textvariable=self._token_var, show="*", width=42)
        entry.pack(side="left", padx=6, fill="x", expand=True)

        show_var = tk.BooleanVar(value=False)

        def _toggle() -> None:
            entry.config(show="" if show_var.get() else "*")

        ttk.Checkbutton(frame, text="Show", variable=show_var, command=_toggle).pack(
            side="left"
        )

        btn_frame = ttk.Frame(dlg)
        btn_frame.pack(fill="x", padx=10, pady=(4, 10))
        ttk.Button(
            btn_frame,
            text="Save & Close",
            command=lambda: (_save_token(self._token_var.get().strip()), dlg.destroy()),
        ).pack(side="right")
        ttk.Button(btn_frame, text="Cancel", command=dlg.destroy).pack(
            side="right", padx=4
        )

    # ------------------------------------------------------------------
    # Court picker
    # ------------------------------------------------------------------

    def _show_court_picker(self) -> None:
        _CourtPickerDialog(self.root, self._selected_courts, self._on_courts_applied)

    def _on_courts_applied(self, selected: set[str]) -> None:
        # Selecting everything is the same as no filter
        if selected >= _all_court_ids():
            selected = set()
        self._selected_courts = selected
        if not selected:
            self._courts_btn_var.set("Courts: All ▾")
        elif len(selected) == 1:
            cid = next(iter(selected))
            label = "SCOTUS" if cid == "scotus" else _COURT_BLUEBOOK.get(cid, cid)
            self._courts_btn_var.set(f"Courts: {label} ▾")
        else:
            self._courts_btn_var.set(f"Courts: {len(selected)} selected ▾")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    _COL_LABELS = {
        "case_name": "Case Name",
        "court": "Court",
        "date_filed": "Date Filed",
        "citation": "Citation",
        "status": "Status",
    }

    def _configure_tree_columns(self, tree: ttk.Treeview) -> None:
        for col, label in self._COL_LABELS.items():
            tree.heading(
                col, text=label,
                command=lambda c=col, t=tree: self._sort_tree(t, c),
            )
        tree.column("case_name", width=310, minwidth=150)
        tree.column("court", width=70, minwidth=50, anchor="center")
        tree.column("date_filed", width=85, minwidth=70, anchor="center")
        tree.column("citation", width=140, minwidth=80)
        tree.column("status", width=110, minwidth=70)

    def _sort_tree(self, tree: ttk.Treeview, col: str) -> None:
        """Sort *tree* by *col*, toggling direction on repeated clicks."""
        current_col, reverse = self._sort_state.get(id(tree), (None, False))
        reverse = (not reverse) if col == current_col else False
        self._sort_state[id(tree)] = (col, reverse)

        rows = [(tree.set(iid, col), iid) for iid in tree.get_children("")]
        rows.sort(key=lambda x: x[0].lower(), reverse=reverse)
        for idx, (_, iid) in enumerate(rows):
            tree.move(iid, "", idx)

        # Update headings to show the active sort indicator.
        for c, label in self._COL_LABELS.items():
            if c == col:
                label += "  ▼" if reverse else "  ▲"
            tree.heading(c, text=label)

    def _format_row(self, item: dict) -> tuple:
        """Return the tuple of column values for inserting a row into the tree."""
        case_name = item.get("caseName") or item.get("case_name") or "(unknown)"
        case_name = re.sub(r"<[^>]+>", "", case_name).strip()
        court = item.get("court") or item.get("court_id") or ""
        date_filed = item.get("dateFiled") or item.get("date_filed") or ""
        citation_str = _pick_citation(item.get("citation", []))
        status = item.get("status") or item.get("precedentialStatus") or ""
        return (case_name, court, date_filed, citation_str, status)

    def _iid_to_idx(self, iid: str) -> int:
        """Convert a tree row iid to an index into self._results."""
        return int(iid)

    def _get_selected_item(self) -> Optional[tuple[int, dict]]:
        """Return (index, result-dict) for whichever tree has a selection."""
        for tree in (self._tree, self._orders_tree):
            sel = tree.selection()
            if sel:
                idx = self._iid_to_idx(sel[0])
                return idx, self._results[idx]
        return None

    def _on_row_select(self, source_tree: ttk.Treeview) -> None:
        sel = source_tree.selection()
        if not sel:
            return
        # Deselect the other trees so only one row is ever active
        other = self._orders_tree if source_tree is self._tree else self._tree
        if other.selection():
            other.selection_remove(*other.selection())
        if self._scholar_tree.selection():
            self._scholar_tree.selection_remove(*self._scholar_tree.selection())
        self._download_btn.config(state="normal")
        self._scholar_btn.config(state="normal")
        self._show_preview(self._iid_to_idx(sel[0]))

    def _on_scholar_row_select(self) -> None:
        sel = self._scholar_tree.selection()
        if not sel:
            return
        for tree in (self._tree, self._orders_tree):
            if tree.selection():
                tree.selection_remove(*tree.selection())
        self._download_btn.config(state="disabled")  # no CourtListener record
        self._scholar_btn.config(state="normal")
        idx = int(sel[0])
        if 0 <= idx < len(self._scholar_results):
            r = self._scholar_results[idx]
            self._set_preview(r.snippet or "(no snippet on the results page)")

    def _selected_scholar_result(self):
        sel = self._scholar_tree.selection()
        if sel:
            idx = int(sel[0])
            if 0 <= idx < len(self._scholar_results):
                return self._scholar_results[idx]
        return None

    def _on_right_click(self, event: tk.Event, tree: ttk.Treeview) -> None:
        """Right-click: open the 'Citing Opinions' window for the clicked row."""
        iid = tree.identify_row(event.y)
        if not iid:
            return
        tree.selection_set(iid)
        idx = self._iid_to_idx(iid)
        if 0 <= idx < len(self._results):
            item = self._results[idx]
            _CitingOpinionsWindow(self.root, self, item)

    def _get_client(self) -> Optional[CourtListenerClient]:
        token = self._token_var.get().strip()
        if not token:
            messagebox.showerror(
                "Missing Token",
                "Please enter your CourtListener API token.\n\n"
                "Go to Settings → API Token…",
            )
            return None
        if self._client is None or self._client._session.headers.get(
            "Authorization"
        ) != f"Token {token}":
            self._client = CourtListenerClient(api_token=token)
            _save_token(token)
        return self._client

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _do_search(self) -> None:
        if self._search_thread and self._search_thread.is_alive():
            return

        client = self._get_client()
        if client is None:
            return

        query = self._query_var.get().strip()
        if not query:
            messagebox.showwarning("Empty Query", "Please enter a search query.")
            return

        # CourtListener accepts space-separated court IDs; empty set = all
        court = " ".join(sorted(self._selected_courts)) or None
        date_from = self._date_from_var.get().strip() or None
        date_to = self._date_to_var.get().strip() or None
        page_size = self._page_size_var.get()

        # Clear previous results
        self._search_btn.config(state="disabled")
        self._download_btn.config(state="disabled")
        self._scholar_btn.config(state="disabled")
        self._status_var.set("Searching…")
        for row in self._tree.get_children():
            self._tree.delete(row)
        for row in self._orders_tree.get_children():
            self._orders_tree.delete(row)
        for row in self._scholar_tree.get_children():
            self._scholar_tree.delete(row)
        self._results.clear()
        self._scholar_results = []
        self._preview_cache.clear()
        self._set_preview("")

        # Google Scholar search runs in parallel with the CourtListener one
        if _SCHOLAR_AVAILABLE:
            if self._scholar is None:
                self._scholar = GoogleScholarFetcher()
            fetcher = self._scholar
            self._scholar_status_var.set("Searching…")

            def scholar_run() -> None:
                try:
                    res = fetcher.search_cases(query, limit=15)
                except Exception as exc:
                    print(f"[scholar] search failed: {exc}")
                    res = []
                self.root.after(0, self._on_scholar_search_results, res)

            threading.Thread(target=scholar_run, daemon=True).start()
        else:
            self._scholar_status_var.set("(needs beautifulsoup4)")

        def run() -> None:
            try:
                data = client.search(
                    query,
                    type="o",
                    court=court,
                    date_filed_min=date_from,
                    date_filed_max=date_to,
                    highlight=True,
                    page_size=page_size,
                )
                self.root.after(0, self._on_results, data)
            except CourtListenerError as exc:
                self.root.after(0, self._on_error, str(exc))
            except Exception as exc:
                self.root.after(0, self._on_error, f"Unexpected error: {exc}")

        self._search_thread = threading.Thread(target=run, daemon=True)
        self._search_thread.start()

    def _on_results(self, data: dict) -> None:
        self._search_btn.config(state="normal")
        results = data.get("results", [])
        count = data.get("count", len(results))
        self._results = results
        # Normalize citations from the API: strip any HTML tags (<mark>, etc.)
        # immediately so every downstream consumer gets clean plain-text strings.
        for item in results:
            raw = item.get("citation")
            if isinstance(raw, list):
                item["citation"] = [re.sub(r"<[^>]+>", "", c).strip() for c in raw]
            elif raw:
                item["citation"] = re.sub(r"<[^>]+>", "", str(raw)).strip()

        for i, item in enumerate(results):
            # Each search result has an 'opinions' list.  The opinion with the
            # most outbound citations is the main opinion for this cluster.
            opinions = item.get("opinions") or []
            main_op = max(opinions, key=lambda o: len(o.get("cites") or []), default=None)

            # Preview text comes from the main opinion's snippet field.
            if main_op:
                raw = main_op.get("snippet") or ""
                text = re.sub(r"<[^>]+>", "", raw).strip()
                if text:
                    self._preview_cache[i] = text

            # Route to orders tree only for SCOTUS cases with ≤ 2 outbound
            # citations.  Published orders don't exist for lower courts, so
            # we leave everything else in the main tree.
            court_val = str(item.get("court_id") or "")
            cites_count = len(main_op.get("cites") or []) if main_op else None
            row = self._format_row(item)
            if "scotus" in court_val and cites_count is not None and cites_count <= 2:
                self._orders_tree.insert("", "end", iid=str(i), values=row)
            else:
                self._tree.insert("", "end", iid=str(i), values=row)

        if results:
            self._status_var.set(
                f"Showing {len(results)} of {count:,} results. "
                "Select a row and click Download PDF (or double-click)."
            )
        else:
            self._status_var.set("No results found.")

    def _set_preview(self, text: str) -> None:
        self._preview_text.config(state="normal")
        self._preview_text.delete("1.0", "end")
        self._preview_text.insert("1.0", text)
        self._preview_text.config(state="disabled")

    def _show_preview(self, idx: int) -> None:
        """Populate the preview strip for CourtListener result at *idx*."""
        text = self._preview_cache.get(idx, "")
        self._set_preview(
            text if text else "(No preview available — download PDF for full opinion)"
        )

    def _on_error(self, message: str) -> None:
        self._search_btn.config(state="normal")
        self._status_var.set(f"Error: {message}")
        messagebox.showerror("API Error", message)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _download_selected(self) -> None:
        selected = self._get_selected_item()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a case first.")
            return

        idx, item = selected

        safe_name = _build_default_filename(item)

        save_path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{safe_name}.pdf",
            title="Save Opinion PDF",
        )
        if not save_path:
            return

        client = self._get_client()
        if client is None:
            return

        self._status_var.set("Resolving PDF URL…")
        self._download_btn.config(state="disabled")
        self._scholar_btn.config(state="disabled")
        self._search_btn.config(state="disabled")

        def run() -> None:
            try:
                print(f"\n[download] raw item keys: {list(item.keys())}")
                print(f"[download] local_path   = {item.get('local_path') or item.get('localPath')!r}")
                print(f"[download] download_url = {item.get('download_url')!r}")
                print(f"[download] cluster_id   = {item.get('cluster_id') or item.get('id')!r}")

                pdf_url = self._resolve_pdf_url(client, item)
                print(f"[download] resolved url = {pdf_url!r}")

                if not pdf_url:
                    # Last-ditch: assemble full case text from CourtListener
                    # (cluster metadata + all sub-opinions) and save as .txt.
                    cluster_id = item.get("cluster_id") or item.get("id")
                    if cluster_id:
                        try:
                            self.root.after(
                                0, self._status_var.set,
                                "No PDF found — fetching opinion text from CourtListener…"
                            )
                            print(f"[download] no PDF found; assembling text for cluster {cluster_id}")
                            text = _assemble_case_text(client, item)
                            if text.strip():
                                txt_path = os.path.splitext(save_path)[0] + ".txt"
                                with open(txt_path, "w", encoding="utf-8") as f:
                                    f.write(text)
                                self.root.after(0, self._on_text_download_done, txt_path)
                                return
                        except Exception as exc:
                            print(f"[download] text assembly failed: {exc}")
                    self.root.after(
                        0,
                        self._on_error,
                        "No downloadable PDF or text found for this opinion.",
                    )
                    return

                self.root.after(0, self._status_var.set, f"Downloading… {pdf_url}")
                print(f"[download] fetching {pdf_url}")
                # Only send the CourtListener API key to CourtListener itself.
                # Use a browser-like UA for all other hosts; government CDNs
                # (LOC, GovInfo) reject Python's default User-Agent.
                if "courtlistener.com" in pdf_url:
                    response = client._session.get(pdf_url, timeout=60, stream=True)
                else:
                    response = _anon_session.get(pdf_url, timeout=60, stream=True)
                ct = response.headers.get("content-type", "")
                print(f"[download] HTTP {response.status_code}  content-type: {ct}")
                response.raise_for_status()

                with open(save_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                self.root.after(0, self._on_download_done, save_path)
            except Exception as exc:
                self.root.after(0, self._on_error, f"Download failed: {exc}")
            finally:
                self.root.after(0, self._restore_buttons)

        threading.Thread(target=run, daemon=True).start()

    def _resolve_pdf_url(
        self, client: CourtListenerClient, item: dict
    ) -> Optional[str]:
        """
        Attempt to find a PDF URL for the selected search result.

        Strategy (local_path always preferred over download_url):
        0. US Reports citation in LOC collection (vols 1-542) → LOC CDN PDF.
           Vols 543+ skip this step and fall through to local_path.
        0.5. Non-SCOTUS cases: try static.case.law (Harvard CAP) first.
             Only falls through if the URL returns a non-200 response.
        1. local_path from the search result (if already present).
        2. Fetch the opinion directly by ID to get its local_path.
        3. download_url from the search result (original court source).
        4. download_url from the fetched opinion record.
        5. Walk the cluster's sub_opinions checking local_path then download_url.
        """
        storage_base = "https://storage.courtlistener.com/"

        # Determine whether this is a SCOTUS case.
        court_val = str(item.get("court_id") or "")
        is_scotus = "scotus" in court_val

        def _head_ok(url: str, label: str) -> bool:
            try:
                resp = _anon_session.head(url, timeout=10, allow_redirects=True)
                if resp.status_code == 200:
                    return True
                print(f"[resolve] {label} returned {resp.status_code}: {url}")
            except Exception as exc:
                print(f"[resolve] {label} check failed ({exc}): {url}")
            return False

        # Gather EVERY citation we know — the search result often exposes only
        # one (frequently a nominative reporter like "19 How. 393"), while the
        # parallel U.S./F. cite that finds a PDF lives on the cluster record.
        all_cites = _gather_all_citations(client, item)
        print(f"[resolve] citations to try: {all_cites}")

        # 0. Official US Reports PDF — try every U.S.-Reports cite among them.
        #    vols 1-542 → LOC CDN; otherwise GovInfo (link service + direct PDF).
        for cite in all_cites:
            loc_url = _us_reports_loc_url(cite)
            gov = _us_reports_govinfo_url(cite)
            if not loc_url and not gov:
                continue
            if loc_url and _head_ok(loc_url, "LOC US Reports"):
                print(f"[resolve] using LOC US Reports PDF: {loc_url}")
                return loc_url
            if gov:
                link_url, direct_url = gov
                if _head_ok(link_url, "GovInfo link"):
                    print(f"[resolve] using GovInfo link URL: {link_url}")
                    return link_url
                if _head_ok(direct_url, "GovInfo direct PDF"):
                    print(f"[resolve] using GovInfo direct PDF URL: {direct_url}")
                    return direct_url

        # 0.5. Non-SCOTUS: the Harvard CAP static.case.law copy.  Try every
        #      parallel cite before giving up.
        if not is_scotus:
            for cite in all_cites:
                if "lexis" in cite.lower():
                    continue
                scl_url = _static_case_law_url(cite)
                if not scl_url:
                    continue
                print(f"[resolve] checking static.case.law: {scl_url}")
                try:
                    head = _anon_session.head(scl_url, timeout=10,
                                              allow_redirects=True)
                    if head.status_code == 200:
                        print(f"[resolve] using static.case.law PDF: {scl_url}")
                        return scl_url
                    print(f"[resolve] static.case.law {head.status_code} for {cite!r}")
                except Exception as exc:
                    print(f"[resolve] static.case.law check failed: {exc}")

        # 1. local_path already present on the search result
        local = item.get("local_path") or item.get("localPath") or ""
        if local:
            url = storage_base + local.lstrip("/")
            if _head_ok(url, "local_path (search result)"):
                print(f"[resolve] using local_path from search result: {local}")
                return url

        # 2. Fetch the opinion directly to get its local_path (preferred over
        #    download_url — CourtListener's stored copy is more reliable than
        #    the original court URL).
        opinion_id = item.get("id")
        fetched_op: Optional[dict] = None
        if opinion_id:
            try:
                print(f"[resolve] fetching opinion {opinion_id} for local_path")
                fetched_op = client.get_opinion(int(opinion_id))
                print(f"[resolve] opinion local_path = {fetched_op.get('local_path')!r}")
                print(f"[resolve] opinion download_url = {fetched_op.get('download_url')!r}")
                local = fetched_op.get("local_path") or ""
                if local:
                    url = storage_base + local.lstrip("/")
                    if _head_ok(url, "local_path (opinion record)"):
                        print(f"[resolve] using local_path from opinion record")
                        return url
            except Exception as exc:
                print(f"[resolve] direct opinion fetch failed: {exc}")

        # 3. download_url from the search result (original court source)
        url = item.get("download_url") or ""
        if url:
            if _head_ok(url, "download_url (search result)"):
                print(f"[resolve] using download_url from search result: {url}")
                return url

        # 4. download_url from the fetched opinion record
        if fetched_op:
            dl = fetched_op.get("download_url") or ""
            if dl:
                if _head_ok(dl, "download_url (opinion record)"):
                    print(f"[resolve] using download_url from opinion record: {dl}")
                    return dl

        # 5. Fall back to cluster → sub_opinions walk
        cluster_id = item.get("cluster_id") or item.get("id")
        if cluster_id:
            try:
                print(f"[resolve] fetching cluster {cluster_id}")
                cluster = client.get_cluster(int(cluster_id), fields="sub_opinions")
                print(f"[resolve] sub_opinions = {cluster.get('sub_opinions')!r}")
                for op_url in cluster.get("sub_opinions", []):
                    print(f"[resolve] fetching sub-opinion {op_url}")
                    op = client._get_url(op_url, {"fields": "download_url,local_path"})
                    print(f"[resolve]   local_path={op.get('local_path')!r}  download_url={op.get('download_url')!r}")
                    local = op.get("local_path") or ""
                    if local:
                        url = storage_base + local.lstrip("/")
                        if _head_ok(url, "local_path (sub-opinion)"):
                            return url
                    dl = op.get("download_url") or ""
                    if dl:
                        if _head_ok(dl, "download_url (sub-opinion)"):
                            return dl
            except Exception as exc:
                print(f"[resolve] cluster walk failed: {exc}")

        return None

    # ------------------------------------------------------------------
    # Google Scholar text fetch
    # ------------------------------------------------------------------

    def _get_scholar(self) -> Optional["GoogleScholarFetcher"]:
        if not _SCHOLAR_AVAILABLE:
            messagebox.showerror(
                "Missing Dependency",
                "Google Scholar fetching requires beautifulsoup4.\n\n"
                "Install it with:\n    pip install beautifulsoup4",
            )
            return None
        if self._scholar is None:
            self._scholar = GoogleScholarFetcher()
        return self._scholar

    def _on_scholar_search_results(self, results: list) -> None:
        self._scholar_results = results
        for row in self._scholar_tree.get_children():
            self._scholar_tree.delete(row)
        for i, r in enumerate(results):
            self._scholar_tree.insert("", "end", iid=str(i), values=(r.title, r.source))
        self._scholar_status_var.set(
            f"{len(results)} results" if results else "no results (blocked?)"
        )

    def _open_selected_scholar_result(self) -> None:
        r = self._selected_scholar_result()
        if r is not None:
            self._open_scholar_url(r.url)

    def _open_scholar_url(self, url: str) -> None:
        """Open a Scholar case page (from the Scholar results column)."""
        fetcher = self._get_scholar()
        if fetcher is None:
            return
        self._status_var.set("Fetching opinion from Google Scholar…")

        def run() -> None:
            result = fetcher.fetch_by_url(url)
            self.root.after(
                0, self._on_scholar_result, result, None, None,
                "opened from Scholar search",
            )

        threading.Thread(target=run, daemon=True).start()

    def _fetch_scholar_text(self) -> None:
        # A row in the Scholar results column: open it directly, unverified.
        if self._selected_scholar_result() is not None:
            self._open_selected_scholar_result()
            return

        selected = self._get_selected_item()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a case first.")
            return

        fetcher = self._get_scholar()
        if fetcher is None:
            return
        client = self._get_client()
        _, item = selected

        self._download_btn.config(state="disabled")
        self._scholar_btn.config(state="disabled")
        self._search_btn.config(state="disabled")
        self._status_var.set("Searching Google Scholar…")

        cluster_id = item.get("cluster_id") or item.get("id")
        vkey = f"verified:cluster:{cluster_id}" if cluster_id else ""

        # Check verified cache first — if found, open immediately
        if vkey:
            cached = fetcher.get_cached(vkey)
            if cached:
                self._restore_buttons()
                self._status_var.set("Scholar text loaded (cached).")
                _ScholarTextWindow(
                    self.root, self, cached[0], cached[1],
                    item=item, note="verified match (cached)",
                )
                return

        def status_cb(msg: str) -> None:
            self.root.after(0, self._status_var.set, msg)

        def run() -> None:
            # Step 1: Quick fetch — get the first Scholar result fast
            primary = _pick_citation(item.get("citation", []))
            quick_result = None
            if primary:
                try:
                    quick_result = fetcher.fetch_by_citation(primary)
                except Exception:
                    pass

            if quick_result:
                # Show the result immediately (unverified)
                url, html = quick_result
                self.root.after(
                    0, self._on_scholar_quick_show,
                    url, html, item, fetcher, client,
                )
            else:
                # No quick result — fall through to the full search
                try:
                    result, cl_text, note = _find_scholar_for_item(
                        client, fetcher, item, status_cb,
                    )
                except Exception as exc:
                    import traceback
                    traceback.print_exc()
                    result, cl_text, note = None, None, str(exc)
                self.root.after(
                    0, self._on_scholar_result, result, item, cl_text, note,
                )

        threading.Thread(target=run, daemon=True).start()

    def _on_scholar_quick_show(
        self, url: str, html: str, item: dict,
        fetcher, client,
    ) -> None:
        """Open the Scholar text immediately, then verify in background."""
        self._restore_buttons()
        self._status_var.set("Scholar text loaded — verifying…")
        win = _ScholarTextWindow(
            self.root, self, url, html, item=item,
            note="verifying against CourtListener…",
        )

        def verify() -> None:
            cluster_id = item.get("cluster_id") or item.get("id")
            vkey = f"verified:cluster:{cluster_id}" if cluster_id else ""
            cl_text: Optional[str] = None
            if client is not None and cluster_id:
                try:
                    cl_text = _assemble_case_text(client, item)
                except Exception:
                    pass
            if cl_text is None:
                # Can't verify — accept what we have
                self.root.after(0, self._on_verify_done, win, True,
                                url, html, cl_text, None, vkey, fetcher)
                return
            sim = text_similarity(
                blocks_to_text(parse_opinion_blocks(html)), cl_text,
            )
            if sim >= _SCHOLAR_MATCH_THRESHOLD:
                if vkey:
                    fetcher.put_cached(vkey, url, html)
                self.root.after(0, self._on_verify_done, win, True,
                                url, html, cl_text, sim, vkey, fetcher)
                return
            # First result didn't match — run full search for better match
            def status_cb(msg: str) -> None:
                self.root.after(0, self._status_var.set, msg)
            try:
                result, cl_text2, note = _find_scholar_for_item(
                    client, fetcher, item, status_cb,
                )
            except Exception:
                result = None
                cl_text2 = cl_text
            self.root.after(0, self._on_verify_done, win, False,
                            url, html, cl_text2 or cl_text, sim,
                            vkey, fetcher, result)

        threading.Thread(target=verify, daemon=True).start()

    def _on_verify_done(
        self, win, matched: bool, orig_url: str, orig_html: str,
        cl_text: Optional[str], sim: Optional[float],
        vkey: str, fetcher, better_result=None,
    ) -> None:
        try:
            if not win._win.winfo_exists():
                return
        except tk.TclError:
            return

        if matched:
            note = "verified against CourtListener"
            if sim is None:
                note = "unverified (no CourtListener text)"
            win._note = note
            if cl_text is not None:
                win._cl_text = cl_text
            win._status_var.set(
                f"{len(win._scholar_text):,} characters | "
                f"Google Scholar version | {note}"
            )
            return

        # Verification failed
        if better_result is not None:
            url2, html2 = better_result
            # Replace the window contents with the verified match
            win._scholar_url = url2
            win._source_var.set(url2)
            win._blocks = parse_opinion_blocks(html2)
            win._scholar_text = (
                blocks_to_text(win._blocks) or _strip_html(html2)
            )
            win._parts = segment_blocks(win._blocks)
            win._refine_part_labels(win._parts)
            win._note = "verified against CourtListener (replaced)"
            if cl_text is not None:
                win._cl_text = cl_text
            # Rebuild part selector
            part_values = ["Full opinion"] + [
                f"{i + 1}. {p.label}" for i, p in enumerate(win._parts)
            ]
            win._part_combo.config(values=part_values)
            win._current_part = None
            win._part_combo.current(0)
            win._render_scholar()
            self._status_var.set(
                "Scholar text replaced — initial result didn't match."
            )
        else:
            # No better match found — warn the user
            sim_pct = f"{sim:.0%}" if sim is not None else "unknown"
            win._note = f"unverified (similarity {sim_pct})"
            win._status_var.set(
                f"{len(win._scholar_text):,} characters | "
                f"Google Scholar version | WARNING: may be wrong case "
                f"(similarity {sim_pct})"
            )
            messagebox.showwarning(
                "Possible Mismatch",
                f"The Google Scholar text shown may not match this case.\n\n"
                f"Best similarity score: {sim_pct}\n"
                f"Threshold: {_SCHOLAR_MATCH_THRESHOLD:.0%}\n\n"
                f"The text is displayed but may be for a different case.",
                parent=win._win,
            )

    def _spotlight_verify_scholar(
        self, win, url: str, html: str, item: dict, fetcher, client,
    ) -> None:
        """Background verification for Scholar text opened from the spotlight.

        Same logic as ``_on_scholar_quick_show``'s verify thread, but
        self-contained — doesn't touch the main window's status bar or
        buttons.
        """
        cluster_id = item.get("cluster_id") or item.get("id")
        vkey = f"verified:cluster:{cluster_id}" if cluster_id else ""
        cl_text: Optional[str] = None
        if client is not None and cluster_id:
            try:
                cl_text = _assemble_case_text(client, item)
            except Exception:
                pass
        if cl_text is None:
            self.root.after(0, self._on_verify_done, win, True,
                            url, html, cl_text, None, vkey, fetcher)
            return
        sim = text_similarity(
            blocks_to_text(parse_opinion_blocks(html)), cl_text,
        )
        if sim >= _SCHOLAR_MATCH_THRESHOLD:
            if vkey:
                fetcher.put_cached(vkey, url, html)
            self.root.after(0, self._on_verify_done, win, True,
                            url, html, cl_text, sim, vkey, fetcher)
            return
        try:
            result, cl_text2, note = _find_scholar_for_item(
                client, fetcher, item, lambda m: None,
            )
        except Exception:
            result = None
            cl_text2 = cl_text
        self.root.after(0, self._on_verify_done, win, False,
                        url, html, cl_text2 or cl_text, sim,
                        vkey, fetcher, result)

    def _on_scholar_result(
        self,
        result: Optional[tuple[str, str]],
        item: Optional[dict] = None,
        cl_text: Optional[str] = None,
        note: str = "",
    ) -> None:
        self._restore_buttons()
        if result is None:
            target_item = dict(item) if item else {}
            has_cluster = bool(
                target_item.get("cluster_id") or target_item.get("id")
            )
            if not has_cluster:
                self._status_var.set("Google Scholar text unavailable.")
                messagebox.showwarning(
                    "Scholar Text Unavailable",
                    "Could not find a Google Scholar opinion matching this case."
                    + (f"\n\n({note})" if note else ""),
                )
                return
            self._status_var.set(
                "Scholar unavailable — loading CourtListener text…"
            )
            self._scholar_btn.config(state="disabled")
            client = self._get_client()
            if client is None:
                self._status_var.set("Google Scholar text unavailable.")
                return

            def run() -> None:
                try:
                    parts, blocks, plain, cluster = _assemble_case_parts(
                        client, target_item,
                    )
                except Exception as exc:
                    import traceback
                    traceback.print_exc()
                    parts, blocks, plain, cluster = [], [], "", {}
                self.root.after(
                    0, self._on_cl_fallback_ready,
                    parts, blocks, plain, target_item, cl_text, note,
                )

            threading.Thread(target=run, daemon=True).start()
            return

        url, html = result
        self._status_var.set(
            f"Scholar text loaded — {note}" if note else f"Scholar text loaded from {url}"
        )
        _ScholarTextWindow(
            self.root, self, url, html, item=item, cl_text=cl_text, note=note
        )

    def _on_cl_fallback_ready(
        self, parts, blocks, plain, item, cl_text, note,
    ) -> None:
        self._restore_buttons()
        if not parts and not blocks:
            self._status_var.set("Google Scholar text unavailable.")
            messagebox.showwarning(
                "Scholar Text Unavailable",
                "Could not find a Google Scholar opinion matching this case,\n"
                "and CourtListener text could not be loaded either.\n\n"
                + (f"({note})" if note else ""),
            )
            return
        self._status_var.set("Loaded CourtListener text (Scholar unavailable).")
        _ScholarTextWindow(
            self.root, self, "", "",
            item=item, cl_text=cl_text or plain, note=note,
            cl_parts=parts, cl_blocks=blocks,
        )

    def _restore_buttons(self) -> None:
        self._download_btn.config(state="normal")
        self._scholar_btn.config(state="normal")
        self._search_btn.config(state="normal")

    def _on_download_done(self, path: str) -> None:
        self._status_var.set(f"Saved: {path}")
        if messagebox.askyesno(
            "Download Complete", f"PDF saved to:\n{path}\n\nOpen it now?"
        ):
            self._open_file(path)

    def _on_text_download_done(self, path: str) -> None:
        self._status_var.set(f"Saved: {path}")
        if messagebox.askyesno(
            "Text Saved",
            f"No PDF was available.\nOpinion text saved to:\n{path}\n\nOpen it now?",
        ):
            self._open_file(path)

    @staticmethod
    def _open_file(path: str) -> None:
        if sys.platform == "win32":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])


class _CourtPickerDialog:
    """
    Checkbox-tree dialog for choosing which courts to search.

    The tree mirrors ``court_catalog.CATALOG``: Federal (Supreme Court,
    Courts of Appeals, District Courts, Specialized) and State (each state
    with its appellate courts).  Clicking a group toggles everything under
    it; groups show ☑ / ☐ / ◪ for all / none / some selected.  An empty
    selection means "all courts" (no filter).
    """

    _GLYPH_ALL, _GLYPH_NONE, _GLYPH_SOME = "☑", "☐", "◪"

    def __init__(
        self,
        parent: tk.Misc,
        selected: set[str],
        on_apply,
    ) -> None:
        self._on_apply = on_apply
        self._checked: set[str] = set(selected)
        self._labels: dict[str, str] = {}        # tree iid → bare label
        self._group_leaves: dict[str, set[str]] = {}  # group iid → descendant ids

        win = tk.Toplevel(parent)
        self._win = win
        win.title("Select Courts")
        win.geometry("440x560")
        win.minsize(360, 400)
        win.transient(parent)
        win.grab_set()

        tree_frame = ttk.Frame(win)
        tree_frame.pack(fill="both", expand=True, padx=8, pady=(8, 4))
        self._tree = ttk.Treeview(tree_frame, show="tree", selectmode="none")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        self._tree.pack(side="left", fill="both", expand=True)

        self._build_nodes("", _COURT_CATALOG)
        # Open the two top-level branches so the structure is visible
        for iid in self._tree.get_children(""):
            self._tree.item(iid, open=True)
        self._refresh_glyphs()
        self._tree.bind("<Button-1>", self._on_click)

        bot = ttk.Frame(win)
        bot.pack(fill="x", padx=8, pady=(0, 8))
        self._count_var = tk.StringVar()
        ttk.Label(bot, textvariable=self._count_var, foreground="gray").pack(
            side="left"
        )
        ttk.Button(bot, text="Apply", command=self._apply).pack(side="right")
        ttk.Button(bot, text="Cancel", command=win.destroy).pack(
            side="right", padx=4
        )
        ttk.Button(bot, text="Clear", command=self._clear).pack(side="right", padx=4)
        self._update_count()

    # -- tree construction ---------------------------------------------------

    def _build_nodes(self, parent_iid: str, nodes) -> set[str]:
        leaves: set[str] = set()
        for label_or_id, payload in nodes:
            if isinstance(payload, list):
                iid = self._tree.insert(parent_iid, "end", text=label_or_id)
                self._labels[iid] = label_or_id
                sub = self._build_nodes(iid, payload)
                self._group_leaves[iid] = sub
                leaves |= sub
            else:
                cid, label = label_or_id, payload
                self._tree.insert(parent_iid, "end", iid=cid, text=label)
                self._labels[cid] = label
                leaves.add(cid)
        return leaves

    # -- interaction -----------------------------------------------------------

    def _on_click(self, event: tk.Event) -> None:
        # Let clicks on the expander triangle expand/collapse as usual
        if "indicator" in self._tree.identify_element(event.x, event.y):
            return
        iid = self._tree.identify_row(event.y)
        if not iid:
            return
        if iid in self._group_leaves:
            leaves = self._group_leaves[iid]
            if leaves <= self._checked:
                self._checked -= leaves
            else:
                self._checked |= leaves
        else:
            self._checked.symmetric_difference_update({iid})
        self._refresh_glyphs()
        self._update_count()

    def _refresh_glyphs(self) -> None:
        for iid, label in self._labels.items():
            if iid in self._group_leaves:
                leaves = self._group_leaves[iid]
                if leaves and leaves <= self._checked:
                    glyph = self._GLYPH_ALL
                elif leaves & self._checked:
                    glyph = self._GLYPH_SOME
                else:
                    glyph = self._GLYPH_NONE
            else:
                glyph = self._GLYPH_ALL if iid in self._checked else self._GLYPH_NONE
            self._tree.item(iid, text=f"{glyph} {label}")

    def _update_count(self) -> None:
        n = len(self._checked)
        self._count_var.set(
            "All courts (no filter)" if n == 0 else f"{n} court(s) selected"
        )

    def _clear(self) -> None:
        self._checked.clear()
        self._refresh_glyphs()
        self._update_count()

    def _apply(self) -> None:
        self._on_apply(set(self._checked))
        self._win.destroy()


_OP_ID_RE = re.compile(r"/opinions/(\d+)/?")

# A pinpoint page following a case citation: ", 171" or ", 171-72" — but
# not the volume of a parallel citation (", 510 A.2d 562"), recognized by
# the capital letter that follows the number.
_PINCITE_AFTER_RE = re.compile(
    r",\s*(\d{1,5})(?:\s*[-–—]\s*\d{1,5})?(?!\d|\s*[A-Z])"
)


def _extract_opinion_id(url: str) -> Optional[int]:
    """Parse an opinion ID out of a CourtListener opinions URL."""
    m = _OP_ID_RE.search(str(url))
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# RTF generation + rich clipboard (used by the Scholar text window)
# ---------------------------------------------------------------------------

# Citations recognized inside opinion text (made clickable → Scholar lookup).
# Pattern: volume, reporter abbreviation, page.
_TEXT_CITE_RE = re.compile(
    r"\b\d{1,4}\s+"
    r"(?:U\.\s?S\.(?!\s?C)|S\.\s?Ct\.|L\.\s?Ed\.(?:\s?2d)?|"
    r"F\.\s?Supp\.(?:\s?[23]d)?|F\.\s?(?:2d|3d|4th)|F\.\s?App[’']x|Fed\.\s?Appx\.|B\.R\.|"
    r"A\.(?:2d|3d)?|P\.(?:2d|3d)?|N\.E\.(?:2d|3d)?|N\.W\.(?:2d)?|S\.E\.(?:2d)?|"
    r"S\.W\.(?:2d|3d)?|So\.(?:\s?[23]d)?|Cal\.\s?Rptr\.(?:\s?[23]d)?|"
    r"N\.Y\.S\.(?:2d|3d)?|Ohio\s?St\.\s?(?:2d|3d)?|Ill\.\s?2d|Wis\.\s?2d|Wn\.\s?(?:2d|App\.))"
    r"\s+\d{1,5}\b"
)


# A citation line in the Scholar header: each parallel cite sits on its own
# centered line, e.g. "306 Md. 556 (1986)" / "510 A.2d 562" / "87 F.4th 563 (2023)"
_HEADER_CITE_RE = re.compile(
    r"^\s*(\d{1,4})\s+([A-Z][A-Za-z0-9.'’ ]{0,24}?)\s+(\d{1,5})\s*(?:\(|$)"
)

# A line that is *only* a reporter citation (optionally with a year), e.g.
# "512 U.S. 477 (1994)" — the running reference at the top of an opinion.
_CITE_ONLY_LINE_RE = re.compile(
    r"^\d{1,4}\s+[A-Z][A-Za-z0-9.'’ ]{0,30}?\s+\d{1,5}(?:\s*\(\d{4}\))?$"
)

# Marker opening a footnote body, e.g. "[4] …" or "* …" (fallback when the
# parser found no footnote anchor ids)
_FN_BODY_MARK_RE = re.compile(r"^\s*(?:\[([^\]\s]{1,6})\]|(\*{1,3}|†|‡))(?=\s|$)")


def _fix_name_case(name: str) -> str:
    """Render an all-caps surname from an opinion header in normal case:
    REHNQUIST → Rehnquist, O'CONNOR → O'Connor, McAULIFFE → McAuliffe."""
    def fix(wd: str) -> str:
        alpha = [c for c in wd if c.isalpha()]
        if len(alpha) <= 2 or sum(c.isupper() for c in alpha) <= len(alpha) // 2:
            return wd  # already mixed case (Wood, St.)
        out = "'".join(
            p[:1].upper() + p[1:].lower() if p else p for p in wd.split("'")
        )
        if out.startswith("Mc") and len(out) > 2:
            out = "Mc" + out[2].upper() + out[3:]
        return out

    return " ".join(fix(w) for w in name.split())


_CAPTION_SMALL_WORDS = {
    "of", "the", "and", "on", "in", "for", "a", "an", "ex", "rel", "re", "et", "al",
}


def _titlecase_caps(s: str) -> str:
    """Normal-case an all-caps caption fragment: 'MERCY HOSPITAL' →
    'Mercy Hospital', 'UNITED STATES' → 'United States', 'IN RE GAULT' →
    'In re Gault'.  Mixed-case words and abbreviations pass through."""
    out: list[str] = []
    for i, w in enumerate(s.split()):
        if not w.isupper() or re.fullmatch(r"(?:[A-Z]\.)+,?", w):
            out.append(w)
            continue
        core = w.lower()
        if i > 0 and core.strip(".,'") in _CAPTION_SMALL_WORDS:
            out.append(core)
            continue
        word = "'".join(p[:1].upper() + p[1:] if p else p for p in core.split("'"))
        if word.startswith("Mc") and len(word) > 2:
            word = "Mc" + word[2].upper() + word[3:]
        out.append(word)
    return " ".join(out)


def _caption_party(s: str) -> str:
    """One side of a Scholar caption → its Bluebook party name.  Drops the
    procedural designation and 'et al.'; when Scholar mixes cases, the
    all-caps run is the operative name ('Brent BREWBAKER' → 'Brewbaker',
    'UNITED STATES of America' → 'United States')."""
    s = s.split(",")[0].strip().strip(".;")
    s = re.sub(r"\s+et\s+al\.?$", "", s, flags=re.IGNORECASE).strip()
    tokens = s.split()
    caps = [w for w in tokens if w.isupper() and len(w.strip(".,'")) > 1]
    if caps and len(caps) < len(tokens):
        s = " ".join(caps)
    return _titlecase_caps(s)


_CIRCUIT_ORDINALS = {
    # Spelled-out (opinion headers) and digit ordinals (results bylines).
    "first": "ca1", "second": "ca2", "third": "ca3", "fourth": "ca4",
    "fifth": "ca5", "sixth": "ca6", "seventh": "ca7", "eighth": "ca8",
    "ninth": "ca9", "tenth": "ca10", "eleventh": "ca11",
    "1st": "ca1", "2nd": "ca2", "3rd": "ca3", "4th": "ca4", "5th": "ca5",
    "6th": "ca6", "7th": "ca7", "8th": "ca8", "9th": "ca9", "10th": "ca10",
    "11th": "ca11",
}

# Google Scholar prefixes a state result's court with the state's Bluebook
# abbreviation minus periods/spaces ("N.D." → "ND", "Cal." → "Cal").  Map
# that key to the state's court list (court of last resort first) so the
# prefix selects the right CourtListener court ids.
_SCHOLAR_STATE_PREFIX: dict[str, list[tuple[str, str, str]]] = {}
for _state_name, _state_courts in _STATE_COURTS:
    _pref_key = _state_courts[0][1].replace(".", "").replace(" ", "").lower()
    _SCHOLAR_STATE_PREFIX.setdefault(_pref_key, _state_courts)


def _classify_state_court(text: str, courts: list[tuple[str, str, str]]) -> str:
    """Pick a state's CourtListener court id from a court description,
    matching against the catalog's court labels (rule of last resort first)."""
    t = re.sub(r"\s+", " ", text or "").strip().lower()
    high = courts[0][0]
    inter = courts[1][0] if len(courts) > 1 else ""

    def by_label(*keywords: str) -> str:
        for cid, _abbr, label in courts:
            ll = label.lower()
            if any(k in ll for k in keywords):
                return cid
        return ""

    # Most specific named courts first, matched to the catalog's labels.
    if "criminal" in t:
        hit = by_label("criminal")
        if hit:
            return hit
    if "civil" in t:
        hit = by_label("civil")
        if hit:
            return hit
    if "appellate division" in t:
        return "nyappdiv" if high == "ny" else (by_label("appellate division")
                                                or inter or high)
    if "special appeals" in t:
        return by_label("special") or inter or high
    if "commonwealth" in t:
        return by_label("commonwealth") or inter or high
    if "superior" in t:
        return by_label("superior") or inter or high
    # Generic intermediate appellate court (its name varies by state:
    # "Court of Appeal(s)", "Appeals Court", "Appellate Court", "District
    # Court of Appeal").  Maryland, New York, and D.C. name their highest
    # court this way instead.
    if (re.search(r"courts? of appeal", t) or "appeals court" in t
            or "appellate court" in t):
        return high if high in ("md", "ny", "dc") else (inter or high)
    if "supreme" in t:
        return high
    return high


def _scholar_court_id(blocks) -> str:
    """CourtListener court ID inferred from the Scholar header's court line
    (used when a case was opened from Scholar with no CourtListener record)."""
    for b in blocks[:8]:
        if b.kind != "center":
            continue
        t = re.sub(r"\s+", " ", b.text()).strip().rstrip(".").lower()
        if not t or "court" not in t or "district court" in t or "bankruptcy" in t:
            continue
        if "supreme court" in t and "united states" in t:
            return "scotus"
        m = re.search(
            r"court of appeals,? (?:for the )?(\w+(?: of columbia)?) circuit", t
        )
        if m:
            word = m.group(1)
            if word == "federal":
                return "cafc"
            if "columbia" in word:
                return "cadc"
            return _CIRCUIT_ORDINALS.get(word, "")
        for state, courts in _STATE_COURTS:
            if state.lower() not in t:
                continue
            return _classify_state_court(t, courts)
    return ""


def _scholar_caption_name(blocks) -> str:
    """Bluebook case name derived from the Scholar page's party caption."""
    for b in blocks[:8]:
        if b.kind != "center":
            continue
        t = re.sub(r"\s+", " ", b.text()).strip().rstrip(".")
        if not t or _HEADER_CITE_RE.match(t) or t.startswith(("No.", "Nos.")):
            continue
        # Google Scholar renders the party separator in lowercase ("… v. …")
        # even for ALL-CAPS captions ("MERCY HOSPITAL, INC. v. JACKSON"), so
        # a lowercase "v."/"vs." is the reliable separator and never collides
        # with an uppercase middle initial like the "V." in "Francis V.
        # Lorenzo".  Only fall back to a case-insensitive split (for a caption
        # that happens to capitalize the separator) when no lowercase one is
        # found.
        sides = re.split(r"\s+vs?\.\s+", t, maxsplit=1)
        if len(sides) != 2:
            sides = re.split(r"\s+[vV]s?\.\s+", t, maxsplit=1)
        if len(sides) == 2:
            left, right = _caption_party(sides[0]), _caption_party(sides[1])
            if left and right:
                return f"{left} v. {right}"
        if re.match(r"(?:IN\s+RE|EX\s+PARTE|(?:IN\s+THE\s+)?MATTER\s+OF)\b", t, re.IGNORECASE):
            return _titlecase_caps(t.split(",")[0].strip())
    return ""


def _scholar_source_segments(source: str) -> list[str]:
    """A Scholar result byline reads "<citations> - <court>, <year> -
    Google Scholar"; split it on the dashes and drop the publisher tail."""
    segs = [s.strip() for s in re.split(r"\s+-\s+", source or "") if s.strip()]
    while segs and segs[-1].lower() == "google scholar":
        segs.pop()
    return segs


def _scholar_court_desc_to_id(desc: str) -> str:
    """Map a Scholar court description to a CourtListener court id.  Handles
    state-prefixed bylines ("Cal: Court of Appeal", "La: Court of Appeals,
    4th Circuit") and federal ones ("Supreme Court", "Court of Appeals, 9th
    Circuit"), keeping a state's own appellate circuits out of the federal
    circuits."""
    desc = re.sub(r"\s+", " ", desc or "").strip().rstrip(".")
    if not desc:
        return ""
    # State-prefixed: a Bluebook state abbreviation, then a colon.
    m = re.match(r"([A-Za-z][A-Za-z.]{0,5}):\s*(.+)$", desc)
    if m:
        key = m.group(1).replace(".", "").lower()
        courts = _SCHOLAR_STATE_PREFIX.get(key)
        if courts:
            return _classify_state_court(m.group(2), courts)
        desc = m.group(2)  # unknown prefix — classify the remainder generically
    low = desc.lower()
    if low in ("supreme court", "us supreme court", "u.s. supreme court",
               "united states supreme court") or (
            "supreme court" in low and "united states" in low):
        return "scotus"
    m = re.search(
        r"court of appeals,?\s*(?:for the\s+)?(\w+(?: of columbia)?)\s+circuit", low
    )
    if m:
        word = m.group(1)
        if word == "federal":
            return "cafc"
        if "columbia" in word or word == "dc":
            return "cadc"
        return _CIRCUIT_ORDINALS.get(word, "")
    return ""


def _scholar_source_to_court_id(source: str) -> str:
    """Parse a Scholar result's source byline into a CourtListener court id."""
    segs = _scholar_source_segments(source)
    if not segs:
        return ""
    court_year = re.sub(r",?\s*(1[6-9]\d{2}|20\d{2})\s*$", "", segs[-1])
    return _scholar_court_desc_to_id(court_year)


def _scholar_source_year(source: str) -> str:
    """Extract the decision year from a Scholar source byline (preferring the
    year trailing the court segment over any stray year in the citations)."""
    segs = _scholar_source_segments(source)
    if segs:
        m = re.search(r"(1[6-9]\d{2}|20\d{2})\s*$", segs[-1])
        if m:
            return m.group(1)
    m = re.search(r"\b(1[6-9]\d{2}|20\d{2})\b", source or "")
    return m.group(1) if m else ""


def _normalize_scholar_cite(cite: str) -> str:
    """Normalize a Scholar-style reporter citation to Bluebook form: restore
    the periods Scholar drops from multi-capital reporters ("US" → "U.S.",
    "NW" → "N.W.") and fix reporter spacing ("F. 3d" → "F.3d")."""
    cite = re.sub(r"\s+", " ", cite or "").strip().strip(",")
    m = re.match(r"^(\d+)\s+(.+?)\s+(\d+)$", cite)
    if not m:
        return cite
    vol, rep, page = m.group(1), m.group(2), m.group(3)
    rep = re.sub(r"\b([A-Z]{2,})\b",
                 lambda mm: ".".join(mm.group(1)) + ".", rep)
    rep = _respace_reporter(rep)
    return f"{vol} {rep} {page}"


def _scholar_source_cite(source: str) -> str:
    """Pick the best reporter citation from a Scholar byline's leading
    citation segment ("529 NW 2d 155" / "512 US 477, 114 S. Ct. 2364 …"),
    normalized to Bluebook form."""
    segs = _scholar_source_segments(source)
    if len(segs) < 2:
        return ""  # only a court/year segment, no citations
    cites = []
    for part in segs[0].split(","):
        part = part.strip()
        if not part or "…" in part or "..." in part:
            continue  # skip truncated parallel cites
        norm = _normalize_scholar_cite(part)
        if re.match(r"^\d+\s+.+\s+\d+$", norm):
            cites.append(norm)
    return _pick_citation(cites) if cites else ""


def _rtf_escape(s: str) -> str:
    out: list[str] = []
    for ch in s:
        if ch in "\\{}":
            out.append("\\" + ch)
        elif ch == "\n":
            out.append("\\line ")
        elif ord(ch) < 128:
            out.append(ch)
        else:
            cp = ord(ch)
            if cp > 32767:  # RTF \u takes a signed 16-bit value
                cp -= 65536
            out.append(f"\\u{cp}?")
    return "".join(out)


# Color table: 1 = star-pagination marker (purple), 2 = dissent (dark red),
# 3 = concurrence (dark green).  Citation links stay black in copied and
# exported text; the blue is only an on-screen affordance.  The dissent/
# concurrence colors are used only on the running heading of a section in the
# RTF export — opinion body text is always black.
_RTF_HEADER = (
    "{\\rtf1\\ansi\\deff0"
    "{\\fonttbl{\\f0\\froman Times New Roman;}}"
    "{\\colortbl ;\\red142\\green68\\blue173;"
    "\\red163\\green21\\blue21;\\red26\\green122\\blue60;}"
    "\\f0\\fs22\n"
)


def _rtf_document(
    body: str, two_columns: bool = False, page_footer: bool = False
) -> str:
    sect = "\\sectd\\sbknone\\cols2\\colsx432\n" if two_columns else ""
    footer = "{\\footer\\pard\\qc\\fs18\\chpgn\\par}\n" if page_footer else ""
    return _RTF_HEADER + sect + footer + body + "}"


def _run_to_rtf(seg: str, active: set[str], part_colors: bool = False) -> str:
    codes: list[str] = []
    for t in active:
        if t.startswith("fnt_") and len(t) == 8:
            italic, bold, small, sup = (c == "1" for c in t[4:])
            if italic:
                codes.append("\\i")
            if bold:
                codes.append("\\b")
            if small:
                codes.append("\\fs18")
            if sup:
                codes.append("\\super\\fs16")
    if "underline" in active:
        codes.append("\\ul")
    if "pagenum" in active:
        codes.append("\\cf1\\b")
    elif part_colors and "part-dissent" in active:
        codes.append("\\cf2")
    elif part_colors and "part-concurrence" in active:
        codes.append("\\cf3")
    esc = _rtf_escape(seg)
    return "{" + "".join(codes) + " " + esc + "}" if codes else esc


def _fn_bookmark(side: str, fid: str) -> str:
    """RTF bookmark name for a footnote anchor: the in-text reference
    ("fnref") or the footnote body ("fndef")."""
    safe = re.sub(r"\W+", "_", str(fid))
    return ("FNR_" if side == "fnref" else "FNB_") + safe


def _dump_to_rtf(
    txt: tk.Text, start: str, end: str, part_colors: bool = False,
    fn_links: Optional[dict[str, tuple[str, str]]] = None,
) -> str:
    """Convert a Tk Text range (with the Scholar window's tags) to an RTF
    body.  `fn_links` maps link-tag names to ("fnref"|"fndef", id);
    matching runs become RTF bookmark/hyperlink pairs so footnote markers
    stay clickable in the exported document."""
    fn_links = fn_links or {}
    out: list[str] = []
    # Seed with tags already open at *start*; dump only reports transitions.
    active: set[str] = set(txt.tag_names(start))
    active.discard("sel")
    par_open = False
    pending_marks: list[str] = []   # bookmarks to emit at the next run
    marks_done: set[str] = set()    # bookmark names must be unique

    def par_prefix() -> str:
        if "center" in active:
            return "\\pard\\qc\\sa120 "
        if "blockquote" in active:
            return "\\pard\\li720\\ri720\\sa120 "
        return "\\pard\\sa120 "

    def queue_mark(tag: str) -> None:
        side, fid = fn_links[tag]
        name = _fn_bookmark(side, fid)
        if name not in marks_done:
            marks_done.add(name)
            pending_marks.append(
                "{\\*\\bkmkstart " + name + "}{\\*\\bkmkend " + name + "}"
            )

    def fn_target() -> Optional[str]:
        for t in active:
            if t in fn_links:
                side, fid = fn_links[t]
                # a reference links to the body and vice versa
                return _fn_bookmark("fndef" if side == "fnref" else "fnref",
                                    fid)
        return None

    for t in active:
        if t in fn_links:
            queue_mark(t)
    for key, value, _index in txt.dump(start, end, text=True, tag=True):
        if key == "tagon":
            active.add(value)
            if value in fn_links:
                queue_mark(value)
        elif key == "tagoff":
            active.discard(value)
        elif key == "text":
            for i, seg in enumerate(value.split("\n")):
                if i and par_open:
                    out.append("\\par\n")
                    par_open = False
                if seg:
                    if not par_open:
                        out.append(par_prefix())
                        par_open = True
                    if pending_marks:
                        out.extend(pending_marks)
                        pending_marks.clear()
                    run = _run_to_rtf(seg, active, part_colors)
                    target = fn_target()
                    if target:
                        run = ("{\\field{\\*\\fldinst{HYPERLINK \\\\l \""
                               + target + "\"}}{\\fldrslt " + run + "}}")
                    out.append(run)
    if par_open:
        out.append("\\par\n")
    return "".join(out)


def _copy_rich_clipboard(widget: tk.Misc, rtf: str, plain: str) -> str:
    """
    Put *rtf* on the system clipboard (with *plain* as fallback where the
    platform allows both).  Returns a short description of what was copied.
    """
    rtf_bytes = rtf.encode("ascii", "replace")
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            user32 = ctypes.windll.user32
            kernel32 = ctypes.windll.kernel32
            kernel32.GlobalAlloc.restype = ctypes.c_void_p
            kernel32.GlobalAlloc.argtypes = [wintypes.UINT, ctypes.c_size_t]
            kernel32.GlobalLock.restype = ctypes.c_void_p
            kernel32.GlobalLock.argtypes = [ctypes.c_void_p]
            kernel32.GlobalUnlock.argtypes = [ctypes.c_void_p]
            user32.OpenClipboard.argtypes = [ctypes.c_void_p]
            user32.SetClipboardData.restype = ctypes.c_void_p
            user32.SetClipboardData.argtypes = [wintypes.UINT, ctypes.c_void_p]

            CF_UNICODETEXT = 13
            GMEM_MOVEABLE = 0x0002
            cf_rtf = user32.RegisterClipboardFormatW("Rich Text Format")

            def set_data(fmt: int, data: bytes) -> None:
                handle = kernel32.GlobalAlloc(GMEM_MOVEABLE, len(data))
                ptr = kernel32.GlobalLock(handle)
                ctypes.memmove(ptr, data, len(data))
                kernel32.GlobalUnlock(handle)
                user32.SetClipboardData(fmt, handle)

            if not user32.OpenClipboard(None):
                raise OSError("OpenClipboard failed")
            try:
                user32.EmptyClipboard()
                set_data(cf_rtf, rtf_bytes + b"\x00")
                set_data(CF_UNICODETEXT, plain.encode("utf-16-le") + b"\x00\x00")
            finally:
                user32.CloseClipboard()
            return "formatted text (RTF)"
        except Exception as exc:
            print(f"[copy] Windows RTF clipboard failed: {exc}")
    elif sys.platform == "darwin":
        try:
            subprocess.run(
                ["pbcopy", "-Prefer", "rtf"], input=rtf_bytes, check=True, timeout=10
            )
            return "formatted text (RTF)"
        except Exception as exc:
            print(f"[copy] pbcopy RTF failed: {exc}")
    else:
        candidates = []
        if os.environ.get("WAYLAND_DISPLAY"):
            candidates.append(["wl-copy", "--type", "text/rtf"])
        candidates.append(["xclip", "-selection", "clipboard", "-t", "text/rtf"])
        for cmd in candidates:
            try:
                subprocess.run(cmd, input=rtf_bytes, check=True, timeout=10)
                return "formatted text (RTF)"
            except Exception as exc:
                print(f"[copy] {cmd[0]} RTF failed: {exc}")
    widget.clipboard_clear()
    widget.clipboard_append(plain)
    return "plain text (no RTF clipboard tool available)"


# Minimum word-shingle containment between the Scholar candidate and the
# CourtListener text to accept them as the same opinion.  Containment is
# stricter than an intuitive "percent similar": same-opinion pairs score
# well above this even across OCR/edition differences, while different
# opinions score near zero.
_SCHOLAR_MATCH_THRESHOLD = 0.60

# Opinion-text font size (pt), remembered across windows within a session
# so a reader's A+/A− choice carries over to the next case they open.
_OPINION_FONT_PT = 11
_OPINION_FONT_MIN = 7
_OPINION_FONT_MAX = 24


def _find_scholar_for_item(
    client: Optional[CourtListenerClient],
    fetcher: "GoogleScholarFetcher",
    item: dict,
    status,
) -> tuple[Optional[tuple[str, str]], Optional[str], str]:
    """
    Locate this CourtListener case's opinion on Google Scholar, verifying
    candidates against the CourtListener text before accepting them.

    Search stages, in order:
      1. the primary citation (walking down the results list),
      2. alternate reporter citations from the CourtListener cluster,
      3. the case name (with variants such as United States ↔ US).

    Returns (result, cl_text, note): *result* is (url, opinion_html) or
    None if no candidate was similar enough; *cl_text* is the assembled
    CourtListener text (so the viewer's toggle is instant); *note*
    describes the verification outcome.
    """
    cluster_id = item.get("cluster_id") or item.get("id")
    vkey = f"verified:cluster:{cluster_id}" if cluster_id else ""
    if vkey:
        cached = fetcher.get_cached(vkey)
        if cached:
            return cached, None, "verified match (cached)"

    cl_text: Optional[str] = None
    if client is not None and cluster_id:
        status("Fetching CourtListener text for comparison…")
        try:
            cl_text = _assemble_case_text(client, item)
        except Exception as exc:
            print(f"[verify] CourtListener text unavailable: {exc}")

    tried: set[str] = set()
    best_sim = 0.0

    def try_url(url: str) -> Optional[tuple[str, str]]:
        nonlocal best_sim
        if url in tried:
            return None
        tried.add(url)
        res = fetcher.fetch_by_url(url)
        if not res:
            return None
        if cl_text is None:
            return res  # nothing to verify against; accept the first hit
        sim = text_similarity(blocks_to_text(parse_opinion_blocks(res[1])), cl_text)
        print(f"[verify] similarity {sim:.2f} for {url}")
        best_sim = max(best_sim, sim)
        return res if sim >= _SCHOLAR_MATCH_THRESHOLD else None

    # --- assemble the search stages ---
    primary = _pick_citation(item.get("citation", []))
    alt_cites: list[str] = []
    raw = item.get("citation")
    if isinstance(raw, list):
        alt_cites += [c for c in raw if c and c != primary]
    if client is not None and cluster_id:
        try:
            rec = client.get_cluster(int(cluster_id), fields="citations")
            for c in _cluster_citations_to_strings(rec.get("citations")):
                if c != primary and c not in alt_cites:
                    alt_cites.append(c)
        except Exception as exc:
            print(f"[verify] cluster citations fetch failed: {exc}")
    alt_cites = [c for c in alt_cites if not _NOISE_CITE_RE.search(c)][:4]

    case_name = re.sub(
        r"<[^>]+>", "", item.get("caseName") or item.get("case_name") or ""
    ).strip()
    date_filed = item.get("dateFiled") or item.get("date_filed") or ""
    year = date_filed[:4] if len(date_filed) >= 4 else ""
    name_variants: list[str] = []
    if case_name:
        name_variants.append(case_name)
        v = re.sub(r"\bUnited States\b", "US", case_name)
        if v not in name_variants:
            name_variants.append(v)
        v = re.sub(r"\bU\.? ?S\.?\b", "United States", case_name)
        if v not in name_variants:
            name_variants.append(v)

    stages: list[tuple[str, int, str]] = []  # (query, results to try, description)
    if primary:
        stages.append((f'"{primary}"', 4, f"citation {primary}"))
    for c in alt_cites:
        stages.append((f'"{c}"', 2, f"alternate citation {c}"))
    for nm in name_variants:
        q = f"{nm} {year}".strip()
        stages.append((q, 3, f"case name {nm!r}"))

    fetches = 0
    _MAX_FETCHES = 10
    for q, take, desc in stages:
        if fetches >= _MAX_FETCHES:
            break
        status(f"Searching Scholar by {desc}…")
        results = fetcher.search_cases(q, limit=take)
        for r in results[:take]:
            if fetches >= _MAX_FETCHES:
                break
            fetches += 1
            status(f"Comparing candidate: {r.title[:60]}…")
            hit = try_url(r.url)
            if hit:
                if cl_text is not None and vkey:
                    fetcher.put_cached(vkey, *hit)
                note = (
                    "verified against CourtListener"
                    if cl_text is not None
                    else "unverified (no CourtListener text to compare)"
                )
                return hit, cl_text, note

    print(f"[verify] gave up; best similarity {best_sim:.2f}")
    return None, cl_text, f"best candidate similarity {best_sim:.0%}"


class _TextFinder:
    """Ctrl-F find bar for a Text widget: highlights every match, steps
    through them with Enter / Shift+Enter (also F3 / Shift+F3), and
    closes with Escape.  Case-insensitive plain-text search."""

    def __init__(self, win: tk.Misc, txt: tk.Text,
                 before_widget: tk.Misc) -> None:
        self._win, self._txt = win, txt
        self._before = before_widget
        self._visible = False
        self._matches: list[tuple[str, str]] = []
        self._cur = -1
        self._pending: Optional[str] = None  # debounce timer id

        txt.tag_configure("findmatch", background="#fff3b0")
        txt.tag_configure("findcur", background="#ffb347")
        bar = self._bar = ttk.Frame(win)
        ttk.Label(bar, text="Find:").pack(side="left", padx=(8, 4))
        self._var = tk.StringVar()
        self._entry = ttk.Entry(bar, textvariable=self._var, width=28)
        self._entry.pack(side="left")
        ttk.Button(bar, text="▼", width=2,
                   command=lambda: self.step(+1)).pack(side="left", padx=2)
        ttk.Button(bar, text="▲", width=2,
                   command=lambda: self.step(-1)).pack(side="left")
        self._count_var = tk.StringVar()
        ttk.Label(bar, textvariable=self._count_var,
                  foreground="gray").pack(side="left", padx=8)
        ttk.Button(bar, text="✕", width=2,
                   command=self.close).pack(side="right", padx=(0, 4))

        self._entry.bind("<Return>", lambda _e: self.step(+1))
        self._entry.bind("<Shift-Return>", lambda _e: self.step(-1))
        self._entry.bind("<KeyRelease>", self._on_key)
        self._entry.bind("<Escape>", lambda _e: self.close())
        win.bind("<Control-f>", lambda _e: self.open() or "break")
        win.bind("<F3>", lambda _e: self.step(+1))
        win.bind("<Shift-F3>", lambda _e: self.step(-1))
        win.bind("<Escape>", lambda _e: self.close() if self._visible
                 else None)

    def open(self) -> None:
        if not self._visible:
            self._bar.pack(fill="x", padx=8, pady=(4, 0),
                           before=self._before)
            self._visible = True
        self._entry.focus_set()
        self._entry.select_range(0, "end")
        if self._var.get():
            self.refresh()

    def close(self) -> None:
        if not self._visible:
            return
        self._bar.pack_forget()
        self._visible = False
        self._clear_tags()
        self._count_var.set("")
        self._txt.focus_set()

    def _clear_tags(self) -> None:
        self._txt.tag_remove("findmatch", "1.0", "end")
        self._txt.tag_remove("findcur", "1.0", "end")

    def _on_key(self, event) -> None:
        if event.keysym in ("Return", "Escape", "F3"):
            return
        if self._pending:
            self._win.after_cancel(self._pending)
        self._pending = self._win.after(250, self.refresh)

    def refresh(self) -> None:
        """Re-run the search (also called after the text is re-rendered)."""
        self._pending = None
        if not self._visible:
            return
        self._clear_tags()
        self._matches, self._cur = [], -1
        needle = self._var.get()
        if not needle:
            self._count_var.set("")
            return
        txt = self._txt
        idx = "1.0"
        n = tk.IntVar()
        while True:
            idx = txt.search(needle, idx, stopindex="end", nocase=True,
                             count=n)
            if not idx or not n.get():
                break
            end = f"{idx}+{n.get()}c"
            self._matches.append((idx, end))
            txt.tag_add("findmatch", idx, end)
            idx = end
        if self._matches:
            # start at the first match at or below the current view
            top = txt.index("@0,0")
            first = next(
                (i for i, (s, _e) in enumerate(self._matches)
                 if txt.compare(s, ">=", top)), 0,
            )
            self._goto(first)
        else:
            self._count_var.set("no matches")

    def step(self, delta: int) -> None:
        if not self._visible:
            self.open()
            return
        if not self._matches:
            self.refresh()
            return
        self._goto((self._cur + delta) % len(self._matches))

    def _goto(self, i: int) -> None:
        self._cur = i
        start, end = self._matches[i]
        txt = self._txt
        txt.tag_remove("findcur", "1.0", "end")
        txt.tag_add("findcur", start, end)
        txt.see(start)
        self._count_var.set(f"{i + 1} of {len(self._matches)}")


class _PdfPane(ttk.Frame):
    """A scrollable, lazily-rendered view of a PDF, embedded in the opinion
    window (pypdfium2 + Pillow).

    Pages are rendered to images only as they scroll near the viewport, and
    pages that scroll far away are released again, so even a long opinion stays
    light on memory.  Construction raises ImportError when pypdfium2/Pillow are
    not installed — the caller then offers to open the PDF in a browser.
    """

    _PAD = 12        # vertical gap between pages (px)
    _SCROLL_PX = 60  # wheel-notch scroll distance (px); canvas uses 1px units
    _MARGIN = 18     # small even margin drawn around the cropped page (px)
    _BBOX_SCALE = 0.6   # low-res render scale used to detect the content box
    _INK_THRESH = 185   # grayscale < this counts as "ink" (ignores scan bg)
    _PROFILE_MIN = 2    # min avg ink (0-255) for a row/col to count as content
    _PAD_FRAC = 0.006   # tiny expansion of the detected box so glyphs aren't clipped

    def __init__(self, parent: tk.Misc, pdf_bytes: bytes, width: int = 800) -> None:
        super().__init__(parent)
        import pypdfium2 as pdfium
        from PIL import ImageTk  # noqa: F401  (availability check at construct)

        self._doc = pdfium.PdfDocument(pdf_bytes)
        self._target_w = max(240, int(width))
        self._inner_w = max(1, self._target_w - 2 * self._MARGIN)
        self._photos: dict[int, object] = {}   # page → PhotoImage (kept alive)
        self._img_ids: dict[int, int] = {}      # page → canvas image id

        canvas = tk.Canvas(self, bg="#d9d9d9", highlightthickness=0,
                           yscrollincrement=1)
        vsb = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=self._on_yview)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        self._canvas, self._vsb = canvas, vsb

        # Lay out one slot per page.  A quick low-resolution render detects each
        # page's content box, so the wide blank margins of court PDFs are
        # cropped down to a small, even margin; the slot is sized from the
        # cropped content (full rendering still happens lazily, on scroll).
        self._slots: list[tuple] = []  # (y, slot_h, frac_box, render_scale)
        y = self._PAD
        for i in range(len(self._doc)):
            page = self._doc[i]
            try:
                w_pt, h_pt = page.get_size()
                try:
                    lo = page.render(scale=self._BBOX_SCALE).to_pil()
                    frac = self._content_frac(lo)
                except Exception:
                    frac = (0.0, 0.0, 1.0, 1.0)
            finally:
                page.close()
            fl, ft, fr, fb = frac
            cw_pt = max(1.0, (fr - fl) * w_pt)
            ch_pt = max(1.0, (fb - ft) * h_pt)
            render_scale = self._inner_w / cw_pt
            slot_h = int(round(ch_pt * render_scale)) + 2 * self._MARGIN
            canvas.create_rectangle(
                self._PAD, y, self._PAD + self._target_w, y + slot_h,
                fill="white", outline="#b8b8b8")
            self._slots.append((y, slot_h, frac, render_scale))
            y += slot_h + self._PAD
        canvas.configure(
            scrollregion=(0, 0, self._target_w + 2 * self._PAD, y))

        canvas.bind("<Configure>", lambda _e: self._render_visible())
        canvas.bind("<MouseWheel>", self._on_wheel)            # Windows / macOS
        canvas.bind("<Button-4>", lambda _e: self._wheel(-1))  # X11 wheel up
        canvas.bind("<Button-5>", lambda _e: self._wheel(1))   # X11 wheel down
        canvas.bind("<Enter>", lambda _e: canvas.focus_set())
        self.after(60, self._render_visible)

    def _content_frac(self, img) -> tuple:
        """Fractional content box (l, t, r, b in 0..1) of `img` — the area
        holding actual text/figures, found from row/column ink projections so
        scanner speckle in the margins doesn't defeat the crop.  Returns the
        full page when nothing plausible is found."""
        from PIL import Image
        full = (0.0, 0.0, 1.0, 1.0)
        W, H = img.size
        if W < 8 or H < 8:
            return full
        mask = img.convert("L").point(
            lambda p: 255 if p < self._INK_THRESH else 0)
        cols = mask.resize((W, 1), Image.BOX).getdata()  # avg ink per column
        rows = mask.resize((1, H), Image.BOX).getdata()  # avg ink per row

        def span(profile, n):
            idx = [k for k, v in enumerate(profile) if v > self._PROFILE_MIN]
            return (idx[0], idx[-1] + 1) if idx else (0, n)

        l, r = span(cols, W)
        t, b = span(rows, H)
        fl, ft = l / W - self._PAD_FRAC, t / H - self._PAD_FRAC
        fr, fb = r / W + self._PAD_FRAC, b / H + self._PAD_FRAC
        fl, ft = max(0.0, fl), max(0.0, ft)
        fr, fb = min(1.0, fr), min(1.0, fb)
        # Ignore implausible crops (blank page, or so tight it's likely noise).
        if (fr - fl) < 0.15 or (fb - ft) < 0.15:
            return full
        return (fl, ft, fr, fb)

    def _on_yview(self, first: str, last: str) -> None:
        self._vsb.set(first, last)
        self._render_visible()

    def _on_wheel(self, e) -> None:
        self._wheel(-1 if e.delta > 0 else 1)

    def _wheel(self, direction: int) -> None:
        self._canvas.yview_scroll(direction * self._SCROLL_PX, "units")

    def _render_visible(self) -> None:
        c = self._canvas
        try:
            top = c.canvasy(0)
            view_h = c.winfo_height()
        except tk.TclError:
            return
        lo, hi = top - view_h, top + 2 * view_h   # ~one screen of buffer
        for i, (y, slot_h, _frac, _scale) in enumerate(self._slots):
            near = (y + slot_h) >= lo and y <= hi
            if near and i not in self._img_ids:
                self._render_page(i)
            elif not near and i in self._img_ids:
                c.delete(self._img_ids.pop(i))
                self._photos.pop(i, None)

    def _render_page(self, i: int) -> None:
        from PIL import Image, ImageTk
        y, slot_h, frac, scale = self._slots[i]
        page = self._doc[i]
        try:
            full = page.render(scale=scale).to_pil()
        finally:
            page.close()
        fl, ft, fr, fb = frac
        W, H = full.size
        content = full.crop((int(fl * W), int(ft * H),
                             int(round(fr * W)), int(round(fb * H))))
        # Snap to the exact content box so every page lines up with a uniform
        # margin, then mount it on a white page of the slot's size.
        inner_h = max(1, slot_h - 2 * self._MARGIN)
        if content.size != (self._inner_w, inner_h):
            content = content.resize((self._inner_w, inner_h), Image.LANCZOS)
        canvas_img = Image.new("RGB", (self._target_w, slot_h), "white")
        canvas_img.paste(content, (self._MARGIN, self._MARGIN))
        photo = ImageTk.PhotoImage(canvas_img)
        self._photos[i] = photo
        self._img_ids[i] = self._canvas.create_image(
            self._PAD, y, anchor="nw", image=photo)

    def destroy(self) -> None:
        try:
            self._doc.close()
        except Exception:
            pass
        super().destroy()


class _ScholarTextWindow:
    """
    Rich viewer for a Google Scholar opinion.

    Renders the opinion with its original formatting (paragraphs, centering,
    italics, footnote markers), highlights the reporter star-pagination
    markers, makes case citations clickable (fetching the cited case from
    Scholar in a new window), and offers:
      • Copy + Cite — copies selection (or all) with formatting and appends
        a Bluebook citation pin-cited from the star pagination,
      • Export RTF — two-column RTF named after the Bluebook caption,
      • View PDF — the official opinion PDF, shown in-app (Download PDF there),
      • a toggle to the CourtListener version of the text.
    """

    _PAGENUM_COLOR = "#8e44ad"   # muted purple — visible but not loud
    _LINK_COLOR = "#1a56b0"
    _DISSENT_COLOR = "#a31515"   # dark red — top-of-window label & RTF headings
    _CONCUR_COLOR = "#1a7a3c"    # dark green
    _DISSENT_BG = "#fbeeee"      # very light red — full-view box behind a dissent
    _CONCUR_BG = "#eef7f0"       # very light green — box behind a concurrence
    # In the full-opinion view the region behind a concurrence/dissent gets a
    # light background tint; the body text itself stays black and the active
    # part is named, in color, at the top of the window.
    _PART_BOX_TAGS = {"dissent": "box-dissent", "concurrence": "box-concurrence"}
    _PART_LABEL_COLORS = {"dissent": _DISSENT_COLOR, "concurrence": _CONCUR_COLOR}

    def __init__(
        self,
        parent: tk.Misc,
        app: "CourtListenerGUI",
        url: str,
        opinion_html: str,
        item: Optional[dict] = None,
        cl_text: Optional[str] = None,
        note: str = "",
        cl_parts: Optional[list] = None,
        cl_blocks: Optional[list] = None,
    ) -> None:
        self._app = app
        self._item = item or {}
        self._scholar_url = url
        self._note = note
        self._cl_primary = cl_parts is not None and not opinion_html
        if self._cl_primary:
            # Opened as a CourtListener-primary window (Scholar failed).
            self._blocks = cl_blocks or []
            try:
                self._scholar_text = blocks_to_text(self._blocks)
            except Exception:
                self._scholar_text = cl_text or ""
            self._parts = cl_parts or []
            self._cl_parts = cl_parts or []
            self._cl_blocks = cl_blocks or []
        else:
            self._blocks = parse_opinion_blocks(opinion_html)
            self._scholar_text = (
                blocks_to_text(self._blocks) or _strip_html(opinion_html)
            )
            self._parts = segment_blocks(self._blocks)
            self._cl_parts = None
            self._cl_blocks = None
        self._current_part: Optional[int] = None  # None = full opinion
        # Page in effect at the start of each part, for pin cites when a
        # single part is displayed (no preceding star marker on screen).
        self._part_start_pages: list[Optional[int]] = []
        page: Optional[int] = None
        for part in self._parts:
            self._part_start_pages.append(page)
            for b in part.blocks:
                for s in b.spans:
                    if s.pagenum:
                        m = re.search(r"\d+", s.text)
                        if m:
                            page = int(m.group(0))
        self._cl_text: Optional[str] = cl_text
        self._mode = "courtlistener" if self._cl_primary else "scholar"
        self._pdf_pane: Optional[_PdfPane] = None  # set while viewing the PDF
        self._pdf_url: Optional[str] = None
        self._pdf_bytes: Optional[bytes] = None
        self._link_actions: dict[str, tuple[str, str]] = {}
        self._link_n = 0
        self._fonts: dict[str, tkfont.Font] = {}
        self._fn_text: dict[str, str] = {}  # footnote id → body text (for hover tips)
        self._fn_tip: Optional[tk.Toplevel] = None
        self._is_scotus = False  # set by _compute_bluebook_parts
        self._bb = self._compute_bluebook_parts()
        if not self._cl_primary:
            self._refine_part_labels(self._parts)

        self._win = tk.Toplevel(parent)
        self._win.title(
            self._bb["name"] or (
                "CourtListener Opinion Text" if self._cl_primary
                else "Google Scholar Opinion Text"
            )
        )
        self._win.geometry("860x680")
        self._win.minsize(500, 300)
        self._build_ui()
        if self._cl_primary:
            self._render_cl_blocks()
        else:
            self._render_scholar()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        win = self._win

        url_frame = ttk.Frame(win)
        url_frame.pack(fill="x", padx=8, pady=(8, 0))
        ttk.Label(url_frame, text="Source:").pack(side="left")
        self._source_var = tk.StringVar(value=self._scholar_url)
        ttk.Entry(url_frame, textvariable=self._source_var, state="readonly").pack(
            side="left", fill="x", expand=True, padx=4
        )

        # Part navigation: what you're viewing, and a selector to filter
        view_frame = ttk.Frame(win)
        view_frame.pack(fill="x", padx=8, pady=(6, 0))
        ttk.Label(view_frame, text="Viewing:").pack(side="left")
        self._view_label_var = tk.StringVar(value="Full opinion")
        self._view_label = ttk.Label(
            view_frame,
            textvariable=self._view_label_var,
            font=("TkDefaultFont", 10, "bold"),
        )
        self._view_label.pack(side="left", padx=(4, 12))
        part_values = ["Full opinion"] + [
            f"{i + 1}. {p.label}" for i, p in enumerate(self._parts)
        ]
        self._part_combo = ttk.Combobox(
            view_frame, state="readonly", width=44, values=part_values
        )
        self._part_combo.current(0)
        self._part_combo.pack(side="right")
        self._part_combo.bind("<<ComboboxSelected>>", self._on_part_selected)
        if len(self._parts) <= 1:
            self._part_combo.config(state="disabled")

        text_frame = ttk.Frame(win)
        text_frame.pack(fill="both", expand=True, padx=8, pady=4)
        base = tkfont.Font(family=self._opinion_font_family(), size=_OPINION_FONT_PT)
        self._fonts["base"] = base
        self._family = base.actual("family")
        self._base_size = base.actual("size")
        txt = tk.Text(text_frame, wrap="word", font=base, padx=14, pady=10)
        self._text = txt
        vsb = ttk.Scrollbar(text_frame, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=self._on_yscroll)
        vsb.pack(side="right", fill="y")
        txt.pack(side="left", fill="both", expand=True)
        self._text_frame, self._vsb = text_frame, vsb
        self._details_frame: Optional[ttk.Frame] = None
        self._details_loaded = False

        txt.tag_configure("center", justify="center")
        txt.tag_configure("blockquote", lmargin1=36, lmargin2=36, rmargin=36)
        txt.tag_configure("heading", spacing1=6, spacing3=4)
        txt.tag_configure("underline", underline=True)
        # Full-view part boxes: a light background tint, kept at the bottom of
        # the tag stack so the selection highlight, citation links and page
        # markers all show above it.  Part text is no longer colored — only
        # this subtle box and the top-of-window label distinguish the parts.
        txt.tag_configure("box-dissent", background=self._DISSENT_BG)
        txt.tag_configure("box-concurrence", background=self._CONCUR_BG)
        txt.tag_lower("box-dissent")
        txt.tag_lower("box-concurrence")
        fnhead_font = tkfont.Font(
            family=self._family, size=max(self._base_size - 2, 8), weight="bold"
        )
        self._fonts["fnhead"] = fnhead_font
        txt.tag_configure(
            "fnhead", font=fnhead_font, foreground="#666666", spacing1=10
        )
        # Star-pagination markers are reporter page references the app
        # interleaves into the text, not the Court's prose, so they stay in
        # the default serif even when a SCOTUS body switches to Century
        # Schoolbook.
        pagenum_font = tkfont.Font(
            family="Georgia", size=max(self._base_size - 1, 8), weight="bold"
        )
        self._fonts["pagenum"] = pagenum_font
        txt.tag_configure(
            "pagenum", font=pagenum_font, foreground=self._PAGENUM_COLOR
        )
        txt.tag_configure("citelink", foreground=self._LINK_COLOR)
        txt.tag_bind("citelink", "<Enter>", lambda _e: txt.config(cursor="hand2"))
        txt.tag_bind("citelink", "<Leave>", lambda _e: txt.config(cursor=""))
        txt.tag_configure("jumpflash", background="#fff2a8")
        self._finder = _TextFinder(win, txt, text_frame)

        btn_frame = ttk.Frame(win)
        btn_frame.pack(fill="x", padx=8, pady=(0, 8))
        self._btn_frame = btn_frame  # PDF/text panes pack just above this
        ttk.Button(btn_frame, text="Copy + Cite", command=self._copy_formatted).pack(
            side="right", padx=(4, 0)
        )
        # In text view this exports RTF; in PDF view it becomes "Download PDF".
        self._export_btn = ttk.Button(
            btn_frame, text="Export RTF…", command=self._export_rtf
        )
        self._export_btn.pack(side="right", padx=4)
        self._toggle_btn = ttk.Button(
            btn_frame, text="CourtListener Text", command=self._toggle_source
        )
        self._toggle_btn.pack(side="right", padx=4)

        # Text-size controls (also Ctrl +/−/0 and Ctrl+mouse wheel)
        ttk.Button(
            btn_frame, text="A−", width=3, command=lambda: self._zoom(-1)
        ).pack(side="left")
        ttk.Button(
            btn_frame, text="A+", width=3, command=lambda: self._zoom(+1)
        ).pack(side="left", padx=(2, 8))
        self._details_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            btn_frame, text="Case details", variable=self._details_var,
            command=self._toggle_details,
        ).pack(side="left", padx=(0, 8))
        for seq in ("<Control-plus>", "<Control-equal>", "<Control-KP_Add>"):
            win.bind(seq, lambda _e: self._zoom(+1))
        for seq in ("<Control-minus>", "<Control-KP_Subtract>"):
            win.bind(seq, lambda _e: self._zoom(-1))
        win.bind("<Control-0>", lambda _e: self._zoom(0))
        txt.bind(
            "<Control-MouseWheel>",
            lambda e: self._zoom(+1 if e.delta > 0 else -1) or "break",
        )
        txt.bind("<Control-Button-4>", lambda _e: self._zoom(+1) or "break")
        txt.bind("<Control-Button-5>", lambda _e: self._zoom(-1) or "break")
        # Ctrl-C copies with the Bluebook citation appended (the plain
        # default copy is suppressed); the find bar's entry keeps native
        # copy since this is bound to the text widget only.
        for seq in ("<Control-c>", "<Command-c>"):
            try:
                txt.bind(seq, lambda _e: self._copy_formatted() or "break")
            except tk.TclError:
                pass  # modifier not supported on this platform

        self._status_var = tk.StringVar()
        ttk.Label(btn_frame, textvariable=self._status_var, foreground="gray").pack(
            side="left", fill="x", expand=True
        )

    def _zoom(self, delta: int) -> None:
        """Grow/shrink every font in the window; delta 0 resets to default.
        Tk re-renders widgets when a named Font object is reconfigured, so
        resizing the shared Font instances restyles all existing text."""
        global _OPINION_FONT_PT
        new = 11 if delta == 0 else max(
            _OPINION_FONT_MIN, min(_OPINION_FONT_MAX, self._base_size + delta)
        )
        if new == self._base_size:
            return
        self._base_size = new
        _OPINION_FONT_PT = new
        for name, f in self._fonts.items():
            if name == "base":
                f.configure(size=new)
            elif name == "fnhead":
                f.configure(size=max(new - 2, 8))
            elif name == "pagenum":
                f.configure(size=max(new - 1, 8))
            elif name.startswith("fnt_"):
                small, sup = name[6] == "1", name[7] == "1"
                f.configure(
                    size=max(new - (3 if sup else 2 if small else 0), 7)
                )
        self._status_var.set(f"Text size: {new} pt")

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _font_tag(self, italic: bool, bold: bool, small: bool, sup: bool,
                  family: Optional[str] = None) -> str:
        fam = family or self._family
        prefix = "fnt_" if fam == self._family else "fna_"
        name = prefix + "".join("1" if f else "0" for f in (italic, bold, small, sup))
        if name not in self._fonts:
            size = self._base_size - (3 if sup else 2 if small else 0)
            f = tkfont.Font(
                family=fam,
                size=max(size, 7),
                slant="italic" if italic else "roman",
                weight="bold" if bold else "normal",
            )
            self._fonts[name] = f
            self._text.tag_configure(name, font=f, offset=4 if sup else 0)
        return name

    # Century Schoolbook is the Supreme Court's house typeface; prefer the
    # first installed variant for SCOTUS opinions.  Names vary by platform
    # (URW "Century Schoolbook L" on Linux, the TeX Gyre Schola clone, etc.).
    _SCOTUS_FONT_FAMILIES = (
        "Century Schoolbook",
        "New Century Schoolbook",
        "Century Schoolbook L",
        "Century Schoolbook Std",
        "TeX Gyre Schola",
        "Century",
    )

    def _opinion_font_family(self) -> str:
        """Body font for the opinion: Century Schoolbook for Supreme Court
        decisions (its house typeface), the default serif otherwise."""
        if self._is_scotus:
            available = {f.lower() for f in tkfont.families(self._win)}
            for fam in self._SCOTUS_FONT_FAMILIES:
                if fam.lower() in available:
                    return fam
        return "Georgia"

    def _new_link(self, action: tuple[str, str]) -> str:
        self._link_n += 1
        tag = f"lnk{self._link_n}"
        self._link_actions[tag] = action
        self._text.tag_bind(
            tag, "<Button-1>", lambda _e, t=tag: self._follow_link(t)
        )
        if action[0] == "fnref":
            # Hovering an in-text footnote marker previews the note's text.
            fid = action[1]
            self._text.tag_bind(
                tag, "<Enter>", lambda e, i=fid: self._show_fn_tip(e, i), add="+"
            )
            self._text.tag_bind(
                tag, "<Leave>", lambda _e: self._hide_fn_tip(), add="+"
            )
        return tag

    # ------------------------------------------------------------------
    # Footnote hover tooltip
    # ------------------------------------------------------------------
    def _show_fn_tip(self, event, fid: str) -> None:
        """Pop up the footnote's text next to the hovered marker."""
        text = self._fn_text.get(fid)
        if not text:
            return
        self._hide_fn_tip()
        tip = tk.Toplevel(self._text)
        tip.wm_overrideredirect(True)
        try:
            tip.attributes("-topmost", True)
        except tk.TclError:
            pass
        tk.Label(
            tip, text=text, justify="left", wraplength=460,
            background="#fffbe6", foreground="#000000",
            relief="solid", borderwidth=1,
            font=(self._family, max(self._base_size - 2, 8)),
            padx=8, pady=5,
        ).pack()
        tip.wm_geometry(f"+{event.x_root + 14}+{event.y_root + 18}")
        self._fn_tip = tip

    def _hide_fn_tip(self) -> None:
        if self._fn_tip is not None:
            self._fn_tip.destroy()
            self._fn_tip = None

    def _insert_span(self, span, block_tags: tuple, neutral: bool = False) -> None:
        txt = self._text
        tags = list(block_tags)
        if span.pagenum:
            m = re.search(r"\d+", span.text)
            if m:
                self._cur_page = int(m.group(0))
                # where each star page begins, for pin-cited link arrivals
                self._page_pos.setdefault(self._cur_page,
                                          txt.index("end-1c"))
            tags.append("pagenum")
            txt.insert("end", span.text, tuple(tags))
            return
        if span.fnref and span.fnref not in self._fnref_pages:
            # Page in effect where the footnote is referenced — that's the
            # page a Bluebook "n.N" pin cite uses.
            self._fnref_pages[span.fnref] = self._cur_page
        tags.append(self._font_tag(
            span.italic, span.bold, span.small, span.sup,
            family="Georgia" if neutral else None,
        ))
        if span.underline:
            tags.append("underline")
        if span.fnref:
            # In-text footnote marker: click jumps to the footnote body
            self._fn_ref_pos.setdefault(span.fnref, txt.index("end-1c"))
            tags += ["citelink", self._new_link(("fnref", span.fnref))]
            txt.insert("end", span.text, tuple(tags))
            return
        if span.fndef:
            # Footnote-body marker: click jumps back to the reference
            self._fn_def_pos[span.fndef] = txt.index("end-1c")
            tags += ["citelink", self._new_link(("fndef", span.fndef))]
            txt.insert("end", span.text, tuple(tags))
            return
        if span.link:
            tags += ["citelink", self._new_link(("url", span.link))]
            txt.insert("end", span.text, tuple(tags))
            return
        # Plain text: make recognizable citations clickable
        self._insert_plain_with_links(span.text, tuple(tags))

    def _insert_plain_with_links(self, text: str, tags: tuple) -> None:
        """Insert text, turning case citations, U.S. Code citations, and
        C.F.R. citations into clickable links (Scholar lookup / OLRC and
        eCFR statute viewers)."""
        txt = self._text
        matches: list[tuple[int, int, str, re.Match]] = []
        for m in _TEXT_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "cite", m))
        for m in us_code.USC_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "usc", m))
        for m in ecfr.CFR_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "cfr", m))
        for m in fed_rules.RULE_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "rule", m))
        for c in state_statutes.iter_cites(text):
            matches.append((c.start, c.end, "statestat", c))
        for m in statutes_at_large.STAT_CITE_RE.finditer(text):
            if statutes_at_large.url_for(m):  # only link volumes GovInfo has
                matches.append((m.start(), m.end(), "stat", m))
        matches.sort(key=lambda t: (t[0], -t[1]))
        pos = 0
        for start, end, kind, m in matches:
            if start < pos:
                continue  # overlapping match — first/longest wins
            if start > pos:
                txt.insert("end", text[pos:start], tags)
            if kind == "cite":
                cite = re.sub(r"\s+", " ", m.group(0)).replace("U. S.", "U.S.")
                cite = cite.replace("’", "'")  # straight apostrophe for the search query
                # A pincite right after ("365 U.S. 167, 171") rides along
                # so the opened case can jump to that page.  A number that
                # opens a parallel cite ("556, 510 A.2d 562") is excluded
                # by the capital letter that follows it.
                pin_m = _PINCITE_AFTER_RE.match(text, end)
                if pin_m:
                    cite += "@" + pin_m.group(1)
                action = ("cite", cite)
            elif kind == "usc":
                action = ("usc", us_code.cite_spec(m))
            elif kind == "rule":
                action = ("rule", fed_rules.cite_spec(m))
            elif kind == "statestat":
                # In-app for priority states (once a parser exists), else a
                # browser link-out.  `m` here is a state_statutes.Cite record.
                action = state_statutes.action_for(m)
            elif kind == "stat":
                # Statutes at Large → free GovInfo scan, opened in the browser.
                action = ("browse", statutes_at_large.url_for(m))
            else:
                action = ("cfr", ecfr.cite_spec(m))
            ltags = tags + ("citelink", self._new_link(action))
            txt.insert("end", text[start:end], ltags)
            pos = end
        if pos < len(text):
            txt.insert("end", text[pos:], tags)

    def _render_footnotes(self, footnotes: list, part_tag: Optional[str]) -> None:
        """Insert a part's footnote blocks, recording each note's rendered
        region and number so copied selections can be pin-cited (page n.N)."""
        txt = self._text
        open_region: Optional[list] = None  # [start_index, note_number, page]

        def close_region() -> None:
            nonlocal open_region
            if open_region is not None:
                self._fn_regions.append(
                    (open_region[0], txt.index("end-1c"),
                     open_region[1], open_region[2])
                )
                open_region = None

        last_fid: Optional[str] = None
        for block in footnotes:
            first = block.spans[0] if block.spans else None
            num = ""
            page: Optional[int] = None
            if first is not None and first.fndef:
                num = first.text.strip().strip("[]")
                page = self._fnref_pages.get(first.fndef)
            else:
                body_text = "".join(
                    s.text for s in block.spans if not s.pagenum
                ).lstrip()
                m = _FN_BODY_MARK_RE.match(body_text)
                if m:
                    num = (m.group(1) or m.group(2) or "").strip()
            # Record the note's text, keyed by its anchor id, for hover tips.
            body = re.sub(
                r"\s+", " ",
                "".join(s.text for s in block.spans if not s.pagenum),
            ).strip()
            if first is not None and first.fndef:
                last_fid = first.fndef
                self._fn_text[last_fid] = body
            elif last_fid is not None and body:
                self._fn_text[last_fid] += " " + body
            if num:
                close_region()
                open_region = [txt.index("end-1c"), num, page]
            self._insert_block(block, part_tag)
        close_region()

    def _insert_block(self, block, part_tag: Optional[str]) -> None:
        if block.kind == "center":
            block_tags: tuple = ("center",)
        elif block.kind == "blockquote":
            block_tags = ("blockquote",)
        elif block.kind == "heading":
            block_tags = ("heading",)
        else:
            block_tags = ()
        if part_tag:
            block_tags = block_tags + (part_tag,)
        # The reporter citation lines at the top of a SCOTUS opinion ("512
        # U.S. 477 (1994)") are reference scaffolding, not the Court's prose,
        # so keep them out of the Century Schoolbook body face.
        neutral = self._is_scotus and block.kind in ("center", "heading") and bool(
            _CITE_ONLY_LINE_RE.match(
                re.sub(r"\s+", " ",
                       "".join(s.text for s in block.spans if not s.pagenum)).strip()
            )
        )
        for span in block.spans:
            self._insert_span(span, block_tags, neutral=neutral)
        self._text.insert("end", "\n\n", block_tags)

    def _render_scholar(self) -> None:
        txt = self._text
        txt.config(state="normal")
        txt.delete("1.0", "end")
        self._hide_fn_tip()
        self._link_actions.clear()
        self._fn_text.clear()
        self._fnref_pages: dict[str, Optional[int]] = {}
        self._fn_regions: list[tuple[str, str, str, Optional[int]]] = []
        self._part_regions: list[tuple[str, str, int]] = []
        self._rendered_parts = self._parts  # parts list _part_regions indexes
        self._scroll_part: Optional[int] = None
        self._fn_ref_pos: dict[str, str] = {}  # footnote id → in-text marker index
        self._fn_def_pos: dict[str, str] = {}  # footnote id → body marker index
        self._page_pos: dict[int, str] = {}    # star page → start index
        self._cur_page: Optional[int] = None
        if not self._parts:
            txt.insert("1.0", self._scholar_text)
        else:
            if self._current_part is None:
                shown = list(enumerate(self._parts))
            else:
                shown = [(self._current_part, self._parts[self._current_part])]
            for pi, part in shown:
                part_start = txt.index("end-1c")
                if self._part_start_pages:
                    self._cur_page = self._part_start_pages[pi] or self._cur_page
                for block in part.blocks:
                    self._insert_block(block, None)  # body text stays black
                if part.footnotes:
                    txt.insert("end", "Footnotes\n\n", ("fnhead",))
                    self._render_footnotes(part.footnotes, None)
                part_end = txt.index("end-1c")
                self._part_regions.append((part_start, part_end, pi))
                if self._current_part is None:
                    box = self._PART_BOX_TAGS.get(part.kind)
                    if box:  # light tint behind concurrences/dissents
                        txt.tag_add(box, part_start, part_end)
        txt.config(state="disabled")
        self._mode = "scholar"
        self._source_var.set(self._scholar_url)
        # From the Scholar view, offer the official PDF (the CourtListener text
        # is invariably worse, so it's no longer offered here).
        self._toggle_btn.config(text="View PDF", command=self._view_pdf,
                                state="normal")
        self._export_btn.config(text="Export RTF…", command=self._export_rtf)
        if len(self._parts) > 1:
            self._part_combo.config(state="readonly")
        if self._current_part is None:
            self._view_label_var.set("Full opinion")
            self._view_label.config(foreground="black")
        else:
            part = self._parts[self._current_part]
            self._view_label_var.set(part.label)
            self._view_label.config(
                foreground=self._PART_LABEL_COLORS.get(part.kind, "black")
            )
        extra = f" | {self._note}" if self._note else ""
        self._status_var.set(
            f"{len(self._scholar_text):,} characters | Google Scholar version{extra}"
        )
        self._finder.refresh()

    def _on_part_selected(self, _event=None) -> None:
        idx = self._part_combo.current()
        self._current_part = None if idx <= 0 else idx - 1
        if self._cl_primary or self._mode == "courtlistener":
            self._render_cl_blocks()
        else:
            self._render_scholar()

    def _on_yscroll(self, first: str, last: str) -> None:
        """Keep the scrollbar in sync and, in the full-opinion view, colour the
        top-of-window label to name the part now at the top of the page."""
        vsb = getattr(self, "_vsb", None)
        if vsb is not None:
            vsb.set(first, last)
        self._update_scroll_part()

    def _update_scroll_part(self) -> None:
        """In the full-opinion view, name+colour the part at the top of the
        viewport (so scrolling into a concurrence/dissent colours the header).
        A single selected part keeps its fixed label, so this no-ops there."""
        if getattr(self, "_current_part", None) is not None:
            return
        parts = getattr(self, "_rendered_parts", None)
        regions = getattr(self, "_part_regions", None)
        if not parts or not regions:
            return
        txt = self._text
        try:
            top = txt.index("@0,0")
        except tk.TclError:
            return
        pi = None
        for rs, rend, p in regions:
            if txt.compare(top, ">=", rs) and txt.compare(top, "<", rend):
                pi = p
                break
        if pi is None or pi == getattr(self, "_scroll_part", None):
            return
        self._scroll_part = pi
        kind = parts[pi].kind
        if kind in ("concurrence", "dissent"):
            self._view_label_var.set(parts[pi].label)
            self._view_label.config(
                foreground=self._PART_LABEL_COLORS.get(kind, "black"))
        else:
            self._view_label_var.set("Full opinion")
            self._view_label.config(foreground="black")

    def _render_cl_blocks(self) -> None:
        """Render CourtListener opinion parts with full block formatting."""
        parts = self._cl_parts or self._parts
        # Update part selector to reflect CL parts
        part_values = ["Full opinion"] + [
            f"{i + 1}. {p.label}" for i, p in enumerate(parts)
        ]
        self._part_combo.config(values=part_values)
        if self._current_part is None:
            self._part_combo.current(0)
        txt = self._text
        txt.config(state="normal")
        txt.delete("1.0", "end")
        self._hide_fn_tip()
        self._link_actions.clear()
        self._fn_text.clear()
        self._fnref_pages: dict[str, Optional[int]] = {}
        self._fn_regions: list[tuple[str, str, str, Optional[int]]] = []
        self._part_regions: list[tuple[str, str, int]] = []
        self._rendered_parts = parts  # parts list _part_regions indexes
        self._scroll_part: Optional[int] = None
        self._fn_ref_pos: dict[str, str] = {}
        self._fn_def_pos: dict[str, str] = {}
        self._page_pos: dict[int, str] = {}
        self._cur_page: Optional[int] = None
        if not parts:
            self._insert_plain_with_links(self._cl_text or "(no text)", ())
        else:
            if self._current_part is None:
                shown = list(enumerate(parts))
            else:
                shown = [(self._current_part, parts[self._current_part])]
            for pi, part in shown:
                part_start = txt.index("end-1c")
                for block in part.blocks:
                    self._insert_block(block, None)  # body text stays black
                if part.footnotes:
                    txt.insert("end", "Footnotes\n\n", ("fnhead",))
                    self._render_footnotes(part.footnotes, None)
                part_end = txt.index("end-1c")
                self._part_regions.append((part_start, part_end, pi))
                if self._current_part is None:
                    box = self._PART_BOX_TAGS.get(part.kind)
                    if box:  # light tint behind concurrences/dissents
                        txt.tag_add(box, part_start, part_end)
        txt.config(state="disabled")
        self._mode = "courtlistener"
        self._source_var.set("CourtListener (REST API)")
        toggle_label = (
            "Google Scholar Text" if self._scholar_url else "Scholar unavailable"
        )
        self._toggle_btn.config(
            text=toggle_label, command=self._toggle_source,
            state="normal" if self._scholar_url else "disabled",
        )
        if len(parts) > 1:
            self._part_combo.config(state="readonly")
        else:
            self._part_combo.config(state="disabled")
        if self._current_part is None:
            self._view_label_var.set("Full opinion")
            self._view_label.config(foreground="black")
        else:
            part = parts[self._current_part]
            self._view_label_var.set(part.label)
            self._view_label.config(
                foreground=self._PART_LABEL_COLORS.get(part.kind, "black")
            )
        char_count = len(self._cl_text or self._scholar_text or "")
        self._status_var.set(
            f"{char_count:,} characters | CourtListener version"
        )
        self._finder.refresh()

    def _show_courtlistener(self) -> None:
        if self._cl_parts or self._cl_blocks:
            self._render_cl_blocks()
            return
        txt = self._text
        txt.config(state="normal")
        txt.delete("1.0", "end")
        self._insert_plain_with_links(self._cl_text or "(no text)", ())
        txt.config(state="disabled")
        self._mode = "courtlistener"
        self._source_var.set("CourtListener (assembled from the REST API)")
        self._toggle_btn.config(text="Google Scholar Text",
                                command=self._toggle_source, state="normal")
        self._part_combo.config(state="disabled")
        self._view_label_var.set("CourtListener text")
        self._view_label.config(foreground="black")
        self._status_var.set(
            f"{len(self._cl_text or ''):,} characters | CourtListener version"
        )
        self._finder.refresh()

    # ------------------------------------------------------------------
    # Bluebook citation
    # ------------------------------------------------------------------

    def _compute_bluebook_parts(self) -> dict[str, str]:
        item = self._item
        name = re.sub(
            r"<[^>]+>", "", item.get("caseName") or item.get("case_name") or ""
        ).strip()

        # Scholar's header lists each parallel cite on its own line.  When
        # the page has star pagination, pick the reporter the stars follow
        # (the first star falls just past that cite's first page); without
        # stars, prefer a recognized national/regional reporter, the
        # Bluebook default for state cases.
        header = "  ".join(
            b.text() for b in self._blocks[:8] if b.kind in ("center", "heading")
        )
        cands: list[tuple[str, int]] = []
        for b in self._blocks[:8]:
            if b.kind not in ("center", "heading"):
                continue
            t = re.sub(r"\s+", " ", b.text()).strip()
            t = re.sub(r"\bU\.\s+S\.", "U.S.", t)
            t = re.sub(r"\b(\d{1,4})\s+US\s+(\d{1,5})\b", r"\1 U.S. \2", t)
            m = _HEADER_CITE_RE.match(t)
            if m:
                vol, rep, page = m.group(1), m.group(2).strip(" ,"), m.group(3)
                cands.append((f"{vol} {rep} {page}", int(page)))
        cite = ""
        if cands:
            first_star: Optional[int] = None
            for b in self._blocks:
                for s in b.spans:
                    if s.pagenum:
                        mm = re.search(r"\d+", s.text)
                        if mm:
                            first_star = int(mm.group(0))
                            break
                if first_star is not None:
                    break
            if first_star is not None:
                fits = [
                    (first_star - p, c)
                    for c, p in cands
                    if 0 <= first_star - p <= 400
                ]
                if fits:
                    cite = min(fits)[1]
            if not cite:
                cite = next(
                    (c for c, _p in cands if _TEXT_CITE_RE.fullmatch(c)),
                    cands[0][0],
                )
        if not cite:
            header_norm = re.sub(r"\bU\.\s+S\.", "U.S.", header)
            header_norm = re.sub(
                r"\b(\d{1,4})\s+US\s+(\d{1,5})\b", r"\1 U.S. \2", header_norm
            )
            m = _TEXT_CITE_RE.search(header_norm)
            if m:
                cite = re.sub(r"\s+", " ", m.group(0))
        if not cite:
            cite = _pick_citation(item.get("citation", []))

        date_filed = item.get("dateFiled") or item.get("date_filed") or ""
        year = date_filed[:4] if len(date_filed) >= 4 else ""
        if not year:
            years = re.findall(r"\b(1[6-9]\d{2}|20\d{2})\b", header)
            if years:
                year = years[-1]

        if not name:
            name = _scholar_caption_name(self._blocks)
        if not name and self._blocks:
            first = self._blocks[0].text().strip()
            name = re.split(r",\s*\d{1,4}\s", first)[0].strip().rstrip(",")[:120]

        court_id = str(item.get("court_id") or "").strip().lower()
        if not court_id:
            court_id = _scholar_court_id(self._blocks)
        is_scotus = "scotus" in court_id or bool(
            re.match(r"\d+\s+(U\.S\.|S\.\s?Ct\.|L\.\s?Ed\.)", cite)
        )
        self._is_scotus = is_scotus
        court_abbr = ""
        if not is_scotus:
            fallback = str(item.get("court") or court_id).strip() if court_id else ""
            court_abbr = _court_for_paren(cite, court_id, fallback)
        name = abbreviate_case_name(name)
        cite = _respace_reporter_in_cite(cite)
        return {"name": name, "cite": cite, "court": court_abbr, "year": year}

    def _writer_parenthetical(self, part) -> str:
        """
        Bluebook writer parenthetical for a separate opinion (rule 10.6.1):
        "Rehnquist, J., dissenting", "Wood, J., dissenting from the denial
        of rehearing en banc", or "per curiam" for unsigned opinions.
        Empty for the header and signed majority opinions.
        """
        def block_text(b) -> str:
            t = re.sub(r"\s+", " ", b.text()).strip()
            return re.sub(r"^(?:\*\d+\s+)+", "", t)  # leading page markers

        if part.kind == "majority":
            for b in part.blocks[:3]:
                bt = block_text(b)
                if re.match(r"PER\s+CURIAM\b", bt, re.IGNORECASE):
                    return "per curiam"
                # "JUSTICE O'CONNOR announced the judgment of the Court…" —
                # a lead opinion without a majority (Bluebook rule 10.6.1)
                if re.search(
                    r"announced the judgment of the Court", bt, re.IGNORECASE
                ):
                    return "plurality opinion"
            return ""
        if part.kind not in ("concurrence", "dissent") or not part.blocks:
            return ""
        t = block_text(part.blocks[0])
        m = re.search(r"\b(?:concurring|dissenting)\b", t, re.IGNORECASE)
        if not m:
            return ""
        phrase = t[m.start():].rstrip(" .:;")
        phrase = re.sub(r"\s*\[[^\]]{1,6}\]$", "", phrase)  # trailing footnote marker
        head = t[: m.start()].strip().rstrip(", ")
        head = re.sub(r"^(?:MR\.|MRS\.|MS\.)\s+", "", head, flags=re.IGNORECASE)
        # The chief-justice test must look only at the author's own
        # designation — "JUSTICE THOMAS, with whom THE CHIEF JUSTICE and
        # JUSTICE ALITO join, dissenting" is Thomas, J., not C.J.
        segs = [s.strip() for s in head.split(",")]
        author_seg = segs[0]
        role_seg = segs[1] if len(segs) > 1 else ""
        is_chief = bool(
            re.match(r"(?:THE\s+)?CHIEF\s+JUSTICE\b", author_seg, re.IGNORECASE)
        ) or bool(
            re.fullmatch(
                r"(?:THE\s+)?(?:Chief\s+(?:Justice|Judge)|C\.\s?J\.)",
                role_seg,
                re.IGNORECASE,
            )
        )
        title = "C.J." if is_chief else "J."
        name = re.sub(
            r"^(?:THE\s+)?(?:CHIEF\s+)?JUSTICE\s+", "", author_seg, flags=re.IGNORECASE
        ).strip()
        name = _fix_name_case(name)
        if not name:
            return ""
        return f"{name}, {title}, {phrase}"

    def _majority_author(self, part) -> str:
        """Running-head label for the lead opinion: 'Blackmun, J.',
        'Sykes, C.J.', 'per curiam', or '' when no author is identified.
        Case-insensitive: Google Scholar renders many opinions' attribution
        lines in mixed case ('Justice Barrett delivered the opinion…')."""
        for b in part.blocks[:3]:
            t = re.sub(r"\s+", " ", b.text()).strip()
            t = re.sub(r"^(?:\*\d+\s+)+", "", t)
            if re.match(r"PER\s+CURIAM\b", t, re.IGNORECASE):
                return "per curiam"
            m = re.match(
                r"(?:(?:MR\.|MRS\.|MS\.)\s+)?(CHIEF\s+)?JUSTICE\s+([A-Z][\w.'’-]+)\s+"
                r"(?:delivered|announced)",
                t, re.IGNORECASE,
            )
            if m:
                title = "C.J." if m.group(1) else "J."
                return f"{_fix_name_case(m.group(2))}, {title}"
            m = re.match(
                r"([A-Z][\w.'’ -]{0,40}?),\s*((?:Chief\s+)?(?:Senior\s+)?"
                r"(?:Circuit\s+|District\s+)?Judge)\s*[.:;]?\s*$",
                t, re.IGNORECASE,
            )
            if m:
                title = ("C.J." if re.search(r"\bChief\b", m.group(2), re.IGNORECASE)
                         else "J.")
                return f"{_fix_name_case(m.group(1).split(',')[0])}, {title}"
        return ""

    def _refine_part_labels(self, parts: list) -> None:
        """Sharpen the lead opinion's label.  ``segment_blocks`` calls every
        lead opinion "Majority Opinion", but a lead opinion that stands alone
        — no concurrences and no dissents — is simply the "Opinion" (as it is
        for a one-judge district court).  A Supreme Court lead opinion that
        only announces the judgment is a "Plurality Opinion"; otherwise it is
        the "Majority Opinion".  Each type is then followed by its author, the
        way the concurrence and dissent headers already name theirs."""
        maj = next((p for p in parts if p.kind == "majority"), None)
        if maj is None:
            return
        has_dissent = any(p.kind == "dissent" for p in parts)
        has_concurrence = any(p.kind == "concurrence" for p in parts)
        signal = self._writer_parenthetical(maj)  # "" | per curiam | plurality
        if signal == "plurality opinion" and self._is_scotus:
            base = "Plurality Opinion"
        elif not has_dissent and not has_concurrence:
            base = "Opinion"
        else:
            base = "Majority Opinion"
        author = self._majority_author(maj)  # "Name, J." | "per curiam" | ""
        if signal == "per curiam" or author == "per curiam":
            author = "Per Curiam"
        maj.label = f"{base} ({author})" if author else base

    def _bluebook_citation(
        self, pin: Optional[str], writer: str = ""
    ) -> tuple[str, str]:
        """Return (plain, rtf-fragment) forms of the Bluebook citation."""
        bb = self._bb
        name, cite, court, year = bb["name"], bb["cite"], bb["court"], bb["year"]
        rest = ""
        if cite:
            rest = f", {cite}"
            if pin:
                m = _CITE_PARSE_RE.match(cite)
                if not (m and pin == m.group(3)):  # skip pin equal to first page
                    rest += f", {pin}"
        paren_inner = " ".join(p for p in (court, year) if p)
        if paren_inner:
            rest += f" ({paren_inner})"
        if writer:
            rest += f" ({writer})"
        rest += "."
        # Bluebook abbreviations ("Ass'n", "Int'l", "Dep't", "F. App'x"),
        # possessives, and names like O'Connor take a typographic apostrophe
        # (right single quotation mark) when copied or exported.
        name = name.replace("'", "’")
        rest = rest.replace("'", "’")
        if name:
            plain = f"{name}{rest}"
            rtf = (
                "\\par\\pard\\sa120 {\\i "
                + _rtf_escape(name)
                + "}"
                + _rtf_escape(rest)
                + "\\par\n"
            )
        else:
            plain = rest.lstrip(", ")
            rtf = "\\par\\pard\\sa120 " + _rtf_escape(plain) + "\\par\n"
        return plain, rtf

    @staticmethod
    def _page_num_from(s: str) -> Optional[int]:
        m = re.search(r"\d+", s)
        return int(m.group(0)) if m else None

    def _pin_for_range(self, start: str, end: str) -> Optional[str]:
        """Pinpoint page(s) for the text between *start* and *end*, derived
        from the star-pagination markers (Bluebook-style range, e.g. 120-21)."""
        txt = self._text
        start_page: Optional[int] = None
        prev = txt.tag_prevrange("pagenum", start)
        if prev:
            start_page = self._page_num_from(txt.get(*prev))
        else:
            # No star marker on screen before the selection: in a part view
            # use the page in effect where the part begins; otherwise the
            # text sits on the cite's first page.
            if self._current_part is not None and self._part_start_pages:
                start_page = self._part_start_pages[self._current_part]
            if start_page is None:
                m = _CITE_PARSE_RE.match(self._bb["cite"])
                if m:
                    start_page = int(m.group(3))
        if start_page is None:
            return None
        end_page = start_page
        idx = start
        while True:
            rng = txt.tag_nextrange("pagenum", idx, end)
            if not rng:
                break
            p = self._page_num_from(txt.get(*rng))
            if p is not None:
                end_page = p
            idx = rng[1]
        if end_page <= start_page:
            return str(start_page)
        sa, sb = str(start_page), str(end_page)
        if len(sa) == len(sb) and len(sa) > 2 and sa[:-2] == sb[:-2]:
            sb = sb[-2:]  # Bluebook: drop repetitious digits, keep last two
        return f"{sa}-{sb}"

    @staticmethod
    def _format_note_numbers(nums: list[str]) -> str:
        """Bluebook note pins: n.4 / nn.4-5 (consecutive) / nn.4 & 6."""
        runs: list[list[str]] = []
        for n in nums:
            if (
                runs
                and n.isdigit()
                and runs[-1][-1].isdigit()
                and int(n) == int(runs[-1][-1]) + 1
            ):
                runs[-1].append(n)
            else:
                runs.append([n])
        parts = [f"{r[0]}-{r[-1]}" if len(r) > 1 else r[0] for r in runs]
        prefix = "nn." if len(nums) > 1 else "n."
        return prefix + " & ".join(parts)

    def _pin_with_footnotes(self, start: str, end: str) -> Optional[str]:
        """
        Pinpoint for the selection, footnote-aware (Bluebook rule 3.2(b)):
        material in a footnote cites as "page n.N"; several notes as
        "nn.4-5" / "nn.4 & 6"; text plus a note on the same page as
        "page & n.N".
        """
        txt = self._text
        regions = [
            r for r in self._fn_regions
            if txt.compare(r[0], "<", end) and txt.compare(r[1], ">", start)
        ]
        if not regions:
            return self._pin_for_range(start, end)

        fallback_page: Optional[int] = None
        m = _CITE_PARSE_RE.match(self._bb["cite"])
        if m:
            fallback_page = int(m.group(3))

        # Group selected notes by the page they're cited on (document order)
        page_groups: dict[Optional[int], list[str]] = {}
        for _rs, _re, num, page in regions:
            page_groups.setdefault(page, []).append(num)
        note_strs = []
        for page, nums in page_groups.items():
            p = page if page is not None else fallback_page
            s = self._format_note_numbers(nums)
            note_strs.append(f"{p} {s}" if p is not None else s)
        notes = ", ".join(note_strs)

        # Does the selection also cover opinion text before the notes?
        first_rs = regions[0][0]
        text_before = (
            txt.compare(start, "<", first_rs)
            and txt.get(start, first_rs).strip() != ""
        )
        if not text_before:
            return notes
        text_pin = self._pin_for_range(start, first_rs)
        if text_pin is None:
            return notes
        if len(page_groups) == 1:
            (page, nums), = page_groups.items()
            p = page if page is not None else fallback_page
            if p is not None and text_pin == str(p):
                return f"{text_pin} & {self._format_note_numbers(nums)}"
        return f"{text_pin}, {notes}"

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _copy_formatted(self) -> None:
        txt = self._text
        try:
            start, end = txt.index("sel.first"), txt.index("sel.last")
            selected = True
        except tk.TclError:
            start, end = "1.0", "end-1c"
            selected = False
        pin = (
            self._pin_with_footnotes(start, end)
            if (selected and self._mode == "scholar")
            else None
        )
        writer = ""
        if self._mode == "scholar" and self._parts:
            pi = self._current_part
            if pi is None and selected:
                for rs, rend, p in self._part_regions:
                    if txt.compare(start, ">=", rs) and txt.compare(start, "<", rend):
                        pi = p
                        break
            if pi is not None:
                writer = self._writer_parenthetical(self._parts[pi])
        plain_cite, rtf_cite = self._bluebook_citation(pin, writer)
        body = _dump_to_rtf(txt, start, end, fn_links=self._fn_link_map())
        rtf = _rtf_document(body + rtf_cite)
        plain = txt.get(start, end).rstrip() + "\n\n" + plain_cite + "\n"
        how = _copy_rich_clipboard(self._win, rtf, plain)
        what = "selection" if selected else "full text"
        self._status_var.set(f"Copied {what} as {how}; citation appended.")

    def _fn_link_map(self) -> dict[str, tuple[str, str]]:
        """Link tags that anchor footnote jumps, for RTF bookmarks."""
        return {t: a for t, a in self._link_actions.items()
                if a[0] in ("fnref", "fndef")}

    def _filename_item(self) -> dict:
        if self._item:
            return self._item
        bb = self._bb
        return {
            "caseName": bb["name"],
            "citation": [bb["cite"]] if bb["cite"] else [],
            "dateFiled": f"{bb['year']}-01-01" if bb["year"] else "",
            "court_id": "scotus" if not bb["court"] else "",
            "court": bb["court"],
        }

    def _build_export_rtf(self) -> str:
        """
        Two-column RTF of the full opinion, one section per separate
        opinion: the header and majority share the first section, and each
        concurrence/dissent starts a new page (numbering continues).  Every
        section carries a running head with the Bluebook citation and the
        opinion's author, and a page-number footer.  The running head is
        coloured by opinion kind (dissent red, concurrence green); the body
        text is black.
        """
        txt = self._text
        case_line = self._bluebook_citation(None)[0].rstrip(".")

        main_end = -1
        for i, (_rs, _re, pi) in enumerate(self._part_regions):
            if self._parts[pi].kind in ("header", "majority"):
                main_end = i
            else:
                break
        main_regions = self._part_regions[: main_end + 1]
        rest_regions = self._part_regions[main_end + 1:]

        # (author label, start, end, kind)
        sections: list[tuple[str, str, str, str]] = []
        if main_regions:
            maj = next(
                (self._parts[pi] for _rs, _re, pi in main_regions
                 if self._parts[pi].kind == "majority"),
                None,
            )
            label = self._majority_author(maj) if maj is not None else ""
            sections.append((label, main_regions[0][0], main_regions[-1][1],
                             "majority"))
        for rs, rend, pi in rest_regions:
            sections.append((self._writer_parenthetical(self._parts[pi]), rs,
                             rend, self._parts[pi].kind))

        # Colour only the running heading by opinion kind (dissent red,
        # concurrence green); the body text of every opinion stays black.
        head_cf = {"dissent": "\\cf2 ", "concurrence": "\\cf3 "}
        out: list[str] = []
        for i, (label, rs, rend, kind) in enumerate(sections):
            out.append(
                "\\sectd\\sbknone\\cols2\\colsx432\n"
                if i == 0
                else "\\sect\\sectd\\sbkpage\\cols2\\colsx432\n"
            )
            head = f"{case_line} — {label}" if label else case_line
            out.append(
                "{\\header\\pard\\qc\\fs18\\i " + head_cf.get(kind, "")
                + _rtf_escape(head) + "\\par}\n"
            )
            out.append("{\\footer\\pard\\qc\\fs18\\chpgn\\par}\n")
            out.append(_dump_to_rtf(txt, rs, rend, part_colors=False,
                                    fn_links=self._fn_link_map()))
        return _RTF_HEADER + "".join(out) + "}"

    def _export_rtf(self) -> None:
        if self._mode == "scholar" and self._parts:
            # Export the full opinion even from a single-part view
            prev = self._current_part
            if prev is not None:
                self._current_part = None
                self._render_scholar()
            try:
                rtf = self._build_export_rtf()
            finally:
                if prev is not None:
                    self._current_part = prev
                    self._render_scholar()
        else:
            body = _dump_to_rtf(self._text, "1.0", "end-1c",
                                fn_links=self._fn_link_map())
            rtf = _rtf_document(body, two_columns=True, page_footer=True)
        default = _build_default_filename(self._filename_item())
        path = filedialog.asksaveasfilename(
            defaultextension=".rtf",
            filetypes=[("Rich Text Format", "*.rtf"), ("All files", "*.*")],
            initialfile=f"{default}.rtf",
            title="Export Opinion as RTF (two columns)",
            parent=self._win,
        )
        if not path:
            return
        with open(path, "w", encoding="ascii", errors="replace") as f:
            f.write(rtf)
        self._status_var.set(f"Exported RTF: {path}")
        if messagebox.askyesno(
            "Export Complete", f"RTF saved to:\n{path}\n\nOpen it now?", parent=self._win
        ):
            CourtListenerGUI._open_file(path)

    # ------------------------------------------------------------------
    # Case details side panel (authors and joins per opinion)
    # ------------------------------------------------------------------

    def _details_panel(self) -> ttk.Frame:
        if self._details_frame is None:
            f = ttk.Frame(self._text_frame)
            ttk.Label(
                f, text="Opinions & Joins",
                font=("TkDefaultFont", 9, "bold"),
            ).pack(anchor="w", padx=6, pady=(4, 2))
            body = tk.Text(
                f, width=34, wrap="word", font=("TkDefaultFont", 9),
                state="disabled", padx=8, pady=4, relief="flat",
                background="#f7f5ef",
            )
            dvsb = ttk.Scrollbar(f, orient="vertical", command=body.yview)
            body.configure(yscrollcommand=dvsb.set)
            dvsb.pack(side="right", fill="y")
            body.pack(side="left", fill="both", expand=True)
            body.tag_configure("h", font=("TkDefaultFont", 9, "bold"),
                               spacing1=10)
            body.tag_configure("lbl", font=("TkDefaultFont", 9, "italic"),
                               foreground="#666666")
            self._details_text = body
            self._details_frame = f
        return self._details_frame

    def _toggle_details(self) -> None:
        if self._details_var.get():
            self._details_panel().pack(side="right", fill="y",
                                       before=self._vsb)
            if not self._details_loaded:
                self._load_details()
        elif self._details_frame is not None:
            self._details_frame.pack_forget()

    def _set_details(self, lines: list[tuple[str, str]]) -> None:
        body = self._details_text
        body.config(state="normal")
        body.delete("1.0", "end")
        for style, text in lines:
            body.insert("end", text + "\n", (style,) if style else ())
        body.config(state="disabled")

    def _load_details(self) -> None:
        """Fetch authorship/join data from CourtListener (author_str /
        joined_by_str per sub-opinion — well populated for SCOTUS); when
        that yields nothing, fall back to what the Scholar text itself
        says (the syllabus line-up paragraph and separator headers)."""
        self._details_loaded = True
        self._set_details([("lbl", "Loading case details…")])
        client = (
            self._app._get_client()
            if self._app._token_var.get().strip() else None
        )
        item = dict(self._item)
        cite = self._bb["cite"]

        def run() -> None:
            lines: list[tuple[str, str]] = []
            try:
                if client is None:
                    raise RuntimeError("no CourtListener token configured")
                cid = item.get("cluster_id") or item.get("id")
                if not cid:
                    if not cite:
                        raise RuntimeError("no citation to locate the case")
                    data = client.search(f"citation:({cite})", type="o",
                                         page_size=1)
                    results = data.get("results") or []
                    if not results:
                        data = client.search(f'"{cite}"', type="o",
                                             page_size=1)
                        results = data.get("results") or []
                    if not results:
                        raise RuntimeError("case not found on CourtListener")
                    cid = (results[0].get("cluster_id")
                           or results[0].get("id"))
                cluster = client.get_cluster(
                    int(cid), fields="judges,sub_opinions")
                ops = []
                for url in cluster.get("sub_opinions") or []:
                    try:
                        ops.append(client._get_url(url, {
                            "fields": "ordering_key,type,author_str,"
                                      "joined_by_str,per_curiam",
                        }))
                    except Exception as exc:
                        print(f"[details] sub-opinion fetch failed: {exc}")
                ops.sort(key=lambda o: (o.get("ordering_key") is None,
                                        o.get("ordering_key") or 0))
                lines = self._details_lines_cl(cluster, ops)
            except Exception as exc:
                print(f"[details] {exc}")
            if not lines:
                lines = self._details_lines_parts()
            if not lines:
                lines = [("lbl", "No authorship details available "
                                 "for this case.")]
            self._post(self._set_details, lines)

        threading.Thread(target=run, daemon=True).start()

    @staticmethod
    def _details_lines_cl(cluster: dict, ops: list[dict]
                          ) -> list[tuple[str, str]]:
        lines: list[tuple[str, str]] = []
        judges = _strip_html(cluster.get("judges") or "").strip()
        if judges:
            lines += [("h", "Panel"), ("", judges)]
        any_data = bool(judges)
        for op in ops:
            label = _OPINION_TYPE_LABELS.get(op.get("type") or "",
                                             "Opinion")
            author = (op.get("author_str") or "").strip()
            joined = (op.get("joined_by_str") or "").strip()
            if op.get("per_curiam") and not author:
                author = "Per curiam"
            lines.append(("h", label))
            if author:
                lines.append(("", f"Author: {author}"))
            if joined:
                lines.append(("", f"Joined by: {joined}"))
            if author or joined:
                any_data = True
            else:
                lines.append(("lbl", "No authorship data"))
        # all-empty CourtListener data -> let the Scholar fallback try
        return lines if any_data else []

    def _details_lines_parts(self) -> list[tuple[str, str]]:
        """Authorship gleaned from the Scholar text: the syllabus
        "delivered the opinion … joined" paragraph plus each separate
        opinion's header line."""
        def clean(raw: str) -> str:
            t = re.sub(r"\s+", " ", raw).strip()
            return re.sub(r"^(?:\*\d+\s+)+", "", t)  # leading page markers

        lines: list[tuple[str, str]] = []
        header = next((p for p in self._parts if p.kind == "header"), None)
        if header is not None:
            for b in header.blocks:
                t = clean(b.text())
                if len(t) < 900 and re.search(
                    r"delivered the opinion|announced the judgment"
                    r"|filed (?:a|an) (?:concurring|dissenting)"
                    r"|join(?:ed|ing)\b",
                    t, re.IGNORECASE,
                ):
                    if not lines:
                        lines.append(("h", "Line-up"))
                    lines.append(("", _fix_name_case(t)))
        for part in self._parts:
            if part.kind == "header":
                continue
            if part.kind == "majority":
                lines.append(("h", part.label or "Opinion"))
                for b in part.blocks[:3]:
                    t = clean(b.text())
                    if len(t) <= 200 and re.search(
                        r"delivered the opinion|announced the judgment",
                        t, re.IGNORECASE,
                    ):
                        lines.append(("", _fix_name_case(t)))
                        break
            else:
                lines.append(("h", "Dissent" if part.kind == "dissent"
                              else "Concurrence"))
                lines.append(("", _fix_name_case(clean(part.label))))
        return lines

    # ------------------------------------------------------------------
    # Citation links
    # ------------------------------------------------------------------

    def _post(self, fn, *args) -> None:
        try:
            self._win.after(0, fn, *args)
        except tk.TclError:
            pass  # window closed while a background fetch was running

    def _jump_to(self, pos: str) -> None:
        txt = self._text
        txt.see(pos)
        txt.tag_remove("jumpflash", "1.0", "end")
        txt.tag_add("jumpflash", f"{pos} linestart", f"{pos} lineend")
        self._win.after(
            1400, lambda: txt.tag_remove("jumpflash", "1.0", "end")
        )

    def _follow_link(self, tag: str) -> None:
        action = self._link_actions.get(tag)
        if not action:
            return
        kind, value = action
        if kind == "fnref":
            pos = self._fn_def_pos.get(value)
            if pos:
                self._jump_to(pos)
            return
        if kind == "fndef":
            pos = self._fn_ref_pos.get(value)
            if pos:
                self._jump_to(pos)
            return
        if kind in _STATUTE_SOURCES:
            self._open_statute(kind, value)
            return
        if kind == "browse":
            # Link-out to an external source (e.g. a state statute we don't
            # render in-app); open it in the user's browser.
            webbrowser.open(value)
            self._status_var.set("Opened in your browser.")
            return
        # CourtListener opinion URL: fetch structured text from CL directly
        if kind == "url" and "courtlistener.com/opinion/" in value:
            self._follow_cl_link(value)
            return
        fetcher = self._app._get_scholar()
        cite, _, pin = value.partition("@") if kind == "cite" else (value, "", "")
        label = cite if kind == "cite" else "cited case"
        if fetcher is None:
            if kind == "cite":
                self._follow_cite_via_cl(cite, pin)
            else:
                self._status_var.set("Google Scholar is not available.")
            return
        self._status_var.set(f"Fetching {label} from Google Scholar…")

        def run() -> None:
            if kind == "url":
                result = fetcher.fetch_by_url(value)
            else:
                result = fetcher.fetch_by_citation(cite)
            self._post(self._on_link_ready, result, cite, pin)

        threading.Thread(target=run, daemon=True).start()

    def _follow_cl_link(self, url: str) -> None:
        """Open a CourtListener opinion URL with structured block rendering."""
        client = self._app._get_client()
        if client is None:
            return
        m = re.search(r"/opinion/(\d+)/", url)
        if not m:
            return
        opinion_id = m.group(1)
        self._status_var.set("Fetching opinion from CourtListener…")

        def run() -> None:
            try:
                op = client.get_opinion(
                    int(opinion_id),
                    fields="cluster,html_with_citations,html,plain_text",
                )
                cluster_url = op.get("cluster") or ""
                cm = re.search(r"/(\d+)/", cluster_url)
                if cm:
                    item = {"cluster_id": cm.group(1)}
                    parts, blocks, plain, cluster = _assemble_case_parts(
                        client, item,
                    )
                    self._post(self._on_cl_link_ready, parts, blocks, plain, item)
                else:
                    self._post(self._on_cl_link_error, "Could not resolve cluster.")
            except Exception as exc:
                self._post(self._on_cl_link_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _on_cl_link_ready(self, parts, blocks, plain, item) -> None:
        self._status_var.set("Cited case loaded from CourtListener.")
        _ScholarTextWindow(
            self._win, self._app, "", "",
            item=item, cl_text=plain,
            cl_parts=parts, cl_blocks=blocks,
        )

    def _on_cl_link_error(self, msg: str) -> None:
        self._status_var.set(f"CourtListener: {msg}")

    def _follow_cite_via_cl(self, cite: str, pin: str = "") -> None:
        """Follow a citation link using CourtListener when Scholar is unavailable."""
        client = self._app._get_client()
        if client is None:
            return
        self._status_var.set(f"Fetching {cite} from CourtListener…")

        def run() -> None:
            try:
                data = client.search(f'"{cite}"', type="o", page_size=1)
                results = data.get("results") or []
                if not results:
                    self._post(self._on_cl_link_error, f"No match for {cite!r}.")
                    return
                target = results[0]
                parts, blocks, plain, cluster = _assemble_case_parts(
                    client, target,
                )
                self._post(self._on_cl_link_ready, parts, blocks, plain, target)
            except Exception as exc:
                self._post(self._on_cl_link_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _on_link_ready(self, result: Optional[tuple[str, str]],
                       cite: str = "", pin: str = "") -> None:
        if not result:
            # Scholar failed — try CourtListener as fallback
            if cite:
                self._follow_cite_via_cl(cite, pin)
            else:
                self._status_var.set(
                    "Google Scholar: cited case not found (or blocked)."
                )
            return
        url, html = result
        self._status_var.set("Cited case loaded.")
        win = _ScholarTextWindow(self._win, self._app, url, html, item=None)
        if cite and pin:
            win.jump_to_cite_page(cite, pin)

    def jump_to_cite_page(self, cite: str, pin: str) -> None:
        """Scroll to and flash the star marker for a pin-cited page, when
        this window's star pagination follows the same reporter as the
        citation that was clicked."""
        m_link = _CITE_PARSE_RE.match(cite)
        m_here = _CITE_PARSE_RE.match(self._bb["cite"])
        if not (m_link and m_here):
            return
        norm = lambda r: re.sub(r"[\s.]", "", r).lower()
        if norm(m_link.group(2)) != norm(m_here.group(2)):
            self._status_var.set(
                f"Pin page {pin} is in {m_link.group(2).strip()}; this text "
                f"is paginated by {m_here.group(2).strip()}."
            )
            return
        m_page = re.match(r"\d+", pin)
        pos = self._page_pos.get(int(m_page.group(0))) if m_page else None
        if pos:
            self._jump_to(pos)
            self._status_var.set(f"Jumped to page *{m_page.group(0)}.")
        else:
            self._status_var.set(f"Page *{pin} not marked in this text.")

    def _open_statute(self, kind: str, spec: str) -> None:
        """Fetch a U.S. Code (OLRC) or C.F.R. (eCFR) section and show it."""
        _fetch_statute_window(self._win, kind, spec, self._status_var.set)

    # ------------------------------------------------------------------
    # CourtListener toggle
    # ------------------------------------------------------------------

    def _toggle_source(self) -> None:
        if self._mode == "courtlistener":
            if self._cl_primary and not self._scholar_url:
                return
            self._render_scholar()
            return
        if self._cl_parts:
            self._render_cl_blocks()
            return
        if self._cl_text is not None:
            self._show_courtlistener()
            return
        client = self._app._get_client()
        if client is None:
            return
        self._toggle_btn.config(state="disabled")
        self._status_var.set("Fetching CourtListener text…")
        item = dict(self._item)
        cite = self._bb["cite"]

        def run() -> None:
            try:
                target = item
                if not (target.get("cluster_id") or target.get("id")):
                    if not cite:
                        raise RuntimeError(
                            "No citation available to locate this case on CourtListener."
                        )
                    data = client.search(f"citation:({cite})", type="o", page_size=1)
                    results = data.get("results") or []
                    if not results:
                        data = client.search(f'"{cite}"', type="o", page_size=1)
                        results = data.get("results") or []
                    if not results:
                        raise RuntimeError(f"No CourtListener match for {cite!r}.")
                    target = results[0]
                parts, blocks, plain, cluster = _assemble_case_parts(
                    client, target,
                )
                text = _assemble_case_text(client, target) if not plain else plain
                self._post(self._on_cl_ready, text, parts, blocks)
            except Exception as exc:
                self._post(self._on_cl_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _on_cl_ready(self, text: str, parts=None, blocks=None) -> None:
        self._cl_text = text
        if parts:
            self._cl_parts = parts
            self._cl_blocks = blocks
            self._render_cl_blocks()
        else:
            self._show_courtlistener()

    def _on_cl_error(self, msg: str) -> None:
        self._toggle_btn.config(state="normal")
        self._status_var.set(f"CourtListener: {msg}")
        messagebox.showerror("CourtListener", msg, parent=self._win)

    # ------------------------------------------------------------------
    # PDF view (official opinion PDF, shown in-app)
    # ------------------------------------------------------------------

    def _pdf_item(self) -> dict:
        """The search-result-shaped dict used to resolve a PDF URL.  Falls back
        to the Bluebook citation when this window wasn't opened from a result."""
        item = dict(self._item) if self._item else {}
        if not item.get("citation") and self._bb.get("cite"):
            item["citation"] = [self._bb["cite"]]
        return item

    def _view_pdf(self) -> None:
        """Resolve and show the official PDF of the opinion inside the window."""
        try:
            import pypdfium2  # noqa: F401
            from PIL import ImageTk  # noqa: F401
        except ImportError:
            if messagebox.askyesno(
                "PDF viewer not installed",
                "Viewing PDFs inside the app needs two Python packages:\n\n"
                "    pip install pypdfium2 Pillow\n\n"
                "Open the PDF in your web browser instead?",
                parent=self._win,
            ):
                self._open_pdf_in_browser()
            return
        client = self._app._get_client()
        self._pdf_url = None
        self._toggle_btn.config(state="disabled")
        self._status_var.set("Locating a PDF of the opinion…")
        item = self._pdf_item()

        def run() -> None:
            try:
                url = (self._app._resolve_pdf_url(client, item)
                       if client is not None else None)
                if not url:
                    self._post(self._on_pdf_error,
                               "No PDF is available for this opinion.")
                    return
                self._pdf_url = url  # so a fetch failure can offer the browser
                resp = _anon_session.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.content
                if not data.startswith(b"%PDF"):
                    self._post(self._on_pdf_error,
                               "The source returned something that isn't a PDF.")
                    return
                self._post(self._show_pdf, data, url)
            except Exception as exc:
                self._post(self._on_pdf_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _show_pdf(self, data: bytes, url: str) -> None:
        width = max(self._text.winfo_width() - 24, 520)
        try:
            pane = _PdfPane(self._win, data, width=width)
        except Exception as exc:  # pragma: no cover - render/lib failure
            self._on_pdf_error(str(exc))
            return
        # Swap the text view for the PDF pane (kept above the button row).
        self._text_frame.pack_forget()
        pane.pack(fill="both", expand=True, padx=8, pady=4,
                  before=self._btn_frame)
        self._pdf_pane = pane
        self._pdf_url = url
        self._pdf_bytes = data
        self._mode = "pdf"
        self._part_combo.config(state="disabled")
        self._view_label_var.set("PDF of opinion")
        self._view_label.config(foreground="black")
        self._source_var.set(url)
        self._toggle_btn.config(text="Google Scholar Text",
                                command=self._back_from_pdf, state="normal")
        # In PDF view, the RTF export becomes a "Download PDF" action.
        self._export_btn.config(text="Download PDF", command=self._download_pdf)
        self._status_var.set("Showing the official PDF of the opinion.")

    def _download_pdf(self) -> None:
        """Save the PDF currently being viewed to a file the user chooses."""
        data = getattr(self, "_pdf_bytes", None)
        if not data:
            return
        default = _build_default_filename(self._filename_item())
        path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{default}.pdf",
            title="Download Opinion PDF",
            parent=self._win,
        )
        if not path:
            return
        try:
            with open(path, "wb") as fh:
                fh.write(data)
        except Exception as exc:
            messagebox.showerror("Download PDF", str(exc), parent=self._win)
            return
        self._status_var.set(f"Saved PDF to {path}")

    def _back_from_pdf(self) -> None:
        """Return from the PDF to the Google Scholar text view."""
        if self._pdf_pane is not None:
            self._pdf_pane.destroy()
            self._pdf_pane = None
        self._text_frame.pack(fill="both", expand=True, padx=8, pady=4,
                              before=self._btn_frame)
        self._render_scholar()  # restores the label, combo and "View PDF" button

    def _on_pdf_error(self, msg: str) -> None:
        self._toggle_btn.config(text="View PDF", command=self._view_pdf,
                                state="normal")
        self._status_var.set(f"PDF: {msg}")
        if self._pdf_url and messagebox.askyesno(
            "PDF", f"{msg}\n\nOpen the PDF in your web browser instead?",
            parent=self._win,
        ):
            webbrowser.open(self._pdf_url)
        elif not self._pdf_url:
            messagebox.showinfo("PDF", msg, parent=self._win)

    def _open_pdf_in_browser(self) -> None:
        """Resolve the PDF URL in the background and open it in the browser."""
        client = self._app._get_client()
        if client is None:
            return
        self._status_var.set("Locating a PDF of the opinion…")
        item = self._pdf_item()

        def run() -> None:
            try:
                url = self._app._resolve_pdf_url(client, item)
            except Exception:
                url = None
            self._post(self._after_resolve_for_browser, url)

        threading.Thread(target=run, daemon=True).start()

    def _after_resolve_for_browser(self, url: Optional[str]) -> None:
        if url:
            webbrowser.open(url)
            self._status_var.set("Opened the PDF in your browser.")
        else:
            self._status_var.set("No PDF is available for this opinion.")
            messagebox.showinfo(
                "PDF", "No PDF is available for this opinion.", parent=self._win)


# Cross-references in the U.S. Code's own style: "section 3142(f) of
# title 18", "section 102 of this title" (resolved against the open doc).
_USC_XREF_RE = re.compile(
    r"\bsections?\s+(\d+[a-zA-Z0-9]*(?:[-–—]\d+[a-zA-Z0-9]*)?)"
    r"((?:\((?:\d{1,3}|[ivxIVX]{2,4}|[a-zA-Z]{1,3})\))*)"
    r"\s+of\s+(?:[Tt]itle\s+(\d{1,2})|this\s+title)",
    re.IGNORECASE,
)

# Bare section references inside a C.F.R. provision ("§ 1614.106(a)"),
# resolved against the open title.
_CFR_SECREF_RE = re.compile(
    r"§§?\s*(\d+[a-zA-Z]?\.\d+[a-zA-Z0-9]*)"
    r"((?:\((?:\d{1,3}|[ivxIVX]{2,4}|[a-zA-Z]{1,3})\))*)"
)

# A reporter citation on a hand-typed line: volume, reporter, page.
# Broader than _TEXT_CITE_RE (any capitalized reporter form, so official
# state reporters like "306 Md. 556" work) since the input is a citation
# list, not running prose.
_LINE_CITE_RE = re.compile(
    r"(\d{1,4})\s+([A-Z][A-Za-z0-9.'’ ]{0,24}?)\s+(\d{1,5})(?=[\s,;.)(]|$)"
)


def _parse_citation_line(line: str) -> Optional[tuple[str, str, str]]:
    """Parse "Name v. Name, 365 U.S. 167, 171 (1961)" into
    (case name, citation, pin) — name and pin may be empty."""
    m = _LINE_CITE_RE.search(line)
    if not m:
        return None
    cite = re.sub(r"\s+", " ",
                  f"{m.group(1)} {m.group(2)} {m.group(3)}")
    cite = cite.replace("U. S.", "U.S.").replace("’", "'")
    cite = _respace_reporter_in_cite(cite)
    name = line[: m.start()].strip().rstrip(",;–—- ").strip()
    pin_m = _PINCITE_AFTER_RE.match(line, m.end())
    pin = pin_m.group(1) if pin_m else ""
    return name, cite, pin


# A hand-typed statute/regulation lookup: "42 USC 1983(b)", "29 cfr
# 1614.105(a)", with or without periods and the section symbol.
_STATUTE_QUERY_RE = re.compile(
    r"^\s*(\d{1,2})\s*"
    r"(u\.?\s*s\.?\s*c\.?\s*a?\.?|c\.?\s*f\.?\s*r\.?)\s*"
    r"(?:§§?|sec(?:tions?)?\.?)?\s*"
    r"(\d[\w.–—-]*)"
    r"((?:\s*\(\w{1,4}\))*)\s*$",
    re.IGNORECASE,
)


def _parse_statute_query(query: str) -> Optional[tuple[str, str]]:
    """Parse a typed citation into ("usc"|"cfr"|"rule"|"statestat", spec), or
    None.  Federal-rule queries ("fre 404(b)", "Fed. R. Civ. P. 56") and state
    statute queries ("Cal. Penal Code § 187") never start with a volume number,
    so they can't collide with the U.S.C./C.F.R. form and are tried first."""
    rule = fed_rules.parse_query(query)
    if rule:
        return rule
    statestat = state_statutes.parse_query(query)
    if statestat:
        return statestat
    m = _STATUTE_QUERY_RE.match(query or "")
    if not m:
        return None
    kind = "cfr" if "f" in m.group(2).lower() else "usc"
    section = m.group(3).rstrip(".").replace("–", "-").replace("—", "-")
    if not section or (kind == "cfr" and "." not in section):
        return None  # CFR sections are part.section ("1614.105")
    subs = re.findall(r"\(([^)]+)\)", m.group(4) or "")
    return kind, f"{m.group(1)}:{section}:{','.join(subs)}"


# Registry of statute/rule sources, keyed by the action `kind` carried on a
# citation link.  Each module exposes the same contract (a CITE_RE,
# cite_spec/spec_label, load_section(title, section), and a Doc with
# paras/label/source_name/source_note/url/kind/bluebook_cite/neighbors), so
# one viewer serves them all.  ``_SOURCE_HOST`` is only the name shown in the
# "Fetching … from <host>" status line.
_STATUTE_SOURCES: dict[str, object] = {
    "usc": us_code,
    "cfr": ecfr,
    "rule": fed_rules,
    "statestat": state_statutes,  # in-app state statutes (CA; more to follow)
}
_SOURCE_HOST: dict[str, str] = {
    "usc": "uscode.house.gov",
    "cfr": "ecfr.gov",
    "rule": "law.cornell.edu",
    "statestat": "the official source",
}


def _fetch_statute_window(parent: tk.Misc, kind: str, spec: str,
                          status=lambda _s: None) -> None:
    """Fetch a statute, regulation or federal rule section in a background
    thread and open a _StatuteWindow over `parent` when it arrives."""
    mod = _STATUTE_SOURCES[kind]
    host = _SOURCE_HOST.get(kind, "the source")
    title, section, subs = spec.split(":", 2)
    label = mod.spec_label(spec)

    def safe_status(s: str) -> None:
        try:
            status(s)
        except tk.TclError:
            pass  # the window owning the status display was closed

    def post(fn, *args) -> None:
        try:
            parent.after(0, fn, *args)
        except tk.TclError:
            pass

    safe_status(f"Fetching {label} from {host}…")

    def run() -> None:
        try:
            doc = mod.load_section(title, section)
        except Exception as exc:
            post(safe_status, str(exc))
            return

        def show() -> None:
            safe_status(f"{label} loaded.")
            _StatuteWindow(parent, doc,
                           tuple(s for s in subs.split(",") if s))

        post(show)

    threading.Thread(target=run, daemon=True).start()


def _open_statute_action(parent: tk.Misc, action: tuple[str, str],
                         status=lambda _s: None) -> None:
    """Carry out a parsed statute-lookup action: open the in-app viewer, or —
    for a state we only link out to (N.Y., Tex., other states) — open the
    official source in the browser."""
    kind, value = action
    if kind == "browse":
        webbrowser.open(value)
        status("Opened in your browser.")
        return
    _fetch_statute_window(parent, kind, value, status)


def _dump_statute_rtf(txt: tk.Text, start: str, end: str) -> str:
    """Convert a range of the statute viewer's Text widget (with its
    sechead/headline/enum/credit/ind* tags) to an RTF body that keeps the
    bolding and hanging indents."""
    out: list[str] = []
    active: set[str] = set(txt.tag_names(start))
    active.discard("sel")
    par_open = False

    def par_prefix() -> str:
        if "sechead" in active:
            return "\\pard\\sb60\\sa180 "
        ind = 0
        for t in active:
            if t.startswith("ind") and t[3:].isdigit():
                ind = int(t[3:])
        return f"\\pard\\li{240 * ind + 180}\\fi-180\\sa100 "

    def run_codes() -> str:
        codes = ""
        if active & {"sechead", "headline", "enum", "notehead"}:
            codes += "\\b"
        if "sechead" in active:
            codes += "\\fs26"
        elif active & {"credit", "notebody"}:
            codes += "\\fs18"
        return codes

    for key, value, _index in txt.dump(start, end, text=True, tag=True):
        if key == "tagon":
            active.add(value)
        elif key == "tagoff":
            active.discard(value)
        elif key == "text":
            for i, seg in enumerate(value.split("\n")):
                if i and par_open:
                    out.append("\\par\n")
                    par_open = False
                if seg:
                    if not par_open:
                        out.append(par_prefix())
                        par_open = True
                    codes = run_codes()
                    esc = _rtf_escape(seg)
                    out.append("{" + codes + " " + esc + "}" if codes
                               else esc)
    if par_open:
        out.append("\\par\n")
    return "".join(out)


class _StatuteWindow:
    """
    Reader for a statute or regulation section — U.S. Code from the
    Office of the Law Revision Counsel (uscode.house.gov) or C.F.R. from
    the eCFR (www.ecfr.gov).  Both sources are parsed into the same
    (kind, indent, text) stream, so one window serves both.

    Formatting follows the statutory hierarchy: the section heading and
    subdivision headings are bold, inline enumerators ("(a)", "(1)(A)")
    are bold, and each nesting level is indented with a hanging indent so
    wrapped lines stay aligned under their text.  When the citation that
    opened the window pin-cites a subdivision ("§ 922(g)(1)"), the view
    scrolls there and flashes it.  Source credit is shown small below the
    text; long editorial/statutory notes sit behind a toggle.
    """

    def __init__(self, parent: tk.Misc, doc, highlight: tuple = ()) -> None:
        self._doc = doc
        self._highlight = tuple(highlight)
        self._has_notes = any(k.startswith("note") for k, _i, _t in doc.paras)
        self._neighbors: tuple = (None, None)
        self._link_actions: dict[str, tuple[str, str]] = {}
        self._link_n = 0
        self._win = tk.Toplevel(parent)
        self._win.title(f"{doc.label} — {doc.source_name}")
        self._win.geometry("760x640")
        self._win.minsize(440, 280)
        self._base_size = _OPINION_FONT_PT
        self._build_ui()
        self._render()
        self._refresh_neighbors()

    def _build_ui(self) -> None:
        win = self._win
        top = ttk.Frame(win)
        top.pack(fill="x", padx=8, pady=(8, 0))
        ttk.Label(top, text="Source:").pack(side="left")
        self._src_var = tk.StringVar(value=self._doc.url)
        ttk.Entry(top, textvariable=self._src_var, state="readonly").pack(
            side="left", fill="x", expand=True, padx=4
        )
        ttk.Button(
            top, text="Open in Browser",
            command=lambda: webbrowser.open(self._doc.url),
        ).pack(side="right")
        self._next_btn = ttk.Button(
            top, text="Next § ▶", width=8, state="disabled",
            command=lambda: self._go_neighbor(1),
        )
        self._next_btn.pack(side="right", padx=(2, 8))
        self._prev_btn = ttk.Button(
            top, text="◀ Prev §", width=8, state="disabled",
            command=lambda: self._go_neighbor(0),
        )
        self._prev_btn.pack(side="right")

        frame = ttk.Frame(win)
        frame.pack(fill="both", expand=True, padx=8, pady=4)
        s = self._base_size
        fam = "Georgia"
        self._fonts = {
            "base": tkfont.Font(family=fam, size=s),
            "bold": tkfont.Font(family=fam, size=s, weight="bold"),
            "sechead": tkfont.Font(family=fam, size=s + 2, weight="bold"),
            "small": tkfont.Font(family=fam, size=max(s - 2, 8)),
        }
        txt = tk.Text(frame, wrap="word", font=self._fonts["base"],
                      padx=14, pady=10)
        self._text = txt
        vsb = ttk.Scrollbar(frame, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        txt.pack(side="left", fill="both", expand=True)

        txt.tag_configure("sechead", font=self._fonts["sechead"],
                          spacing1=4, spacing3=12)
        txt.tag_configure("headline", font=self._fonts["bold"], spacing1=8)
        txt.tag_configure("enum", font=self._fonts["bold"])
        txt.tag_configure("credit", font=self._fonts["small"],
                          foreground="#555555", spacing1=14)
        txt.tag_configure("notehead", font=self._fonts["bold"],
                          foreground="#444444", spacing1=14)
        txt.tag_configure("notebody", font=self._fonts["small"],
                          foreground="#444444")
        for i in range(7):
            margin = 10 + 26 * i
            txt.tag_configure(f"ind{i}", lmargin1=margin,
                              lmargin2=margin + 22, spacing3=6)
        txt.tag_configure("jumpflash", background="#fff2a8")
        txt.tag_configure("citelink", foreground="#1a56b0")
        txt.tag_bind("citelink", "<Enter>",
                     lambda _e: txt.config(cursor="hand2"))
        txt.tag_bind("citelink", "<Leave>",
                     lambda _e: txt.config(cursor=""))
        self._finder = _TextFinder(win, txt, frame)

        btns = ttk.Frame(win)
        btns.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btns, text="A−", width=3,
                   command=lambda: self._zoom(-1)).pack(side="left")
        ttk.Button(btns, text="A+", width=3,
                   command=lambda: self._zoom(+1)).pack(side="left",
                                                        padx=(2, 8))
        self._notes_var = tk.BooleanVar(value=False)
        self._notes_btn = ttk.Checkbutton(
            btns, text="Show notes", variable=self._notes_var,
            command=self._render,
        )
        self._notes_btn.pack(side="left", padx=8)
        if not self._has_notes:
            self._notes_btn.config(state="disabled")
        ttk.Button(btns, text="Copy + Cite",
                   command=self._copy_cite).pack(side="right", padx=(4, 0))
        ttk.Button(btns, text="Export RTF…",
                   command=self._export_rtf).pack(side="right", padx=4)
        # Status doubles as the provenance note until an action overwrites it
        self._status_var = tk.StringVar(value=self._doc.source_note)
        ttk.Label(btns, textvariable=self._status_var,
                  foreground="gray").pack(side="left", padx=8)
        for seq in ("<Control-plus>", "<Control-equal>", "<Control-KP_Add>"):
            win.bind(seq, lambda _e: self._zoom(+1))
        for seq in ("<Control-minus>", "<Control-KP_Subtract>"):
            win.bind(seq, lambda _e: self._zoom(-1))
        txt.bind(
            "<Control-MouseWheel>",
            lambda e: self._zoom(+1 if e.delta > 0 else -1) or "break",
        )
        txt.bind("<Control-Button-4>", lambda _e: self._zoom(+1) or "break")
        txt.bind("<Control-Button-5>", lambda _e: self._zoom(-1) or "break")
        # Ctrl-C copies with the Bluebook citation appended, pin-cited to
        # the selection's subdivision (the plain default copy is
        # suppressed); the find bar's entry keeps native copy since this
        # is bound to the text widget only.
        for seq in ("<Control-c>", "<Command-c>"):
            try:
                txt.bind(seq, lambda _e: self._copy_cite() or "break")
            except tk.TclError:
                pass  # modifier not supported on this platform

    _ENUM_LEAD_RE = re.compile(r"((?:\((?:\d{1,3}|[a-zA-Z]{1,4})\)\s*)+)")

    def _render(self) -> None:
        txt = self._text
        txt.config(state="normal")
        txt.delete("1.0", "end")
        show_notes = self._notes_var.get()
        path: list[str] = []
        target = list(self._highlight)
        target_pos: Optional[str] = None
        # (position, enumerator path) per enumerated paragraph, for the
        # pin-cite jump and for citing a selection in _copy_cite
        self._anchors: list[tuple[str, tuple]] = []
        for kind, ind, text in self._doc.paras:
            if kind.startswith("note") and not show_notes:
                continue
            text = educate_quotes(text)
            indtag = f"ind{min(ind, 6)}"
            # Track the enumerator path: a paragraph at indent level N
            # replaces the path from depth N down.
            m = self._ENUM_LEAD_RE.match(text) if kind in ("body", "head") \
                else None
            lead = m.group(1) if m else ""
            if lead:
                enums = re.findall(r"\(([^)]+)\)", lead)
                path[ind:] = enums
                self._anchors.append((txt.index("end-1c"), tuple(path)))
                if (target and target_pos is None
                        and path[:len(target)] == target):
                    target_pos = txt.index("end-1c")
            if kind == "sechead":
                txt.insert("end", text + "\n", ("sechead",))
            elif kind == "head":
                txt.insert("end", text + "\n", ("headline", indtag))
            elif kind == "body":
                if lead:
                    txt.insert("end", lead.rstrip() + " ",
                               ("enum", indtag))
                    self._insert_refs(text[len(lead):].lstrip(), (indtag,))
                else:
                    self._insert_refs(text, (indtag,))
                txt.insert("end", "\n", (indtag,))
            elif kind == "credit":
                txt.insert("end", text + "\n", ("credit",))
            elif kind == "note-head":
                txt.insert("end", text + "\n", ("notehead",))
            elif kind == "note-body":
                self._insert_refs(text, ("notebody", indtag))
                txt.insert("end", "\n", ("notebody", indtag))
        txt.config(state="disabled")
        self._finder.refresh()
        if target_pos:
            txt.see(target_pos)
            txt.tag_add("jumpflash", f"{target_pos} linestart",
                        f"{target_pos} lineend")
            self._win.after(
                1800,
                lambda: txt.tag_remove("jumpflash", "1.0", "end"),
            )

    def _insert_refs(self, text: str, tags: tuple) -> None:
        """Insert paragraph text, linking citations to other U.S. Code /
        C.F.R. provisions — explicit citations plus the document's own
        cross-reference style ("section 102 of title 5"; "§ 1614.106")."""
        refs: list[tuple[int, int, str, str]] = []
        for m in us_code.USC_CITE_RE.finditer(text):
            refs.append((m.start(), m.end(), "usc", us_code.cite_spec(m)))
        for m in ecfr.CFR_CITE_RE.finditer(text):
            refs.append((m.start(), m.end(), "cfr", ecfr.cite_spec(m)))
        for m in fed_rules.RULE_CITE_RE.finditer(text):
            refs.append((m.start(), m.end(), "rule", fed_rules.cite_spec(m)))
        for c in state_statutes.iter_cites(text):
            kind, value = state_statutes.action_for(c)
            refs.append((c.start, c.end, kind, value))
        for m in statutes_at_large.STAT_CITE_RE.finditer(text):
            url = statutes_at_large.url_for(m)
            if url:  # Statutes at Large → free GovInfo scan (browser)
                refs.append((m.start(), m.end(), "browse", url))
        if self._doc.kind == "usc":
            for m in _USC_XREF_RE.finditer(text):
                title = m.group(3) or self._doc.title
                section = (m.group(1).replace("–", "-").replace("—", "-"))
                subs = re.findall(r"\(([^)]+)\)", m.group(2) or "")
                refs.append((m.start(), m.end(), "usc",
                             f"{title}:{section}:{','.join(subs)}"))
        elif self._doc.kind == "cfr":
            for m in _CFR_SECREF_RE.finditer(text):
                subs = re.findall(r"\(([^)]+)\)", m.group(2) or "")
                refs.append((m.start(), m.end(), "cfr",
                             f"{self._doc.title}:{m.group(1)}:"
                             f"{','.join(subs)}"))
        refs.sort(key=lambda r: (r[0], -r[1]))
        txt = self._text
        pos = 0
        for start, end, kind, spec in refs:
            if start < pos:
                continue  # overlapping match — first/longest wins
            if start > pos:
                txt.insert("end", text[pos:start], tags)
            ltags = tags + ("citelink", self._new_link((kind, spec)))
            txt.insert("end", text[start:end], ltags)
            pos = end
        if pos < len(text):
            txt.insert("end", text[pos:], tags)

    def _new_link(self, action: tuple[str, str]) -> str:
        self._link_n += 1
        tag = f"lnk{self._link_n}"
        self._link_actions[tag] = action
        self._text.tag_bind(
            tag, "<Button-1>", lambda _e, t=tag: self._follow_link(t)
        )
        return tag

    def _follow_link(self, tag: str) -> None:
        action = self._link_actions.get(tag)
        if not action:
            return
        kind, value = action
        if kind == "browse":
            # Cross-reference to a source we don't render in-app (e.g. a state
            # statute) — open it in the user's browser.
            webbrowser.open(value)
            self._status_var.set("Opened in your browser.")
            return
        _fetch_statute_window(self._win, kind, value, self._status_var.set)

    # ------------------------------------------------------------------
    # Previous/next provision
    # ------------------------------------------------------------------

    def _refresh_neighbors(self) -> None:
        """Resolve the adjacent sections in the background (the C.F.R.
        side may fetch the title's structure tree) and grey the buttons
        accordingly."""
        self._prev_btn.config(state="disabled")
        self._next_btn.config(state="disabled")
        doc = self._doc

        def run() -> None:
            nb = doc.neighbors()

            def apply() -> None:
                if self._doc is not doc:
                    return  # user already navigated elsewhere
                self._neighbors = nb
                self._prev_btn.config(
                    state="normal" if nb[0] else "disabled")
                self._next_btn.config(
                    state="normal" if nb[1] else "disabled")

            try:
                self._win.after(0, apply)
            except tk.TclError:
                pass

        threading.Thread(target=run, daemon=True).start()

    def _go_neighbor(self, which: int) -> None:
        target = self._neighbors[which]
        if not target:
            return
        mod = _STATUTE_SOURCES[self._doc.kind]
        self._prev_btn.config(state="disabled")
        self._next_btn.config(state="disabled")
        self._status_var.set(
            f"Fetching {'previous' if which == 0 else 'next'} section…"
        )

        def run() -> None:
            try:
                doc = mod.load_section(*target)
            except Exception as exc:
                msg = str(exc)

                def fail() -> None:
                    self._status_var.set(msg)
                    self._refresh_neighbors()

                try:
                    self._win.after(0, fail)
                except tk.TclError:
                    pass
                return
            try:
                self._win.after(0, self._load_doc, doc)
            except tk.TclError:
                pass

        threading.Thread(target=run, daemon=True).start()

    def _load_doc(self, doc, highlight: tuple = ()) -> None:
        """Show another section in this same window (prev/next nav)."""
        self._doc = doc
        self._highlight = tuple(highlight)
        self._has_notes = any(k.startswith("note") for k, _i, _t in doc.paras)
        self._notes_btn.config(
            state="normal" if self._has_notes else "disabled")
        self._win.title(f"{doc.label} — {doc.source_name}")
        self._src_var.set(doc.url)
        self._status_var.set(doc.source_note)
        self._render()
        self._text.yview_moveto(0.0)
        self._refresh_neighbors()

    def _pin_for(self, index: str) -> tuple:
        """Enumerator path of the paragraph containing a text index, for
        a pinpoint citation of the selection."""
        txt = self._text
        best: tuple = ()
        for pos, path in self._anchors:
            if txt.compare(pos, "<=", index):
                best = path
            else:
                break
        return best

    def _copy_cite(self) -> None:
        """Copy the selection (or all) with formatting, appending the
        Bluebook citation — pin-cited to the selection's subdivision."""
        txt = self._text
        try:
            start, end = txt.index("sel.first"), txt.index("sel.last")
            selected = True
        except tk.TclError:
            start, end = "1.0", "end-1c"
            selected = False
        subs = self._pin_for(start) if selected else ()
        cite = self._doc.bluebook_cite(subs) + "."
        body = _dump_statute_rtf(txt, start, end)
        rtf = _rtf_document(body + "\\pard\\sa120 " + _rtf_escape(cite)
                            + "\\par\n")
        plain = txt.get(start, end).rstrip() + "\n\n" + cite + "\n"
        how = _copy_rich_clipboard(self._win, rtf, plain)
        what = "selection" if selected else "full text"
        self._status_var.set(f"Copied {what} as {how}; citation appended.")

    def _export_rtf(self) -> None:
        """Export the section as RTF with a heading block: the citation,
        then provenance, then the formatted text."""
        head = (
            "\\pard\\qc\\sa60{\\b\\fs30 "
            + _rtf_escape(self._doc.bluebook_cite()) + "}\\par\n"
            "\\pard\\qc\\sa240{\\fs18 "
            + _rtf_escape(f"{self._doc.source_note} — {self._doc.url}")
            + "}\\par\n"
        )
        body = _dump_statute_rtf(self._text, "1.0", "end-1c")
        rtf = _rtf_document(head + body)
        default = self._doc.label.replace("§", "Sec.")
        path = filedialog.asksaveasfilename(
            defaultextension=".rtf",
            filetypes=[("Rich Text Format", "*.rtf"), ("All files", "*.*")],
            initialfile=f"{default}.rtf",
            title="Export Statute as RTF",
            parent=self._win,
        )
        if not path:
            return
        with open(path, "w", encoding="ascii", errors="replace") as f:
            f.write(rtf)
        self._status_var.set(f"Exported RTF: {path}")
        if messagebox.askyesno(
            "Export Complete", f"RTF saved to:\n{path}\n\nOpen it now?",
            parent=self._win,
        ):
            CourtListenerGUI._open_file(path)

    def _zoom(self, delta: int) -> None:
        global _OPINION_FONT_PT
        new = max(_OPINION_FONT_MIN,
                  min(_OPINION_FONT_MAX, self._base_size + delta))
        if new == self._base_size:
            return
        self._base_size = new
        _OPINION_FONT_PT = new
        self._fonts["base"].configure(size=new)
        self._fonts["bold"].configure(size=new)
        self._fonts["sechead"].configure(size=new + 2)
        self._fonts["small"].configure(size=max(new - 2, 8))


class _CitingOpinionsWindow:
    """
    Popup window listing all opinions that cite the selected case,
    sorted by depth of treatment (number of times cited within the
    citing document, descending).

    Data strategy (single stage)
    -----------------------------
    1. Resolve the cited opinion's numeric ID from its cluster
       (``/api/rest/v4/opinions/?cluster=<id>``).
    2. Fetch citing opinions sorted by depth from the citations endpoint
       (``/api/rest/v4/citations/?cited_opinion=<id>&ordering=-depth``).
    3. In parallel (thread pool), resolve each citing opinion URL →
       opinion record → cluster ID.
    4. In parallel, fetch each cluster's case name, date, and citation.
    5. Display the merged results immediately with depth populated.

    Falls back to a plain ``cites:(cluster_id)`` search (depth shown as
    "–") when step 1 fails (opinion not in citations database).
    """

    _COLS = ("case_name", "court", "date_filed", "citation", "depth")
    _COL_LABELS = {
        "case_name": "Case Name",
        "court":     "Court",
        "date_filed": "Date Filed",
        "citation":  "Citation",
        "depth":     "Depth",
    }

    def __init__(
        self,
        parent: tk.Tk | tk.Toplevel,
        app: "CourtListenerGUI",
        cited_item: dict,
    ) -> None:
        self._app = app
        self._cited_item = cited_item
        self._cluster_id = cited_item.get("cluster_id") or cited_item.get("id")
        # Cached after first load so pagination doesn't re-fetch it
        self._cited_op_id: Optional[int] = None

        # Pagination: history[i] is the citations-endpoint next-URL that
        # leads TO page i+1 (None = page 1, string URL = page 2+).
        self._cursor_history: list[Optional[str]] = [None]
        self._history_idx: int = 0
        self._next_cursor: Optional[str] = None
        self._total_count: int = 0
        self._page_results: list[dict] = []

        # Background fetch cancellation: replaced each time _load_page() is
        # called so any in-flight background thread knows to stop.
        self._bg_stop = threading.Event()

        case_name = re.sub(
            r"<[^>]+>",
            "",
            cited_item.get("caseName") or cited_item.get("case_name") or "?",
        ).strip()

        self._win = tk.Toplevel(parent)
        self._win.title(f"Citing: {case_name}")
        self._win.geometry("950x480")
        self._win.minsize(700, 300)
        self._win.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build_ui(case_name)
        self._load_page()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self, case_name: str) -> None:
        # ── status bar (top) ──────────────────────────────────────────
        top = ttk.Frame(self._win)
        top.pack(fill="x", padx=6, pady=(6, 0))
        ttk.Label(top, text=f"Opinions citing:  {case_name}", font=("TkDefaultFont", 9, "italic")).pack(side="left")
        self._status_var = tk.StringVar(value="Loading…")
        ttk.Label(top, textvariable=self._status_var, foreground="gray").pack(side="right")

        # ── treeview ─────────────────────────────────────────────────
        tree_frame = ttk.Frame(self._win)
        tree_frame.pack(fill="both", expand=True, padx=6, pady=4)

        self._tree = ttk.Treeview(
            tree_frame,
            columns=self._COLS,
            show="headings",
            selectmode="browse",
        )
        for col, label in self._COL_LABELS.items():
            self._tree.heading(col, text=label)
        self._tree.column("case_name",  width=320, minwidth=160)
        self._tree.column("court",      width=80,  minwidth=50,  anchor="center")
        self._tree.column("date_filed", width=85,  minwidth=70,  anchor="center")
        self._tree.column("citation",   width=150, minwidth=90)
        self._tree.column("depth",      width=55,  minwidth=40,  anchor="center")

        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        self._tree.pack(side="left", fill="both", expand=True)

        self._tree.bind("<Double-1>", lambda _e: self._download_selected())

        # ── bottom button bar ────────────────────────────────────────
        bot = ttk.Frame(self._win)
        bot.pack(fill="x", padx=6, pady=(0, 6))

        self._prev_btn = ttk.Button(bot, text="◀  Prev", command=self._go_prev, state="disabled")
        self._prev_btn.pack(side="left", padx=(0, 4))

        self._page_var = tk.StringVar(value="Page 1")
        ttk.Label(bot, textvariable=self._page_var, width=10, anchor="center").pack(side="left")

        self._next_btn = ttk.Button(bot, text="Next  ▶", command=self._go_next, state="disabled")
        self._next_btn.pack(side="left", padx=(4, 20))

        self._dl_btn = ttk.Button(bot, text="Download PDF", command=self._download_selected, state="disabled")
        self._dl_btn.pack(side="right", padx=(4, 0))

        self._scholar_btn = ttk.Button(bot, text="Google Scholar", command=self._open_scholar, state="disabled")
        self._scholar_btn.pack(side="right", padx=4)

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def _page_num(self) -> int:
        return self._history_idx + 1

    def _go_next(self) -> None:
        next_cur = self._next_cursor
        if not next_cur:
            return
        # If we're at the end of history, append new cursor
        if self._history_idx + 1 >= len(self._cursor_history):
            self._cursor_history.append(next_cur)
        self._history_idx += 1
        self._load_page()

    def _go_prev(self) -> None:
        if self._history_idx <= 0:
            return
        self._history_idx -= 1
        self._load_page()

    def _current_cursor(self) -> Optional[str]:
        return self._cursor_history[self._history_idx]

    # ------------------------------------------------------------------
    # Data loading  (Phase 1 – search results)
    # ------------------------------------------------------------------

    def _on_close(self) -> None:
        self._bg_stop.set()
        self._win.destroy()

    def _cancel_bg_fetch(self) -> None:
        """Signal any running background fetch to stop and arm a fresh event."""
        self._bg_stop.set()
        self._bg_stop = threading.Event()

    def _set_buttons_loading(self) -> None:
        self._prev_btn.config(state="disabled")
        self._next_btn.config(state="disabled")
        self._dl_btn.config(state="disabled")
        self._scholar_btn.config(state="disabled")

    # ------------------------------------------------------------------
    # Data loading – single stage (citations endpoint → parallel cluster
    # fetches), falls back to plain search when depth data unavailable
    # ------------------------------------------------------------------

    def _load_page(self) -> None:
        self._set_buttons_loading()
        self._status_var.set("Loading…")
        self._cancel_bg_fetch()
        bg_stop = self._bg_stop   # capture this run's stop-event for closures
        cluster_id = self._cluster_id
        # Re-use the cited opinion ID resolved on page 1
        known_op_id = self._cited_op_id
        client = self._app._get_client()
        if client is None:
            return

        def fetch_case(entry: dict) -> Optional[dict]:
            if bg_stop.is_set():
                return None
            op_url = str(entry.get("citing_opinion", ""))
            citing_op_id = _extract_opinion_id(op_url)
            if citing_op_id is None:
                return None
            try:
                opinion = client.get_opinion(citing_op_id, fields="cluster")
                cid = _extract_cluster_id(str(opinion.get("cluster", "")))
                if cid is None:
                    return None
                cluster_rec = client.get_cluster(
                    int(cid), fields="case_name,citations,date_filed,docket"
                )
                cite_strs = _cluster_citations_to_strings(
                    cluster_rec.get("citations", [])
                )
                court_id = ""
                docket_url = str(cluster_rec.get("docket", ""))
                if docket_url:
                    docket_rec = client._get_url(docket_url, {"fields": "court"})
                    court_id = _extract_court_id(str(docket_rec.get("court", "")))
                return {
                    "caseName":   cluster_rec.get("case_name", ""),
                    "case_name":  cluster_rec.get("case_name", ""),
                    "citation":   cite_strs,
                    "dateFiled":  cluster_rec.get("date_filed", ""),
                    "date_filed": cluster_rec.get("date_filed", ""),
                    "cluster_id": cid,
                    "court":    court_id,
                    "court_id": court_id,
                    "_depth": entry.get("depth", 0),
                }
            except Exception:
                return None

        _FIRST_PAGE = 20  # number of cases to detail-fetch before showing results

        def start_bg_details(remaining: list[dict], loaded_so_far: int, total: int) -> None:
            """Resolve case details for entries beyond the first page in the background."""
            def run_bg() -> None:
                loaded = loaded_so_far
                # Process in batches matching the API page size so UI updates
                # progressively rather than all at once at the very end.
                batch_size = _FIRST_PAGE
                for start in range(0, len(remaining), batch_size):
                    if bg_stop.is_set():
                        return
                    chunk = remaining[start:start + batch_size]
                    with ThreadPoolExecutor(max_workers=8) as pool:
                        raw = list(pool.map(fetch_case, chunk))
                    if bg_stop.is_set():
                        return
                    batch = [r for r in raw if r is not None]
                    loaded += len(batch)
                    is_final = (start + batch_size) >= len(remaining)
                    self._win.after(
                        0, self._append_bg_results, batch, loaded, total, is_final
                    )
            threading.Thread(target=run_bg, daemon=True).start()

        def run() -> None:
            try:
                # ── Step 1: resolve cited opinion ID (once only) ──────
                op_id = known_op_id
                if op_id is None:
                    self._win.after(0, self._status_var.set, "Resolving opinion ID…")
                    cluster_rec = client.get_cluster(
                        int(cluster_id), fields="sub_opinions"
                    )
                    sub_ops = cluster_rec.get("sub_opinions") or []
                    ids = [_extract_opinion_id(u) for u in sub_ops]
                    ids = [i for i in ids if i is not None]
                    op_id = ids[0] if ids else None
                    self._cited_op_id = op_id

                if op_id is None:
                    # No opinion ID found; fall back to plain search
                    self._win.after(0, self._status_var.set, "Fetching (search fallback)…")
                    data = client.search(
                        f"cites:({cluster_id})", type="o", page_size=20
                    )
                    self._win.after(0, self._on_fallback_results, data)
                    return

                if bg_stop.is_set():
                    return

                # ── Step 2: fetch ALL pages to get the full depth-sorted list ──
                self._win.after(0, self._status_var.set, "Fetching citing opinions…")
                all_entries: list[dict] = []
                next_api_url: Optional[str] = None
                while True:
                    if next_api_url:
                        page_data = client._get_url(next_api_url)
                    else:
                        page_data = client.list_citing_opinions(cited_opinion_id=op_id)
                    all_entries.extend(page_data.get("results", []))
                    next_api_url = page_data.get("next")
                    self._win.after(
                        0, self._status_var.set,
                        f"Fetched {len(all_entries)} citing opinions…",
                    )
                    if not next_api_url:
                        break

                all_entries.sort(key=lambda e: e.get("depth", 0), reverse=True)
                total_count = len(all_entries)

                if bg_stop.is_set():
                    return

                if not all_entries:
                    self._win.after(0, self._on_page_ready, [], 0, None)
                    return

                # ── Step 3: resolve case details for the top N entries ────────
                first_page = all_entries[:_FIRST_PAGE]
                rest = all_entries[_FIRST_PAGE:]

                self._win.after(
                    0, self._status_var.set,
                    f"Fetching details for top {len(first_page)} cases…",
                )
                with ThreadPoolExecutor(max_workers=8) as pool:
                    raw = list(pool.map(fetch_case, first_page))

                if bg_stop.is_set():
                    return

                results = [r for r in raw if r is not None]
                results.sort(key=lambda r: r.get("_depth", 0), reverse=True)

                if rest:
                    # Show first batch immediately; resolve the rest in background
                    self._win.after(
                        0, self._on_first_batch_ready, results, total_count
                    )
                    start_bg_details(rest, len(results), total_count)
                else:
                    # Everything fit in the first batch
                    self._win.after(0, self._on_page_ready, results, total_count, None)

            except Exception as exc:
                import traceback; traceback.print_exc()
                self._win.after(0, self._status_var.set, f"Error: {exc}")
                self._win.after(0, self._restore_buttons)

        threading.Thread(target=run, daemon=True).start()

    def _on_page_ready(
        self,
        results: list[dict],
        total: int,
        next_url: Optional[str],
    ) -> None:
        """Populate treeview from citations-endpoint results (depth filled)."""
        self._page_results = results
        self._total_count = total
        self._next_cursor = next_url

        self._tree.delete(*self._tree.get_children())
        for i, item in enumerate(results):
            depth = item.get("_depth", 0)
            row = self._format_row(item, depth=str(depth))
            self._tree.insert("", "end", iid=str(i), values=row)

        self._update_status_and_nav()

    def _on_first_batch_ready(self, results: list[dict], total: int) -> None:
        """Display the first page of results while more are loading in the background."""
        self._page_results = list(results)
        self._total_count = total
        self._next_cursor = None

        self._tree.delete(*self._tree.get_children())
        for i, item in enumerate(results):
            depth = item.get("_depth", 0)
            row = self._format_row(item, depth=str(depth))
            self._tree.insert("", "end", iid=str(i), values=row)

        shown = len(results)
        self._page_var.set(f"Page {self._page_num()}")
        self._status_var.set(
            f"Showing {shown:,} of {total:,} citing opinions · Loading more…"
        )
        self._prev_btn.config(state="normal" if self._history_idx > 0 else "disabled")
        self._next_btn.config(state="disabled")
        has = bool(results)
        self._dl_btn.config(state="normal" if has else "disabled")
        self._scholar_btn.config(state="normal" if has else "disabled")

    def _append_bg_results(
        self, batch: list[dict], loaded: int, total: int, final: bool
    ) -> None:
        """Append a background-fetched batch of results to the treeview."""
        offset = len(self._page_results)
        self._page_results.extend(batch)
        for i, item in enumerate(batch):
            depth = item.get("_depth", 0)
            row = self._format_row(item, depth=str(depth))
            self._tree.insert("", "end", iid=str(offset + i), values=row)

        if final:
            self._status_var.set(
                f"Page {self._page_num()} · {loaded:,} of {total:,} citing opinions"
                if total else f"Page {self._page_num()} · {loaded:,} results"
            )
            has = bool(self._page_results)
            self._dl_btn.config(state="normal" if has else "disabled")
            self._scholar_btn.config(state="normal" if has else "disabled")
        else:
            self._status_var.set(
                f"Showing {loaded:,} of {total:,} citing opinions · Loading more…"
            )

    def _on_fallback_results(self, data: dict) -> None:
        """Populate treeview from plain search API results (no depth)."""
        results = data.get("results", [])
        self._total_count = data.get("count", len(results))
        self._next_cursor = data.get("next")

        for item in results:
            raw = item.get("citation")
            if isinstance(raw, list):
                item["citation"] = [re.sub(r"<[^>]+>", "", c).strip() for c in raw]
            elif raw:
                item["citation"] = re.sub(r"<[^>]+>", "", str(raw)).strip()

        self._page_results = results
        self._tree.delete(*self._tree.get_children())
        for i, item in enumerate(results):
            row = self._format_row(item, depth="–")
            self._tree.insert("", "end", iid=str(i), values=row)

        self._update_status_and_nav()

    def _update_status_and_nav(self) -> None:
        page = self._page_num()
        self._page_var.set(f"Page {page}")
        shown = len(self._page_results)
        total = self._total_count
        self._status_var.set(
            f"Page {page} · {shown} of {total:,} citing opinions"
            if total else f"Page {page} · {shown} results"
        )
        self._prev_btn.config(state="normal" if self._history_idx > 0 else "disabled")
        self._next_btn.config(state="normal" if self._next_cursor else "disabled")
        has = bool(self._page_results)
        self._dl_btn.config(state="normal" if has else "disabled")
        self._scholar_btn.config(state="normal" if has else "disabled")

    def _format_row(self, item: dict, depth: str = "") -> tuple:
        case_name = re.sub(
            r"<[^>]+>",
            "",
            item.get("caseName") or item.get("case_name") or "(unknown)",
        ).strip()
        court = item.get("court") or item.get("court_id") or ""
        date_filed = item.get("dateFiled") or item.get("date_filed") or ""
        cite_str = _pick_citation(item.get("citation", []))
        return (case_name, court, date_filed, cite_str, depth)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _get_selected(self) -> Optional[dict]:
        sel = self._tree.selection()
        if not sel:
            return None
        idx = int(sel[0])
        if 0 <= idx < len(self._page_results):
            return self._page_results[idx]
        return None

    def _download_selected(self) -> None:
        item = self._get_selected()
        if not item:
            messagebox.showinfo("No Selection", "Please select a case first.", parent=self._win)
            return

        client = self._app._get_client()
        if client is None:
            return

        safe_name = _build_default_filename(item)
        save_path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{safe_name}.pdf",
            title="Save Opinion PDF",
            parent=self._win,
        )
        if not save_path:
            return

        self._set_buttons_loading()
        self._status_var.set("Resolving PDF URL…")

        def run() -> None:
            try:
                pdf_url = self._app._resolve_pdf_url(client, item)
                if not pdf_url:
                    cluster_id = item.get("cluster_id") or item.get("id")
                    if cluster_id:
                        self._win.after(0, self._status_var.set, "No PDF – fetching text…")
                        text = _assemble_case_text(client, item)
                        if text.strip():
                            txt_path = os.path.splitext(save_path)[0] + ".txt"
                            with open(txt_path, "w", encoding="utf-8") as f:
                                f.write(text)
                            self._win.after(0, self._on_dl_done, txt_path, True)
                            return
                    self._win.after(0, self._status_var.set,
                                    "No downloadable PDF or text found.")
                    self._win.after(0, self._restore_buttons)
                    return

                self._win.after(0, self._status_var.set, f"Downloading… {pdf_url}")
                if "courtlistener.com" in pdf_url:
                    resp = client._session.get(pdf_url, timeout=60, stream=True)
                else:
                    resp = _anon_session.get(pdf_url, timeout=60, stream=True)
                resp.raise_for_status()
                with open(save_path, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        f.write(chunk)
                self._win.after(0, self._on_dl_done, save_path, False)
            except Exception as exc:
                self._win.after(0, self._status_var.set, f"Download failed: {exc}")
                self._win.after(0, self._restore_buttons)

        threading.Thread(target=run, daemon=True).start()

    def _on_dl_done(self, path: str, is_text: bool) -> None:
        self._restore_buttons()
        self._status_var.set(f"Saved: {path}")
        label = "Text Saved" if is_text else "Download Complete"
        msg = (
            f"Opinion text saved to:\n{path}\n\nOpen it now?"
            if is_text else
            f"PDF saved to:\n{path}\n\nOpen it now?"
        )
        if messagebox.askyesno(label, msg, parent=self._win):
            CourtListenerGUI._open_file(path)

    def _restore_buttons(self) -> None:
        has = bool(self._page_results)
        self._dl_btn.config(state="normal" if has else "disabled")
        self._scholar_btn.config(state="normal" if has else "disabled")
        self._prev_btn.config(state="normal" if self._history_idx > 0 else "disabled")
        self._next_btn.config(state="normal" if self._next_cursor else "disabled")

    # ------------------------------------------------------------------
    # Google Scholar  (reuses the main app's fetcher + text window)
    # ------------------------------------------------------------------

    def _open_scholar(self) -> None:
        item = self._get_selected()
        if not item:
            messagebox.showinfo("No Selection", "Please select a case first.",
                                parent=self._win)
            return

        fetcher = self._app._get_scholar()
        if fetcher is None:
            return
        client = self._app._get_client()

        self._scholar_btn.config(state="disabled")
        self._status_var.set("Searching Google Scholar…")

        def status_cb(msg: str) -> None:
            try:
                self._win.after(0, self._status_var.set, msg)
            except tk.TclError:
                pass

        def run() -> None:
            try:
                result, cl_text, note = _find_scholar_for_item(
                    client, fetcher, item, status_cb
                )
            except Exception as exc:
                import traceback
                traceback.print_exc()
                result, cl_text, note = None, None, str(exc)
            try:
                self._win.after(0, self._on_scholar_done, result, item, cl_text, note)
            except tk.TclError:
                pass

        threading.Thread(target=run, daemon=True).start()

    def _on_scholar_done(
        self,
        result: Optional[tuple[str, str]],
        item: Optional[dict] = None,
        cl_text: Optional[str] = None,
        note: str = "",
    ) -> None:
        self._restore_buttons()
        if result is None:
            self._status_var.set("Google Scholar text unavailable.")
            messagebox.showwarning(
                "Scholar Text Unavailable",
                "Could not find a Google Scholar opinion matching this case.\n\n"
                + (f"({note})" if note else ""),
                parent=self._win,
            )
            return
        url, html = result
        self._status_var.set(
            f"Scholar text loaded — {note}" if note else f"Scholar text loaded from {url}"
        )
        _ScholarTextWindow(
            self._win, self._app, url, html, item=item, cl_text=cl_text, note=note
        )


def main() -> None:
    root = tk.Tk()
    app = CourtListenerGUI(root)

    # Run in the background by default: rather than greeting the user with the
    # full search window, GetCases starts hidden and waits.  Ctrl+Space opens
    # the quick-search popup; 's' + Enter opens the full window; 'q' + Enter
    # quits.  When there's no terminal to drive it, fall back to showing the
    # window so the app stays discoverable.
    if _stdin_is_tty():
        root.withdraw()
        app._root_hidden = True
        app._print_background_help()

        # A background thread watches stdin so the user can open the window
        # ('s') or quit ('q') even while it is hidden.
        def _watch_stdin() -> None:
            try:
                for line in sys.stdin:
                    cmd = line.strip().lower()
                    if cmd == "q":
                        try:
                            root.after(0, root.destroy)
                        except Exception:
                            pass
                        return
                    if cmd == "s":
                        try:
                            root.after(0, app._show_main_window)
                        except Exception:
                            pass
            except Exception:
                pass

        threading.Thread(target=_watch_stdin, daemon=True).start()

    root.mainloop()


if __name__ == "__main__":
    main()

