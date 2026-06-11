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

import requests as _requests

from courtlistener import CourtListenerClient, CourtListenerError
from court_catalog import (
    CATALOG as _COURT_CATALOG,
    COURT_BLUEBOOK as _COURT_BLUEBOOK,
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



def _build_default_filename(item: dict) -> str:
    """
    Return a sanitized default filename (without extension) for saving an opinion.

    Format: ``Case Name, Reporter Cite (Court YEAR)``
    For SCOTUS the court abbreviation is omitted: ``Case Name, Reporter Cite (YEAR)``
    Falls back gracefully when citation or date are missing.
    """
    # Case name
    case_name = re.sub(
        r"<[^>]+>", "",
        item.get("caseName") or item.get("case_name") or "opinion"
    ).strip()

    # Best citation (U.S. Reports > S.Ct. > Federal Reporters > others)
    citation_str = _pick_citation(item.get("citation", []))

    # Year from date filed
    date_filed = item.get("dateFiled") or item.get("date_filed") or ""
    year = date_filed[:4] if len(date_filed) >= 4 else ""

    # Court abbreviation (absent for SCOTUS)
    court_id = str(item.get("court_id") or item.get("court") or "").strip().lower()
    is_scotus = "scotus" in court_id
    if is_scotus:
        court_abbr = ""
    else:
        court_abbr = _COURT_BLUEBOOK.get(court_id, "")
        if not court_abbr:
            # Fall back to whatever the API gave us for the court display name
            court_abbr = str(item.get("court") or court_id).strip()

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
        c if c.isalnum() or c in " .,()-_'" else "_"
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
    return s.strip("-")


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
        parse_opinion_blocks,
        segment_blocks,
        text_similarity,
    )

    _SCHOLAR_AVAILABLE = True
except ImportError:
    _SCHOLAR_AVAILABLE = False


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

        self._build_ui()

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
    # Settings dialog
    # ------------------------------------------------------------------

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

        # 0. Official US Reports PDF.
        #    vols 1-542  → LOC CDN (exact per-opinion PDF); if LOC fails,
        #                  GovInfo is tried next (available from vol 2 onward).
        #    vols 543+   → GovInfo link service only (redirects to per-opinion PDF)
        citations = item.get("citation", [])
        if isinstance(citations, list):
            us_cite = next((c for c in citations if " U.S. " in c), None)
        else:
            us_cite = str(citations) if citations and " U.S. " in str(citations) else None
        if us_cite:
            loc_url = _us_reports_loc_url(us_cite)
            if loc_url:
                if _head_ok(loc_url, "LOC US Reports"):
                    print(f"[resolve] using LOC US Reports PDF: {loc_url}")
                    return loc_url
            govinfo_urls = _us_reports_govinfo_url(us_cite)
            if govinfo_urls:
                link_url, direct_url = govinfo_urls
                if _head_ok(link_url, "GovInfo link"):
                    print(f"[resolve] using GovInfo link URL: {link_url}")
                    return link_url
                if _head_ok(direct_url, "GovInfo direct PDF"):
                    print(f"[resolve] using GovInfo direct PDF URL: {direct_url}")
                    return direct_url

        # 0.5. For non-SCOTUS cases try the Harvard CAP static.case.law copy
        #      first.  A HEAD request confirms availability before committing.
        #      If the cites from the search result all fail, also try alternate
        #      cites from the cluster record before giving up on static.case.law.
        if not is_scotus:
            cites = citations if isinstance(citations, list) else (
                [str(citations)] if citations else []
            )
            tried_cites: set[str] = set()

            def _try_static_case_law(cite_list: list[str]) -> Optional[str]:
                for cite in cite_list:
                    tried_cites.add(cite)
                    if "lexis" in cite.lower():
                        continue
                    scl_url = _static_case_law_url(cite)
                    if not scl_url:
                        continue
                    print(f"[resolve] checking static.case.law: {scl_url}")
                    try:
                        head = _anon_session.head(scl_url, timeout=10, allow_redirects=True)
                        if head.status_code == 200:
                            print(f"[resolve] using static.case.law PDF: {scl_url}")
                            return scl_url
                        print(f"[resolve] static.case.law returned {head.status_code} for {cite!r}")
                    except Exception as exc:
                        print(f"[resolve] static.case.law check failed: {exc}")
                return None

            result = _try_static_case_law(cites)
            if result:
                return result

            # The search result may only expose a subset of citations.
            # Fetch the cluster record to get any alternate cites not already tried.
            cluster_id_for_cites = item.get("cluster_id") or item.get("id")
            if cluster_id_for_cites:
                try:
                    print(f"[resolve] fetching cluster {cluster_id_for_cites} for alternate citations")
                    cites_resp = client.get_cluster(int(cluster_id_for_cites), fields="citations")
                    alt_cites: list[str] = []
                    for c in (cites_resp.get("citations") or []):
                        if isinstance(c, str):
                            alt_cites.append(re.sub(r"<[^>]+>", "", c).strip())
                        elif isinstance(c, dict):
                            vol = c.get("volume") or ""
                            rep = c.get("reporter") or ""
                            page = c.get("page") or ""
                            if vol and rep and page:
                                alt_cites.append(f"{vol} {rep} {page}")
                    new_cites = [c for c in alt_cites if c not in tried_cites]
                    if new_cites:
                        print(f"[resolve] trying {len(new_cites)} alternate cite(s) from cluster")
                        result = _try_static_case_law(new_cites)
                        if result:
                            return result
                except Exception as exc:
                    print(f"[resolve] cluster cite fetch failed: {exc}")

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

        def status_cb(msg: str) -> None:
            self.root.after(0, self._status_var.set, msg)

        def run() -> None:
            try:
                result, cl_text, note = _find_scholar_for_item(
                    client, fetcher, item, status_cb
                )
            except Exception as exc:
                import traceback
                traceback.print_exc()
                result, cl_text, note = None, None, str(exc)
            self.root.after(0, self._on_scholar_result, result, item, cl_text, note)

        threading.Thread(target=run, daemon=True).start()

    def _on_scholar_result(
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
                + (f"({note})\n\n" if note else "")
                + "Google may have blocked the request, the case may not be "
                "indexed, or every candidate differed too much from the "
                "CourtListener text.",
            )
            return

        url, html = result
        self._status_var.set(
            f"Scholar text loaded — {note}" if note else f"Scholar text loaded from {url}"
        )
        _ScholarTextWindow(
            self.root, self, url, html, item=item, cl_text=cl_text, note=note
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


# Color table index 1 = star-pagination marker.  Citation links stay black
# in copied/exported text; the blue is only an on-screen affordance.
_RTF_HEADER = (
    "{\\rtf1\\ansi\\deff0"
    "{\\fonttbl{\\f0\\froman Times New Roman;}}"
    "{\\colortbl ;\\red142\\green68\\blue173;}"
    "\\f0\\fs22\n"
)


def _rtf_document(body: str, two_columns: bool = False) -> str:
    sect = "\\sectd\\sbknone\\cols2\\colsx432\n" if two_columns else ""
    return _RTF_HEADER + sect + body + "}"


def _run_to_rtf(seg: str, active: set[str]) -> str:
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
    esc = _rtf_escape(seg)
    return "{" + "".join(codes) + " " + esc + "}" if codes else esc


def _dump_to_rtf(txt: tk.Text, start: str, end: str) -> str:
    """Convert a Tk Text range (with the Scholar window's tags) to an RTF body."""
    out: list[str] = []
    # Seed with tags already open at *start*; dump only reports transitions.
    active: set[str] = set(txt.tag_names(start))
    active.discard("sel")
    par_open = False

    def par_prefix() -> str:
        if "center" in active:
            return "\\pard\\qc\\sa120 "
        if "blockquote" in active:
            return "\\pard\\li720\\ri720\\sa120 "
        return "\\pard\\sa120 "

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
                    out.append(_run_to_rtf(seg, active))
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
      • Save as .txt,
      • a toggle to the CourtListener version of the text.
    """

    _PAGENUM_COLOR = "#8e44ad"   # muted purple — visible but not loud
    _LINK_COLOR = "#1a56b0"
    _DISSENT_COLOR = "#a31515"   # dark red
    _CONCUR_COLOR = "#1a7a3c"    # dark green
    _PART_COLOR_TAGS = {"dissent": "part-dissent", "concurrence": "part-concurrence"}
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
    ) -> None:
        self._app = app
        self._item = item or {}
        self._scholar_url = url
        self._note = note
        self._blocks = parse_opinion_blocks(opinion_html)
        self._scholar_text = blocks_to_text(self._blocks) or _strip_html(opinion_html)
        self._parts = segment_blocks(self._blocks)
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
        self._mode = "scholar"
        self._link_actions: dict[str, tuple[str, str]] = {}
        self._link_n = 0
        self._fonts: dict[str, tkfont.Font] = {}
        self._bb = self._compute_bluebook_parts()

        self._win = tk.Toplevel(parent)
        self._win.title(self._bb["name"] or "Google Scholar Opinion Text")
        self._win.geometry("860x680")
        self._win.minsize(500, 300)
        self._build_ui()
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
        base = tkfont.Font(family="Georgia", size=11)
        self._fonts["base"] = base
        self._family = base.actual("family")
        self._base_size = base.actual("size")
        txt = tk.Text(text_frame, wrap="word", font=base, padx=14, pady=10)
        self._text = txt
        vsb = ttk.Scrollbar(text_frame, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        txt.pack(side="left", fill="both", expand=True)

        txt.tag_configure("center", justify="center")
        txt.tag_configure("blockquote", lmargin1=36, lmargin2=36, rmargin=36)
        txt.tag_configure("heading", spacing1=6, spacing3=4)
        txt.tag_configure("underline", underline=True)
        # Part colors (configured before pagenum/citelink so those keep
        # priority and stay purple/blue inside colored parts)
        txt.tag_configure("part-dissent", foreground=self._DISSENT_COLOR)
        txt.tag_configure("part-concurrence", foreground=self._CONCUR_COLOR)
        fnhead_font = tkfont.Font(
            family=self._family, size=max(self._base_size - 2, 8), weight="bold"
        )
        self._fonts["fnhead"] = fnhead_font
        txt.tag_configure(
            "fnhead", font=fnhead_font, foreground="#666666", spacing1=10
        )
        pagenum_font = tkfont.Font(
            family=self._family, size=max(self._base_size - 1, 8), weight="bold"
        )
        self._fonts["pagenum"] = pagenum_font
        txt.tag_configure(
            "pagenum", font=pagenum_font, foreground=self._PAGENUM_COLOR
        )
        txt.tag_configure("citelink", foreground=self._LINK_COLOR)
        txt.tag_bind("citelink", "<Enter>", lambda _e: txt.config(cursor="hand2"))
        txt.tag_bind("citelink", "<Leave>", lambda _e: txt.config(cursor=""))

        btn_frame = ttk.Frame(win)
        btn_frame.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btn_frame, text="Copy + Cite", command=self._copy_formatted).pack(
            side="right", padx=(4, 0)
        )
        ttk.Button(btn_frame, text="Export RTF…", command=self._export_rtf).pack(
            side="right", padx=4
        )
        ttk.Button(btn_frame, text="Save as .txt…", command=self._save_txt).pack(
            side="right", padx=4
        )
        self._toggle_btn = ttk.Button(
            btn_frame, text="CourtListener Text", command=self._toggle_source
        )
        self._toggle_btn.pack(side="right", padx=4)

        self._status_var = tk.StringVar()
        ttk.Label(btn_frame, textvariable=self._status_var, foreground="gray").pack(
            side="left", fill="x", expand=True
        )

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _font_tag(self, italic: bool, bold: bool, small: bool, sup: bool) -> str:
        name = "fnt_" + "".join("1" if f else "0" for f in (italic, bold, small, sup))
        if name not in self._fonts:
            size = self._base_size - (3 if sup else 2 if small else 0)
            f = tkfont.Font(
                family=self._family,
                size=max(size, 7),
                slant="italic" if italic else "roman",
                weight="bold" if bold else "normal",
            )
            self._fonts[name] = f
            self._text.tag_configure(name, font=f, offset=4 if sup else 0)
        return name

    def _new_link(self, action: tuple[str, str]) -> str:
        self._link_n += 1
        tag = f"lnk{self._link_n}"
        self._link_actions[tag] = action
        self._text.tag_bind(
            tag, "<Button-1>", lambda _e, t=tag: self._follow_link(t)
        )
        return tag

    def _insert_span(self, span, block_tags: tuple) -> None:
        txt = self._text
        tags = list(block_tags)
        if span.pagenum:
            tags.append("pagenum")
            txt.insert("end", span.text, tuple(tags))
            return
        tags.append(self._font_tag(span.italic, span.bold, span.small, span.sup))
        if span.underline:
            tags.append("underline")
        if span.link:
            tags += ["citelink", self._new_link(("url", span.link))]
            txt.insert("end", span.text, tuple(tags))
            return
        # Plain text: make recognizable citations clickable
        pos = 0
        for m in _TEXT_CITE_RE.finditer(span.text):
            if m.start() > pos:
                txt.insert("end", span.text[pos:m.start()], tuple(tags))
            cite = re.sub(r"\s+", " ", m.group(0)).replace("U. S.", "U.S.")
            cite = cite.replace("’", "'")  # straight apostrophe for the search query
            ltags = tags + ["citelink", self._new_link(("cite", cite))]
            txt.insert("end", m.group(0), tuple(ltags))
            pos = m.end()
        if pos < len(span.text):
            txt.insert("end", span.text[pos:], tuple(tags))

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
        for span in block.spans:
            self._insert_span(span, block_tags)
        self._text.insert("end", "\n\n", block_tags)

    def _render_scholar(self) -> None:
        txt = self._text
        txt.config(state="normal")
        txt.delete("1.0", "end")
        self._link_actions.clear()
        if not self._parts:
            txt.insert("1.0", self._scholar_text)
        else:
            shown = (
                self._parts
                if self._current_part is None
                else [self._parts[self._current_part]]
            )
            for part in shown:
                part_tag = self._PART_COLOR_TAGS.get(part.kind)
                for block in part.blocks:
                    self._insert_block(block, part_tag)
                if part.footnotes:
                    fnhead_tags = ("fnhead",) + ((part_tag,) if part_tag else ())
                    txt.insert("end", "Footnotes\n\n", fnhead_tags)
                    for block in part.footnotes:
                        self._insert_block(block, part_tag)
        txt.config(state="disabled")
        self._mode = "scholar"
        self._source_var.set(self._scholar_url)
        self._toggle_btn.config(text="CourtListener Text", state="normal")
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

    def _on_part_selected(self, _event=None) -> None:
        idx = self._part_combo.current()
        self._current_part = None if idx <= 0 else idx - 1
        self._render_scholar()  # selecting a part always shows the Scholar text

    def _show_courtlistener(self) -> None:
        txt = self._text
        txt.config(state="normal")
        txt.delete("1.0", "end")
        txt.insert("1.0", self._cl_text or "(no text)")
        txt.config(state="disabled")
        self._mode = "courtlistener"
        self._source_var.set("CourtListener (assembled from the REST API)")
        self._toggle_btn.config(text="Google Scholar Text", state="normal")
        self._part_combo.config(state="disabled")
        self._view_label_var.set("CourtListener text")
        self._view_label.config(foreground="black")
        self._status_var.set(
            f"{len(self._cl_text or ''):,} characters | CourtListener version"
        )

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

        if not name and self._blocks:
            first = self._blocks[0].text().strip()
            name = re.split(r",\s*\d{1,4}\s", first)[0].strip().rstrip(",")[:120]

        court_id = str(item.get("court_id") or "").strip().lower()
        is_scotus = "scotus" in court_id or bool(
            re.match(r"\d+\s+(U\.S\.|S\.\s?Ct\.|L\.\s?Ed\.)", cite)
        )
        court_abbr = ""
        if not is_scotus:
            court_abbr = _COURT_BLUEBOOK.get(court_id, "")
            if not court_abbr and court_id:
                court_abbr = str(item.get("court") or court_id).strip()
        return {"name": name, "cite": cite, "court": court_abbr, "year": year}

    def _bluebook_citation(self, pin: Optional[str]) -> tuple[str, str]:
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
        rest += "."
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
            self._pin_for_range(start, end)
            if (selected and self._mode == "scholar")
            else None
        )
        plain_cite, rtf_cite = self._bluebook_citation(pin)
        body = _dump_to_rtf(txt, start, end)
        rtf = _rtf_document(body + rtf_cite)
        plain = txt.get(start, end).rstrip() + "\n\n" + plain_cite + "\n"
        how = _copy_rich_clipboard(self._win, rtf, plain)
        what = "selection" if selected else "full text"
        self._status_var.set(f"Copied {what} as {how}; citation appended.")

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

    def _export_rtf(self) -> None:
        body = _dump_to_rtf(self._text, "1.0", "end-1c")
        rtf = _rtf_document(body, two_columns=True)
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

    def _current_text(self) -> str:
        if self._mode == "courtlistener":
            return self._cl_text or ""
        return self._scholar_text

    def _save_txt(self) -> None:
        default = _build_default_filename(self._filename_item())
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=f"{default}.txt",
            title="Save Opinion Text",
            parent=self._win,
        )
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self._current_text())
            messagebox.showinfo("Saved", f"Text saved to:\n{path}", parent=self._win)

    # ------------------------------------------------------------------
    # Citation links
    # ------------------------------------------------------------------

    def _post(self, fn, *args) -> None:
        try:
            self._win.after(0, fn, *args)
        except tk.TclError:
            pass  # window closed while a background fetch was running

    def _follow_link(self, tag: str) -> None:
        action = self._link_actions.get(tag)
        if not action:
            return
        kind, value = action
        fetcher = self._app._get_scholar()
        if fetcher is None:
            return
        label = value if kind == "cite" else "cited case"
        self._status_var.set(f"Fetching {label} from Google Scholar…")

        def run() -> None:
            if kind == "url":
                result = fetcher.fetch_by_url(value)
            else:
                result = fetcher.fetch_by_citation(value)
            self._post(self._on_link_ready, result)

        threading.Thread(target=run, daemon=True).start()

    def _on_link_ready(self, result: Optional[tuple[str, str]]) -> None:
        if not result:
            self._status_var.set("Google Scholar: cited case not found (or blocked).")
            return
        url, html = result
        self._status_var.set("Cited case loaded.")
        _ScholarTextWindow(self._win, self._app, url, html, item=None)

    # ------------------------------------------------------------------
    # CourtListener toggle
    # ------------------------------------------------------------------

    def _toggle_source(self) -> None:
        if self._mode == "courtlistener":
            self._render_scholar()
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
                    # Opened from a citation link — locate the case on
                    # CourtListener by its citation.
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
                text = _assemble_case_text(client, target)
                self._post(self._on_cl_ready, text)
            except Exception as exc:
                self._post(self._on_cl_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _on_cl_ready(self, text: str) -> None:
        self._cl_text = text
        self._show_courtlistener()

    def _on_cl_error(self, msg: str) -> None:
        self._toggle_btn.config(state="normal")
        self._status_var.set(f"CourtListener: {msg}")
        messagebox.showerror("CourtListener", msg, parent=self._win)


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
    CourtListenerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

