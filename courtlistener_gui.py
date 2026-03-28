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

from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Optional

import requests as _requests

from courtlistener import COURTS, CourtListenerClient, CourtListenerError

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

# Bluebook (21st ed.) abbreviations keyed by CourtListener court ID.
# SCOTUS is absent intentionally — the court name is omitted for SCOTUS cites.
_COURT_BLUEBOOK: dict[str, str] = {
    # Federal circuits
    "ca1":   "1st Cir.",
    "ca2":   "2d Cir.",
    "ca3":   "3d Cir.",
    "ca4":   "4th Cir.",
    "ca5":   "5th Cir.",
    "ca6":   "6th Cir.",
    "ca7":   "7th Cir.",
    "ca8":   "8th Cir.",
    "ca9":   "9th Cir.",
    "ca10":  "10th Cir.",
    "ca11":  "11th Cir.",
    "cadc":  "D.C. Cir.",
    "cafc":  "Fed. Cir.",
    "cavet": "Vet. App.",
    "caaf":  "C.A.A.F.",
    # Federal district courts
    "akd":   "D. Alaska",
    "almd":  "M.D. Ala.",   "alnd":  "N.D. Ala.",   "alsd":  "S.D. Ala.",
    "ared":  "E.D. Ark.",   "arwd":  "W.D. Ark.",
    "azd":   "D. Ariz.",
    "cacd":  "C.D. Cal.",   "caed":  "E.D. Cal.",
    "cand":  "N.D. Cal.",   "casd":  "S.D. Cal.",
    "cod":   "D. Colo.",
    "ctd":   "D. Conn.",
    "ded":   "D. Del.",
    "dcd":   "D.D.C.",
    "flmd":  "M.D. Fla.",   "flnd":  "N.D. Fla.",   "flsd":  "S.D. Fla.",
    "gamd":  "M.D. Ga.",    "gand":  "N.D. Ga.",     "gasd":  "S.D. Ga.",
    "gud":   "D. Guam",
    "hid":   "D. Haw.",
    "idd":   "D. Idaho",
    "ilcd":  "C.D. Ill.",   "ilnd":  "N.D. Ill.",    "ilsd":  "S.D. Ill.",
    "innd":  "N.D. Ind.",   "insd":  "S.D. Ind.",
    "iand":  "N.D. Iowa",   "iasd":  "S.D. Iowa",
    "ksd":   "D. Kan.",
    "kyed":  "E.D. Ky.",    "kywd":  "W.D. Ky.",
    "laed":  "E.D. La.",    "lamd":  "M.D. La.",     "lawd":  "W.D. La.",
    "med":   "D. Me.",
    "mdd":   "D. Md.",
    "mad":   "D. Mass.",
    "mied":  "E.D. Mich.",  "miwd":  "W.D. Mich.",
    "mnd":   "D. Minn.",
    "msnd":  "N.D. Miss.",  "mssd":  "S.D. Miss.",
    "moed":  "E.D. Mo.",    "mowd":  "W.D. Mo.",
    "mtd":   "D. Mont.",
    "ned":   "D. Neb.",
    "nvd":   "D. Nev.",
    "nhd":   "D.N.H.",
    "njd":   "D.N.J.",
    "nmd":   "D.N.M.",
    "nmid":  "D.N. Mar. I.",
    "nyed":  "E.D.N.Y.",    "nynd":  "N.D.N.Y.",
    "nysd":  "S.D.N.Y.",    "nywd":  "W.D.N.Y.",
    "nced":  "E.D.N.C.",    "ncmd":  "M.D.N.C.",     "ncwd":  "W.D.N.C.",
    "ndd":   "D.N.D.",
    "ohnd":  "N.D. Ohio",   "ohsd":  "S.D. Ohio",
    "oked":  "E.D. Okla.",  "oknd":  "N.D. Okla.",   "okwd":  "W.D. Okla.",
    "ord":   "D. Or.",
    "paed":  "E.D. Pa.",    "pamd":  "M.D. Pa.",     "pawd":  "W.D. Pa.",
    "prd":   "D.P.R.",
    "rid":   "D.R.I.",
    "scd":   "D.S.C.",
    "sdd":   "D.S.D.",
    "tned":  "E.D. Tenn.",  "tnmd":  "M.D. Tenn.",   "tnwd":  "W.D. Tenn.",
    "txed":  "E.D. Tex.",   "txnd":  "N.D. Tex.",
    "txsd":  "S.D. Tex.",   "txwd":  "W.D. Tex.",
    "utd":   "D. Utah",
    "vtd":   "D. Vt.",
    "vaed":  "E.D. Va.",    "vawd":  "W.D. Va.",
    "vid":   "D.V.I.",
    "waed":  "E.D. Wash.",  "wawd":  "W.D. Wash.",
    "wvnd":  "N.D. W. Va.", "wvsd":  "S.D. W. Va.",
    "wied":  "E.D. Wis.",   "wiwd":  "W.D. Wis.",
    "wyd":   "D. Wyo.",
    # Specialised federal courts
    "cit":   "Ct. Int'l Trade",
    "uscfc": "Fed. Cl.",
    "tax":   "T.C.",
    "bap1":  "B.A.P. 1st Cir.", "bap2": "B.A.P. 2d Cir.",
    "bap6":  "B.A.P. 6th Cir.", "bap8": "B.A.P. 8th Cir.",
    "bap9":  "B.A.P. 9th Cir.", "bap10": "B.A.P. 10th Cir.",
    # State supreme courts (CourtListener IDs)
    "ala":   "Ala.", "alactapp": "Ala. Crim. App.", "alacivapp": "Ala. Civ. App.",
    "alaska": "Alaska",
    "ariz":  "Ariz.", "arizctapp": "Ariz. Ct. App.",
    "ark":   "Ark.", "arkctapp": "Ark. Ct. App.",
    "cal":   "Cal.", "calctapp": "Cal. Ct. App.",
    "colo":  "Colo.", "coloctapp": "Colo. App.",
    "conn":  "Conn.", "connappct": "Conn. App.",
    "del":   "Del.", "delsuperct": "Del. Super. Ct.",
    "dc":    "D.C.",
    "fla":   "Fla.", "fladistctapp": "Fla. Dist. Ct. App.",
    "ga":    "Ga.", "gactapp": "Ga. Ct. App.",
    "haw":   "Haw.", "hawapp": "Haw. Ct. App.",
    "idaho": "Idaho", "idahoctapp": "Idaho Ct. App.",
    "ill":   "Ill.", "illappct": "Ill. App. Ct.",
    "ind":   "Ind.", "indctapp": "Ind. Ct. App.",
    "iowa":  "Iowa", "iowactapp": "Iowa Ct. App.",
    "kan":   "Kan.", "kanctapp": "Kan. Ct. App.",
    "ky":    "Ky.", "kyctapp": "Ky. Ct. App.",
    "la":    "La.", "lactapp": "La. Ct. App.",
    "me":    "Me.",
    "md":    "Md.", "mdctspecapp": "Md. App.",
    "mass":  "Mass.", "massappct": "Mass. App. Ct.",
    "mich":  "Mich.", "michctapp": "Mich. Ct. App.",
    "minn":  "Minn.", "minnctapp": "Minn. Ct. App.",
    "miss":  "Miss.", "missctapp": "Miss. Ct. App.",
    "mo":    "Mo.", "moctapp": "Mo. Ct. App.",
    "mont":  "Mont.",
    "neb":   "Neb.", "nebctapp": "Neb. Ct. App.",
    "nev":   "Nev.",
    "nh":    "N.H.",
    "nj":    "N.J.", "njsuperctappdiv": "N.J. Super. Ct. App. Div.",
    "nm":    "N.M.", "nmctapp": "N.M. Ct. App.",
    "ny":    "N.Y.", "nyappdiv": "N.Y. App. Div.",
    "nc":    "N.C.", "ncctapp": "N.C. Ct. App.",
    "nd":    "N.D.",
    "ohio":  "Ohio", "ohioctapp": "Ohio Ct. App.",
    "okla":  "Okla.", "oklacrimapp": "Okla. Crim. App.", "oklacivapp": "Okla. Civ. App.",
    "or":    "Or.", "orctapp": "Or. Ct. App.",
    "pa":    "Pa.", "pasuperct": "Pa. Super. Ct.", "pacommwct": "Pa. Commw. Ct.",
    "ri":    "R.I.",
    "sc":    "S.C.", "scctapp": "S.C. Ct. App.",
    "sd":    "S.D.",
    "tenn":  "Tenn.", "tennctapp": "Tenn. Ct. App.", "tenncrimapp": "Tenn. Crim. App.",
    "tex":   "Tex.", "texapp": "Tex. App.",
    "utah":  "Utah", "utahctapp": "Utah Ct. App.",
    "vt":    "Vt.",
    "va":    "Va.", "vactapp": "Va. Ct. App.",
    "wash":  "Wash.", "washctapp": "Wash. Ct. App.",
    "wva":   "W. Va.",
    "wis":   "Wis.", "wisctapp": "Wis. Ct. App.",
    "wyo":   "Wyo.",
}


def _build_default_filename(item: dict) -> str:
    """
    Return a sanitized default filename (without extension) for saving an opinion.

    Format: ``Case Name, Reporter Cite, (Court YEAR)``
    For SCOTUS the court abbreviation is omitted: ``Case Name, Reporter Cite, (YEAR)``
    Falls back gracefully when citation or date are missing.
    """
    # Case name
    case_name = re.sub(
        r"<[^>]+>", "",
        item.get("caseName") or item.get("case_name") or "opinion"
    ).strip()

    # Best citation: prefer U.S. Reports for SCOTUS, else first non-Lexis cite
    citations = item.get("citation", [])
    if isinstance(citations, list):
        clean = [re.sub(r"<[^>]+>", "", c).strip() for c in citations
                 if "lexis" not in c.lower()]
        us_cite = next((c for c in clean if " U.S. " in c), None)
        citation_str = us_cite or (clean[0] if clean else "")
    else:
        raw = str(citations).strip()
        citation_str = "" if "lexis" in raw.lower() else re.sub(r"<[^>]+>", "", raw).strip()

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

    # Assemble parts, skipping empty ones
    parts = [p for p in [case_name, citation_str, paren] if p]
    raw_name = ", ".join(parts)

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
    from google_scholar import GoogleScholarFetcher

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

        ttk.Label(row2, text="Court:").pack(side="left")
        self._court_var = tk.StringVar(value="(any)")
        court_choices = ["(any)"] + sorted(COURTS.keys())
        ttk.Combobox(
            row2,
            textvariable=self._court_var,
            values=court_choices,
            width=10,
            state="readonly",
        ).pack(side="left", padx=(4, 12))

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

        paned = ttk.PanedWindow(results_frame, orient="horizontal")
        paned.pack(fill="both", expand=True)

        # -- Left pane: main results tree + orders tree --
        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=3)

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

        # -- Right pane: preview panel (narrow — user can drag sash to resize) --
        right_frame = ttk.LabelFrame(paned, text="Preview", padding=4, width=160)
        paned.add(right_frame, weight=1)

        preview_inner = ttk.Frame(right_frame)
        preview_inner.pack(fill="both", expand=True)
        self._preview_text = tk.Text(
            preview_inner,
            wrap="word",
            state="disabled",
            font=("TkDefaultFont", 9),
            relief="flat",
            background="#f5f5f5",
        )
        preview_vsb = ttk.Scrollbar(
            preview_inner, orient="vertical", command=self._preview_text.yview
        )
        self._preview_text.configure(yscrollcommand=preview_vsb.set)
        preview_vsb.pack(side="right", fill="y")
        self._preview_text.pack(side="left", fill="both", expand=True)


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
        citations = item.get("citation", [])
        if isinstance(citations, list):
            printed = [c for c in citations if "lexis" not in c.lower()]
            us_reports = next((c for c in printed if " U.S. " in c), None)
            citation_str = us_reports or (printed[0] if printed else "")
        else:
            citation_str = str(citations) if citations and "lexis" not in str(citations).lower() else ""
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
        # Deselect the other tree so only one row is ever active
        other = self._orders_tree if source_tree is self._tree else self._tree
        if other.selection():
            other.selection_remove(*other.selection())
        self._download_btn.config(state="normal")
        self._scholar_btn.config(state="normal")
        self._show_preview(self._iid_to_idx(sel[0]))

    def _on_right_click(self, event: tk.Event, tree: ttk.Treeview) -> None:
        """Right-click: open the 'Citing Opinions' window for the clicked row."""
        iid = tree.identify_row(event.y)
        if not iid:
            return
        tree.selection_set(iid)
        idx = self._iid_to_idx(iid)
        if 0 <= idx < len(self._results):
            item = self._results[idx]
            client = self._get_client()
            if client is None:
                return
            _CitingOpinionsWindow(self.root, client, item)

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

        court = self._court_var.get()
        if court == "(any)":
            court = None
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
        self._results.clear()
        self._preview_cache.clear()
        self._preview_text.config(state="normal")
        self._preview_text.delete("1.0", "end")
        self._preview_text.config(state="disabled")

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

    def _show_preview(self, idx: int) -> None:
        """Populate the right-hand preview panel for result at *idx*."""
        self._preview_text.config(state="normal")
        self._preview_text.delete("1.0", "end")
        text = self._preview_cache.get(idx, "")
        self._preview_text.insert(
            "1.0",
            text if text else "(No preview available — download PDF for full opinion)",
        )
        self._preview_text.config(state="disabled")

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

    def _fetch_scholar_text(self) -> None:
        selected = self._get_selected_item()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a case first.")
            return

        fetcher = self._get_scholar()
        if fetcher is None:
            return

        _, item = selected

        citations = item.get("citation", [])
        citation_str = citations[0] if isinstance(citations, list) and citations else ""
        case_name = item.get("caseName") or item.get("case_name") or ""
        date_filed = item.get("dateFiled") or item.get("date_filed") or ""
        year = date_filed[:4] if date_filed else None

        self._download_btn.config(state="disabled")
        self._scholar_btn.config(state="disabled")
        self._search_btn.config(state="disabled")
        self._status_var.set("Searching Google Scholar…")

        def run() -> None:
            result = None
            if citation_str:
                print(f"[scholar] trying citation: {citation_str!r}")
                result = fetcher.fetch_by_citation(citation_str)
            if result is None and case_name:
                print(f"[scholar] falling back to case name: {case_name!r} ({year})")
                result = fetcher.fetch_by_name(case_name, year)

            self.root.after(0, self._on_scholar_result, result)

        threading.Thread(target=run, daemon=True).start()

    def _on_scholar_result(self, result: Optional[tuple[str, str]]) -> None:
        self._restore_buttons()
        if result is None:
            self._status_var.set("Google Scholar: no text found.")
            messagebox.showwarning(
                "Scholar Not Found",
                "Could not find this opinion on Google Scholar.\n\n"
                "Google may have blocked the request, or the case may not be indexed.\n"
                "Check the terminal for details.",
            )
            return

        url, text = result
        self._status_var.set(f"Scholar text loaded from {url}")
        self._show_scholar_window(url, text)

    def _show_scholar_window(self, url: str, text: str) -> None:
        win = tk.Toplevel(self.root)
        win.title("Google Scholar Opinion Text")
        win.geometry("800x600")

        # URL bar
        url_frame = ttk.Frame(win)
        url_frame.pack(fill="x", padx=8, pady=(8, 0))
        ttk.Label(url_frame, text="Source:").pack(side="left")
        url_var = tk.StringVar(value=url)
        ttk.Entry(url_frame, textvariable=url_var, state="readonly").pack(
            side="left", fill="x", expand=True, padx=4
        )

        # Text area
        text_frame = ttk.Frame(win)
        text_frame.pack(fill="both", expand=True, padx=8, pady=4)
        txt = tk.Text(text_frame, wrap="word", font=("TkDefaultFont", 10))
        vsb = ttk.Scrollbar(text_frame, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        txt.pack(side="left", fill="both", expand=True)
        txt.insert("1.0", text)
        txt.config(state="disabled")

        # Save button
        def save_text() -> None:
            path = filedialog.asksaveasfilename(
                defaultextension=".txt",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
                title="Save Opinion Text",
                parent=win,
            )
            if path:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(text)
                messagebox.showinfo("Saved", f"Text saved to:\n{path}", parent=win)

        btn_frame = ttk.Frame(win)
        btn_frame.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btn_frame, text="Save as .txt…", command=save_text).pack(side="right")
        ttk.Label(
            btn_frame,
            text=f"{len(text):,} characters  |  cached locally",
            foreground="gray",
        ).pack(side="left")

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


_OP_ID_RE = re.compile(r"/opinions/(\d+)/?")


def _extract_opinion_id(url: str) -> Optional[int]:
    """Parse an opinion ID out of a CourtListener opinions URL."""
    m = _OP_ID_RE.search(str(url))
    return int(m.group(1)) if m else None


class _CitingOpinionsWindow:
    """
    Popup window that lists all opinions citing a given case, sorted by
    depth of treatment (most citations within the citing document first).

    Data strategy
    -------------
    Phase 1 – fast display:
        Use the search API (``cites:(cluster_id)``) to get full case data
        (name, court, date, citation) for page 1.  Results appear
        immediately.

    Phase 2 – depth enrichment (background):
        1. Resolve the cited case's opinion ID from its cluster.
        2. Call ``/api/rest/v4/citations/?cited_opinion=<id>&ordering=-depth``
           to get citing opinion IDs sorted by depth.
        3. Match against the opinion IDs embedded in search results
           (``result["opinions"][*]["id"]``).
        4. Re-sort the displayed page by depth descending and populate the
           Depth column.

    If Phase 2 cannot be completed (API shape differs, etc.) the window
    still functions normally with depth shown as "–".
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
        client: CourtListenerClient,
        cited_item: dict,
    ) -> None:
        self._parent = parent
        self._client = client
        self._cited_item = cited_item
        self._cluster_id = cited_item.get("cluster_id") or cited_item.get("id")

        # Pagination state
        self._cursor_history: list[Optional[str]] = [None]   # history[0] = page 1
        self._history_idx: int = 0                            # current position
        self._next_cursor: Optional[str] = None
        self._total_count: int = 0

        # Current page results + depth map (opinion_id → depth)
        self._page_results: list[dict] = []
        self._depth_map: dict[int, int] = {}

        case_name = re.sub(
            r"<[^>]+>",
            "",
            cited_item.get("caseName") or cited_item.get("case_name") or "?",
        ).strip()

        self._win = tk.Toplevel(parent)
        self._win.title(f"Citing: {case_name}")
        self._win.geometry("950x480")
        self._win.minsize(700, 300)

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

    def _set_buttons_loading(self) -> None:
        self._prev_btn.config(state="disabled")
        self._next_btn.config(state="disabled")
        self._dl_btn.config(state="disabled")
        self._scholar_btn.config(state="disabled")

    def _load_page(self) -> None:
        self._set_buttons_loading()
        self._status_var.set("Loading…")
        cursor = self._current_cursor()
        cluster_id = self._cluster_id

        def run() -> None:
            try:
                if cursor:
                    data = self._client._get_url(cursor)
                else:
                    data = self._client.search(
                        f"cites:({cluster_id})",
                        type="o",
                        page_size=20,
                    )
                self._win.after(0, self._on_search_results, data)
            except Exception as exc:
                self._win.after(0, self._status_var.set, f"Error: {exc}")

        threading.Thread(target=run, daemon=True).start()

    def _on_search_results(self, data: dict) -> None:
        results = data.get("results", [])
        self._total_count = data.get("count", len(results))
        self._next_cursor = data.get("next")  # full URL for next page

        # Normalise citations (strip HTML tags)
        for item in results:
            raw = item.get("citation")
            if isinstance(raw, list):
                item["citation"] = [re.sub(r"<[^>]+>", "", c).strip() for c in raw]
            elif raw:
                item["citation"] = re.sub(r"<[^>]+>", "", str(raw)).strip()

        self._page_results = results

        # Populate tree with placeholder depth
        self._tree.delete(*self._tree.get_children())
        for i, item in enumerate(results):
            row = self._format_row(item, depth="")
            self._tree.insert("", "end", iid=str(i), values=row)

        page = self._page_num()
        self._page_var.set(f"Page {page}")
        shown = len(results)
        total = self._total_count
        self._status_var.set(
            f"Page {page} · {shown} of {total:,} citing opinions"
            if total else f"Page {page} · {shown} results"
        )

        # Update nav buttons
        self._prev_btn.config(state="normal" if self._history_idx > 0 else "disabled")
        self._next_btn.config(state="normal" if self._next_cursor else "disabled")

        if results:
            self._dl_btn.config(state="normal")
            self._scholar_btn.config(state="normal")

        # Kick off depth enrichment in background
        if results:
            threading.Thread(target=self._enrich_depth, daemon=True).start()

    def _format_row(self, item: dict, depth: str | int = "") -> tuple:
        case_name = re.sub(r"<[^>]+>", "", item.get("caseName") or item.get("case_name") or "(unknown)").strip()
        court = item.get("court") or item.get("court_id") or ""
        date_filed = item.get("dateFiled") or item.get("date_filed") or ""
        citations = item.get("citation", [])
        if isinstance(citations, list):
            printed = [c for c in citations if "lexis" not in c.lower()]
            us = next((c for c in printed if " U.S. " in c), None)
            cite_str = us or (printed[0] if printed else "")
        else:
            cite_str = str(citations) if citations and "lexis" not in str(citations).lower() else ""
        return (case_name, court, date_filed, cite_str, str(depth))

    # ------------------------------------------------------------------
    # Phase 2 – depth enrichment
    # ------------------------------------------------------------------

    def _enrich_depth(self) -> None:
        """
        Background task: fetch citation depth for the cited opinion and
        update the Depth column in the treeview, then re-sort by depth.
        """
        try:
            # Step 1: get the opinion ID(s) for the cited cluster
            cluster_id = self._cluster_id
            ops_resp = self._client.list_opinions(
                cluster=int(cluster_id),
                fields="id",
                page_size=5,
            )
            op_ids = [o["id"] for o in ops_resp.get("results", []) if o.get("id")]
            if not op_ids:
                return

            # Step 2: fetch citations for the primary opinion, sorted by depth
            cite_resp = self._client.list_citing_opinions(
                cited_opinion_id=op_ids[0],
                page_size=20,
                fields="citing_opinion,depth",
            )
            # Build map: citing_opinion_id → depth
            depth_map: dict[int, int] = {}
            for obj in cite_resp.get("results", []):
                op_url = obj.get("citing_opinion") or ""
                oid = _extract_opinion_id(op_url)
                dep = obj.get("depth")
                if oid is not None and dep is not None:
                    depth_map[oid] = int(dep)

            if not depth_map:
                return

            # Step 3: match search results via their sub-opinion IDs
            # Each search result has result["opinions"][*]["id"]
            result_depths: dict[int, int] = {}  # result_index → depth
            for i, item in enumerate(self._page_results):
                sub_ops = item.get("opinions") or []
                for sop in sub_ops:
                    sop_id = sop.get("id")
                    if sop_id and sop_id in depth_map:
                        result_depths[i] = depth_map[sop_id]
                        break

            if not result_depths:
                return

            self._depth_map = depth_map
            self._win.after(0, self._apply_depth, result_depths)

        except Exception as exc:
            print(f"[citing] depth enrichment failed: {exc}")

    def _apply_depth(self, result_depths: dict[int, int]) -> None:
        """Update the Depth column and re-sort the treeview by depth."""
        # Collect (iid, depth) pairs; unmatched items get depth -1 for sort
        rows: list[tuple[str, int]] = []
        for iid in self._tree.get_children():
            idx = int(iid)
            depth = result_depths.get(idx, -1)
            rows.append((iid, depth))

        # Sort descending by depth (unmatched at bottom)
        rows.sort(key=lambda x: x[1], reverse=True)

        for pos, (iid, depth) in enumerate(rows):
            idx = int(iid)
            item = self._page_results[idx]
            display_depth = str(depth) if depth >= 0 else "–"
            new_values = self._format_row(item, depth=display_depth)
            self._tree.item(iid, values=new_values)
            self._tree.move(iid, "", pos)

        # Update status to note depth info is live
        cur = self._status_var.get()
        self._status_var.set(cur.rstrip(".") + "  (sorted by depth)")

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
                # We need a temporary GUI proxy to reuse _resolve_pdf_url;
                # instead we inline the minimal resolution here.
                pdf_url = _resolve_pdf_for_item(self._client, item)
                if not pdf_url:
                    cluster_id = item.get("cluster_id") or item.get("id")
                    if cluster_id:
                        self._win.after(0, self._status_var.set, "No PDF – fetching text…")
                        text = _assemble_case_text(self._client, item)
                        if text.strip():
                            txt_path = os.path.splitext(save_path)[0] + ".txt"
                            with open(txt_path, "w", encoding="utf-8") as f:
                                f.write(text)
                            self._win.after(0, self._on_dl_done, txt_path, True)
                            return
                    self._win.after(0, self._status_var.set, "No downloadable PDF or text found.")
                    self._win.after(0, self._restore_buttons)
                    return

                self._win.after(0, self._status_var.set, f"Downloading… {pdf_url}")
                if "courtlistener.com" in pdf_url:
                    resp = self._client._session.get(pdf_url, timeout=60, stream=True)
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
    # Google Scholar
    # ------------------------------------------------------------------

    def _open_scholar(self) -> None:
        item = self._get_selected()
        if not item:
            messagebox.showinfo("No Selection", "Please select a case first.", parent=self._win)
            return
        citations = item.get("citation", [])
        cite = (citations[0] if isinstance(citations, list) and citations else str(citations or "")).strip()
        case_name = re.sub(r"<[^>]+>", "", item.get("caseName") or item.get("case_name") or "").strip()
        query = cite or case_name
        if not query:
            return
        url = "https://scholar.google.com/scholar?q=" + urllib.parse.quote(query)
        webbrowser.open(url)


def _resolve_pdf_for_item(client: CourtListenerClient, item: dict) -> Optional[str]:
    """
    Minimal PDF URL resolver for use outside the main GUI class.
    Tries local_path → download_url from the item dict only (no sub-opinion
    walk or LOC/GovInfo logic).  The full resolver lives in
    ``CourtListenerGUI._resolve_pdf_url``; callers that need the full
    resolution should use that instead.
    """
    local = item.get("local_path") or item.get("localPath") or ""
    if local:
        return "https://storage.courtlistener.com/" + local.lstrip("/")
    url = item.get("download_url") or ""
    if url:
        return url
    return None


def main() -> None:
    root = tk.Tk()
    CourtListenerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

