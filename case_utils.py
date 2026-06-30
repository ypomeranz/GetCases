"""Case/result helpers shared by GetCases front ends."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Optional

try:
    from bluebook_names import abbreviate_case_name
except Exception:  # pragma: no cover - fallback for partial installs.
    def abbreviate_case_name(name: str) -> str:
        return name

try:
    from court_catalog import COURT_BLUEBOOK as _COURT_BLUEBOOK
except Exception:  # pragma: no cover
    _COURT_BLUEBOOK = {}


_CITE_PARSE_RE = re.compile(r"^(\d+)\s+(.+)\s+(\d+)")
_NOISE_CITE_RE = re.compile(r"lexis|westlaw|\bwl\b", re.IGNORECASE)
_REPORTER_SERIES_RE = re.compile(r"\b\d*(?:2d|3d|4th|5th|6th)\b\.?|\b\d+\b")
_SCOTUS_REPORTERS = {
    "U.S.",
    "S. Ct.",
    "S.Ct.",
    "L. Ed.",
    "L. Ed. 2d",
    "L.Ed.",
    "L.Ed.2d",
}
_CITE_PRIORITY = [
    re.compile(r" U\.S\. "),
    re.compile(r" S\. Ct\. "),
    re.compile(r" F\.4th "),
    re.compile(r" F\.3d "),
    re.compile(r" F\.2d "),
    re.compile(r" F\. \d"),
    re.compile(r" F\. Supp\. 3d "),
    re.compile(r" F\. Supp\. 2d "),
    re.compile(r" F\. Supp\. "),
    re.compile(r" B\.R\. "),
]
_FED_APPX_RE = re.compile(r"F(?:ed)?\.?\s*App['\u2019]?x\.?", re.IGNORECASE)
_CASE_LAW_REPORTER_ALIASES = {
    "fed-rep": "f",
    "fed-rep-2d": "f2d",
    "fed-rep-3d": "f3d",
    "fappx": "f-appx",
    "fedappx": "f-appx",
    "fed-appx": "f-appx",
}


@dataclass(frozen=True)
class CaseRow:
    case_name: str
    court: str
    date_filed: str
    citation: str
    status: str


def strip_html(value: object) -> str:
    """Return text with tags removed and whitespace normalized."""
    text = re.sub(r"<[^>]+>", "", str(value or ""))
    return re.sub(r"\s+", " ", text).strip()


def cluster_citations_to_strings(citations: object) -> list[str]:
    """Convert CourtListener citation dictionaries or strings to text."""
    out: list[str] = []
    for cite in citations or []:
        if isinstance(cite, dict):
            vol = cite.get("volume", "")
            reporter = cite.get("reporter", "")
            page = cite.get("page", "")
            if vol and reporter and page:
                out.append(f"{vol} {reporter} {page}")
        elif str(cite).strip():
            out.append(strip_html(cite))
    return out


def citation_list(citations: object) -> list[str]:
    """Normalize an API citation field into a clean list of citation strings."""
    if isinstance(citations, str):
        values: Iterable[object] = [citations]
    else:
        values = citations or []
    return [strip_html(c) for c in values if strip_html(c)]


def normalize_result_citations(item: dict) -> None:
    """Clean a search-result citation field in place."""
    raw = item.get("citation")
    if isinstance(raw, list):
        item["citation"] = citation_list(raw)
    elif raw:
        item["citation"] = strip_html(raw)


def pick_citation(citations: object) -> str:
    """Choose the best reporter citation for display and filenames."""
    clean = citation_list(citations)
    if not clean:
        return ""
    non_noise = [c for c in clean if not _NOISE_CITE_RE.search(c)]
    pool = non_noise or clean
    for pattern in _CITE_PRIORITY:
        hit = next((c for c in pool if pattern.search(f" {c} ")), None)
        if hit:
            return hit
    return pool[0]


def case_name(item: dict, default: str = "(unknown)") -> str:
    return strip_html(item.get("caseName") or item.get("case_name") or default)


def format_case_row(item: dict) -> CaseRow:
    return CaseRow(
        case_name=case_name(item),
        court=str(item.get("court") or item.get("court_id") or ""),
        date_filed=str(item.get("dateFiled") or item.get("date_filed") or ""),
        citation=pick_citation(item.get("citation", [])),
        status=str(item.get("status") or item.get("precedentialStatus") or ""),
    )


def preview_from_item(item: dict) -> str:
    """Return the best available snippet text for a CourtListener result."""
    opinions = item.get("opinions") or []
    main_op = max(opinions, key=lambda op: len(op.get("cites") or []), default=None)
    if not main_op:
        return ""
    return strip_html(main_op.get("snippet") or "")


def is_scotus_order(item: dict) -> bool:
    opinions = item.get("opinions") or []
    main_op = max(opinions, key=lambda op: len(op.get("cites") or []), default=None)
    cite_count = len(main_op.get("cites") or []) if main_op else None
    return (
        "scotus" in str(item.get("court_id") or "")
        and cite_count is not None
        and cite_count <= 2
    )


def court_for_parenthetical(citation: str, court_id: str, fallback: str = "") -> str:
    """Return the Bluebook court string for a filename/date parenthetical."""
    court_id = (court_id or "").strip().lower()
    match = _CITE_PARSE_RE.match(citation or "")
    reporter = match.group(2).strip() if match else ""
    if "scotus" in court_id or reporter in _SCOTUS_REPORTERS:
        return ""
    abbr = _COURT_BLUEBOOK.get(court_id, "") or (fallback or "").strip()
    if not abbr or not reporter:
        return abbr
    rep_tokens = [t for t in _REPORTER_SERIES_RE.sub(" ", reporter).split() if t]
    court_tokens = abbr.split()
    meaningful = [t for t in court_tokens if t != "Ct."]
    if meaningful and all(t in rep_tokens for t in meaningful):
        return ""
    if (
        rep_tokens
        and len(court_tokens) > 1
        and rep_tokens[0].replace(".", "").lower().startswith(
            court_tokens[0].replace(".", "").lower()
        )
    ):
        return " ".join(court_tokens[1:])
    return abbr


def build_default_filename(item: dict) -> str:
    """Return a sanitized opinion filename stem."""
    name = abbreviate_case_name(case_name(item, "opinion"))
    cite = pick_citation(item.get("citation", []))
    date_filed = str(item.get("dateFiled") or item.get("date_filed") or "")
    year = date_filed[:4] if len(date_filed) >= 4 else ""
    court_id = str(item.get("court_id") or item.get("court") or "")
    court = court_for_parenthetical(cite, court_id, str(item.get("court") or court_id))

    if court and year:
        paren = f"({court} {year})"
    elif year:
        paren = f"({year})"
    elif court:
        paren = f"({court})"
    else:
        paren = ""

    main = ", ".join(p for p in (name, cite) if p)
    raw_name = f"{main} {paren}".strip() if paren else main
    safe = "".join(
        c if c.isalnum() or c in " .,()-_'&" else "_"
        for c in raw_name
    )
    return safe[:120].strip() or "opinion"


def extract_cluster_id(value: object) -> Optional[int]:
    match = re.search(r"/clusters/(\d+)/?", str(value or ""))
    return int(match.group(1)) if match else None


def extract_opinion_id(value: object) -> Optional[int]:
    match = re.search(r"/opinions/(\d+)/?", str(value or ""))
    return int(match.group(1)) if match else None


def is_federal_appendix_cite(citation: object) -> bool:
    """Return whether a citation is to the Federal Appendix."""
    return _FED_APPX_RE.search(str(citation or "")) is not None


def federal_appendix_cite(item: dict) -> Optional[str]:
    """Return the Federal Appendix citation on an item, if any."""
    raw = item.get("citation")
    cites = raw if isinstance(raw, list) else [raw] if raw else []
    for cite in cites:
        clean = strip_html(cite)
        if is_federal_appendix_cite(clean):
            return clean
    return None


def static_case_law_url(citation: str) -> Optional[str]:
    """Return the static.case.law PDF URL candidate for a reporter citation."""
    citation = strip_html(citation)
    match = _CITE_PARSE_RE.match(citation)
    if not match:
        return None
    volume, reporter, page = match.group(1), match.group(2).strip(), match.group(3)
    slug = _slugify_case_law_reporter(reporter)
    if not slug:
        return None
    try:
        page_num = int(page)
    except ValueError:
        return None
    return f"https://static.case.law/{slug}/{volume}/case-pdfs/{page_num:04d}-01.pdf"


def _slugify_case_law_reporter(reporter: str) -> str:
    slug = reporter.lower().replace(" ", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return _CASE_LAW_REPORTER_ALIASES.get(slug, slug)
