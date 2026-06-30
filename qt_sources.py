"""Qt-facing source lookup and rendering helpers.

The legal-source modules already expose a shared document shape:
``label``, ``heading``, ``source_name``, ``source_note``, ``url``,
``paras`` and ``bluebook_cite``.  This module adapts that shape into a
single HTML renderer for the PySide6 front end.
"""

from __future__ import annotations

import html
import re
import urllib.parse
from dataclasses import dataclass
from typing import Callable, Optional

import constitution
import ecfr
import eng_rep
import fed_rules
import state_statutes
import us_code
from citations import detect_links


StatusCallback = Callable[[str], None]


_STATUTE_QUERY_RE = re.compile(
    r"^\s*(\d{1,2})\s*"
    r"(u\.?\s*s\.?\s*c\.?\s*a?\.?|c\.?\s*f\.?\s*r\.?)\s*"
    r"(?:\u00a7\u00a7?|sec(?:tions?)?\.?)?\s*"
    r"(\d[\w.\-\u2013\u2014]*)"
    r"((?:\s*\(\w{1,4}\))*)\s*$",
    re.IGNORECASE,
)

SOURCE_HOST = {
    "usc": "uscode.house.gov",
    "cfr": "ecfr.gov",
    "rule": "law.cornell.edu",
    "statestat": "the official source",
    "const": "the U.S. Constitution",
}


@dataclass(frozen=True)
class LoadedSource:
    kind: str
    spec: str
    doc: object
    subs: tuple[str, ...] = ()


def parse_lookup(query: str) -> Optional[tuple[str, str]]:
    """Parse a typed source lookup into the app's ``(kind, spec)`` action."""
    query = (query or "").strip()
    if not query:
        return None
    for parser in (fed_rules.parse_query, constitution.parse_query, state_statutes.parse_query):
        action = parser(query)
        if action:
            return action
    m = _STATUTE_QUERY_RE.match(query)
    if not m:
        return None
    kind = "cfr" if "f" in m.group(2).lower() else "usc"
    section = m.group(3).rstrip(".").replace("\u2013", "-").replace("\u2014", "-")
    if not section or (kind == "cfr" and "." not in section):
        return None
    subs = re.findall(r"\(([^)]+)\)", m.group(4) or "")
    return kind, f"{m.group(1)}:{section}:{','.join(subs)}"


def load_source(kind: str, spec: str, status: StatusCallback | None = None) -> LoadedSource:
    """Load a legal-source document for a link action."""
    if status:
        status(f"Fetching from {SOURCE_HOST.get(kind, 'source')}...")
    if kind == "usc":
        title, section, subs = _split_simple_spec(spec)
        return LoadedSource(kind, spec, us_code.load_section(title, section), tuple(subs))
    if kind == "cfr":
        title, section, subs = _split_simple_spec(spec)
        return LoadedSource(kind, spec, ecfr.load_section(title, section), tuple(subs))
    if kind == "rule":
        set_key, rule, subs = _split_simple_spec(spec)
        return LoadedSource(kind, spec, fed_rules.load_section(set_key, rule), tuple(subs))
    if kind == "const":
        title, section, sub = (spec.split(":", 2) + ["", "", ""])[:3]
        subs = tuple(s for s in (sub,) if s)
        return LoadedSource(kind, spec, constitution.load_section(title, section), subs)
    if kind == "statestat":
        key, section, subs = _split_state_spec(spec)
        return LoadedSource(kind, spec, state_statutes.load_section(key, section), tuple(subs))
    raise ValueError(f"Unsupported source kind: {kind!r}")


def source_title(loaded: LoadedSource) -> str:
    doc = loaded.doc
    return str(getattr(doc, "heading", None) or getattr(doc, "label", "Source"))


def source_body(loaded: LoadedSource) -> str:
    """Render a loaded source document into the app HTML body."""
    doc = loaded.doc
    label = str(getattr(doc, "label", source_title(loaded)))
    source = str(getattr(doc, "source_name", "Source"))
    note = str(getattr(doc, "source_note", ""))
    url = str(getattr(doc, "url", ""))
    cite = ""
    try:
        cite = str(doc.bluebook_cite(loaded.subs))
    except Exception:
        cite = label

    parts = [
        f"<h1>{html.escape(source_title(loaded))}</h1>",
        '<div class="source-meta">',
        f"<strong>{html.escape(label)}</strong>",
        f"<span>{html.escape(source)}</span>",
    ]
    if note:
        parts.append(f"<span>{html.escape(note)}</span>")
    if cite:
        parts.append(f"<span>Bluebook: {html.escape(cite)}</span>")
    if url:
        parts.append(
            f'<span><a href="{html.escape(url, quote=True)}">Official source</a></span>'
        )
    parts.append("</div>")

    for kind, indent, text in getattr(doc, "paras", []) or []:
        text = str(text or "")
        if not text:
            continue
        cls = f"para {html.escape(kind)} indent-{max(0, min(int(indent or 0), 8))}"
        parts.append(f'<p class="{cls}">{_linkify(text)}</p>')
    return "\n".join(parts)


def english_reports_url(spec: str) -> str:
    """Best available CommonLII URL for an English Reports citation spec."""
    cases = eng_rep.resolve(spec)
    if cases:
        return cases[0].web_url
    parsed = eng_rep.parse_spec(spec)
    if parsed:
        return eng_rep.search_url(*parsed)
    return "https://www.commonlii.org/uk/cases/EngR/"


def _split_simple_spec(spec: str) -> tuple[str, str, list[str]]:
    title, section, subs = (spec.split(":", 2) + ["", "", ""])[:3]
    return title, section, [s for s in subs.split(",") if s]


def _split_state_spec(spec: str) -> tuple[str, str, list[str]]:
    key, rest = spec.split(":", 1)
    section, _, subs = rest.rpartition(":")
    return key, section, [s for s in subs.split(",") if s]


def _linkify(text: str) -> str:
    links = detect_links(text)
    if not links:
        return html.escape(text)
    out: list[str] = []
    pos = 0
    for start, end, action in links:
        if start < pos:
            continue
        out.append(html.escape(text[pos:start]))
        kind, value = action
        href = "getcases://open?" + urllib.parse.urlencode({"kind": kind, "value": value})
        out.append(f'<a class="cite" href="{href}">{html.escape(text[start:end])}</a>')
        pos = end
    out.append(html.escape(text[pos:]))
    return "".join(out)
