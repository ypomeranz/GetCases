"""Fetch and parse Code of Federal Regulations sections from the eCFR
(www.ecfr.gov), the GPO/OFR's continuously updated official edition.

The versioner API serves one section as XML:

    https://www.ecfr.gov/api/versioner/v1/full/{date}/title-{T}.xml
        ?part={P}&section={S}

with {date} taken from /api/versioner/v1/titles.json ("up_to_date_as_of").
A section arrives as <DIV8 TYPE="SECTION"> holding a <HEAD>, <P>/<FP>
paragraphs, and a <CITA> source credit.  Unlike the OLRC's U.S. Code
pages, the XML does not mark indentation, so nesting is inferred from the
enumerators themselves following the CFR drafting convention
(a) -> (1) -> (i) -> (A), resolving the "(i) after (h)" ambiguity by
preferring a successor at an open level over starting a deeper one.

``parse_section_xml`` emits the same (kind, indent, text) stream as
``us_code.parse_section`` so one viewer window renders both sources.
"""

from __future__ import annotations

import datetime as _dt
import html as _html
import re
import threading
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Citation recognition
# ---------------------------------------------------------------------------

# "29 C.F.R. § 1614.105(a)(1)", "40 CFR 261.4(b)", "12 C. F. R. §226.18".
# The dotted part.section number is required, so "29 C.F.R. Part 1910"
# (a whole part, often enormous) is deliberately not matched.
CFR_CITE_RE = re.compile(
    r"\b(\d{1,2})\s+C\.?\s?F\.?\s?R\.?\s*"
    r"(?:§§?|[Ss]ec(?:tions?)?\.?)?\s*"
    r"(\d+[a-zA-Z]?\.\d+[a-zA-Z0-9]*"
    r"(?:[-–—]\d+[a-zA-Z0-9]*(?:\.\d+[a-zA-Z0-9]*)*)?)"
    r"((?:\s?\((?:\d{1,3}|[ivxIVX]{2,4}|[a-zA-Z]{1,3})\))*)"
)


def cite_spec(m: re.Match) -> str:
    """Compact "title:section:sub,sub" spec from a CFR_CITE_RE match."""
    section = m.group(2).replace("–", "-").replace("—", "-")
    subs = re.findall(r"\(([^)]+)\)", m.group(3) or "")
    return f"{m.group(1)}:{section}:{','.join(subs)}"


def spec_label(spec: str) -> str:
    title, section, subs = spec.split(":", 2)
    tail = "".join(f"({s})" for s in subs.split(",") if s)
    return f"{title} C.F.R. § {section}{tail}"


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/xml",
}

_API = "https://www.ecfr.gov/api/versioner/v1"


@dataclass
class CfrSection:
    title: str
    section: str
    url: str          # human-readable eCFR page, for "Open in Browser"
    date: str         # the issue date the text reflects
    paras: list[tuple[str, int, str]] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.title} C.F.R. § {self.section}"

    @property
    def source_name(self) -> str:
        return "eCFR (GPO/OFR)"

    @property
    def source_note(self) -> str:
        return f"eCFR official edition, current as of {self.date}"

    def bluebook_cite(self, subs: tuple = ()) -> str:
        """Bluebook citation (rule 14.2): C.F.R. cites carry the year of
        the edition cited — here the eCFR currency date's year."""
        tail = "".join(f"({s})" for s in subs)
        return f"{self.title} C.F.R. § {self.section}{tail} ({self.date[:4]})"

    @property
    def kind(self) -> str:
        return "cfr"

    def neighbors(self) -> tuple[tuple[str, str] | None,
                                 tuple[str, str] | None]:
        """Adjacent sections in the title, from the (cached) structure
        tree.  Network failures simply yield (None, None)."""
        try:
            order = _section_order(self.title, self.date)
            i = order.index(self.section)
        except Exception:
            return None, None
        prev = (self.title, order[i - 1]) if i > 0 else None
        nxt = (self.title, order[i + 1]) if i + 1 < len(order) else None
        return prev, nxt


_dates: dict[str, str] = {}
_cache: dict[tuple[str, str], CfrSection] = {}
_lock = threading.Lock()


def _issue_date(title: str) -> str:
    """Latest issue date for a title from titles.json; today on failure."""
    with _lock:
        if title in _dates:
            return _dates[title]
    date = _dt.date.today().isoformat()
    try:
        import requests

        resp = requests.get(f"{_API}/titles.json", headers=_HEADERS,
                            timeout=20)
        resp.raise_for_status()
        for entry in resp.json().get("titles", []):
            if str(entry.get("number")) == str(int(title)):
                date = (entry.get("up_to_date_as_of")
                        or entry.get("latest_issue_date") or date)
                break
    except Exception:
        pass  # fall back to today; the full endpoint accepts recent dates
    with _lock:
        _dates[title] = date
    return date


def load_section(title: str, section: str) -> CfrSection:
    """Fetch and parse a CFR section, cached.  For a range ("1.1-1.5"),
    falls back to the first section.  Raises RuntimeError on failure."""
    title, section = str(title).strip(), str(section).strip()
    key = (title, section)
    with _lock:
        if key in _cache:
            return _cache[key]

    import requests

    candidates = [section]
    if "-" in section:
        candidates.append(section.split("-", 1)[0])
    date = _issue_date(title)
    last_err = "section not found"
    for cand in candidates:
        part = cand.split(".", 1)[0]
        api_url = (f"{_API}/full/{date}/title-{title}.xml"
                   f"?part={part}&section={cand}")
        try:
            resp = requests.get(api_url, headers=_HEADERS, timeout=30)
            if resp.status_code == 404:
                last_err = f"no such section {title} C.F.R. § {cand}"
                continue
            resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(f"ecfr.gov: {exc}") from exc
        paras = parse_section_xml(resp.text)
        if paras:
            doc = CfrSection(
                title=title, section=cand, date=date,
                url=f"https://www.ecfr.gov/current/title-{title}/"
                    f"section-{cand}",
                paras=paras,
            )
            with _lock:
                _cache[key] = doc
            return doc
    raise RuntimeError(f"ecfr.gov: {last_err}")


_order_cache: dict[str, list[str]] = {}


def _sections_from_structure(node: dict) -> list[str]:
    """Document-order section identifiers from an eCFR structure tree."""
    out: list[str] = []
    if node.get("type") == "section" and not node.get("reserved"):
        ident = node.get("identifier")
        if ident:
            out.append(str(ident))
    for child in node.get("children") or []:
        out.extend(_sections_from_structure(child))
    return out


def _section_order(title: str, date: str) -> list[str]:
    """Ordered section identifiers for a title (cached per process)."""
    with _lock:
        if title in _order_cache:
            return _order_cache[title]

    import requests

    resp = requests.get(f"{_API}/structure/{date}/title-{title}.json",
                        headers=_HEADERS, timeout=60)
    resp.raise_for_status()
    order = _sections_from_structure(resp.json())
    with _lock:
        _order_cache[title] = order
    return order


# ---------------------------------------------------------------------------
# XML parsing
#
# eCFR XML carries no indentation markup, so nesting is inferred from the
# enumerators per the CFR drafting hierarchy (a) -> (1) -> (i) -> (A),
# using the engine shared with the U.S. Code module.
# ---------------------------------------------------------------------------

from us_code import CFR_HIERARCHY, infer_enum_level  # noqa: E402

_DIV8_RE = re.compile(
    r"<DIV8\b[^>]*TYPE=\"SECTION\"[^>]*>(.*?)</DIV8>",
    re.IGNORECASE | re.DOTALL,
)
_ELEM_RE = re.compile(
    r"<(HEAD|P|FP|CITA|SOURCE|AUTH|NOTE)\b[^>]*>(.*?)</\1>",
    re.IGNORECASE | re.DOTALL,
)
_ENUM_LEAD_RE = re.compile(
    r"^((?:\((?:\d{1,3}|[a-zA-Z]{1,5})\)\s*)+)"
)


def _clean(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", "", fragment)
    text = _html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_section_xml(xml: str) -> list[tuple[str, int, str]]:
    """Parse eCFR section XML into the (kind, indent, text) stream used by
    the statute viewer (same contract as us_code.parse_section)."""
    m = _DIV8_RE.search(xml)
    if not m:
        return []
    body = m.group(1)
    paras: list[tuple[str, int, str]] = []
    stack: list[tuple[str, str]] = []
    for em in _ELEM_RE.finditer(body):
        tag = em.group(1).upper()
        text = _clean(em.group(2))
        if not text:
            continue
        if tag == "HEAD":
            paras.append(("sechead", 0, text))
        elif tag in ("CITA", "SOURCE"):
            paras.append(("credit", 0, text.strip("[]")))
        elif tag in ("AUTH", "NOTE"):
            paras.append(("note-body", 0, text))
        else:  # P / FP
            # No enumerator means a continuation of the open item, which
            # stays at that item's depth rather than returning flush left
            level = max(len(stack) - 1, 0)
            lead = _ENUM_LEAD_RE.match(text)
            if lead:
                enums = re.findall(r"\(([^)]+)\)", lead.group(1))
                lvl = infer_enum_level(enums, stack, CFR_HIERARCHY)
                if lvl is not None:
                    level = lvl
            paras.append(("body", min(level, 6), text))
    return paras


if __name__ == "__main__":
    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    # --- citation regex ---
    for text, want in [
        ("29 C.F.R. § 1614.105(a)(1)", "29:1614.105:a,1"),
        ("40 CFR 261.4(b)", "40:261.4:b"),
        ("12 C. F. R. §226.18(d)", "12:226.18:d"),
        ("17 C.F.R. § 240.10b-5", "17:240.10b-5:"),
        ("see 17 C.F.R. § 240.10b-5. Next sentence", "17:240.10b-5:"),
        ("8 C.F.R. §§ 1003.1-1003.8", "8:1003.1-1003.8:"),
        ("29 C.F.R. §§ 1614.105(a)(2)", "29:1614.105:a,2"),
    ]:
        m = CFR_CITE_RE.search(text)
        got = cite_spec(m) if m else None
        check(got == want, f"{text!r} -> {got!r}")
    for text in ("29 C.F.R. Part 1910", "42 U.S.C. § 1983",
                 "501 U.S. 32", "Fed. R. Civ. P. 56"):
        check(CFR_CITE_RE.search(text) is None, f"no match in {text!r}")
    check(spec_label("29:1614.105:a,1") == "29 C.F.R. § 1614.105(a)(1)",
          "label")

    # --- indent inference ---
    seq = ["a", "1", "2", "i", "ii", "b", "1", "i", "A", "B", "2", "c",
           "h", "i", "j"]
    stack: list[tuple[str, str]] = []
    levels = [infer_enum_level([e], stack, CFR_HIERARCHY) for e in seq]
    want_lv = [0, 1, 1, 2, 2, 0, 1, 2, 3, 3, 1, 0, 0, 0, 0]
    check(levels == want_lv, f"levels {list(zip(seq, levels))!r}")
    stack = []
    check(infer_enum_level(["a", "1"], stack, CFR_HIERARCHY) == 0 and
          infer_enum_level(["i"], stack, CFR_HIERARCHY) == 2,
          "multi-enum (a)(1) then (i)")
    check(infer_enum_level(["See"], stack, CFR_HIERARCHY) is None,
          "non-enumerator token rejected")

    # --- XML parsing, authentic eCFR shape ---
    sample = """<?xml version="1.0"?>
<DIV5 N="1614" TYPE="PART"><HEAD>PART 1614—FEDERAL SECTOR EEO</HEAD>
<DIV8 N="1614.105" TYPE="SECTION" NODE="29:4.1.4.1.10.1.13.5">
<HEAD>§ 1614.105   Pre-complaint processing.</HEAD>
<P>(a) Aggrieved persons must consult a <I>Counselor</I> prior to filing
 a complaint. &#x201C;Quoted.&#x201D;</P>
<P>(1) An aggrieved person must initiate contact within 45 days.</P>
<P>(2) The agency shall extend the 45-day time limit&mdash;</P>
<P>(i) when the individual shows reasonable cause;</P>
<P>(ii) for other reasons considered sufficient.</P>
<P>An unenumerated continuation of clause (ii).</P>
<P>(b) At the initial counseling session, Counselors must advise
 individuals in writing.</P>
<CITA TYPE="N">[57 FR 12146, Apr. 9, 1992, as amended at 64 FR 37659,
 July 12, 1999]</CITA>
</DIV8></DIV5>"""
    paras = parse_section_xml(sample)
    check(paras[0] == ("sechead", 0, "§ 1614.105 Pre-complaint processing."),
          f"head: {paras[0]!r}")
    got = [(k, i) for k, i, _t in paras]
    check(got == [("sechead", 0), ("body", 0), ("body", 1), ("body", 1),
                  ("body", 2), ("body", 2), ("body", 2), ("body", 0),
                  ("credit", 0)],
          f"kinds/indents: {got!r}")
    check("Counselor" in paras[1][2] and "<I>" not in paras[1][2],
          "inline tags stripped")
    check(paras[-1][0] == "credit" and paras[-1][2].startswith("57 FR"),
          f"credit: {paras[-1]!r}")
    check(not any("PART 1614" in t for _k, _i, t in paras),
          "part heading outside DIV8 dropped")

    # Section ordering from a structure tree (for prev/next navigation)
    tree = {"type": "title", "identifier": "29", "children": [
        {"type": "chapter", "children": [
            {"type": "part", "identifier": "1614", "children": [
                {"type": "subpart", "children": [
                    {"type": "section", "identifier": "1614.101"},
                    {"type": "section", "identifier": "1614.102"},
                    {"type": "section", "identifier": "1614.103",
                     "reserved": True},
                    {"type": "section", "identifier": "1614.105"},
                ]},
            ]},
        ]},
    ]}
    order = _sections_from_structure(tree)
    check(order == ["1614.101", "1614.102", "1614.105"],
          f"structure order (reserved skipped): {order!r}")

    raise SystemExit(1 if failed else 0)
