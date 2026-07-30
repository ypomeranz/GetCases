"""Fetch and parse Code of Federal Regulations sections from the eCFR
(www.ecfr.gov), the GPO/OFR's continuously updated official edition.

The versioner API serves one section as XML:

    https://www.ecfr.gov/api/versioner/v1/full/{date}/title-{T}.xml
        ?part={P}&section={S}

with {date} taken from /api/versioner/v1/titles.json ("up_to_date_as_of").
A section's human-readable page supplies the eCFR's authoritative
``indent-N`` classes and inline emphasis.  The XML endpoint supplies source
credits and remains the formatting fallback; because its <P>/<FP> elements do
not mark indentation, that fallback infers nesting from the enumerators using
the CFR drafting convention (a) -> (1) -> (i) -> (A) -> (1) -> (i).

``parse_section_xml`` emits the same (kind, indent, text) stream as
``us_code.parse_section`` so one viewer window renders both sources.
"""

from __future__ import annotations

import datetime as _dt
import html as _html
import re
import threading
import xml.etree.ElementTree as _ET
from dataclasses import dataclass, field
from html.parser import HTMLParser as _HTMLParser

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
_HTML_HEADERS = {
    **_HEADERS,
    "Accept": "text/html,application/xhtml+xml",
}

_API = "https://www.ecfr.gov/api/versioner/v1"


@dataclass
class CfrSection:
    title: str
    section: str
    url: str          # human-readable eCFR page, for "Open in Browser"
    date: str         # the issue date the text reflects
    paras: list[tuple[str, int, str]] = field(default_factory=list)
    # Inline style ranges per paragraph: (start, end, bold|italic|bolditalic).
    # Keeping these separate preserves the shared three-field ``paras``
    # contract used by the other statute sources and older saved records.
    para_styles: list[list[tuple[int, int, str]]] = field(default_factory=list)
    site_formatting: bool = False

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
        human_url = (
            f"https://www.ecfr.gov/current/title-{title}/section-{cand}"
        )
        site_paras: list[tuple[str, int, str]] = []
        site_styles: list[list[tuple[int, int, str]]] = []
        try:
            site_resp = requests.get(
                human_url, headers=_HTML_HEADERS, timeout=30,
            )
            if site_resp.status_code != 404:
                site_resp.raise_for_status()
                site_paras, site_styles = parse_section_html(
                    site_resp.text, cand,
                )
        except Exception as exc:
            last_err = str(exc)

        api_url = (f"{_API}/full/{date}/title-{title}.xml"
                   f"?part={part}&section={cand}")
        xml_paras: list[tuple[str, int, str]] = []
        try:
            resp = requests.get(api_url, headers=_HEADERS, timeout=30)
            if resp.status_code == 404:
                last_err = f"no such section {title} C.F.R. § {cand}"
            else:
                resp.raise_for_status()
                # The API omits the charset in its Content-Type; the XML
                # declaration itself identifies the payload as UTF-8.
                xml_paras = parse_section_xml(
                    resp.content.decode("utf-8", "replace")
                )
        except Exception as exc:
            if not site_paras:
                raise RuntimeError(f"ecfr.gov: {exc}") from exc
            last_err = str(exc)

        if site_paras:
            # The website supplies authoritative indentation and inline
            # emphasis; retain credits/notes that are available only in XML.
            extras = [
                para for para in xml_paras
                if para[0] in ("credit", "note-head", "note-body")
            ]
            paras = [*site_paras, *extras]
            para_styles = [*site_styles, *([[]] * len(extras))]
        else:
            paras = xml_paras
            para_styles = [[] for _para in paras]
        if paras:
            doc = CfrSection(
                title=title, section=cand, date=date,
                url=human_url,
                paras=paras,
                para_styles=para_styles,
                site_formatting=bool(site_paras),
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
    r"<(HEAD|P|FP|HED|PSPACE|CITA|SOURCE|AUTH|NOTE)\b[^>]*>(.*?)</\1>",
    re.IGNORECASE | re.DOTALL,
)
_ENUM_LEAD_RE = re.compile(
    r"^((?:\((?:\d{1,3}|[a-zA-Z]{1,5})\)\s*)+)"
)
_INLINE_ENUM_RE = re.compile(
    r"[\u2013\u2014]\s*((?:\((?:\d{1,3}|[a-zA-Z]{1,5})\)\s*)+)"
)


def _normalized_styled_text(
    segments: list[tuple[str, frozenset[str]]],
) -> tuple[str, list[tuple[int, int, str]]]:
    """Collapse HTML whitespace while retaining contiguous emphasis ranges."""
    chars: list[str] = []
    styles_at: list[frozenset[str]] = []
    for raw, styles in segments:
        for char in raw:
            if char.isspace():
                if chars and chars[-1] != " ":
                    chars.append(" ")
                    styles_at.append(styles)
            else:
                chars.append(char)
                styles_at.append(styles)
    while chars and chars[-1] == " ":
        chars.pop()
        styles_at.pop()

    ranges: list[tuple[int, int, str]] = []
    start = 0
    while start < len(chars):
        active = styles_at[start]
        end = start + 1
        while end < len(chars) and styles_at[end] == active:
            end += 1
        if active:
            style = (
                "bolditalic"
                if {"bold", "italic"}.issubset(active)
                else "bold" if "bold" in active else "italic"
            )
            ranges.append((start, end, style))
        start = end
    return "".join(chars), ranges


class _SectionHTMLParser(_HTMLParser):
    """Read one rendered eCFR ``div.section`` without the surrounding chrome."""

    def __init__(self, section: str = "") -> None:
        super().__init__(convert_charrefs=True)
        self.section = str(section or "")
        self.paras: list[tuple[str, int, str]] = []
        self.para_styles: list[list[tuple[int, int, str]]] = []
        self._section_depth = 0
        self._capture = ""
        self._capture_indent = 0
        self._segments: list[tuple[str, frozenset[str]]] = []
        self._styles: list[str] = []

    @staticmethod
    def _attrs(attrs) -> dict[str, str]:
        return {str(key): str(value or "") for key, value in attrs}

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        attributes = self._attrs(attrs)
        if tag == "div":
            if self._section_depth:
                self._section_depth += 1
            else:
                classes = set(attributes.get("class", "").split())
                ident = attributes.get("id", "")
                if "section" in classes and (
                    not self.section or ident == self.section
                ):
                    self._section_depth = 1
            return
        if not self._section_depth:
            return
        if not self._capture and tag in ("h4", "p"):
            self._capture = tag
            self._segments = []
            self._styles = []
            if tag == "p":
                match = re.search(
                    r"(?:^|\s)indent-(\d+)(?:\s|$)",
                    attributes.get("class", ""),
                )
                self._capture_indent = max(
                    0, int(match.group(1)) - 1,
                ) if match else 0
            return
        if not self._capture:
            return
        if tag in ("strong", "b"):
            self._styles.append("bold")
        elif tag in ("em", "i"):
            self._styles.append("italic")
        elif tag == "br":
            self._segments.append((" ", frozenset(self._styles)))

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self._capture and tag == self._capture:
            text, ranges = _normalized_styled_text(self._segments)
            if text:
                kind = "sechead" if tag == "h4" else "body"
                indent = 0 if tag == "h4" else min(self._capture_indent, 6)
                self.paras.append((kind, indent, text))
                self.para_styles.append(ranges)
            self._capture = ""
            self._segments = []
            self._styles = []
        elif self._capture and tag in ("strong", "b", "em", "i"):
            style = "bold" if tag in ("strong", "b") else "italic"
            for index in range(len(self._styles) - 1, -1, -1):
                if self._styles[index] == style:
                    del self._styles[index]
                    break
        if tag == "div" and self._section_depth:
            self._section_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._capture and data:
            self._segments.append((data, frozenset(self._styles)))


def parse_section_html(
    html: str, section: str = "",
) -> tuple[
    list[tuple[str, int, str]],
    list[list[tuple[int, int, str]]],
]:
    """Parse the eCFR website's authoritative indentation/emphasis markup."""
    parser = _SectionHTMLParser(section)
    try:
        parser.feed(html or "")
        parser.close()
    except Exception:
        return [], []
    return parser.paras, parser.para_styles


def _clean(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", "", fragment)
    text = _html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _structural_enums(text: str) -> list[str]:
    """Opening subdivision markers represented by one eCFR paragraph.

    Treasury regulations commonly put several logical levels in one ``<P>``:
    ``(b) Applicable taxpayer—(1) In general`` or even
    ``(v) Affordable coverage—(A) In general—(1) ...``.  Looking only at the
    marker at character zero loses the deeper open levels and makes every
    following sibling appear flush left.  A marker immediately after the CFR's
    heading dash is part of that opening hierarchy; cross-reference markers in
    the prose are not.
    """
    lead = _ENUM_LEAD_RE.match(text or "")
    if not lead:
        return []
    enums = re.findall(r"\(([^)]+)\)", lead.group(1))
    for nested in _INLINE_ENUM_RE.finditer(text, lead.end()):
        enums.extend(re.findall(r"\(([^)]+)\)", nested.group(1)))
    return enums


def _append_body(
    paras: list[tuple[str, int, str]],
    text: str,
    stack: list[tuple[str, str]],
) -> None:
    """Append one ordinary CFR paragraph and update its open hierarchy."""
    # No enumerator means a continuation of the open item, which stays at that
    # item's depth rather than returning flush left.
    level = max(len(stack) - 1, 0)
    enums = _structural_enums(text)
    if enums:
        lvl = infer_enum_level(enums, stack, CFR_HIERARCHY)
        if lvl is not None:
            level = lvl
    paras.append(("body", min(level, 6), text))


def _section_element(xml: str) -> "_ET.Element | None":
    """The first ``DIV8 TYPE=SECTION`` in an eCFR response, if XML-valid."""
    try:
        root = _ET.fromstring(xml)
    except _ET.ParseError:
        return None

    def local_tag(elem: _ET.Element) -> str:
        return str(elem.tag).rsplit("}", 1)[-1].upper()

    for elem in root.iter():
        if (local_tag(elem) == "DIV8"
                and str(elem.attrib.get("TYPE") or "").upper() == "SECTION"):
            return elem
    return None


def _element_text(elem: _ET.Element) -> str:
    return re.sub(r"\s+", " ", _html.unescape(
        "".join(elem.itertext())
    )).strip()


def parse_section_xml(xml: str) -> list[tuple[str, int, str]]:
    """Parse eCFR section XML into the (kind, indent, text) stream used by
    the statute viewer (same contract as us_code.parse_section)."""
    section = _section_element(xml)
    if section is not None:
        paras: list[tuple[str, int, str]] = []
        stack: list[tuple[str, str]] = []
        for elem in section:
            tag = str(elem.tag).rsplit("}", 1)[-1].upper()
            text = _element_text(elem)
            if tag == "HEAD" and text:
                paras.append(("sechead", 0, text))
            elif tag in ("CITA", "SOURCE") and text:
                paras.append(("credit", 0, text.strip("[]")))
            elif tag in ("AUTH", "NOTE") and text:
                paras.append(("note-body", 0, text))
            elif tag in ("P", "FP") and text:
                _append_body(paras, text, stack)
            elif tag == "EXAMPLE":
                # EXAMPLE is a self-contained branch below the currently open
                # CFR paragraph.  Its (i)/(ii) markers must not close or mutate
                # the regulation's main subdivision stack.
                base = min(len(stack), 6)
                for child in elem:
                    ctag = str(child.tag).rsplit("}", 1)[-1].upper()
                    ctext = _element_text(child)
                    if not ctext:
                        continue
                    if ctag == "HED":
                        paras.append(("head", base, ctext))
                    elif ctag in ("P", "FP", "PSPACE"):
                        paras.append(("body", min(base + 1, 6), ctext))
        return paras

    # Defensive fallback for a malformed response: retain the former tolerant
    # regex parser rather than making the entire section unavailable.
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
        elif tag == "HED":
            paras.append(("head", min(len(stack), 6), text))
        elif tag == "PSPACE":
            paras.append(("body", min(len(stack) + 1, 6), text))
        else:  # P / FP
            _append_body(paras, text, stack)
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
