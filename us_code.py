"""Fetch and parse U.S. Code sections from the Office of the Law Revision
Counsel (uscode.house.gov), the House's official online edition.

The OLRC "prelim" edition serves one section per page at

    https://uscode.house.gov/view.xhtml?req=granuleid:
        USC-prelim-title{T}-section{S}&num=0&edition=prelim

Pages are machine-generated with a stable structure: HTML comments
``<!-- field-start:statute -->`` … ``<!-- field-end:statute -->`` delimit
the head, statute text, source credit, and notes, and every paragraph
carries a CSS class encoding its role and indentation depth
(``statutory-body``, ``statutory-body-1em``, …, ``subsection-head``).
``parse_section()`` walks those markers; the GUI renders the resulting
(kind, indent, text) stream with bolding and indentation.
"""

from __future__ import annotations

import html as _html
import re
import threading
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Citation recognition
# ---------------------------------------------------------------------------

# "42 U.S.C. § 1983", "28 U. S. C. §2254(d)(1)", "18 U.S.C. §§ 922(g)(1)",
# "15 U.S.C.A. § 78j(b)", "42 U.S.C. § 2000e-2(a)", "5 U.S.C. 552".
# A parenthesized subdivision is 1-4 alphanumerics but never 4 digits, so a
# trailing year parenthetical "(1982)" is not swallowed.
USC_CITE_RE = re.compile(
    r"\b(\d{1,2})\s+U\.\s?S\.\s?C\.?\s?(?:A\.)?\s*"
    r"(?:§§?|[Ss]ec(?:tions?|s)?\.?)?\s*"
    r"(\d+[a-zA-Z0-9]*(?:[-–—]\d+[a-zA-Z0-9]*)?)"
    r"((?:\s?\((?:\d{1,3}|[ivxIVX]{2,4}|[a-zA-Z]{1,3})\))*)"
)


def cite_spec(m: re.Match) -> str:
    """Compact "title:section:sub,sub" spec from a USC_CITE_RE match."""
    section = m.group(2).replace("–", "-").replace("—", "-")
    subs = re.findall(r"\(([^)]+)\)", m.group(3) or "")
    return f"{m.group(1)}:{section}:{','.join(subs)}"


def spec_label(spec: str) -> str:
    """Display form of a cite_spec: '42 U.S.C. § 1983(b)(1)'."""
    title, section, subs = spec.split(":", 2)
    tail = "".join(f"({s})" for s in subs.split(",") if s)
    return f"{title} U.S.C. § {section}{tail}"


# ---------------------------------------------------------------------------
# Enumerator-level inference (shared with ecfr.py)
#
# The OLRC's HTML indentation classes mirror the *print* layout, where a
# paragraph opening "(a)(1)" sits flush left and the following "(2)" sits
# flush too.  For on-screen reading we want logical depth instead — each
# enumerator type at its own indent — so nesting is inferred from the
# enumerators themselves.  Hierarchies differ: U.S.C. runs
# (a) -> (1) -> (A) -> (i) -> (I); C.F.R. runs (a) -> (1) -> (i) -> (A).
# The "(i) after (h)" ambiguity is resolved by preferring a successor at
# an already-open level over starting a deeper one.
# ---------------------------------------------------------------------------

USC_HIERARCHY = ("a", "1", "A", "i", "I")
CFR_HIERARCHY = ("a", "1", "i", "A")

ENUM_LEAD_RE = re.compile(r"^((?:\((?:\d{1,3}|[a-zA-Z]{1,5})\)\s*)+)")

_ROMAN_VALS = {"i": 1, "v": 5, "x": 10, "l": 50, "c": 100, "d": 500,
               "m": 1000}


def _roman_to_int(s: str) -> int:
    total, prev = 0, 0
    for ch in reversed(s.lower()):
        v = _ROMAN_VALS.get(ch, 0)
        total += v if v >= prev else -v
        prev = max(prev, v)
    return total


def _enum_value(enum: str, kind: str) -> int:
    """Ordinal of an enumerator interpreted as `kind` (one of "1", "a",
    "A", "i", "I"), or 0 if it doesn't fit that kind."""
    if kind == "1":
        return int(enum) if enum.isdigit() else 0
    if kind in ("a", "A"):
        ok = enum.islower() if kind == "a" else enum.isupper()
        # single letters a..z, then repeated letters (aa), (bb), ...
        if ok and enum.isalpha() and len(set(enum.lower())) == 1:
            return 26 * (len(enum) - 1) + ord(enum[0].lower()) - ord("a") + 1
        return 0
    if kind in ("i", "I"):
        ok = enum.islower() if kind == "i" else enum.isupper()
        if ok and enum and all(c in _ROMAN_VALS for c in enum.lower()):
            return _roman_to_int(enum)
        return 0
    return 0


def infer_enum_level(enums: list[str], stack: list[tuple[str, str]],
                     hierarchy: tuple[str, ...]) -> int | None:
    """Indent level for a paragraph opening with `enums`, updating `stack`
    (open levels as (kind, enum) pairs) in place.  Returns None — leaving
    the stack untouched — when the first token cannot be an enumerator at
    all ("(See)"), so the caller can keep its fallback indent."""
    e = enums[0]
    if not any(_enum_value(e, k) for k in ("1", "a", "A", "i", "I")):
        return None
    level = kind = None
    # 1) successor of an open level, deepest first — "(i)" after "(h)"
    #    continues that level rather than starting romans
    for lvl in range(len(stack) - 1, -1, -1):
        k, prev = stack[lvl]
        if _enum_value(prev, k) and \
                _enum_value(e, k) == _enum_value(prev, k) + 1:
            level, kind = lvl, k
            break
    # 2) the first value one level deeper
    if level is None:
        k = hierarchy[min(len(stack), len(hierarchy) - 1)]
        if _enum_value(e, k) == 1:
            level, kind = len(stack), k
    # 3) the first value of some shallower level
    if level is None:
        for lvl in range(min(len(stack), len(hierarchy)) - 1, -1, -1):
            if _enum_value(e, hierarchy[lvl]) == 1:
                level, kind = lvl, hierarchy[lvl]
                break
    if level is None:  # give up gracefully: sibling of the deepest level
        level = max(len(stack) - 1, 0)
        kind = hierarchy[min(level, len(hierarchy) - 1)]
    del stack[level:]
    stack.append((kind, e))
    # further enumerators in the same paragraph open deeper levels
    for extra in enums[1:]:
        k = hierarchy[min(len(stack), len(hierarchy) - 1)]
        stack.append((k, extra))
    return level


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}


def section_url(title: str, section: str) -> str:
    return (
        "https://uscode.house.gov/view.xhtml?req=granuleid:"
        f"USC-prelim-title{title}-section{section}&num=0&edition=prelim"
    )


@dataclass
class UscSection:
    title: str
    section: str
    url: str
    # (kind, indent, text); kind in {"sechead", "head", "body", "credit",
    # "note-head", "note-body"}
    paras: list[tuple[str, int, str]] = field(default_factory=list)
    # deepest container granule from the page breadcrumbs, e.g.
    # "title26-chapter48" — its table of sections gives document order
    container: str | None = None

    @property
    def kind(self) -> str:
        return "usc"

    def neighbors(self) -> tuple[tuple[str, str] | None,
                                 tuple[str, str] | None]:
        """Adjacent sections, from the container's table of sections
        (fetched lazily and cached).  Failures yield (None, None)."""
        if not self.container:
            print(f"[usc-nav] no container breadcrumb found for "
                  f"§ {self.section}")
            return None, None
        try:
            order = _container_sections(self.container)
        except Exception as exc:
            print(f"[usc-nav] section list fetch failed for "
                  f"{self.container}: {exc}")
            return None, None
        try:
            i = next(idx for idx, s in enumerate(order)
                     if s.lower() == self.section.lower())
        except StopIteration:
            print(f"[usc-nav] § {self.section} not in {self.container} "
                  f"list ({len(order)} sections: {order[:8]}…)")
            return None, None
        prev = (self.title, order[i - 1]) if i > 0 else None
        nxt = (self.title, order[i + 1]) if i + 1 < len(order) else None
        return prev, nxt

    @property
    def heading(self) -> str:
        for kind, _i, text in self.paras:
            if kind == "sechead":
                return text
        return f"{self.title} U.S.C. § {self.section}"

    @property
    def label(self) -> str:
        return f"{self.title} U.S.C. § {self.section}"

    @property
    def source_name(self) -> str:
        return "U.S. Code (OLRC)"

    @property
    def source_note(self) -> str:
        return "OLRC preliminary edition (current law)"

    def bluebook_cite(self, subs: tuple = ()) -> str:
        """Bluebook citation (rule 12.3); current official code, so no
        edition year: '42 U.S.C. § 1983(b)(1)'."""
        tail = "".join(f"({s})" for s in subs)
        return f"{self.title} U.S.C. § {self.section}{tail}"


_cache: dict[tuple[str, str], UscSection] = {}
_cache_lock = threading.Lock()


def load_section(title: str, section: str) -> UscSection:
    """Fetch and parse a section, with an in-memory cache.  For a range or
    hyphenated section that the OLRC does not know ("78a-78pp"), falls back
    to the part before the dash.  Raises RuntimeError with a readable
    message on failure."""
    title, section = str(title).strip(), str(section).strip()
    key = (title, section)
    with _cache_lock:
        if key in _cache:
            return _cache[key]

    import requests

    candidates = [section]
    if "-" in section:
        candidates.append(section.split("-", 1)[0])
    last_err = "section not found"
    for cand in candidates:
        url = section_url(title, cand)
        try:
            resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(f"uscode.house.gov: {exc}") from exc
        paras = parse_section(resp.text)
        if paras:
            doc = UscSection(title=title, section=cand, url=url, paras=paras)
            doc.container = _find_container(resp.text)
            with _cache_lock:
                _cache[key] = doc
            return doc
        last_err = f"no text found for {title} U.S.C. § {cand}"
    raise RuntimeError(f"uscode.house.gov: {last_err}")


# Previous/next navigation.  The OLRC viewer's own prev/next controls are
# JSF form postbacks (no usable hrefs), so neighbors are derived instead
# from the page's breadcrumb trail: the deepest container granule (part,
# subchapter, or chapter) is fetched — streamed, stopping at the end of
# its "analysis" field — and its table of sections gives document order.

_GRANULE_PATH_RE = re.compile(
    r"granuleid(?:%3[Aa]|:)USC-prelim-([A-Za-z0-9\-]+)"
)


def _find_container(page_html: str) -> str | None:
    """Deepest non-section granule path in the page's breadcrumb links,
    e.g. "title26-chapter48"."""
    best = None
    for m in _GRANULE_PATH_RE.finditer(page_html):
        path = m.group(1)
        if "-section" in path.lower():
            continue
        # breadcrumbs run shallow -> deep, so on equal depth keep the
        # later one (Subtitle D and Chapter 48 both have one dash)
        if best is None or path.count("-") >= best.count("-"):
            best = path
    return best


# The closing comment is optional so a streamed buffer cut mid-comment
# still parses; the cell classes only occur inside analyses, so falling
# back to the whole buffer when the field markers are absent is safe.
_ANALYSIS_REGION_RE = re.compile(
    r"<!--\s*field-start:analysis\s*-->(.*?)"
    r"(?:<!--\s*field-end:analysis|\Z)",
    re.DOTALL,
)
_ANALYSIS_LEFT_RE = re.compile(
    r'class="(?:two|three)-column-analysis-style-content-left"[^>]*>'
    r"(.*?)</(?:div|td)>",
    re.DOTALL,
)
_ANALYSIS_SEC_RE = re.compile(
    r"\[?(\d+[A-Za-z0-9]*(?:[-–]\d+[A-Za-z0-9]*)?)"
)
# Fallback ordering: the section heads themselves, in document order
_SECHEAD_NUM_RE = re.compile(
    r'<h3\s+class="section-head"[^>]*>\s*(?:<[^>]+>\s*)*\[?§+\s*'
    r"(\d+[A-Za-z0-9]*(?:[-–]\d+[A-Za-z0-9]*)?)",
)


def _sections_from_analysis(page_html: str) -> list[str]:
    """Ordered section numbers from a container page's table of sections.
    A range entry ("5001 to 5011. Repealed.") contributes its first
    number; bracketed (repealed/omitted) entries are kept — the OLRC
    serves a page for them too.  When no analysis cells are found, the
    section heads in the buffer provide the order instead."""
    m = _ANALYSIS_REGION_RE.search(page_html)
    region = m.group(1) if m else page_html
    out: list[str] = []
    for cell in _ANALYSIS_LEFT_RE.finditer(region):
        text = _clean(cell.group(1))
        sec = _ANALYSIS_SEC_RE.match(text)
        if sec:
            ident = sec.group(1).replace("–", "-")
            if not out or out[-1] != ident:
                out.append(ident)
    if not out:
        for head in _SECHEAD_NUM_RE.finditer(page_html):
            ident = head.group(1).replace("–", "-")
            if not out or out[-1] != ident:
                out.append(ident)
    return out


_order_cache: dict[str, list[str]] = {}


def _container_sections(container: str) -> list[str]:
    """Ordered sections of a container granule, cached.  The container
    page holds the whole unit's text, so the response is streamed and cut
    off once the table of sections has arrived."""
    with _cache_lock:
        if container in _order_cache:
            return _order_cache[container]

    import requests

    url = ("https://uscode.house.gov/view.xhtml?req=granuleid:"
           f"USC-prelim-{container}&num=0&edition=prelim")
    resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=30,
                        stream=True)
    try:
        resp.raise_for_status()
        buf = b""
        for chunk in resp.iter_content(65536):
            buf += chunk
            if b"field-end:analysis" in buf or len(buf) > 2_000_000:
                break
    finally:
        resp.close()
    order = _sections_from_analysis(buf.decode("utf-8", "replace"))
    print(f"[usc-nav] {container}: {len(order)} sections "
          f"from {len(buf):,} bytes")
    if order:
        with _cache_lock:
            _order_cache[container] = order
    return order


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

# CSS class → indentation depth, per the OLRC page generator's conventions.
_INDENT_CLASSES: dict[str, int] = {}
for _lvl, _classes in {
    0: ["statutory-body", "statutory-body-block", "statutory-body-block-1em",
        "statutory-body-flush2_hang4", "statutory-body-flush0_hang2",
        "tableftnt", "note-body", "note-body-flush0_hang1",
        "note-body-block"],
    1: ["statutory-body-1em", "statutory-body-flush2_hang3",
        "statutory-body-block-2em", "note-body-1em",
        "note-body-flush0_hang2", "note-body-flush1_hang2"],
    2: ["statutory-body-2em", "note-body-2em", "note-body-flush3_hang4"],
    3: ["statutory-body-3em", "statutory-body-block-4em", "note-body-3em"],
    4: ["statutory-body-4em", "usc28aForm-left", "usc28aform-right"],
    5: ["statutory-body-5em"],
    6: ["statutory-body-6em"],
}.items():
    for _c in _classes:
        _INDENT_CLASSES[_c] = _lvl

# Headed subdivisions inside the statute text ("(b) Penalties" lines)
_SUB_HEAD_INDENT = {
    "subsection-head": 0, "paragraph-head": 0, "subparagraph-head": 1,
    "clause-head": 2, "subclause-head": 3, "subsubclause-head": 4,
}

_NOTE_HEAD_CLASSES = {"note-head", "note-sub-head", "analysis-subhead"}

# Fields whose content is shown as statute vs. notes; anything else inside
# an unrecognized field is treated as a note so quoted statutory text in
# amendment notes never masquerades as current law.
_TOKEN_RE = re.compile(
    r"<!--\s*field-(start|end):([\w-]+)\s*-->"
    r"|<(h\d|p)\b[^>]*?class=\"([^\"]+)\"[^>]*>(.*?)</\3>",
    re.IGNORECASE | re.DOTALL,
)


def _clean(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", "", fragment)
    text = _html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_section(page_html: str) -> list[tuple[str, int, str]]:
    """Parse an OLRC section page into a (kind, indent, text) stream."""
    paras: list[tuple[str, int, str]] = []
    fields: list[str] = []  # stack of open field names
    for m in _TOKEN_RE.finditer(page_html):
        if m.group(1):  # field comment
            name = m.group(2).lower()
            if m.group(1) == "start":
                fields.append(name)
            else:
                while fields and fields.pop() != name:
                    pass
            continue
        cls = m.group(4).split()[0]
        text = _clean(m.group(5))
        if not text:
            continue
        ctx = fields[-1] if fields else None
        if ctx is None:
            # No field markers (format drift): classify by class name
            is_note = cls.startswith("note") or cls == "analysis-subhead"
        else:
            is_note = ctx not in ("statute", "head", "sourcecredit",
                                  "repealedhead", "omittedhead")
        if cls == "section-head" or (
            cls.endswith("-head")
            and ctx in ("head", "repealedhead", "omittedhead")
        ):
            paras.append(("sechead", 0, text))
        elif cls == "source-credit":
            paras.append(("credit", 0, text))
        elif not is_note and cls in _SUB_HEAD_INDENT:
            paras.append(("head", _SUB_HEAD_INDENT[cls], text))
        elif not is_note and cls in _INDENT_CLASSES:
            paras.append(("body", _INDENT_CLASSES[cls], text))
        elif is_note and (cls in _NOTE_HEAD_CLASSES
                          or cls.endswith("-head")):
            paras.append(("note-head", 0, text))
        elif is_note and (cls in _INDENT_CLASSES
                          or cls in _SUB_HEAD_INDENT):
            # note text, or statute-classed text quoted inside a note
            paras.append(("note-body",
                          _INDENT_CLASSES.get(
                              cls, _SUB_HEAD_INDENT.get(cls, 0)),
                          text))
    return _relevel_statute(paras)


def _relevel_statute(
    paras: list[tuple[str, int, str]]
) -> list[tuple[str, int, str]]:
    """Replace the print-derived indents of statute paragraphs with
    logical depth per the U.S.C. hierarchy, so "(a)(1)" followed by
    "(2)" indents "(2)" under "(a)".  An unenumerated paragraph is a
    continuation of the currently open item and stays at its depth
    (never shallower than its class indent, so indented block material
    keeps its offset)."""
    stack: list[tuple[str, str]] = []
    out: list[tuple[str, int, str]] = []
    for kind, ind, text in paras:
        if kind in ("body", "head"):
            lvl = None
            m = ENUM_LEAD_RE.match(text)
            if m:
                enums = re.findall(r"\(([^)]+)\)", m.group(1))
                lvl = infer_enum_level(enums, stack, USC_HIERARCHY)
            if lvl is None:  # continuation of the open item
                lvl = max(ind, len(stack) - 1)
            ind = min(max(lvl, 0), 6)
        out.append((kind, ind, text))
    return out


if __name__ == "__main__":
    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    # --- citation regex ---
    cases = [
        ("42 U.S.C. § 1983.", "42:1983:"),
        ("28 U. S. C. §2254(d)(1)", "28:2254:d,1"),
        ("18 U.S.C. §§ 922(g)(1)", "18:922:g,1"),
        ("15 U.S.C.A. § 78j(b)", "15:78j:b"),
        ("42 U.S.C. § 2000e-2(a)", "42:2000e-2:a"),
        ("see 5 U.S.C. 552", "5:552:"),
        ("15 U.S.C. §§ 78a–78pp", "15:78a-78pp:"),
        ("42 U.S.C. § 1983 (1982)", "42:1983:"),
    ]
    for text, want in cases:
        m = USC_CITE_RE.search(text)
        got = cite_spec(m) if m else None
        check(got == want, f"{text!r} -> {got!r}")
    for text in ("501 U.S. 32", "1988 U.S.C.C.A.N. 5982",
                 "U.S. Const. art. I", "120 U.S. 678"):
        check(USC_CITE_RE.search(text) is None, f"no match in {text!r}")
    check(spec_label("42:1983:") == "42 U.S.C. § 1983", "label plain")
    check(spec_label("18:922:g,1") == "18 U.S.C. § 922(g)(1)", "label subsec")

    # --- parser, against authentic OLRC markup ---
    sample = """
<html><body><div class="uscnav">junk <p class="navhead">nav</p></div>
<!-- field-start:head -->
<h3 class="section-head">&sect;110. Same; income tax</h3>
<!-- field-end:head -->
<!-- field-start:statute -->
<p class="statutory-body">(a) No State, or political subdivision thereof,
 may, for purposes of any income tax levied by such State&mdash;</p>
<p class="statutory-body-1em">(1) treat such Member as a <em>resident</em>
 or domiciliary of such State; or</p>
<p class="statutory-body-1em">(2) treat any compensation paid by the
 United States to such Member as income for services performed within
 such State,</p>
<p class="statutory-body-2em">(A) a deeper clause;</p>
<p class="statutory-body">(b) For purposes of subsection (a)&mdash;</p>
<!-- field-end:statute -->
<!-- field-start:sourcecredit -->
<p class="source-credit">(Added Pub. L. 99&ndash;190, Dec. 19, 1985,
 99 Stat. 1185.)</p>
<!-- field-end:sourcecredit -->
<!-- field-start:notes -->
<h4 class="note-head">Editorial Notes</h4>
<h4 class="note-sub-head">Amendments</h4>
<p class="note-body">1985&mdash;Subsec. (a). Pub. L. 99&ndash;190 added
 text reading as follows:</p>
<p class="statutory-body">(x) quoted statute text inside a note</p>
<!-- field-end:notes -->
</body></html>"""
    paras = parse_section(sample)
    kinds = [(k, i) for k, i, _t in paras]
    check(paras[0] == ("sechead", 0, "§110. Same; income tax"),
          f"section head: {paras[0]!r}")
    check(("body", 0) in kinds and ("body", 1) in kinds
          and ("body", 2) in kinds, f"body indents: {kinds!r}")
    check(any(k == "credit" for k, _i in kinds), "source credit captured")
    check(kinds.count(("note-head", 0)) == 2, "note heads captured")
    check(paras[-1][0] == "note-body",
          f"quoted statute in note stays a note: {paras[-1]!r}")
    check(not any("nav" in t for _k, _i, t in paras), "nav junk dropped")
    body1 = next(t for k, i, t in paras if (k, i) == ("body", 1))
    check("resident" in body1 and "<em>" not in body1,
          "inline tags stripped")

    # Logical releveling: OLRC's print layout puts "(a)(1)" and the
    # following "(2)" both flush left; the reader should indent (2) under
    # (a), and (A)/(i) one level deeper each (U.S.C. hierarchy).
    quirk = """
<!-- field-start:statute -->
<p class="statutory-body">(a)(1) Combined opening paragraph.</p>
<p class="statutory-body">(2) Print-flush sibling of (1).</p>
<p class="statutory-body-1em">(A) A subparagraph.</p>
<p class="statutory-body-2em">(i) A clause.</p>
<p class="statutory-body-2em">(ii) Another clause.</p>
<p class="statutory-body">Continuation paragraph of clause (ii).</p>
<p class="statutory-body">(b) Next subsection.</p>
<p class="statutory-body">Continuation of subsection (b).</p>
<p class="statutory-body">(c) Then (h)-style:</p>
<p class="statutory-body">(h) Skip ahead.</p>
<p class="statutory-body">(i) Letter i, not roman.</p>
<!-- field-end:statute -->"""
    got_lvls = [(t.split()[0], i) for k, i, t in parse_section(quirk)]
    want_lvls = [("(a)(1)", 0), ("(2)", 1), ("(A)", 2), ("(i)", 3),
                 ("(ii)", 3), ("Continuation", 3), ("(b)", 0),
                 ("Continuation", 0), ("(c)", 0), ("(h)", 0),
                 ("(i)", 0)]
    check(got_lvls == want_lvls, f"logical relevel: {got_lvls!r}")

    # Breadcrumb container extraction (real OLRC breadcrumb form: the
    # links are URL-encoded and carry jsessionid/saved parameters)
    crumbs = """
<a href="/view.xhtml;jsessionid=9600DA?req=granuleid%3AUSC-prelim-title26&amp;saved=%7CZ3%3D%7C&amp;edition=prelim" class="link_class">TITLE 26</a>
<a href="/view.xhtml;jsessionid=9600DA?req=granuleid%3AUSC-prelim-title26-subtitleD&amp;edition=prelim" class="link_class">Subtitle D</a>
<a href="/view.xhtml;jsessionid=9600DA?req=granuleid%3AUSC-prelim-title26-chapter48&amp;edition=prelim" class="link_class">CHAPTER 48</a>
"""
    check(_find_container(crumbs) == "title26-chapter48",
          f"container: {_find_container(crumbs)!r}")
    check(_find_container("<p>nothing</p>") is None, "no container -> None")

    # Table-of-sections parsing (verbatim OLRC analysis markup)
    analysis = """
<!-- field-start:analysis -->
<div class="analysis">
<div><div class="analysis-head-left">Sec.</div></div>
<div><div class="two-column-analysis-style-content-left">5000A.</div><div class="two-column-analysis-style-content-right">Requirement to maintain minimum essential coverage.</div></div>
<div><div class="two-column-analysis-style-content-left">5000B.</div><div class="two-column-analysis-style-content-right">Imposition of tax.</div></div>
<div><div class="two-column-analysis-style-content-left">5001 to 5011. Repealed.</div><div class="two-column-analysis-style-content-right"></div></div>
<div><div class="two-column-analysis-style-content-left">[5012.</div><div class="two-column-analysis-style-content-right">Omitted.]</div></div>
<div><div class="two-column-analysis-style-content-left">2000e&#8211;2.</div><div class="two-column-analysis-style-content-right">Hyphenated.</div></div>
</div>
<!-- field-end:analysis -->
<h3 class="section-head">§5000A. Not part of the analysis</h3>"""
    order = _sections_from_analysis(analysis)
    check(order == ["5000A", "5000B", "5001", "5012", "2000e-2"],
          f"analysis order: {order!r}")
    # Streamed buffer cut off mid closing comment still parses
    truncated = analysis.split("<!-- field-end:analysis")[0] \
        + "<!-- field-end:analysis"
    check(_sections_from_analysis(truncated)
          == ["5000A", "5000B", "5001", "5012", "2000e-2"],
          "truncated buffer parses")
    # Fallback: no analysis cells -> order from section heads
    heads = """
<h3 class="section-head">§1981. Equal rights under the law</h3>
<p class="statutory-body">text</p>
<h3 class="section-head">§1981a. Damages in cases of intentional
 discrimination</h3>
<h3 class="section-head">[§1982. Repealed]</h3>
<h3 class="section-head">§1983. Civil action for deprivation of rights</h3>
"""
    fb = _sections_from_analysis(heads)
    check(fb == ["1981", "1981a", "1982", "1983"],
          f"section-head fallback: {fb!r}")
    doc = UscSection(title="26", section="5000A", url="u",
                     container="title26-chapter48")
    _order_cache["title26-chapter48"] = order
    check(doc.neighbors() == (None, ("26", "5000B")),
          f"neighbors: {doc.neighbors()!r}")
    doc2 = UscSection(title="26", section="5000B", url="u",
                      container="title26-chapter48")
    check(doc2.neighbors() == (("26", "5000A"), ("26", "5001")),
          f"neighbors mid: {doc2.neighbors()!r}")
    _order_cache.clear()

    # Fallback: same page without field comments still classifies by class
    stripped = re.sub(r"<!--.*?-->", "", sample, flags=re.DOTALL)
    paras2 = parse_section(stripped)
    kinds2 = [(k, i) for k, i, _t in paras2]
    check(("body", 0) in kinds2 and ("body", 2) in kinds2,
          f"fallback bodies: {kinds2!r}")
    check(("note-head", 0) in kinds2 and ("note-body", 0) in kinds2,
          "fallback notes")
    check(paras2[0][0] == "sechead", "fallback section head")

    raise SystemExit(1 if failed else 0)
