"""Analyze a Supreme Court **slip opinion** PDF (the format supremecourt.gov
publishes on decision day) using its very regular layout.

Two jobs, both driven by the running heads the Court prints at the top of every
page:

  * :func:`detect_sections` — split the opinion into its parts (Syllabus,
    Opinion of the Court, and each concurrence / dissent) with the page each
    starts on, for a jump-to-section navigator.
  * :func:`to_clean_text` — convert the PDF back to readable, copyable text:
    strip the running heads, page numbers and section-divider rules; undo the
    line-wrap hyphenation; and rebuild paragraphs (and indented block quotes)
    from the glyph geometry, preserving the structure the PDF encodes only
    visually.

Every page's top three lines are a running head:

    <even page>   ``17   CASE NAME v.``  /  ``OTHER PARTY``  /  ``<Section>``
    <odd page>    ``Cite as: 609 U. S. ____ (2026)   18``  /  ``<Section>``

and each new part begins on a page led by a ``____`` divider rule.  ``<Section>``
is ``Syllabus``, ``Opinion of the Court``, ``Per Curiam``, ``NAME, J.,
concurring`` / ``dissenting`` (with the usual "in part" / "in the judgment"
variants), or ``Opinion of NAME, J.`` for a mixed separate opinion.

Pure and dependency-free: the input is the per-page glyph data the app already
extracts with pdfium (``[[(char, box_or_None), …], …]``, box = ``(l, b, r, t)``
in PDF points), so this module needs neither pdfium nor tkinter and runs under
``python -X utf8 slip_opinion.py`` for a self-test.
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Line reconstruction from glyph boxes
# ---------------------------------------------------------------------------

@dataclass
class Line:
    text: str
    x0: float      # left edge of the first glyph (PDF points)
    x1: float      # right edge of the last glyph
    y: float       # top of the line (points; larger = higher on the page)
    height: float  # median glyph height, for spacing/paragraph gaps


def _build_line(glyphs: list) -> Line:
    """Assemble one visual line from its glyphs (each ``(char, box)``), left to
    right, inserting a space wherever the horizontal gap is wider than a glyph's
    own advance would explain."""
    glyphs.sort(key=lambda g: g[1][0])   # by left edge
    parts: list[str] = []
    heights: list[float] = []
    prev_r = None
    prev_ch = ""
    for ch, (l, b, r, t) in glyphs:
        h = t - b
        heights.append(h)
        # Insert a space for a wide gap — but never right before attaching
        # punctuation (".", ",", ")") or right after an opener ("("), so
        # "U. S." doesn't become "U . S .".
        if (prev_r is not None and (l - prev_r) > h * 0.26
                and ch not in ".,;:!?)]}’”'\""
                and prev_ch not in "([{‘“"):
            parts.append(" ")
        parts.append(ch)
        prev_r = r
        prev_ch = ch
    return Line(
        text="".join(parts).strip(),
        x0=glyphs[0][1][0],
        x1=glyphs[-1][1][2],
        y=max(g[1][3] for g in glyphs),
        height=statistics.median(heights),
    )


def group_lines(chars: list) -> list[Line]:
    """Cluster a page's ``(char, box)`` glyphs into visual text lines, top to
    bottom.

    Glyphs are bucketed by their vertical position (not their order in the
    stream): pdfium yields characters in the PDF's content order, which for a
    slip opinion interleaves the running head with the body, so a sequential
    scan would shred every line.  Bucketing by *y* and then sorting each line by
    *x* reconstructs the true lines regardless of stream order.  Whitespace
    glyphs are dropped; spacing is rebuilt from the gaps."""
    glyphs = [(ch, box) for ch, box in chars
              if box is not None and ch and not ch.isspace()]
    if not glyphs:
        return []
    glyphs.sort(key=lambda g: -g[1][3])   # by top edge, highest first
    lines: list[Line] = []
    cur: list = []
    cur_top = cur_h = None
    for ch, box in glyphs:
        _l, b, _r, t = box
        h = t - b
        if cur_top is None:
            cur_top, cur_h = t, h
        elif (cur_top - t) > max(cur_h, h) * 0.5:
            lines.append(_build_line(cur))
            cur, cur_top, cur_h = [], t, h
        else:
            cur_top = (cur_top + t) / 2       # track the line's running center
            cur_h = max(cur_h, h)
        cur.append((ch, box))
    if cur:
        lines.append(_build_line(cur))
    lines.sort(key=lambda ln: -ln.y)
    return lines


# ---------------------------------------------------------------------------
# Running-head / section vocabulary
# ---------------------------------------------------------------------------

# A section running head — the third line of the top matter that names the part.
# The canonical role phrase after a justice's name: "concurring",
# "dissenting in part", "concurring in the judgment in part and dissenting in
# part", …  Bounded to those words so a body sentence that happens to follow
# ("… dissenting in part. I join JUSTICE SOTOMAYOR…") is never swallowed.
_ROLE_TAIL = (
    r"(?:concurring|dissenting)"
    r"(?:\s+(?:in|and|the|part|judgment|concurring|dissenting))*"
)

# ``\s*`` after "of" — the reconstructed running head can lose the space at a
# roman/italic font boundary ("Opinion ofSOTOMAYOR, J.").
_SECTION_RE = re.compile(
    r"^(?:"
    r"Syllabus"
    r"|Per\s+Curiam"
    r"|Opinion\s+of\s+the\s+Court"
    r"|Opinion\s+of\s*[A-Z][A-Za-z.'’\- ]+?,\s*(?:C\.\s*J\.|J\.)"   # "Opinion of SOTOMAYOR, J."
    r"|[A-Z][A-Za-z.'’\-]+,\s*(?:C\.\s*J\.|J\.),\s*" + _ROLE_TAIL +
    r")\s*\.?\s*$"
)

# The body attribution that opens each opinion (used to classify and label an
# "Opinion of X, J." section, whose running head hides the concurring/dissenting
# role).  "JUSTICE KAGAN, with whom … join, dissenting." / "delivered the
# opinion of the Court" / "PER CURIAM".
_ATTRIB_RE = re.compile(
    r"(?:(?:THE\s+)?CHIEF\s+JUSTICE|(?:MR\.\s+)?JUSTICE)\s+([A-Z][A-Za-z’']+)"
    r"(?P<rest>.{0,200}?)(?:\.|$)",
    re.IGNORECASE | re.DOTALL,
)
_PER_CURIAM_RE = re.compile(r"^\s*PER\s+CURIAM", re.IGNORECASE)

# Section-divider rule: a line of underscores the Court prints above each part.
_DIVIDER_RE = re.compile(r"^_{6,}$")
# The slip-opinion banner on the syllabus' first page.
_BANNER_RE = re.compile(r"\(Slip Opinion\)|OCTOBER\s+TERM", re.IGNORECASE)
# A running head naming the parties on an even page ("17  CASE NAME v.").
_NAME_HEAD_RE = re.compile(r"^\d{1,4}\s+[A-Z].*")
_CITE_HEAD_RE = re.compile(r"^Cite as:\s+\d.*U\.\s*S\.")
_PAGENUM_RE = re.compile(r"^[ivxlcdm]{1,7}$|^\d{1,4}$", re.IGNORECASE)


@dataclass
class SlipSection:
    label: str        # display label ("Opinion of the Court", "Thomas, J., concurring")
    kind: str         # syllabus | majority | concurrence | dissent | separate
    start_page: int   # 0-based page index the part begins on


def _title_name(surname_caps: str) -> str:
    """"THOMAS" -> "Thomas", "MCCONNELL" -> "McConnell" (best effort)."""
    s = surname_caps.strip()
    if not s:
        return s
    out = s[0] + s[1:].lower()
    out = re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), out)
    out = re.sub(r"\bO’([a-z])", lambda m: "O’" + m.group(1).upper(), out)
    return out


def _classify(label: str, body: str) -> tuple[str, str]:
    """(kind, display label) for a section from its running-head label and the
    body attribution on its first page."""
    low = label.lower()
    if low == "syllabus":
        return "syllabus", "Syllabus"
    if "per curiam" in low:
        return "majority", "Per Curiam"
    if low == "opinion of the court":
        return "majority", "Opinion of the Court"

    # Separate opinion.  Prefer the explicit "NAME, J., <role>" running head;
    # for the bare "Opinion of NAME, J." form, read the role from the body.
    m = re.match(r"([A-Z][A-Za-z.'’\-]+),\s*(?:C\.\s*J\.|J\.),\s*"
                 r"(" + _ROLE_TAIL + r")", label)
    if m:
        name, role = _title_name(m.group(1)), m.group(2).strip().rstrip(".")
        kind = "dissent" if "dissent" in role.lower() else "concurrence"
        return kind, f"{name}, J., {role}"

    m = re.match(r"Opinion\s+of\s*([A-Z][A-Za-z.'’\- ]+?),\s*(C\.\s*J\.|J\.)$",
                 label)
    if m:
        name = _title_name(m.group(1))
        role = ""
        bm = re.search(r"\b(" + _ROLE_TAIL + r")", body, re.IGNORECASE)
        if bm:
            role = re.sub(r"\s+", " ", bm.group(1)).strip().rstrip(".")
        kind = ("dissent" if "dissent" in role.lower()
                else "concurrence" if "concurr" in role.lower() else "separate")
        title = m.group(2).replace(" ", "")
        label_out = (f"{name}, {title}, {role}" if role
                     else f"Opinion of {name}, {title}")
        return kind, label_out
    return "separate", label


def _page_section_head(lines: list[Line]) -> str:
    """The section running head on a page (its 3rd-ish top line), or ""."""
    for ln in lines[:4]:
        t = re.sub(r"\s+", " ", ln.text).strip()
        if _SECTION_RE.match(t):
            return t
    return ""


def _is_divider_page(lines: list[Line]) -> bool:
    return any(_DIVIDER_RE.match(ln.text.strip()) for ln in lines[:3])


def _body_after_head(lines: list[Line]) -> str:
    """The page's body text with the top running-head lines removed — where an
    opinion's opening attribution lives on a part's first page."""
    body: list[str] = []
    skipping = True
    for ln in lines:
        t = ln.text.strip()
        if skipping and (not t or _DIVIDER_RE.match(t) or _BANNER_RE.search(t)
                         or _CITE_HEAD_RE.match(t) or _NAME_HEAD_RE.match(t)
                         or _SECTION_RE.match(t) or _PAGENUM_RE.match(t)
                         or t.isupper()):
            continue
        skipping = False
        body.append(t)
    return " ".join(body[:6])


def detect_sections(pages: list) -> list[SlipSection]:
    """Split a slip opinion into its parts.  *pages* is the per-page glyph data
    (see the module docstring).  Returns the parts in document order; empty
    when the layout isn't a recognizable slip opinion.

    A part is bounded by its running head: every page of a part carries the
    same ``<Section>`` head (``Syllabus``, ``Opinion of the Court``,
    ``KAGAN, J., dissenting`` …), so a part begins wherever that head first
    changes.  Pages with no recognizable head (a stray blank/figure page)
    inherit the current part and don't split it."""
    page_lines = [group_lines(chars) for chars in pages]
    if not page_lines:
        return []
    heads = [_page_section_head(pl) for pl in page_lines]

    sections: list[SlipSection] = []
    last = None
    for pi, head in enumerate(heads):
        if not head or head == last:
            continue
        # For classification (the concurring/dissenting role of an "Opinion of
        # X, J." part), scan a generous slice of the first page — the
        # attribution sits *below* the caption, not in the top body lines.
        body = " ".join(ln.text for ln in page_lines[pi][:26])
        kind, disp = _classify(head, body)
        if not sections or sections[-1].label != disp:
            sections.append(SlipSection(label=disp, kind=kind, start_page=pi))
        last = head

    # Guarantee a first part starting at page 0 (the syllabus, or — for a slip
    # with none — the opinion), so the navigator always covers the whole PDF.
    if not sections or sections[0].start_page != 0:
        body0 = _body_after_head(page_lines[0])
        if _PER_CURIAM_RE.match(body0):
            first = SlipSection("Per Curiam", "majority", 0)
        elif re.search(r"deliver(?:ed|s)?\s+the\s+opinion", body0, re.I):
            first = SlipSection("Opinion of the Court", "majority", 0)
        else:
            first = SlipSection("Syllabus", "syllabus", 0)
        if sections and sections[0].label == first.label:
            sections[0] = SlipSection(first.label, first.kind, 0)
        else:
            sections.insert(0, first)
    return sections


# ---------------------------------------------------------------------------
# PDF -> clean, copyable text
# ---------------------------------------------------------------------------

_SOFT_HYPHENS = "­​￾�"


# A line that is only punctuation/dashes — a layout artifact of the caption's
# split fonts (the "." of an italic "v." lands as its own line).
_PUNCT_LINE_RE = re.compile(r"^[\s.,;:'’\-–—_]*$")


def _strip_running_head(lines: list[Line]) -> list[Line]:
    """Drop a page's top running-head lines (banner, cite/name head — which
    spans two lines on even pages — section name, divider rules) and any bare
    page-number or punctuation-artifact line, leaving the body."""
    out: list[Line] = []
    in_head = True
    for i, ln in enumerate(lines):
        t = re.sub(r"\s+", " ", ln.text).strip()
        if _PUNCT_LINE_RE.match(t):
            continue  # artifact line — never body, never ends the head
        if in_head and i < 6 and (
            _BANNER_RE.search(t)
            or _CITE_HEAD_RE.match(t) or _NAME_HEAD_RE.match(t)
            or _SECTION_RE.match(t) or re.fullmatch(r"\d{1,4}", t)
            or t in ("SUPREME COURT OF THE UNITED STATES",)
            # 2nd line of the two-line party-name head — but never a body
            # outline heading ("I", "II", "A"), which is short.
            or (i > 0 and len(t) > 3 and t.isupper())
        ):
            continue
        in_head = False
        if _DIVIDER_RE.match(t):
            continue
        out.append(ln)
    return out


def _body_metrics(pages_lines: list[list[Line]]) -> tuple[float, float, float]:
    """(left margin, right margin, body type height) of the opinion body —
    the most common start/end x and the median glyph height of full-measure
    lines — so indents, quotes and footnote-size type can be recognized."""
    xs: list[float] = []
    x1s: list[float] = []
    hs: list[float] = []
    for lines in pages_lines:
        for ln in _strip_running_head(lines):
            if len(ln.text) > 40:            # full-measure body lines only
                xs.append(round(ln.x0))
                x1s.append(round(ln.x1))
                hs.append(ln.height)
    if not xs:
        return 0.0, 612.0, 10.0

    def mode_of(vals, fall):
        try:
            return float(statistics.mode(vals))
        except statistics.StatisticsError:
            return float(fall(vals))

    return (mode_of(xs, min), mode_of(x1s, max),
            statistics.median(hs) if hs else 10.0)


def _dehyphenate(text: str) -> str:
    text = re.sub(r"[" + _SOFT_HYPHENS + r"]", "", text)  # soft hyphens
    # A hyphen at a line break that joins a lowercase continuation is a wrap.
    text = re.sub(r"(\w)-\n(\w)", lambda m: m.group(1) + "\x00" + m.group(2),
                  text)
    text = text.replace("-\x00", "").replace("\x00", "")
    return text


def _tidy(text: str) -> str:
    """Normalize the spacing artifacts of glyph-level reconstruction: no space
    before attaching punctuation or after an opener, tight dashes in number
    ranges ("613 –616" → "613–616"), single spaces."""
    text = re.sub(r"\s+([.,;:!?)\]}’”])", r"\1", text)
    text = re.sub(r"([(\[{‘“])\s+", r"\1", text)
    text = re.sub(r"(\d)\s*([–—-])\s*(\d)", r"\1\2\3", text)
    # The slip type sets em-dashes closed up ("Court—over"); glyph gaps add
    # stray spaces around them.
    text = re.sub(r"[ \t]*—[ \t]*", "—", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text


def to_clean_text(pages: list) -> str:
    """Convert a slip opinion's glyph data to clean, copyable text: running
    heads, page numbers and divider rules removed; wrap hyphenation undone; and
    paragraphs (and indented block quotes) rebuilt from the line geometry.

    A blank line separates paragraphs.  A new paragraph is a line indented past
    the body margin (the Court's first-line indent); a run of lines indented on
    both sides is kept as an indented block quote.  Footnotes — the smaller-type
    lines at a page's foot — are emitted as their own paragraphs at that page's
    end and never merge with a body paragraph continuing onto the next page."""
    pages_lines = [group_lines(chars) for chars in pages]
    body_left, body_right, body_h = _body_metrics(pages_lines)
    indent_min = body_left + 6      # a first-line indent
    # A quote/centered line is inset on *both* sides of the body measure.
    quote_left = body_left + 4
    quote_right = body_right - 14

    paragraphs: list[str] = []
    cur: list[str] = []
    cur_quote = False

    def flush() -> None:
        nonlocal cur, cur_quote
        if cur:
            joined = re.sub(r"\s+", " ", " ".join(cur)).strip()
            if joined:
                paragraphs.append(("\t" + joined) if cur_quote else joined)
        cur, cur_quote = [], False

    # Body paragraphs flow across pages; footnote paragraphs don't.  A body
    # paragraph interrupted by a page's footnotes resumes after them, so body
    # and footnote streams are buffered separately per page and stitched.
    pending_body: list[str] = []     # body continuation carried across pages
    pending_quote = False

    for lines in pages_lines:
        body_lines = _strip_running_head(lines)
        page_body: list[Line] = []
        page_fn: list[Line] = []
        for ln in body_lines:
            (page_fn if ln.height < body_h * 0.88 else page_body).append(ln)

        # Resume the carried body paragraph.
        cur, cur_quote = pending_body, pending_quote
        pending_body, pending_quote = [], False
        for ln in page_body:
            t = ln.text.strip()
            if not t:
                continue
            indented = ln.x0 >= indent_min
            is_quote = ln.x0 >= quote_left and ln.x1 <= quote_right
            if cur and indented and not (cur_quote and is_quote):
                flush()                       # indented opener → new paragraph
            elif cur and cur_quote and not is_quote:
                flush()                       # back to the body measure
            if not cur:
                cur_quote = is_quote
            cur.append(t)
        # Hold the open body paragraph across the footnotes / page break.
        pending_body, pending_quote = cur, cur_quote
        cur, cur_quote = [], False

        # Footnote indents are relative to the footnote block's own margin.
        fn_left = min((ln.x0 for ln in page_fn), default=0.0)
        for ln in page_fn:
            t = ln.text.strip()
            if not t:
                continue
            if cur and ln.x0 >= fn_left + 6:
                flush()                       # a new footnote's indented lead
            cur.append(t)
        if cur:
            flush()

    cur, cur_quote = pending_body, pending_quote
    flush()

    text = "\n\n".join(paragraphs)
    return _tidy(_dehyphenate(text)).strip()


if __name__ == "__main__":  # pragma: no cover - offline self-test
    import sys

    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        if not cond:
            failures.append(msg)
        print(("ok   " if cond else "FAIL ") + msg)

    # Synthetic glyphs: two lines, an indented paragraph opener then a body
    # continuation, exercise group_lines + geometry.
    def mk(text, x0, y, h=10.0):
        chars = []
        x = x0
        for ch in text:
            chars.append((ch, (x, y - h, x + 5, y)))
            x += 6
        chars.append(("\n", None))
        return chars

    page = (mk("Cite as: 609 U. S. ____ (2026) 3", 72, 740)
            + mk("Opinion of the Court", 250, 726)
            + mk("    This is an indented paragraph opener that", 90, 700)
            + mk("runs onto a second, full-measure body line here.", 72, 686))
    lines = group_lines(page)
    check(len(lines) == 4, f"group_lines split 4 lines: {len(lines)}")
    check(lines[0].text.startswith("Cite as:"), "first line is the cite head")

    txt = to_clean_text([page])
    check("Cite as:" not in txt and "Opinion of the Court" not in txt,
          f"running head stripped: {txt!r}")
    check("indented paragraph opener" in txt and "second," in txt,
          f"body lines joined into a paragraph: {txt!r}")

    check(_SECTION_RE.match("Opinion of the Court") is not None, "section: majority")
    check(_SECTION_RE.match("THOMAS, J., concurring") is not None, "section: concur")
    check(_SECTION_RE.match("Opinion of SOTOMAYOR, J.") is not None, "section: mixed")
    check(_SECTION_RE.match("runs onto a second line") is None, "prose is not a section")

    kind, disp = _classify("THOMAS, J., concurring", "")
    check((kind, disp) == ("concurrence", "Thomas, J., concurring"),
          f"classify concurrence: {(kind, disp)}")
    kind, disp = _classify(
        "Opinion of SOTOMAYOR, J.",
        "JUSTICE SOTOMAYOR, concurring in part and dissenting in part.")
    check(kind == "dissent" and "dissenting" in disp,
          f"classify mixed via body: {(kind, disp)}")

    if failures:
        print(f"\n{len(failures)} FAILED")
        sys.exit(1)
    print("\nOK: slip_opinion self-test passed")
