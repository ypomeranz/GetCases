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

import difflib
import re
import statistics
from collections import Counter
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


# ---------------------------------------------------------------------------
# Fuzzy running-head parsing
# ---------------------------------------------------------------------------
# Slip opinions print the section name as a clean line of its own, but the
# scanned US Reports volumes (LOC / GovInfo, roughly pre-1995) run it together
# with the page number and volume cite ("532 Opinion ofthe Court",
# "BRANDEIS, J., dissenting. 260 U. S.") and their OCR garbles words and
# spacing freely ("Opmion of the Court", "Rehnqu ist, J., dissenting",
# "disseltitig").  So heads are matched fuzzily on normalized tokens, and
# pages of the same part are grouped by *identity* (kind + justice name,
# similarity-matched) rather than by the head's exact text — otherwise every
# OCR variant of "Opinion of BRENNAN, J." would begin a new part.


def _sim(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()


def _fuzzy(tok: str, word: str, thresh: float = 0.78) -> bool:
    return tok == word or _sim(tok, word) >= thresh


# Tokens shed from a head line's edges: page numbers and the "470 U. S." /
# "260 U S" volume cite the old running heads append (with its OCR shrapnel).
_EDGE_JUNK = {"U", "S", "US", "T", "I", "L", "IT", "IL", "V"}
# Tokens that are never part of a justice's name.
_NAME_FILLER = {"J", "JJ", "CJ", "C", "MR", "MB", "MESSRS", "THE"}
# Role-phrase vocabulary, canonicalized for display.
_ROLE_CANON = ("DISSENTING", "CONCURRING", "IN", "PART", "AND", "THE",
               "JUDGMENT", "RESULT")


def _digit_heavy(tok: str) -> bool:
    d = sum(c.isdigit() for c in tok)
    return d > 0 and 2 * d >= len(tok)


def _head_tokens(text: str) -> list[str]:
    up = re.sub(r"[’']", "", (text or "").upper())
    toks = re.findall(r"[A-Z0-9]+", up)
    while toks and (_digit_heavy(toks[0]) or toks[0] in _EDGE_JUNK):
        toks.pop(0)
    while toks and (_digit_heavy(toks[-1]) or toks[-1] in _EDGE_JUNK):
        toks.pop()
    return toks


def _role_kind(tok: str) -> str:
    """"dissent"/"concurrence" when *tok* is (an OCR garble of) a role word."""
    if len(tok) < 6:
        return ""
    if _fuzzy(tok, "DISSENTING", 0.72) or _fuzzy(tok, "DISSENTED", 0.8):
        return "dissent"
    if _fuzzy(tok, "CONCURRING", 0.72) or _fuzzy(tok, "CONCURRED", 0.8):
        return "concurrence"
    return ""


def _canon_role_phrase(toks) -> str:
    """Canonical lowercase role phrase from (possibly garbled) role tokens:
    ("CONCURRING","IN","PART","AND","DISSELTITIG","IN","PART") ->
    "concurring in part and dissenting in part"."""
    out: list[str] = []
    for t in toks:
        rk = _role_kind(t)
        if rk:
            out.append("dissenting" if rk == "dissent" else "concurring")
            continue
        for word in _ROLE_CANON[2:]:
            if _fuzzy(t, word, 0.8):
                out.append(word.lower())
                break
    # Keep connectives only where they belong ("in part", "in the judgment",
    # "in the result") — dropped garble in between otherwise strands them
    # ("dissenting in Nos. 759…" -> "dissenting in").
    kept: list[str] = []
    for i, w in enumerate(out):
        nxt = out[i + 1] if i + 1 < len(out) else ""
        if w == "in" and nxt not in ("part", "the", "judgment", "result"):
            continue
        if w == "the" and nxt not in ("judgment", "result"):
            continue
        kept.append(w)
    while kept and kept[-1] in ("in", "and", "the"):
        kept.pop()
    return " ".join(kept)


def _name_groups(toks) -> tuple:
    """Squash name tokens into per-justice keys, splitting on AND:
    ("HARLAN","WHITE","AND","DAY","JJ") -> ("HARLANWHITE","DAY")?  No —
    AND separates the *last* name only in print, but OCR drops commas, so
    every non-filler token before an AND boundary is its own candidate…
    Rather than guess comma placement, tokens between ANDs are joined: the
    common garble is a name split in two ("REHNQU IST"), not two names run
    together, and multi-justice heads almost always write "X, Y and Z"
    whose comma-separated names OCR keeps as separate groups via AND only
    for the final pair.  Practically the squashed key only has to be
    *stable across pages*, which joining achieves."""
    groups: list[str] = []
    cur: list[str] = []
    for t in toks:
        if t == "AND":
            if cur:
                groups.append("".join(cur))
                cur = []
            continue
        if t in _NAME_FILLER or _fuzzy(t, "JUSTICE", 0.8) \
                or _fuzzy(t, "CHIEF", 0.84):
            continue
        cur.append(t)
    if cur:
        groups.append("".join(cur))
    return tuple(g for g in groups if len(g) >= 2)


@dataclass
class _Head:
    kind: str            # syllabus|statement|argument|majority|percuriam|separate|appendix
    key: str             # identity key within the kind
    groups: tuple = ()   # squashed justice-name group(s), for "separate"
    role: tuple = ()     # raw role-tail tokens, for "separate"
    role_kind: str = ""  # dissent | concurrence | "" (bare "Opinion of X, J.")
    disp: tuple = ()     # extra display tokens (the "Argument for …" tail)


def _parse_head_line(text: str):
    """Parse one running-head line into a :class:`_Head`, or ``None``."""
    toks = _head_tokens(text)
    if not toks or len(toks) > 12:
        return None
    squash = "".join(toks)
    if len(squash) < 4 or len(squash) > 60:
        return None
    if _sim(squash, "SYLLABUS") >= 0.8:
        return _Head("syllabus", "syllabus")
    if _sim(squash, "PERCURIAM") >= 0.8:
        return _Head("percuriam", "percuriam")
    if _sim(squash, "OPINIONOFTHECOURT") >= 0.82:
        return _Head("majority", "majority")
    if _sim(squash, "STATEMENTOFTHECASE") >= 0.8:
        return _Head("statement", "statement")
    if _sim(squash[:8], "APPENDIX") >= 0.8:
        return _Head("appendix", "")
    if _sim(squash[:8], "ARGUMENT") >= 0.8:
        return _Head("argument", squash, disp=tuple(toks[1:]))
    # A West-reporter body attribution ("KAVANAUGH, Circuit Judge,
    # dissenting:") is not a running head — those pages have no heads at
    # all, and the attribution scan handles them.
    if any(_fuzzy(t, w, 0.84) for t in toks
           for w in ("JUDGE", "CIRCUIT", "DISTRICT")):
        return None
    # "NAME(, NAME and NAME), J(J)., dissenting/concurring(…)"
    ri = next((i for i, t in enumerate(toks) if _role_kind(t)), None)
    if ri:
        groups = _name_groups(toks[:ri])
        if groups:
            role = tuple(toks[ri:])
            kind = ("dissent"
                    if any(_role_kind(t) == "dissent" for t in role)
                    else "concurrence")
            return _Head("separate", "".join(groups), groups=groups,
                         role=role, role_kind=kind)
        return None
    # "Opinion of NAME, J." (mixed separate opinion; role hidden)
    if _fuzzy(toks[0], "OPINION", 0.78) and len(toks) >= 2:
        rest = list(toks[1:])
        if rest and rest[0] == "OF":
            rest.pop(0)
        elif rest and rest[0].startswith("OF") and len(rest[0]) > 2:
            rest[0] = rest[0][2:]
        if any(t in ("J", "JJ", "CJ") for t in rest):
            groups = _name_groups(rest)
            if groups and _sim("".join(groups), "THECOURT") < 0.7:
                return _Head("separate", "".join(groups), groups=groups)
    return None


def _page_head(lines: list[Line]):
    """The parsed section head of a page — its first parseable top line."""
    real = [ln for ln in lines if not _PUNCT_LINE_RE.match(ln.text)]
    for ln in real[:3]:
        h = _parse_head_line(re.sub(r"\s+", " ", ln.text).strip())
        if h:
            return h
    return None


# ---------------------------------------------------------------------------
# Opening-attribution detection ("MR. JUSTICE BRANDEIS, dissenting.")
# ---------------------------------------------------------------------------

# A West-reporter opinion opener: "SRINIVASAN, Circuit Judge:",
# "KAVANAUGH, Circuit Judge, dissenting:", "PETERS, J.", "SCHAUER, J.,
# dissenting." — the name is printed in (small) caps and the marker is
# followed directly by a terminator or the role.  Not preceded by "(",
# which would be a quoted parenthetical ("(SCALIA, J., dissenting)").
_ATT_WEST_RE = re.compile(
    r"(?<![(\w])"
    r"([A-Z][A-Z'’. -]{2,40}?),\s*"
    r"((?:Senior\s+|Chief\s+|Presiding\s+)?(?:Circuit|District)\s+Judge"
    r"|C\.\s?J\.|P\.\s?J\.|JJ?\.)"
    r"(?:\s*,\s*(dissenting|concurring)[^:.—)]{0,60})?"
    # ":", ".", "—" close the opener; California prints "PETERS, J.—We
    # granted…" whose em-dash OCR often drops, leaving "J." run straight
    # into the text — so a capital letter directly after also closes it.
    r"(?:\s*[:.—]|(?=[A-Z]))"
)

# The same opener split across visual lines (two-column reporters interleave
# the columns, so "KAVANAUGH, Circuit Judge," and "dissenting:" can land on
# successive reconstructed lines).
_ATT_WEST_EOL_RE = re.compile(
    r"(?<![(\w])"
    r"([A-Z][A-Z'’. -]{2,40}?),\s*"
    r"((?:Senior\s+|Chief\s+|Presiding\s+)?(?:Circuit|District)\s+Judge"
    r"|C\.\s?J\.|P\.\s?J\.|JJ?\.)\s*,?\s*$"
)
_ATT_ROLE_LEAD_RE = re.compile(r"\s*(dissenting|concurring)\b|\s*[:—]")

_ATT_BOUNDARY = {"WHOM", "WITH", "JOIN", "JOINS", "JOINED", "FILED", "TOOK"}


def _line_us_attribution(page_lines: list[Line], li: int):
    """A United States Reports opening attribution starting at line *li*:
    "(MR./THE) (CHIEF) JUSTICE NAME(, with whom … join,) <role>." or
    "… delivered the opinion of the Court." / "PER CURIAM".

    Returns ``(kind, name_groups, role_phrase)`` — kind "majority",
    "dissent", "concurrence" or "separate" — or ``None``.  The role may wrap,
    so up to two following lines are scanned for it."""
    toks = re.findall(r"[A-Z0-9]+",
                      re.sub(r"[’']", "", page_lines[li].text.upper()))
    j = 0
    while j < len(toks) and toks[j].isdigit():
        j += 1
    if j < len(toks) - 1 and _fuzzy(toks[j], "PER", 0.99) \
            and _fuzzy(toks[j + 1], "CURIAM", 0.8):
        return ("majority", (), "")
    while j < len(toks) and (toks[j] in ("MR", "MB", "MESSRS", "THE")
                             or _fuzzy(toks[j], "CHIEF", 0.84)):
        j += 1
    if j >= len(toks) or not _fuzzy(toks[j], "JUSTICE", 0.8):
        return None
    window = toks[j + 1:]
    for k in range(li + 1, min(li + 3, len(page_lines))):
        window += re.findall(
            r"[A-Z0-9]+", re.sub(r"[’']", "", page_lines[k].text.upper()))
    window = window[:34]
    # The justice's name: the token(s) right after JUSTICE, before any
    # boundary word / role.
    name: list[str] = []
    for t in window:
        if len(t) == 1 or t.isdigit():
            continue
        if (t in _ATT_BOUNDARY or _role_kind(t)
                or _fuzzy(t, "DELIVERED", 0.8) or _fuzzy(t, "ANNOUNCED", 0.8)
                or _fuzzy(t, "JUSTICE", 0.8)):
            break
        name.append(t)
        if len(name) >= 2:
            break
    for i, t in enumerate(window):
        if _fuzzy(t, "DELIVERED", 0.8) or _fuzzy(t, "DELIVER", 0.86):
            return ("majority", tuple(name), "")
        rk = _role_kind(t)
        if rk:
            phrase = _canon_role_phrase(window[i:i + 10])
            kind = "dissent" if "dissenting" in phrase else (
                "concurrence" if "concurring" in phrase else rk)
            return (kind, tuple(name), phrase)
        if _fuzzy(t, "ANNOUNCED", 0.8):
            return ("separate", tuple(name), "")
    return None


_ATT_BLOCKERS = {"FILED", "POST", "ANTE", "SUPRA", "INFRA", "SEE"}


def _line_name_attribution(page_lines: list, li: int, head: "_Head"):
    """A separate opinion's opening attribution matched by the candidate
    *head*'s own justice name — the fallback when OCR garbles "JUSTICE"
    itself ('MR. JbsTM" CLARK, dissenting in Nos. 759 …'): a body line
    where a name from the head is followed within a few tokens by a role
    word.  The caller keeps this away from the page's top lines (a page's
    own "CLARK, J., dissenting" running head must not confirm itself); the
    syllabus-lineup phrasing ("… filed a dissenting opinion") is rejected
    here."""
    text = page_lines[li].text
    toks = re.findall(r"[A-Z0-9]+", re.sub(r"[’']", "", text.upper()))
    for i, t in enumerate(toks):
        if len(t) < 3 or not any(_sim(t, g) >= 0.7 for g in head.groups):
            continue
        window = toks[i + 1:i + 6]
        for k, t2 in enumerate(window):
            if t2 in _ATT_BLOCKERS:
                break
            rk = _role_kind(t2)
            if rk:
                phrase = _canon_role_phrase(toks[i + 1 + k:i + 1 + k + 12])
                kind = ("dissent" if "dissenting" in phrase else
                        "concurrence" if "concurring" in phrase else rk)
                return (kind, (), phrase)
    return None


def _find_attribution_near(page_lines: list, pi: int, head=None,
                           used: "set | None" = None):
    """The opening attribution proving a separate opinion starts on page *pi*
    — on that page or (an opinion often begins mid-page, so its attribution
    prints just before the running head changes) the page before.

    Returns ``(att, (page, line))`` or ``None``.  *used* holds the
    ``(page, line)`` of attributions already consumed by earlier sections:
    an OCR-garbled head mid-opinion must not restart the same opinion off
    the attribution that already started it."""
    for pg in (pi, pi - 1):
        if pg < 0 or pg >= len(page_lines):
            continue
        lines = page_lines[pg]
        # The running-head zone: the top few real lines, where the part
        # name itself sits — the name-anchored match must not look there.
        real = 0
        head_zone = len(lines)
        for li, ln in enumerate(lines):
            if not _PUNCT_LINE_RE.match(ln.text):
                real += 1
                if real > 3:
                    head_zone = li
                    break
        for li in range(len(lines)):
            att = _line_us_attribution(lines, li)
            if (att is None and head is not None and head.groups
                    and li >= head_zone):
                att = _line_name_attribution(lines, li, head)
            if att and att[0] in ("dissent", "concurrence", "separate"):
                if used is not None and (pg, li) in used:
                    continue
                return att, (pg, li)
    return None


# ---------------------------------------------------------------------------
# Section assembly
# ---------------------------------------------------------------------------

_SMALL_WORDS = {"FOR", "OF", "IN", "THE", "AND", "TO", "A", "ON"}


def _title_tokens(toks) -> str:
    words = []
    for t in toks:
        if _digit_heavy(t):
            continue
        words.append(t.lower() if t in _SMALL_WORDS else _title_name(t))
    return " ".join(words)


def _names_display(groups: tuple) -> str:
    names = [_title_name(g) for g in groups]
    if len(names) <= 1:
        return "".join(names)
    return ", ".join(names[:-1]) + " and " + names[-1]


def _new_sec(head: _Head, pi: int, att=None) -> dict:
    sec = {
        "kind": head.kind, "start": pi, "keys": Counter(),
        "groups": Counter(), "roles": Counter(), "disp": Counter(),
        "att": att,
    }
    _vote(sec, head)
    if att and att[1]:
        # The attribution's (usually cleaner) name joins the identity keys —
        # so later clean-head pages still match a section whose first head
        # was garbled — but not the display votes.
        sec["keys"]["".join(att[1])] += 1
    return sec


def _vote(sec: dict, head: _Head) -> None:
    sec["keys"][head.key] += 1
    if head.groups:
        sec["groups"][head.groups] += 1
    if head.role:
        phrase = _canon_role_phrase(head.role)
        if phrase:
            sec["roles"][phrase] += 1
    if head.disp:
        sec["disp"][head.disp] += 1


def _continues(sec: dict, head: _Head) -> bool:
    """Whether *head* is (an OCR variant of) the running head of *sec*."""
    if head.kind != sec["kind"]:
        return False
    if head.kind in ("syllabus", "statement", "majority", "percuriam"):
        return True
    thresh = 0.7 if head.kind == "argument" else 0.6
    return any(_sim(head.key, k) >= thresh for k in sec["keys"])


def _finalize(sec: dict) -> SlipSection:
    kind = sec["kind"]
    if kind == "syllabus":
        return SlipSection("Syllabus", "syllabus", sec["start"])
    if kind == "statement":
        return SlipSection("Statement of the Case", "syllabus", sec["start"])
    if kind == "argument":
        tail = sec["disp"].most_common(1)
        label = ("Argument " + _title_tokens(tail[0][0])).strip() \
            if tail and tail[0][0] else "Argument"
        return SlipSection(label, "syllabus", sec["start"])
    if kind == "percuriam":
        return SlipSection("Per Curiam", "majority", sec["start"])
    if kind == "majority":
        return SlipSection("Opinion of the Court", "majority", sec["start"])
    # Separate opinion: names from the modal head variant — or, when no two
    # pages agree on the head's spelling (heavy OCR garble), from the
    # opening attribution, which is typeset larger and survives better.
    groups = sec["groups"].most_common(1)
    att_names = tuple((sec.get("att") or (None, (), None))[1] or ())
    if groups and (groups[0][1] > 1 or not att_names):
        use = groups[0][0]
    else:
        use = att_names or (groups[0][0] if groups else ())
    names = _names_display(use) if use else "?"
    marker = "JJ." if len(use) > 1 else "J."
    phrase = sec["roles"].most_common(1)
    role = phrase[0][0] if phrase else ""
    if not role and sec.get("att"):
        att_kind, _n, att_phrase = sec["att"]
        role = att_phrase or {"dissent": "dissenting",
                              "concurrence": "concurring"}.get(att_kind, "")
    if not role:
        return SlipSection(f"Opinion of {names}, {marker}", "separate",
                           sec["start"])
    kind_out = ("dissent" if "dissenting" in role
                else "concurrence" if "concurring" in role else "separate")
    return SlipSection(f"{names}, {marker}, {role}", kind_out, sec["start"])


def _merge_heads(heads: list, page_lines: list) -> list[dict]:
    """Group the per-page parsed heads into contiguous sections, absorbing
    OCR variation and gating each new separate opinion on its opening
    attribution — a page whose head garbles into a *new* justice name mid-
    dissent must not start another section (the old failure mode: a new
    "opinion" on every page)."""
    secs: list[dict] = []
    cur = None
    used: set = set()   # (page, line) of attributions already consumed
    for pi, h in enumerate(heads):
        if h is None or h.kind == "appendix":
            continue  # appendices and unreadable heads inherit the part
        if cur is not None and _continues(cur, h):
            _vote(cur, h)
            continue
        opinion_seen = any(s["kind"] in ("majority", "percuriam", "separate")
                           for s in secs)
        if h.kind in ("syllabus", "statement", "argument", "majority",
                      "percuriam"):
            # Front matter never follows the Court's opinion, and the
            # Court's opinion never restarts — such a head is OCR noise.
            if opinion_seen:
                continue
            cur = _new_sec(h, pi)
            secs.append(cur)
        else:  # a separate opinion — require its opening attribution
            found = _find_attribution_near(page_lines, pi, h, used)
            if found is None:
                continue
            att, (apg, ali) = found
            # A wrapped attribution ("MR. JUSTICE HARLAN, with whom …" /
            # "MR. JUSTICE DAY concurred, dissenting.") matches on each of
            # its lines — consume the whole run so a garbled head on the
            # next page can't restart the same opinion off its tail.
            for k in range(3):
                used.add((apg, ali + k))
            cur = _new_sec(h, pi, att)
            secs.append(cur)
    return secs


def _column_line_sets(chars: list) -> list[list[Line]]:
    """Line sets for the attribution scan: the page's lines as grouped
    normally, plus — because two-column reporters interleave the columns
    when lines are grouped by *y* alone — each half of the page grouped
    separately, so an attribution sitting in one column comes out whole."""
    lines = group_lines(chars)
    sets = [lines]
    boxes = [b for _c, b in chars if b is not None]
    if boxes:
        x0 = min(b[0] for b in boxes)
        x1 = max(b[2] for b in boxes)
        if x1 - x0 > 250:
            mid = (x0 + x1) / 2
            left = [(c, b) for c, b in chars
                    if b is not None and (b[0] + b[2]) / 2 < mid]
            right = [(c, b) for c, b in chars
                     if b is not None and (b[0] + b[2]) / 2 >= mid]
            if left and right:
                sets.append(group_lines(left))
                sets.append(group_lines(right))
    return sets


def _west_attribution(lines: list, li: int):
    """A West-style opinion opener on line *li* — same-line, or wrapping
    onto the next line.  Returns ``(kind, name, role)`` or ``None``."""
    text = lines[li].text.strip()
    m = _ATT_WEST_RE.search(text)
    wrapped = False
    if m is None:
        m = _ATT_WEST_EOL_RE.search(text)
        wrapped = m is not None
        if wrapped:
            nxt = lines[li + 1].text if li + 1 < len(lines) else ""
            r = _ATT_ROLE_LEAD_RE.match(nxt)
            if not r:
                return None
    if m is None or "before" in text[:m.start()].lower():
        return None
    name = m.group(1).strip(" .")
    # Small-caps names come through as caps; a mixed-case match is prose.
    if not name.isupper() or len(name) < 4:
        return None
    role = "" if wrapped else (m.group(3) or "").lower()
    if wrapped:
        r = _ATT_ROLE_LEAD_RE.match(lines[li + 1].text)
        role = (r.group(1) or "").lower() if r else ""
    kind = ("dissent" if role.startswith("dissent")
            else "concurrence" if role.startswith("concur") else "majority")
    return kind, _title_name(name.split(" AND ")[0]), role


def _sections_from_attributions(pages: list) -> list[dict]:
    """Sections for reporter page images with *no* running heads at all
    (static.case.law F./F.2d/F.3d and state-report scans): each opinion is
    found by its opening attribution line — "SRINIVASAN, Circuit Judge:",
    "PETERS, J.—", "SCHAUER, J., dissenting." — in the body text."""
    out: list[dict] = []
    seen: set = set()
    for pi, chars in enumerate(pages):
        for lines in _column_line_sets(chars):
            for li, ln in enumerate(lines):
                att = _line_us_attribution(lines, li)
                hit = None  # (kind, name_display, role_phrase)
                if att:
                    kind, name, phrase = att
                    hit = (kind, _names_display(tuple(name[:1])) or "?",
                           phrase)
                else:
                    west = _west_attribution(lines, li)
                    if west:
                        hit = west
                if not hit:
                    continue
                kind, name, phrase = hit
                key = (kind if kind == "majority" else "sep", name.upper())
                if key in seen:
                    continue
                seen.add(key)
                out.append({"kind": kind, "start": pi, "name": name,
                            "phrase": phrase})
                if len(out) > 10:  # implausible — quoted, not real parts
                    return []
    if not any(s["kind"] == "majority" for s in out):
        return []  # never found the opinion itself — don't trust the rest
    out.sort(key=lambda s: s["start"])
    return out


def _finalize_attribution(sec: dict) -> SlipSection:
    if sec["kind"] == "majority":
        return SlipSection("Opinion of the Court", "majority", sec["start"])
    role = sec["phrase"] or ("dissenting" if sec["kind"] == "dissent"
                             else "concurring")
    kind = ("dissent" if "dissent" in role
            else "concurrence" if "concur" in role else "separate")
    return SlipSection(f"{sec['name']}, J., {role}", kind, sec["start"])


def detect_sections(pages: list) -> list[SlipSection]:
    """Split an opinion PDF into its parts.  *pages* is the per-page glyph
    data (see the module docstring).  Returns the parts in document order;
    a single catch-all part when the layout isn't recognizable.

    Slip opinions and US Reports pages carry a per-page running head naming
    the part; those are matched fuzzily (the scanned volumes' OCR garbles
    them) and grouped by identity, each new separate opinion confirmed by
    its opening attribution ("MR. JUSTICE BRANDEIS, dissenting.").  Reporter
    scans with no running heads (static.case.law) fall back to finding the
    opening attributions themselves."""
    page_lines = [group_lines(chars) for chars in pages]
    if not page_lines:
        return []
    heads = [_page_head(pl) for pl in page_lines]

    # Head-mode needs real running heads (slips, US Reports); a lone parsed
    # "head" in a long document is a body line misread, so fall back to the
    # attribution scan (reporter scans have no heads at all).
    head_pages = sum(1 for h in heads if h is not None)
    if head_pages >= min(2, len(page_lines)):
        sections = [_finalize(s) for s in _merge_heads(heads, page_lines)]
    else:
        sections = [_finalize_attribution(s)
                    for s in _sections_from_attributions(pages)]

    # Consecutive same-label parts collapse (defensive; merging should
    # already have absorbed them).
    deduped: list[SlipSection] = []
    for s in sections:
        if deduped and deduped[-1].label == s.label:
            continue
        deduped.append(s)
    sections = deduped

    # Guarantee a first part starting at page 0 (the syllabus, or — for a
    # PDF with none — the opinion), so the navigator covers the whole PDF.
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

    # --- fuzzy head parsing (clean slip forms and scanned-volume garbles) ---
    def head(text):
        h = _parse_head_line(text)
        return (h.kind, h.key) if h else None

    check(head("Syllabus") == ("syllabus", "syllabus"), "head: syllabus")
    check(head("Opinion of the Court") == ("majority", "majority"),
          "head: majority")
    check(head("532 Opinion ofthe Court") == ("majority", "majority"),
          "head: LOC majority with page number")
    check(head("Opmion of the Court. 260 U. S.") == ("majority", "majority"),
          "head: OCR majority with volume cite")
    check(head("Per Curiam") == ("percuriam", "percuriam"), "head: per curiam")
    check(head("198 U. S. Statement of the Case.")
          == ("statement", "statement"), "head: statement")
    h = _parse_head_line("393 Argument for Plaintiff in Error")
    check(h is not None and h.kind == "argument", "head: argument")
    check(head("THOMAS, J., concurring") == ("separate", "THOMAS"),
          "head: concurrence")
    h = _parse_head_line("436 HARLAN, J., dissenting.")
    check(h is not None and h.role_kind == "dissent" and h.key == "HARLAN",
          f"head: LOC dissent ({h})")
    h = _parse_head_line("Rehnqu ist, J., dissenting 470 U. S.")
    check(h is not None and h.key == "REHNQUIST",
          f"head: split-name dissent ({h})")
    h = _parse_head_line("Kennedy J concurring")
    check(h is not None and h.role_kind == "concurrence"
          and h.key == "KENNEDY", f"head: comma-less concurrence ({h})")
    h = _parse_head_line("Opinion ofBrenn an, J.")
    check(h is not None and h.kind == "separate" and h.key == "BRENNAN"
          and h.role_kind == "", f"head: mixed opinion ({h})")
    h = _parse_head_line("HARLAN, WHITE and DAY, JJ., dissenting. 198 U. S.")
    check(h is not None and h.groups == ("HARLANWHITE", "DAY"),
          f"head: multi-justice dissent ({h})")
    check(head("CLEVELAND BOARD OF EDUCATION v LOUDERMILL 533") is None,
          "head: caption line is not a section")
    check(head("OCTOBER TERM, 1984") is None, "head: term line is not a section")
    check(head("Cite as: 609 U. S. ____ (2026) 18") is None,
          "head: cite line is not a section")

    # --- end-to-end merging over synthetic scanned-volume pages ---
    def pg(*lines):
        chars, y = [], 740
        for text, x in lines:
            chars += mk(text, x, y)
            y -= 16
        return chars

    old_doc = [
        pg(("532 OCTOBER TERM, 1984", 100), ("Syllabus 470 U. S.", 200),
           ("Held: some syllabus text follows here.", 72)),
        pg(("CLEVELAND BD. v LOUDERMILL 533", 100),
           ("532 Opinion ofthe Court", 200),
           ("JUSTICE WHITE delivered the opinion ofthe Court.", 72),
           ("An opinion paragraph.", 72)),
        pg(("534 OCTOBER TERM, 1984", 100),
           ("Opinion of the Court 470 U. S.", 200),
           ("More majority text.", 72)),
        pg(("CLEVELAND BD. v LOUDERMILL 535", 100),
           ("532 Opinion ofBrenn an, J.", 200),
           ("JUSTICE BRENNAN, concurring in part and dissenting in part.", 72)),
        pg(("536 OCTOBER TERM, 1984", 100),
           ("Opinion ofBrennan, J. 470 U. S.", 200),
           ("More Brennan text.", 72)),
        pg(("CLEVELAND BD. v LOUDERMILL 537", 100),
           ("532 Rehnqu ist, J., dissenting", 200),
           ("JUSTICE REHNQUIST, dissenting.", 72),
           ("Dissent text.", 72)),
        pg(("538 OCTOBER TERM, 1984", 100),
           ("Rehnquist, J., dissenting 470 U. S.", 200),
           ("More dissent text.", 72)),
    ]
    secs = detect_sections(old_doc)
    labels = [(s.start_page, s.kind, s.label) for s in secs]
    check(len(secs) == 4, f"old-volume sections: {labels}")
    check(secs[0].kind == "syllabus" and secs[0].start_page == 0,
          f"old-volume syllabus first: {labels}")
    check(secs[1].kind == "majority" and secs[1].start_page == 1,
          f"old-volume majority: {labels}")
    check(secs[2].kind == "dissent" and "Brennan" in secs[2].label
          and "concurring in part" in secs[2].label,
          f"old-volume mixed opinion via attribution: {labels}")
    check(secs[3].kind == "dissent" and "Rehnquist" in secs[3].label,
          f"old-volume dissent: {labels}")

    # An OCR garble of the dissenter's name must NOT start a new part
    # (the "new opinion on every page" failure).
    garbled = old_doc + [
        pg(("CLEVELAND BD. v LOUDERMILL 539", 100),
           ("532 REHNQ UIS T, J., dissenting", 200),
           ("Still the same dissent.", 72)),
    ]
    secs2 = detect_sections(garbled)
    check(len(secs2) == 4,
          f"garbled name page does not split: "
          f"{[(s.start_page, s.label) for s in secs2]}")

    # Reporter scans with no running heads: sections from the opening
    # attributions (static.case.law).
    west_doc = [
        pg(("578", 300), ("Francis V. LORENZO, Petitioner", 150),
           ("Synopsis and headnotes here.", 72)),
        pg(("580", 300), ("SRINIVASAN, Circuit Judge:", 90),
           ("The Securities and Exchange Commission found this.", 72)),
        pg(("590", 300), ("Some more majority text.", 72)),
        pg(("600", 300), ("KAVANAUGH, Circuit Judge, dissenting:", 90),
           ("I respectfully dissent from the panel opinion.", 72)),
    ]
    wsecs = detect_sections(west_doc)
    wl = [(s.start_page, s.kind, s.label) for s in wsecs]
    check(any(s.kind == "majority" and s.start_page == 1 for s in wsecs),
          f"west: majority found: {wl}")
    check(any(s.kind == "dissent" and s.start_page == 3
              and "Kavanaugh" in s.label for s in wsecs),
          f"west: dissent found: {wl}")

    # A modern slip opinion still detects exactly as before.
    slip_doc = [
        pg(("(Slip Opinion) OCTOBER TERM, 2025", 100), ("Syllabus", 250),
           ("HELD: something important.", 72)),
        pg(("Cite as: 609 U. S. ____ (2026)", 100),
           ("Opinion of the Court", 250),
           ("JUSTICE KAGAN delivered the opinion of the Court.", 72)),
        pg(("2 SMITH v. JONES", 100), ("Opinion of the Court", 250),
           ("More text.", 72)),
        pg(("Cite as: 609 U. S. ____ (2026)", 100),
           ("THOMAS, J., concurring", 250),
           ("JUSTICE THOMAS, concurring.", 72)),
    ]
    ssecs = detect_sections(slip_doc)
    sl = [(s.start_page, s.kind, s.label) for s in ssecs]
    check([s.kind for s in ssecs] == ["syllabus", "majority", "concurrence"],
          f"slip sections: {sl}")
    check(ssecs[2].label == "Thomas, J., concurring", f"slip concur label: {sl}")

    if failures:
        print(f"\n{len(failures)} FAILED")
        sys.exit(1)
    print("\nOK: slip_opinion self-test passed")
