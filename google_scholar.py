"""
Google Scholar Opinion Fetcher
==============================
Fetches US case law opinion text from Google Scholar.

Strategy:
  1. Search scholar.google.com/scholar?q="citation"&as_sdt=4 for the citation.
  2. Pull the first scholar_case link from results.
  3. Scrape the #gs_opinion div from that page, keeping its HTML.
  4. Cache everything in a local SQLite database to avoid re-fetching.

The raw opinion HTML can be turned into a lightweight structured document
with ``parse_opinion_blocks``, which preserves paragraphs, centering,
italics, footnote markers, the embedded scholar_case citation links, and
the reporter star-pagination markers (``*123``) Google inserts into the
text.  Unlike a plain ``get_text()`` pass, inline elements are joined with
no separator, so sentences don't acquire stray gaps around formatting.

Requires:
    pip install requests beautifulsoup4
"""

from __future__ import annotations

import re
import sqlite3
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

try:
    import requests
    from bs4 import BeautifulSoup, Comment, NavigableString, Tag
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "google_scholar requires 'requests' and 'beautifulsoup4'.\n"
        "Install with: pip install requests beautifulsoup4"
    ) from exc


SCHOLAR_BASE = "https://scholar.google.com"

_HEADERS = {
    # Realistic browser UA to avoid trivial blocks
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

_DEFAULT_DELAY = 3.0  # seconds between outbound requests
_CACHE_PATH = Path.home() / ".cache" / "courtlistener_scholar.db"


class ScholarError(Exception):
    """Raised when a Scholar fetch fails unrecoverably."""


# ---------------------------------------------------------------------------
# Structured opinion document
# ---------------------------------------------------------------------------


@dataclass
class Span:
    """A run of text with uniform formatting."""

    text: str
    italic: bool = False
    bold: bool = False
    underline: bool = False
    small: bool = False
    sup: bool = False
    pagenum: bool = False  # reporter star-pagination marker, e.g. "*123"
    link: str = ""         # absolute scholar_case URL for a cited case
    fnref: str = ""        # footnote anchor id for an in-text reference
    fndef: str = ""        # footnote anchor id opening a footnote body


@dataclass
class Block:
    """A paragraph-level chunk of the opinion."""

    kind: str = "para"  # para | center | heading | blockquote
    spans: list[Span] = field(default_factory=list)

    def text(self) -> str:
        return "".join(s.text for s in self.spans)


@dataclass
class ScholarResult:
    """One row parsed from a Scholar case-law results page."""

    title: str
    url: str
    source: str = ""   # the green byline, e.g. "Supreme Court, 1973"
    snippet: str = ""


@dataclass
class OpinionPart:
    """A section of an opinion: header, majority, concurrence, or dissent."""

    label: str
    kind: str  # header | majority | concurrence | dissent
    blocks: list[Block] = field(default_factory=list)
    footnotes: list[Block] = field(default_factory=list)


_WS_RE = re.compile(r"\s+")
# Google's promo line at the foot of every scholar_case page
_SAVE_TREES_RE = re.compile(r"^save trees\b", re.IGNORECASE)
_H_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
_BLOCK_TAGS = _H_TAGS | {
    "p", "div", "blockquote", "center", "pre", "table", "tbody", "thead",
    "tr", "ul", "ol", "li", "dl", "dt", "dd",
}
_FMT_KEYS = ("italic", "bold", "underline", "small", "sup")

_D_OPEN, _D_CLOSE = "“", "”"   # " "
_S_OPEN, _S_CLOSE = "‘", "’"   # ' '
# Characters that typically precede an opening quote: start of text,
# whitespace, opening brackets, dashes, or another (already-curled) quote.
_QUOTE_OPENERS = set(" \t\n\r\f([{-–—" + _D_OPEN + _S_OPEN + "'\"")


def educate_quotes(text: str) -> str:
    """
    Convert straight quotes to typographic open/close quotes
    (SmartyPants-style heuristics).

    A quote is treated as opening when it follows the start of the text,
    whitespace, an opening bracket, a dash, or another opening quote
    (covering nested quotes like "'…'"); otherwise it closes.  A single
    quote between/after letters is an apostrophe (don't, Jones'), and one
    before a digit is a decade contraction ('70s).  Typewriter-style
    backtick openers — GPO statute text quotes terms as `term' (``…'' for
    doubles) — become proper opening quotes so they pair with the curled
    close instead of showing as grave accents.
    """
    text = (text.replace("``", _D_OPEN).replace("''", _D_CLOSE)
            .replace("`", _S_OPEN))
    out = list(text)
    for i, ch in enumerate(text):
        if ch == '"':
            prev = out[i - 1] if i else ""
            out[i] = _D_OPEN if (not prev or prev in _QUOTE_OPENERS) else _D_CLOSE
        elif ch == "'":
            prev = out[i - 1] if i else ""
            nxt = text[i + 1] if i + 1 < len(text) else ""
            if prev and prev.isalnum():
                out[i] = _S_CLOSE      # apostrophe: don't, Jones', App'x
            elif nxt.isdigit():
                out[i] = _S_CLOSE      # decade contraction: '70s
            elif (not prev or prev in _QUOTE_OPENERS) and nxt and not nxt.isspace():
                out[i] = _S_OPEN
            else:
                out[i] = _S_CLOSE
    return "".join(out)


def _educate_block_quotes(block: "Block") -> None:
    """Curl quotes across a whole block so pairing context survives span
    boundaries (e.g. a quote directly before an italicized word)."""
    full = "".join(s.text for s in block.spans)
    fixed = educate_quotes(full)
    if fixed == full:
        return
    pos = 0
    for s in block.spans:
        end = pos + len(s.text)
        s.text = fixed[pos:end]
        pos = end


def parse_opinion_blocks(html: str) -> list[Block]:
    """
    Parse Scholar's #gs_opinion HTML into a list of formatted blocks.

    Block boundaries follow the source's block-level tags; inline tags
    (i/em, b/strong, u, small, sup) become span attributes.  Anchors with
    class ``gsl_pagenum`` become page-marker spans, and anchors pointing
    at other scholar_case pages become citation-link spans.
    """
    # CourtListener's combined opinion (reused here for its Google-Scholar-like
    # star pagination) can arrive as an XML document; drop the declaration so it
    # isn't parsed as stray text and bs4 doesn't warn.  Scholar HTML is
    # unaffected (it has no such declaration).
    html = re.sub(r"^\s*<\?xml[^>]*\?>", "", html or "")
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find(id="gs_opinion") or soup
    for bad in root.find_all(["script", "style"]):
        bad.decompose()
    for bad in root.find_all(id="gs_dont_print"):  # "Save trees…" promo line
        bad.decompose()
    # Most pages duplicate each page marker (bare margin label + inline
    # star form); some older ones carry only the bare form.
    has_pagenum2 = root.find("a", class_="gsl_pagenum2") is not None

    blocks: list[Block] = []
    cur: list[Span] = []

    def emit(
        text: str,
        fmt: dict,
        *,
        pagenum: bool = False,
        link: str = "",
        fnref: str = "",
        fndef: str = "",
    ) -> None:
        text = _WS_RE.sub(" ", text)
        if not text:
            return
        if not cur:
            text = text.lstrip()
            if not text:
                return
        elif cur[-1].text.endswith((" ", "\n")) and text.startswith(" "):
            text = text.lstrip(" ")
            if not text:
                return
        last = cur[-1] if cur else None
        if (
            last is not None
            and not pagenum
            and not last.pagenum
            and not (fnref or fndef or last.fnref or last.fndef)
            and link == last.link
            and all(getattr(last, k) == fmt.get(k, False) for k in _FMT_KEYS)
        ):
            last.text += text
        else:
            cur.append(
                Span(
                    text=text,
                    pagenum=pagenum,
                    link=link,
                    fnref=fnref,
                    fndef=fndef,
                    **{k: fmt.get(k, False) for k in _FMT_KEYS},
                )
            )

    def flush(kind: str) -> None:
        nonlocal cur
        while cur and not cur[-1].text.strip():
            cur.pop()
        if cur:
            cur[-1].text = cur[-1].text.rstrip()
            blocks.append(Block(kind=kind, spans=cur))
        cur = []

    def last_pagenum() -> Optional[str]:
        """Page number of the most recent page marker still in the current
        block (skipping trailing whitespace), or None — lets us drop a star
        marker emitted twice in a row (CourtListener does this)."""
        for s in reversed(cur):
            if s.pagenum:
                return s.text.lstrip("*").strip()
            if s.text.strip():
                return None
        return None

    def walk(node: Tag, fmt: dict, kind: str, link: str = "") -> None:
        for child in node.children:
            if isinstance(child, Comment):
                continue
            if isinstance(child, NavigableString):
                emit(str(child), fmt, link=link)
                continue
            if not isinstance(child, Tag):
                continue
            name = (child.name or "").lower()
            if name == "br":
                if cur:
                    cur.append(Span(text="\n"))
                continue
            if name == "hr":
                flush(kind)
                continue
            if name == "a":
                classes = [c.lower() for c in (child.get("class") or [])]
                if "gsl_pagenum" in classes or "gsl_pagenum2" in classes:
                    # Scholar emits the marker twice: a bare margin label
                    # ("115", gsl_pagenum) and the inline star form
                    # ("*115", gsl_pagenum2).  Keep one, always star-prefixed.
                    t = _WS_RE.sub(" ", child.get_text()).strip()
                    if t and (
                        "gsl_pagenum2" in classes or not has_pagenum2
                    ):
                        emit("*" + t.lstrip("*"), fmt, pagenum=True)
                    continue
                aname = str(child.get("name") or child.get("id") or "")
                if "gsl_hash" in classes and aname:
                    # Footnote anchors: in-text reference name="r[N]" links
                    # to the body anchor name="[N]"; N is globally unique
                    # even though the displayed marker restarts per opinion.
                    t = _WS_RE.sub(" ", child.get_text()).strip()
                    if aname.startswith("r["):
                        if t:
                            emit(t, fmt, fnref=aname[1:])
                        continue
                    if aname.startswith("["):
                        if t:
                            emit(t, fmt, fndef=aname)
                        continue
                href = child.get("href") or ""
                if "scholar_case" in href:
                    if href.startswith("/"):
                        href = SCHOLAR_BASE + href
                    # Recurse so the markup inside the anchor (Scholar nests
                    # its italics inside the link) is preserved verbatim.
                    walk(child, fmt, kind, link=href)
                    continue
                walk(child, fmt, kind, link=link)  # footnote anchors etc.
                continue
            if name == "span":
                classes = [c.lower() for c in (child.get("class") or [])]
                if "star-pagination" in classes:
                    # CourtListener marks a reporter page break with
                    # <span class="star-pagination">*1005</span>.  Emit it as a
                    # page marker exactly like Scholar's, so the page gutter and
                    # pin cites work; CL sometimes repeats the same marker twice
                    # in a row, so drop an immediate duplicate.
                    t = _WS_RE.sub(" ", child.get_text()).strip().lstrip("*").strip()
                    if t and last_pagenum() != t:
                        emit("*" + t, fmt, pagenum=True)
                    continue
                # any other span falls through to a generic walk of its children
            if name in _BLOCK_TAGS:
                flush(kind)
                child_fmt = fmt
                if name == "center":
                    child_kind = "center"
                elif name == "blockquote":
                    child_kind = "blockquote"
                elif name in _H_TAGS:
                    child_kind = kind if kind == "center" else "heading"
                    child_fmt = {**fmt, "bold": True}
                else:
                    child_kind = kind
                walk(child, child_fmt, child_kind, link=link)
                flush(child_kind)
                continue
            if name in ("i", "em", "cite"):
                walk(child, {**fmt, "italic": True}, kind, link=link)
            elif name in ("b", "strong"):
                walk(child, {**fmt, "bold": True}, kind, link=link)
            elif name == "u":
                walk(child, {**fmt, "underline": True}, kind, link=link)
            elif name == "small":
                walk(child, {**fmt, "small": True}, kind, link=link)
            elif name in ("sup", "sub"):
                walk(child, {**fmt, "sup": True}, kind, link=link)
            else:
                walk(child, fmt, kind, link=link)

    walk(root, {}, "para")
    flush("para")
    blocks = [b for b in blocks if not _SAVE_TREES_RE.match(b.text().strip())]
    for block in blocks:
        _educate_block_quotes(block)
    return blocks


def blocks_to_text(blocks: list[Block]) -> str:
    """Plain-text rendering of parsed blocks, paragraphs separated by blank lines."""
    parts = [b.text().strip() for b in blocks]
    return "\n\n".join(p for p in parts if p)


def text_similarity(a: str, b: str, n: int = 4) -> float:
    """
    Word n-gram shingle containment between two texts, in [0, 1].

    Containment (|A∩B| / min(|A|, |B|)) rather than Jaccard, so the score
    is not penalized when one source includes extra material the other
    lacks (syllabus, headnotes, parallel cites).  Texts of the same
    opinion typically score far above 0.6; different opinions score
    near 0 even when they discuss the same subject.
    """
    def shingles(t: str) -> set[str]:
        words = re.findall(r"[a-z0-9]+", t.lower())
        return {" ".join(words[i: i + n]) for i in range(len(words) - n + 1)}

    sa, sb = shingles(a), shingles(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / min(len(sa), len(sb))


# --- Opinion segmentation -------------------------------------------------
# A separate-opinion header starts its own block and names a justice/judge
# plus "concurring"/"dissenting", e.g.:
#   MR. JUSTICE STEWART, concurring.
#   Justice O'CONNOR, with whom Justice BRENNAN joins, dissenting.
#   KOZINSKI, Circuit Judge, dissenting:
_SEP_HEADER_RE = re.compile(
    r"^\s*(?:MR\.\s+|MRS\.\s+|MS\.\s+)?"
    r"(?:(?:CHIEF\s+)?JUSTICE\s+\S+|"
    r"[A-Z][\w.'’-]*(?:\s+[A-Z][\w.'’-]*){0,3}\s*,\s*"
    r"(?:C\.\s*J\.|JJ?\.|(?:Chief\s+|Senior\s+|Presiding\s+)?"
    r"(?:Circuit\s+|District\s+|Bankruptcy\s+)?Judge))"
    r".{0,200}?\b(?:concurring|dissenting)",
    re.IGNORECASE | re.DOTALL,
)
# Syllabus disposition lines ("BLACKMUN, J., delivered the opinion…;
# STEWART, J., filed a concurring opinion…") look like separate-opinion
# headers but are not.
_NOT_SEP_RE = re.compile(r"\b(?:filed|delivered|announced)\b", re.IGNORECASE)

# The byline shared with the body of _SEP_HEADER_RE — a justice/judge name
# followed (within a clause) by "concurring"/"dissenting".
_SEP_BYLINE = (
    r"(?:MR\.\s+|MRS\.\s+|MS\.\s+)?"
    r"(?:(?:CHIEF\s+)?JUSTICE\s+\S+|"
    r"[A-Z][\w.'’-]*(?:\s+[A-Z][\w.'’-]*){0,3}\s*,\s*"
    r"(?:C\.\s*J\.|JJ?\.|(?:Chief\s+|Senior\s+|Presiding\s+)?"
    r"(?:Circuit\s+|District\s+|Bankruptcy\s+)?Judge))"
    r".{0,200}?\b(?:concurring|dissenting)\b[^.:]*[.:]"
)
# Scholar sometimes tucks a separate opinion's byline onto the end of the
# disposition blockquote instead of giving it its own paragraph, e.g.
#   "Affirmed.  Justice Thomas, concurring."
# Anchored to a standalone disposition sentence and requiring the byline to
# end the block, so it never fires on a mid-sentence parenthetical such as
# "(Kennedy, J., dissenting)".  Group 1 is the byline to split onto its own
# block/line.
_INLINE_SEP_HEADER_RE = re.compile(
    r"(?:^|\.\s+)"                                  # disposition starts block/sentence
    r"(?:it\s+is\s+so\s+ordered"
    r"|(?:the\s+(?:judgments?|orders?)\s+(?:is|are)\s+)?"
    r"(?:affirmed|reversed|vacated|remanded|dismissed|modified|reinstated)"
    r"[^.]{0,60})\.\s+"                             # ...a short disposition sentence
    r"(" + _SEP_BYLINE + r")\s*$",                  # ...then the inlined byline
    re.IGNORECASE | re.DOTALL,
)


def _split_block_at(block: Block, offset: int) -> tuple[Block, Block]:
    """Split *block* into two at character *offset* in its concatenated
    text, dividing the span that straddles the cut and preserving each
    span's formatting."""
    head: list[Span] = []
    tail: list[Span] = []
    pos = 0
    for s in block.spans:
        end = pos + len(s.text)
        if end <= offset:
            head.append(s)
        elif pos >= offset:
            tail.append(s)
        else:
            cut = offset - pos
            head.append(replace(s, text=s.text[:cut]))
            tail.append(replace(s, text=s.text[cut:]))
        pos = end
    return Block(block.kind, head), Block(block.kind, tail)


def _split_inline_sep_headers(blocks: list[Block]) -> list[Block]:
    """Give a separate-opinion byline its own block when Scholar appended it
    to the disposition block (e.g. "Affirmed.  Justice Thomas, concurring.")
    so segmentation — and the rendered opinion — start it on a new line."""
    out: list[Block] = []
    for b in blocks:
        m = _INLINE_SEP_HEADER_RE.search(b.text())
        if (
            m
            and not _SEP_HEADER_RE.match(_content_text(b))  # not already its own header
            and not _NOT_SEP_RE.search(m.group(1))          # not a syllabus disposition
        ):
            head, tail = _split_block_at(b, m.start(1))
            if head.text().strip():
                out.append(head)
                tail.kind = "para"
                out.append(tail)
                continue
        out.append(b)
    return out

# The majority opinion starts at an attribution block such as
# "MR. JUSTICE BLACKMUN delivered the opinion of the Court." or "PER CURIAM."
_MAJ_PHRASE_RE = re.compile(
    r"delivered the opinion|announced the judgment|^\s*PER\s+CURIAM\b",
    re.IGNORECASE,
)
_MAJ_ATTRIB_RE = re.compile(
    r"^\s*(?:MR\.\s+|MRS\.\s+|MS\.\s+)?"
    r"(?:(?:CHIEF\s+)?JUSTICE\s+\S+|"
    r"[A-Z][\w.'’-]+(?:\s+[A-Z][\w.'’-]+){0,3},\s*(?:C\.\s*)?J\.|PER\s+CURIAM)",
    re.IGNORECASE,
)
# Lower-court style author line standing alone: "KOZINSKI, Circuit Judge:"
_AUTHOR_LINE_RE = re.compile(
    r"^\s*[A-Z][\w.'’ -]{0,50},\s*(?:(?:Chief|Senior|Presiding)\s+)?"
    r"(?:(?:Circuit|District|Bankruptcy)\s+)?(?:Judge|Justice|C\.?\s?J\.|J\.)"
    r"\s*[.:;—-]?\s*$"
)
# Caption front matter that belongs in the header even though it isn't
# centered: how the case arrived and counsel listings (in their several
# house styles: "argued the cause" (SCOTUS), "on brief" (4th Cir.),
# "…, Attorney, …, for Plaintiff-Appellant" (7th Cir.)).
_FRONT_MATTER_RE = re.compile(
    r"^(?:APPEALS?\s+FROM|CERTIORARI\s+TO|ON\s+WRITS?\s+OF|ON\s+PETITION|"
    r"ON\s+APPLICATION|ON\s+APPEAL|IN\s+RE\b|No\.\s*\d|Nos\.\s*\d|Syllabus\b|"
    r"Argued\b|Reargued\b|Decided\b|Submitted\b|Filed\b|Released\b|"
    r"Petition\s+for\b)"
    r"|\bre?argued\s+the\s+cause|\bon\s+the\s+briefs?\b|\bbriefs?\s+(?:of|for|was|were)\b"
    r"|,\s*Attorneys?\s*(?:,|at\b)|\bfor\s+(?:the\s+)?(?:appell(?:ant|ee)s?|"
    r"petitioners?|respondents?|plaintiffs?(?:-appell\w+)?|defendants?(?:-appell\w+)?)\b"
    r"|\bamic(?:us|i)\s+curiae\b|\battorneys?\s+general\b|\bof\s+counsel\b",
    re.IGNORECASE,
)
# The judges line ("Before SYKES, Chief Judge, and …").  In opinions with
# no authorship attribution — e.g. orders denying rehearing en banc — it
# opens the body rather than closing the header.
_PANEL_LINE_RE = re.compile(r"^Before\b", re.IGNORECASE)


# Footnote-body blocks start with the same marker the in-text superscript
# shows: "[54] …", "[*] …", or occasionally a bare "* …".
_FN_MARK_RE = re.compile(r"^(?:\[([^\]\s]{1,6})\]|(\*{1,3}|†|‡))(?=\s|$)")
# In-text reference: a superscript span whose whole text is the marker.
_FN_REF_RE = re.compile(r"^\[?([^\[\]\s]{1,6})\]?$")


def _content_text(b: Block) -> str:
    """Block text without page markers, whitespace-normalized — page
    markers can open a block ("*116 MR. JUSTICE BLACKMUN delivered…") and
    would defeat the start-anchored classification patterns."""
    return _WS_RE.sub(" ", "".join(s.text for s in b.spans if not s.pagenum)).strip()


def _split_footnote_run(blocks: list[Block]) -> tuple[list[Block], list[Block]]:
    """
    Split off the trailing footnote section Scholar appends after the last
    opinion.  Returns (content_blocks, footnote_blocks).

    Preferred: the first block opening with a footnote-body anchor
    (name="[N]") starts the section.  Fallback for pages without those
    anchors: the earliest marker-led block from which marker-led blocks
    dominate through to the end.
    """
    for i, b in enumerate(blocks):
        if i and b.spans and b.spans[0].fndef:
            return blocks[:i], blocks[i:]
    starts = [
        i for i, b in enumerate(blocks) if _FN_MARK_RE.match(_content_text(b))
    ]
    for s in starts:
        if s == 0:
            continue  # a document can't be all footnotes
        run = blocks[s:]
        marked = sum(1 for b in run if _FN_MARK_RE.match(_content_text(b)))
        if marked >= max(1, len(run) // 2):
            return blocks[:s], run
    return blocks, []


def _assign_footnotes(parts: list[OpinionPart], run: list[Block]) -> None:
    """
    Attach each footnote body to the part containing its in-text reference.

    Preferred: Scholar's footnote anchors carry a globally unique id
    (reference name="r[N]" ↔ body name="[N]"), so each body joins its part
    exactly.  Fallback for pages without those anchors: each opinion
    restarts numbering at [1], so bodies are matched to references by
    document order — a pointer walks the reference list and each body
    binds to the next not-yet-consumed reference bearing its marker.
    """
    if not parts:
        return
    ref_part: dict[str, int] = {}  # footnote anchor id → part index
    for pi, part in enumerate(parts):
        for b in part.blocks:
            for s in b.spans:
                if s.fnref and s.fnref not in ref_part:
                    ref_part[s.fnref] = pi
    if ref_part and any(b.spans and b.spans[0].fndef for b in run):
        target = len(parts) - 1
        for b in run:
            if b.spans and b.spans[0].fndef:
                target = ref_part.get(b.spans[0].fndef, len(parts) - 1)
            parts[target].footnotes.append(b)
        return

    refs: list[tuple[str, int]] = []  # (marker, part index), in document order
    for pi, part in enumerate(parts):
        for b in part.blocks:
            for s in b.spans:
                if s.sup:
                    m = _FN_REF_RE.match(s.text.strip())
                    if m:
                        refs.append((m.group(1), pi))

    bodies: list[tuple[str, list[Block]]] = []
    for b in run:
        m = _FN_MARK_RE.match(_content_text(b))
        if m:
            bodies.append((m.group(1) or m.group(2), [b]))
        elif bodies:
            bodies[-1][1].append(b)  # continuation paragraph of the previous note
        else:
            parts[-1].footnotes.append(b)

    ptr = 0
    for marker, blks in bodies:
        target = len(parts) - 1
        j = next((k for k in range(ptr, len(refs)) if refs[k][0] == marker), None)
        if j is None:
            j = next((k for k in range(len(refs)) if refs[k][0] == marker), None)
        if j is not None:
            target = refs[j][1]
            ptr = j + 1
        parts[target].footnotes.extend(blks)


def segment_blocks(blocks: list[Block]) -> list[OpinionPart]:
    """
    Split parsed opinion blocks into parts: header (caption/syllabus),
    majority opinion, and each concurrence/dissent.  The trailing footnote
    section is split off and each footnote is attached to the part that
    references it (``OpinionPart.footnotes``).
    """
    if not blocks:
        return []
    blocks, fn_run = _split_footnote_run(blocks)
    if not blocks:
        return []
    blocks = _split_inline_sep_headers(blocks)
    boundaries: list[tuple[int, str, str]] = []  # (block index, kind, label)
    maj_phrase_idx: Optional[int] = None
    maj_author_idx: Optional[int] = None
    for i, b in enumerate(blocks):
        t = _content_text(b)
        if not t:
            continue
        if len(t) <= 300 and _SEP_HEADER_RE.match(t) and not _NOT_SEP_RE.search(t):
            kind = "dissent" if re.search(r"dissent", t, re.IGNORECASE) else "concurrence"
            label = t if len(t) <= 90 else t[:87] + "…"
            boundaries.append((i, kind, label))
            continue
        if not boundaries:
            if _MAJ_PHRASE_RE.search(t[:160]) and _MAJ_ATTRIB_RE.match(t):
                # Keep the LAST candidate: the syllabus disposition line
                # ("BLACKMUN, J., delivered…") precedes the true opinion
                # start ("MR. JUSTICE BLACKMUN delivered…").
                maj_phrase_idx = i
            elif maj_author_idx is None and len(t) <= 120 and _AUTHOR_LINE_RE.match(t):
                maj_author_idx = i
    maj_idx = maj_phrase_idx if maj_phrase_idx is not None else maj_author_idx
    first_sep = boundaries[0][0] if boundaries else len(blocks)

    parts: list[OpinionPart] = []
    if maj_idx is not None and maj_idx < first_sep:
        if maj_idx > 0:
            # SCOTUS-style pages (attribution phrase) carry a syllabus;
            # lower-court pages found via the author line don't.
            header_label = (
                "Header & Syllabus" if maj_phrase_idx is not None else "Header"
            )
            parts.append(OpinionPart(header_label, "header", list(blocks[:maj_idx])))
        parts.append(
            OpinionPart("Majority Opinion", "majority", list(blocks[maj_idx:first_sep]))
        )
    else:
        # No attribution found: the header is the leading centered caption
        # plus any front-matter paragraphs (how the case arrived, counsel
        # listings) that follow it.  The panel line starts the body.
        j = 0
        while j < first_sep:
            b = blocks[j]
            t = _content_text(b)
            if b.kind in ("center", "heading") or not t:
                j += 1
                continue
            if _PANEL_LINE_RE.match(t):
                break
            if len(t) <= 600 and _FRONT_MATTER_RE.search(t):
                j += 1
                continue
            break
        if 0 < j < first_sep:
            parts.append(OpinionPart("Header", "header", list(blocks[:j])))
        if first_sep > j:
            parts.append(
                OpinionPart(
                    "Majority Opinion" if boundaries else "Opinion",
                    "majority",
                    list(blocks[j:first_sep]),
                )
            )
    for k, (idx, kind, label) in enumerate(boundaries):
        end = boundaries[k + 1][0] if k + 1 < len(boundaries) else len(blocks)
        parts.append(OpinionPart(label, kind, list(blocks[idx:end])))
    parts = [p for p in parts if p.blocks]
    if fn_run:
        _assign_footnotes(parts, fn_run)
    return parts


def link_footnotes_by_marker(parts: list[OpinionPart]) -> None:
    """Give footnotes ref↔body links when an opinion uses the plain ``[N]``
    style — a ``<sup>[N]</sup>`` reference and a ``[N]``-led body paragraph —
    rather than Scholar's ``gsl_hash`` anchors.  CourtListener's combined
    opinion is in this style; without anchors ``parse_opinion_blocks`` can't set
    ``fnref``/``fndef``, so the viewer wouldn't make the markers clickable.

    In each part an in-text marker is paired with the body bearing the same
    number, both get a matching synthetic anchor id, and the body's leading
    ``[N]`` is split into its own marker span — so the viewer links them and
    shows hover tips exactly as for a Scholar page.  A no-op where the anchors
    already exist (the gsl_hash case)."""
    for pi, part in enumerate(parts):
        refs: dict[str, Span] = {}  # marker -> in-text reference span
        for b in part.blocks:
            for s in b.spans:
                if s.sup and not s.fnref and not s.fndef:
                    m = _FN_REF_RE.match(s.text.strip())
                    if m:
                        refs.setdefault(m.group(1), s)
        if not refs:
            continue
        out: list[Block] = []
        for fb in part.footnotes:
            idx = next(
                (i for i, s in enumerate(fb.spans)
                 if s.text.strip() and not s.pagenum and not s.fndef),
                None,
            )
            if idx is not None:
                head = fb.spans[idx]
                mm = _FN_MARK_RE.match(head.text)
                marker = (mm.group(1) or mm.group(2)) if mm else None
                if marker and marker in refs and not refs[marker].fndef:
                    fid = f"m{pi}_{marker}"
                    refs[marker].fnref = fid
                    cut = mm.end()
                    new_spans = [replace(head, text=head.text[:cut], fndef=fid)]
                    if head.text[cut:]:
                        new_spans.append(replace(head, text=head.text[cut:]))
                    fb = Block(
                        kind=fb.kind,
                        spans=fb.spans[:idx] + new_spans + fb.spans[idx + 1:],
                    )
            out.append(fb)
        part.footnotes = out


class GoogleScholarFetcher:
    """
    Fetch and cache US case law text from Google Scholar.

    Parameters
    ----------
    cache_path:
        Path to the SQLite cache file (created on first use).
    delay:
        Minimum seconds to wait between HTTP requests.
    db:
        Optional ``opinion_db.OpinionDB`` — the durable, searchable store.
    name_scorer:
        Optional ``(query, candidate_name) -> float`` in [0, 1] used to rank
        the local database's name candidates when Scholar is blocked (the
        offline fallback in :meth:`search_cases`).  Injected so the same
        name-matching used for CourtListener/Scholar results applies here
        without this module depending on the GUI.  ``name_min`` is the score a
        candidate must clear to be kept.
    """

    def __init__(
        self,
        cache_path: Path = _CACHE_PATH,
        delay: float = _DEFAULT_DELAY,
        db=None,
        name_scorer=None,
        name_min: float = 0.5,
    ) -> None:
        self._delay = delay
        self._last_request: float = 0.0
        # Optional opinion_db.OpinionDB: the durable, searchable store.  When
        # set, every fetched opinion is recorded there, and an opinion already
        # present is served from there instead of being re-fetched.
        self._opinion_db = db
        self._name_scorer = name_scorer
        self._name_min = name_min

        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(cache_path), check_same_thread=False)
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS opinions (
                cache_key  TEXT PRIMARY KEY,
                case_url   TEXT,
                text       TEXT,
                fetched_at REAL
            )
            """
        )
        try:
            self._db.execute("ALTER TABLE opinions ADD COLUMN html TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
        self._db.commit()

        # Per-session memory cache for search-results pages (not persisted:
        # rankings change, and re-searching identical queries within a
        # session is the case worth optimizing).
        self._search_cache: dict[str, list[ScholarResult]] = {}

        # The scholar_case URL found on the results page when the opinion page
        # then failed to load (search succeeded, case page didn't) — consumed
        # by take_post_search_failure() so a caller can fall back to
        # CourtListener and retry this exact opinion in the background.
        self._post_search_failure: Optional[str] = None

        # Back up any opinions that were cached before they were stored in the
        # opinion database (e.g. served from the query cache, which short-circuits
        # before storage).  Runs once in the background; add_opinion de-dupes.
        self._backfill_opinion_db()

    def _backfill_opinion_db(self) -> None:
        """Copy every cached opinion (old query cache) into the opinion database
        in the background, so opinions loaded before the database existed — or
        served from the cache without being stored — are backed up.  Incremental
        and cheap on re-runs: a scholar id already in the database is skipped
        without re-parsing its HTML."""
        if self._opinion_db is None:
            return

        def run() -> None:
            try:
                from opinion_db import scholar_id_from_url
                # URLs only first (cheap) — the HTML is fetched on demand below
                # just for the opinions still missing, so the cache's bulk never
                # loads into memory at once.
                urls = [
                    r[0] for r in self._db.execute(
                        "SELECT DISTINCT case_url FROM opinions "
                        "WHERE html IS NOT NULL AND html != ''"
                    ).fetchall()
                ]
            except Exception as exc:
                print(f"[scholar] opinion-DB backfill skipped: {exc}")
                return
            added = 0
            for url in urls:
                sid = scholar_id_from_url(url or "")
                if not sid or self._opinion_db.get_by_scholar_id(sid) is not None:
                    continue  # no id, or already backed up — no HTML parse
                try:
                    row = self._db.execute(
                        "SELECT html FROM opinions WHERE case_url=? "
                        "AND html IS NOT NULL AND html != '' LIMIT 1",
                        (url,),
                    ).fetchone()
                    if row and row[0] and self._opinion_db.add_opinion(url, row[0]):
                        added += 1
                except Exception:
                    continue
            if added:
                print(f"[scholar] backed up {added} cached opinions into the database")

        threading.Thread(target=run, daemon=True).start()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_by_citation(self, citation: str) -> Optional[tuple[str, str]]:
        """
        Fetch opinion HTML by citation string (e.g. "410 U.S. 113").

        Returns (scholar_url, opinion_html) or None if not found / blocked.
        Result is cached permanently on success.
        """
        self._post_search_failure = None
        key = f"cite:{citation.strip()}"
        cached = self._cache_get(key)
        if cached:
            print(f"[scholar] cache hit for {key!r}")
            self._store_opinion(*cached)  # back the cached opinion up in the DB
            return cached

        # Search the database before Google Scholar: a unique opinion bearing
        # this citation is served from there, no network.  (An ambiguous match
        # falls through so Scholar can disambiguate.)
        db_one = self._db_single(citation)
        if db_one:
            return db_one

        # Wrap the citation in double quotes so Scholar treats it as an exact
        # phrase. (repr() here produced *single* quotes, which Scholar does not
        # treat as a phrase operator -- that returned arbitrary cases from the
        # same reporter volume, many of which 404/500 or lack an opinion div.)
        phrase = f'"{citation.strip()}"'
        search_url = (
            f"{SCHOLAR_BASE}/scholar?q={quote_plus(phrase)}&as_sdt=4"
        )
        print(f"[scholar] searching {search_url}")
        try:
            resp = self._get(search_url)
        except Exception as exc:
            print(f"[scholar] search request failed: {exc}")
            return None

        case_url = self._first_case_url(resp.text)
        if not case_url:
            print("[scholar] no scholar_case link found in results page")
            return None

        # Even after searching Scholar, prefer the database copy if we already
        # have this exact opinion (skip re-downloading the case page).
        db_hit = self._db_by_url(case_url)
        if db_hit:
            self._cache_put(key, *db_hit)
            return db_hit

        result = self._fetch_case_page(case_url)
        if result:
            self._cache_put(key, *result)
            return result
        # The search found the case but the opinion page didn't load (Google is
        # flaky) — record it so the caller can show CourtListener now and retry
        # this exact opinion in the background.
        self._post_search_failure = case_url
        return None

    def fetch_by_name(
        self, case_name: str, year: Optional[str] = None
    ) -> Optional[tuple[str, str]]:
        """
        Fetch opinion HTML by case name, optionally scoped to a year.

        Returns (scholar_url, opinion_html) or None.
        """
        self._post_search_failure = None
        q = f"{case_name} {year}".strip() if year else case_name
        key = f"name:{q}"
        cached = self._cache_get(key)
        if cached:
            print(f"[scholar] cache hit for {key!r}")
            self._store_opinion(*cached)  # back the cached opinion up in the DB
            return cached

        # Search the database before Google Scholar (by party name): a unique
        # match is served from there without a network call.
        db_one = self._db_single(case_name)
        if db_one:
            return db_one

        search_url = f"{SCHOLAR_BASE}/scholar?q={quote_plus(q)}&as_sdt=4"
        print(f"[scholar] searching {search_url}")
        try:
            resp = self._get(search_url)
        except Exception as exc:
            print(f"[scholar] search request failed: {exc}")
            return None

        case_url = self._first_case_url(resp.text)
        if not case_url:
            print("[scholar] no scholar_case link found in results page")
            return None

        db_hit = self._db_by_url(case_url)
        if db_hit:
            self._cache_put(key, *db_hit)
            return db_hit

        result = self._fetch_case_page(case_url)
        if result:
            self._cache_put(key, *result)
            return result
        self._post_search_failure = case_url
        return None

    def take_post_search_failure(self) -> Optional[str]:
        """The scholar_case URL found on the results page when the opinion page
        then failed to load (the search succeeded but the case page didn't),
        returned once and cleared.  ``None`` when the last fetch succeeded or
        failed at the search stage.  Lets a caller fall back to CourtListener
        and retry that exact Scholar opinion in the background."""
        url = self._post_search_failure
        self._post_search_failure = None
        return url

    def search_cases(self, query: str, limit: int = 10) -> list["ScholarResult"]:
        """
        Search Scholar case law (all state and federal courts) and return
        parsed results: title, case URL, byline, and snippet.

        Results are cached in memory for the session.  When Google Scholar is
        unreachable or blocking the IP (the request raises, or the results page
        comes back empty), the search falls back to the local opinion database
        so an already-collected corpus still answers the query offline.
        """
        key = query.strip()
        if key in self._search_cache:
            return self._search_cache[key][:limit]
        url = f"{SCHOLAR_BASE}/scholar?q={quote_plus(query)}&as_sdt=2006"
        print(f"[scholar] searching {url}")
        try:
            resp = self._get(url)
            results = self._parse_results(resp.text)
            print(f"[scholar] parsed {len(results)} case results")
        except Exception as exc:
            print(f"[scholar] search request failed: {exc}")
            results = []
        if not results:
            # Scholar gave us nothing (blocked, rate-limited, or a genuine
            # miss): serve candidates from the local opinion database instead.
            # These are deliberately not cached, so a later online search —
            # once the block clears — supersedes them.
            db_results = self._db_search_results(query, limit)
            if db_results:
                print(
                    f"[scholar] Google Scholar returned nothing; serving "
                    f"{len(db_results)} result(s) from the local opinion database"
                )
            return db_results
        self._search_cache[key] = results
        return results[:limit]

    @staticmethod
    def _db_summary_byline(hit: dict) -> str:
        """Build a Scholar-style byline ("410 U.S. 113 - Supreme Court, 1973")
        from an opinion-database summary, so the GUI helpers that parse a
        Scholar result's ``source`` recover the citation, court, and year for a
        database-sourced fallback result.  Only the unambiguous SCOTUS court id
        is mapped to a description; for other courts the byline carries just the
        citation and year, which is all the GUI needs to display the row."""
        cites = hit.get("cites") or ([hit["cite"]] if hit.get("cite") else [])
        court = (hit.get("court") or "").strip().lower()
        year = (hit.get("year") or "").strip()
        court_desc = "Supreme Court" if court == "scotus" else ""
        left = ", ".join(c for c in cites if c)
        right = ", ".join(p for p in (court_desc, year) if p)
        if left and right:
            return f"{left} - {right}"
        return left or right

    @staticmethod
    def _is_name_query(query: str) -> bool:
        """True when ``query`` is a party-name search rather than a Scholar id
        or a reporter citation — the case that benefits from fuzzy name
        ranking.  Mirrors :meth:`OpinionDB.find`'s own dispatch."""
        q = (query or "").strip()
        if not q or q.isdigit():
            return False
        try:
            import citations
            return citations.CITE_CAPTURE_RE.search(q) is None
        except Exception:
            return True

    def _db_name_hits(self, query: str, limit: int) -> Optional[list[dict]]:
        """Database name candidates re-ranked by the injected name matcher, or
        ``None`` when no scorer is set / the query is not a name search (so the
        caller falls back to the exact :meth:`OpinionDB.find` dispatch).

        Casts a wide net with :meth:`OpinionDB.search_names` (any shared party
        token) and keeps those clearing ``self._name_min``, best first — the
        same name-closeness used to rank CourtListener/Scholar results, so a
        near-miss caption still surfaces from the local store."""
        if self._name_scorer is None or not self._is_name_query(query):
            return None
        try:
            cands = self._opinion_db.search_names(query, max(limit * 4, limit))
        except Exception as exc:
            print(f"[scholar] opinion-DB name search failed: {exc}")
            return None
        scored: list[tuple[float, dict]] = []
        for c in cands:
            try:
                s = self._name_scorer(query, c.get("name") or "")
            except Exception:
                s = 0.0
            if s >= self._name_min:
                scored.append((s, c))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _s, c in scored]

    def _db_search_results(self, query: str, limit: int) -> list["ScholarResult"]:
        """Search the local opinion database and adapt its hits into
        ``ScholarResult`` rows (the offline fallback for :meth:`search_cases`).

        Name queries are ranked with the injected name matcher (see
        :meth:`_db_name_hits`); Scholar-id and citation queries use the exact
        dispatch the "Find Opinion in Database" dialog uses.  Each hit's ``url``
        is a real ``scholar_case`` URL, so the caller's open path
        (``fetch_by_url``) serves the opinion straight from the database without
        a network round-trip."""
        if self._opinion_db is None:
            return []
        hits = self._db_name_hits(query, limit)
        if hits is None:
            try:
                hits = self._opinion_db.find(query)
            except Exception as exc:
                print(f"[scholar] opinion-DB search failed: {exc}")
                return []
        out: list[ScholarResult] = []
        for hit in hits[:limit]:
            sid = hit.get("scholar_id") or ""
            url = hit.get("url") or ""
            if not url and sid:
                url = f"{SCHOLAR_BASE}/scholar_case?case={sid}"
            title = hit.get("name") or hit.get("cite") or sid
            out.append(
                ScholarResult(
                    title=title,
                    url=url,
                    source=self._db_summary_byline(hit),
                    snippet="",
                )
            )
        return out

    @staticmethod
    def _parse_results(html: str) -> list["ScholarResult"]:
        """Extract case-law rows from a Scholar results page."""
        soup = BeautifulSoup(html, "html.parser")
        out: list[ScholarResult] = []
        seen: set[str] = set()
        for div in soup.find_all("div", class_="gs_r"):
            h3 = div.find("h3", class_="gs_rt")
            if not h3:
                continue
            a = h3.find("a", href=True)
            if not a or "scholar_case" not in a["href"]:
                continue
            href = a["href"]
            if href.startswith("/"):
                href = SCHOLAR_BASE + href
            if href in seen:
                continue
            seen.add(href)
            title = _WS_RE.sub(" ", a.get_text()).strip()
            gs_a = div.find("div", class_="gs_a")
            source = _WS_RE.sub(" ", gs_a.get_text()).strip() if gs_a else ""
            rs = div.find("div", class_="gs_rs")
            snippet = _WS_RE.sub(" ", rs.get_text()).strip() if rs else ""
            out.append(ScholarResult(title=title, url=href, source=source, snippet=snippet))
        if not out:
            # Markup changed?  Fall back to bare scholar_case anchors.
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "scholar_case" not in href:
                    continue
                if href.startswith("/"):
                    href = SCHOLAR_BASE + href
                if href in seen:
                    continue
                seen.add(href)
                title = _WS_RE.sub(" ", a.get_text()).strip()
                if title:
                    out.append(ScholarResult(title=title, url=href))
        return out

    def get_cached(self, key: str) -> Optional[tuple[str, str]]:
        """Look up an arbitrary cache key; returns (url, opinion_html) or None."""
        return self._cache_get(key)

    def put_cached(self, key: str, url: str, html: str) -> None:
        """Store (url, opinion_html) under an arbitrary cache key."""
        self._cache_put(key, url, html)

    def fetch_by_url(self, url: str) -> Optional[tuple[str, str]]:
        """
        Fetch a scholar_case page directly by URL — e.g. a citation link
        embedded in another opinion's text.

        Returns (scholar_url, opinion_html) or None.
        """
        db_hit = self._db_by_url(url)
        if db_hit:
            return db_hit

        key = f"url:{url}"
        cached = self._cache_get(key)
        if cached:
            print(f"[scholar] cache hit for {key!r}")
            self._store_opinion(*cached)  # back the cached opinion up in the DB
            return cached

        result = self._fetch_case_page(url)
        if result:
            self._cache_put(key, *result)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Opinion database (durable, searchable store; optional)
    # ------------------------------------------------------------------

    def _db_by_url(self, url: str) -> Optional[tuple[str, str]]:
        """(url, html) for this case from the opinion DB (matched on the Scholar
        ``case=`` id), or None — so an opinion already collected is served from
        the DB instead of being fetched again."""
        if self._opinion_db is None:
            return None
        try:
            rec = self._opinion_db.get_by_url(url)
            if rec and rec.get("html"):
                print(f"[scholar] opinion-DB hit for {rec.get('scholar_id')}")
                return (rec.get("url") or url, rec["html"])
        except Exception as exc:
            print(f"[scholar] opinion-DB lookup failed: {exc}")
        return None

    def _db_single(self, query: str) -> Optional[tuple[str, str]]:
        """(url, html) when the opinion DB holds **exactly one** opinion for
        this query (a citation or a party name), else None.  A unique hit is
        served straight from the DB without touching Scholar; an ambiguous one
        (two cases sharing a name or a reporter page) falls through so Scholar
        can disambiguate."""
        if self._opinion_db is None or not query:
            return None
        try:
            hits = self._opinion_db.find(query)
            if len(hits) == 1:
                rec = self._opinion_db.get_by_scholar_id(hits[0]["scholar_id"])
                if rec and rec.get("html"):
                    print(f"[scholar] opinion-DB unique hit for {query!r}")
                    return (rec.get("url") or "", rec["html"])
        except Exception as exc:
            print(f"[scholar] opinion-DB search failed: {exc}")
        return None

    def _store_opinion(self, url: str, html: str) -> None:
        if self._opinion_db is None:
            return
        try:
            if self._opinion_db.add_opinion(url, html):
                print("[scholar] stored opinion in database")
        except Exception as exc:
            print(f"[scholar] opinion-DB store failed: {exc}")

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)
        self._last_request = time.monotonic()

    def _get(self, url: str) -> requests.Response:
        self._throttle()
        resp = self._session.get(url, timeout=20)
        resp.raise_for_status()
        return resp

    def _first_case_url(self, html: str) -> Optional[str]:
        """Return the first scholar_case href found in a Scholar results page."""
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            if "scholar_case" in href:
                if href.startswith("/"):
                    href = SCHOLAR_BASE + href
                print(f"[scholar] found case url: {href}")
                return href
        return None

    def _fetch_case_page(self, url: str) -> Optional[tuple[str, str]]:
        """Fetch a scholar_case page and extract the opinion HTML."""
        print(f"[scholar] fetching case page {url}")
        try:
            resp = self._get(url)
        except Exception as exc:
            print(f"[scholar] case page request failed: {exc}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        # Primary location Google uses for the opinion body
        opinion_div = soup.find(id="gs_opinion") or soup.find(
            "div", class_="gs_opinion"
        )
        if not opinion_div:
            print("[scholar] #gs_opinion div not found on page")
            return None

        html = str(opinion_div)
        print(f"[scholar] extracted {len(html):,} chars of opinion HTML")
        self._store_opinion(url, html)
        return (url, html)

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def _cache_get(self, key: str) -> Optional[tuple[str, str]]:
        row = self._db.execute(
            "SELECT case_url, html FROM opinions "
            "WHERE cache_key=? AND html IS NOT NULL AND html != ''",
            (key,),
        ).fetchone()
        return row  # (url, html) or None

    def _cache_put(self, key: str, url: str, html: str) -> None:
        text = blocks_to_text(parse_opinion_blocks(html))
        self._db.execute(
            "INSERT OR REPLACE INTO opinions "
            "(cache_key, case_url, text, html, fetched_at) VALUES (?, ?, ?, ?, ?)",
            (key, url, text, html, time.time()),
        )
        self._db.commit()


if __name__ == "__main__":  # pragma: no cover - offline smoke test
    import sys

    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        if not cond:
            failures.append(msg)
        print(("ok   " if cond else "FAIL ") + msg)

    # CourtListener marks reporter page breaks with
    # <span class="star-pagination">*N</span>, sometimes twice in a row.
    # parse_opinion_blocks should turn those into page markers (de-duped) so a
    # combined CourtListener opinion reads like a Google Scholar one.
    cl_html = (
        "<div><center>505 U.S. 1003</center>"
        '<p><span class="star-pagination">*1005</span> '
        '<span class="star-pagination">*1005</span> '
        "Scalia, J., delivered the opinion of the Court.</p>"
        '<p>Coastal land at issue, <span class="star-pagination">*1007</span> '
        "in South Carolina.</p></div>"
    )
    blocks = parse_opinion_blocks(cl_html)
    pages = [s.text for b in blocks for s in b.spans if s.pagenum]
    check(pages == ["*1005", "*1007"],
          f"star-pagination -> page markers, de-duped: {pages}")
    # The markers are flagged pagenum (rendered into the gutter); the body
    # prose — the spans the main text flow uses — is intact without them.
    prose = "".join(
        s.text for b in blocks for s in b.spans if not s.pagenum
    )
    check("*1005" not in prose and "*1007" not in prose,
          "page markers are page spans, separate from the prose flow")
    check("Scalia, J., delivered the opinion of the Court." in prose,
          "surrounding prose preserved")

    # Scholar's own gsl_pagenum markers must still work (no regression).
    sch_html = (
        '<div id="gs_opinion"><p>Before '
        '<a class="gsl_pagenum2" href="#">*152</a> after.</p></div>'
    )
    spages = [s.text for b in parse_opinion_blocks(sch_html)
              for s in b.spans if s.pagenum]
    check(spages == ["*152"], f"Scholar gsl_pagenum still works: {spages}")

    # link_footnotes_by_marker: a <sup>[N]</sup> reference and a [N]-led body
    # (CourtListener's combined-opinion style) get matching anchor ids so the
    # viewer links them like a Scholar page.
    fn_html = (
        "<div><center>1 U.S. 1</center>"
        "<p>Smith, J., delivered the opinion of the Court.</p>"
        "<p>Some reasoning<sup>[1]</sup> and more<sup>[2]</sup>.</p>"
        "<p>[1] First note.</p><p>[2] Second note, longer.</p></div>"
    )
    fparts = segment_blocks(parse_opinion_blocks(fn_html))
    link_footnotes_by_marker(fparts)
    fnrefs = sorted(s.fnref for p in fparts for b in p.blocks
                    for s in b.spans if s.fnref)
    fndefs = sorted(s.fndef for p in fparts for fb in p.footnotes
                    for s in fb.spans if s.fndef)
    check(len(fnrefs) == 2 and fnrefs == fndefs,
          f"[N] footnotes linked: refs={fnrefs} defs={fndefs}")
    # the body marker is split into its own "[N]" span (so only it is clickable)
    marker_spans = [fb.spans[0].text for p in fparts for fb in p.footnotes
                    if fb.spans and fb.spans[0].fndef]
    check(all(re.fullmatch(r"\[.+\]", t) for t in marker_spans) and marker_spans,
          f"body marker split to its own span: {marker_spans}")
    # idempotent: a second pass doesn't double-link or change ids
    link_footnotes_by_marker(fparts)
    fnrefs2 = sorted(s.fnref for p in fparts for b in p.blocks
                     for s in b.spans if s.fnref)
    check(fnrefs2 == fnrefs, "link_footnotes_by_marker is idempotent")

    # search_cases falls back to the local opinion database when Google Scholar
    # is unreachable/blocking (the request raises or the page parses empty).  A
    # tiny stub stands in for opinion_db.OpinionDB.find() so this stays offline.
    import tempfile as _tempfile

    def _toks(s):  # crude party tokeniser for the stub
        return {w for w in re.findall(r"[a-z0-9]+", (s or "").lower())
                if len(w) > 1 and w not in {"the", "of", "and", "v", "vs"}}

    class _StubDB:
        """Minimal stand-in for opinion_db.OpinionDB exercising both retrieval
        paths: exact ``find`` (id/citation) and lenient ``search_names``."""

        def __init__(self, hits):
            self._hits = hits

        def find(self, query):
            q = (query or "").lower()
            return [h for h in self._hits
                    if q in h["name"].lower() or q in (h.get("cite") or "").lower()]

        def search_names(self, query, limit=40):
            qt = _toks(query)
            scored = [(len(qt & _toks(h["name"])), h) for h in self._hits]
            scored = [(n, h) for n, h in scored if n]
            scored.sort(key=lambda x: x[0], reverse=True)
            return [h for _n, h in scored][:limit]

    def _stub_scorer(query, name):  # token-overlap stand-in for _name_match_score
        qt, nt = _toks(query), _toks(name)
        return (len(qt & nt) / len(qt)) if qt else 0.0

    roe_hit = {
        "scholar_id": "12345678901234567890",
        "name": "Roe v. Wade",
        "cite": "410 U.S. 113",
        "cites": ["410 U.S. 113", "93 S. Ct. 705"],
        "court": "scotus",
        "year": "1973",
        "url": "https://scholar.google.com/scholar_case?case=12345678901234567890",
    }
    # Stored under the Bluebook-abbreviated caption; a query spelling the word
    # out in full must still match via the injected fuzzy scorer.
    brown_hit = {
        "scholar_id": "98765432109876543210",
        "name": "Brown v. Board of Ed.",
        "cite": "347 U.S. 483",
        "cites": ["347 U.S. 483"],
        "court": "scotus",
        "year": "1954",
        "url": "https://scholar.google.com/scholar_case?case=98765432109876543210",
    }
    with _tempfile.TemporaryDirectory() as _d:
        f = GoogleScholarFetcher(
            cache_path=Path(_d) / "cache.db", delay=0.0,
            db=_StubDB([roe_hit, brown_hit]),
            name_scorer=_stub_scorer, name_min=0.5,
        )

        def _boom(url):  # simulate Google blocking the IP
            raise ScholarError("403 blocked")

        f._get = _boom  # type: ignore[assignment]
        hits = f.search_cases("Roe v. Wade", limit=10)
        check(len(hits) == 1 and hits[0].title == "Roe v. Wade",
              f"blocked search falls back to opinion DB: {[h.title for h in hits]}")
        check(bool(hits) and "case=12345678901234567890" in hits[0].url,
              "fallback result keeps the scholar_case URL")
        check(bool(hits) and hits[0].source.startswith("410 U.S. 113")
              and "Supreme Court" in hits[0].source and "1973" in hits[0].source,
              f"fallback byline parses (cite/court/year): {hits[0].source!r}")

        # Fuzzy name match: "...Board of Education" finds stored "Board of Ed.",
        # which the DB's all-tokens find_by_party would miss.
        bhits = f.search_cases("Brown v. Board of Education", limit=10)
        check(len(bhits) == 1 and bhits[0].title.startswith("Brown"),
              f"fuzzy name match via injected scorer: {[h.title for h in bhits]}")

        # A citation query still routes through the exact dispatch.
        chits = f.search_cases("347 U.S. 483", limit=10)
        check(len(chits) == 1 and chits[0].title.startswith("Brown"),
              f"citation query uses exact dispatch: {[h.title for h in chits]}")

        check(f.search_cases("nonesuch matter", limit=10) == [],
              "blocked search with no DB match returns empty")

        # With no database attached, a blocked search degrades to empty cleanly.
        f2 = GoogleScholarFetcher(cache_path=Path(_d) / "c2.db", delay=0.0, db=None)
        f2._get = _boom  # type: ignore[assignment]
        check(f2.search_cases("Roe v. Wade") == [],
              "blocked search with no DB returns empty")

    if failures:
        print(f"\n{len(failures)} FAILED")
        sys.exit(1)
    print("\nOK: google_scholar smoke test passed")
