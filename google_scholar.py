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
import time
from dataclasses import dataclass, field
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


class GoogleScholarFetcher:
    """
    Fetch and cache US case law text from Google Scholar.

    Parameters
    ----------
    cache_path:
        Path to the SQLite cache file (created on first use).
    delay:
        Minimum seconds to wait between HTTP requests.
    """

    def __init__(
        self,
        cache_path: Path = _CACHE_PATH,
        delay: float = _DEFAULT_DELAY,
    ) -> None:
        self._delay = delay
        self._last_request: float = 0.0

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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_by_citation(self, citation: str) -> Optional[tuple[str, str]]:
        """
        Fetch opinion HTML by citation string (e.g. "410 U.S. 113").

        Returns (scholar_url, opinion_html) or None if not found / blocked.
        Result is cached permanently on success.
        """
        key = f"cite:{citation.strip()}"
        cached = self._cache_get(key)
        if cached:
            print(f"[scholar] cache hit for {key!r}")
            return cached

        search_url = (
            f"{SCHOLAR_BASE}/scholar?q={quote_plus(repr(citation))}&as_sdt=4"
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

        result = self._fetch_case_page(case_url)
        if result:
            self._cache_put(key, *result)
        return result

    def fetch_by_name(
        self, case_name: str, year: Optional[str] = None
    ) -> Optional[tuple[str, str]]:
        """
        Fetch opinion HTML by case name, optionally scoped to a year.

        Returns (scholar_url, opinion_html) or None.
        """
        q = f"{case_name} {year}".strip() if year else case_name
        key = f"name:{q}"
        cached = self._cache_get(key)
        if cached:
            print(f"[scholar] cache hit for {key!r}")
            return cached

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

        result = self._fetch_case_page(case_url)
        if result:
            self._cache_put(key, *result)
        return result

    def search_cases(self, query: str, limit: int = 10) -> list["ScholarResult"]:
        """
        Search Scholar case law (all state and federal courts) and return
        parsed results: title, case URL, byline, and snippet.

        Results are cached in memory for the session.
        """
        key = query.strip()
        if key in self._search_cache:
            return self._search_cache[key][:limit]
        url = f"{SCHOLAR_BASE}/scholar?q={quote_plus(query)}&as_sdt=2006"
        print(f"[scholar] searching {url}")
        try:
            resp = self._get(url)
        except Exception as exc:
            print(f"[scholar] search request failed: {exc}")
            return []
        results = self._parse_results(resp.text)
        print(f"[scholar] parsed {len(results)} case results")
        self._search_cache[key] = results
        return results[:limit]

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
        key = f"url:{url}"
        cached = self._cache_get(key)
        if cached:
            print(f"[scholar] cache hit for {key!r}")
            return cached

        result = self._fetch_case_page(url)
        if result:
            self._cache_put(key, *result)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
