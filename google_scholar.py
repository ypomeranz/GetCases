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

Fetching:
    Google Scholar fingerprints the TLS handshake (JA3) and header order of
    its callers and challenges ones it doesn't trust with a "/sorry" CAPTCHA
    page.  Counter-intuitively it is curl_cffi's *newest* Chrome profiles that
    get challenged (chrome133a and up as of curl_cffi 0.15), while slightly
    older but still-current Chrome builds and the Edge/Firefox/Safari profiles
    pass — so we impersonate a proven target rather than the bleeding edge, and
    rotate to another fingerprint on the same session if Scholar challenges the
    current one.  This is a fingerprint block, not an IP ban: the same network
    loads Scholar fine in a real browser.  ``requests`` remains a fallback when
    ``curl_cffi`` is unavailable (and is almost always blocked).

Requires:
    pip install beautifulsoup4
    pip install curl_cffi   # strongly recommended; avoids Scholar bot blocks
    pip install requests    # fallback fetcher if curl_cffi is absent
    pip install browser-cookie3   # optional; reuse your browser's Google cookies
                                  # so Scholar throttles far less (see cookies_from)
"""

from __future__ import annotations

import random
import re
import sqlite3
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

try:
    from bs4 import BeautifulSoup, Comment, NavigableString, Tag
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "google_scholar requires 'beautifulsoup4'.\n"
        "Install with: pip install beautifulsoup4"
    ) from exc

# Preferred fetcher: curl_cffi impersonates a real browser's TLS/JA3
# fingerprint and header order, which is what gets past Scholar's bot
# detection.  Plain requests is kept as a fallback for environments where
# curl_cffi isn't installed.
try:
    from curl_cffi import requests as _curl_requests
except ImportError:  # pragma: no cover
    _curl_requests = None

try:
    import requests as _std_requests
except ImportError:  # pragma: no cover
    _std_requests = None

if _curl_requests is None and _std_requests is None:  # pragma: no cover
    raise ImportError(
        "google_scholar requires an HTTP client: 'curl_cffi' (recommended, "
        "evades Scholar bot blocks) or 'requests'.\n"
        "Install with: pip install curl_cffi   (or: pip install requests)"
    )


SCHOLAR_BASE = "https://scholar.google.com"

# Headers we add on top of whatever the fetcher supplies.  When impersonating
# with curl_cffi, the browser profile already sets a complete, self-consistent
# header set (User-Agent, Accept, sec-ch-ua, ordering, …); we leave those
# alone and only nudge the language.  For the plain-requests fallback there is
# no browser profile, so a realistic UA is filled in by ``_browser_headers``.
_HEADERS = {
    "Accept-Language": "en-US,en;q=0.5",
}

# Fallback header set for plain requests (no TLS impersonation): a realistic
# browser UA still avoids the most trivial blocks even if it can't beat JA3
# fingerprinting.
_FALLBACK_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Impersonation targets to try, best-first.  We deliberately AVOID curl_cffi's
# newest Chrome profiles: Scholar's search endpoint challenges curl_cffi's
# bleeding-edge Chrome JA3 (chrome133a and up as of curl_cffi 0.15) with its
# "/sorry" CAPTCHA, while quietly accepting slightly older, proven Chrome builds
# and the Edge/Firefox/Safari families.  (Verified empirically: chrome131/124/
# 120/116, edge101, firefox144 and safari180 all load Scholar search 200; the
# generic "chrome" alias — which maps to the newest — gets a 429.)  Listing
# several across browser families lets the fetcher rotate to a fresh fingerprint
# when one is challenged, which — not the IP — is what Scholar actually blocks.
_PREFERRED_IMPERSONATIONS = (
    "chrome131", "chrome124", "chrome120", "chrome116",
    "edge101",
    "firefox144", "firefox135",
    "safari180", "safari170",
)

_IMP_CANDIDATES_CACHE: Optional[list[str]] = None


def _impersonation_candidates() -> list[str]:
    """Ordered curl_cffi impersonation targets to try, best-first.

    Returns those of ``_PREFERRED_IMPERSONATIONS`` the installed curl_cffi
    supports, then family generics as deeper fallbacks.  If somehow none match
    (a much newer/older curl_cffi), it computes a recent-but-not-bleeding-edge
    Chrome from whatever the library ships, since the newest profiles are the
    ones Scholar challenges.
    """
    global _IMP_CANDIDATES_CACHE
    if _IMP_CANDIDATES_CACHE is not None:
        return _IMP_CANDIDATES_CACHE
    available: set[str] = set()
    try:
        import typing
        from curl_cffi.requests.impersonate import BrowserTypeLiteral
        available = set(typing.get_args(BrowserTypeLiteral))
    except Exception:
        pass

    if not available:
        cands = list(_PREFERRED_IMPERSONATIONS)
    else:
        cands = [t for t in _PREFERRED_IMPERSONATIONS if t in available]
        if not cands:
            # Newest available Chrome, minus the newest few Scholar challenges.
            chromes = sorted(
                (int(m.group(1)), t)
                for t in available
                for m in [re.fullmatch(r"chrome(\d+)", t)]
                if m
            )
            if len(chromes) > 3:
                cands = [chromes[-4][1]]
            elif chromes:
                cands = [chromes[-1][1]]
        for g in ("edge", "firefox", "safari"):  # family generics, last resort
            if g in available and g not in cands:
                cands.append(g)
    if not cands:
        cands = ["chrome"]
    _IMP_CANDIDATES_CACHE = cands
    return cands


_DEFAULT_DELAY = 3.0  # seconds between outbound requests

# Rate-limit handling.  Scholar answers 429 (Too Many Requests) -- and
# occasionally 500/503 -- when it thinks a client is asking too fast; this
# happens even to a browser-impersonating fetcher when many opinions/links are
# pulled in quick succession.  We retry such responses with exponential backoff
# (honoring any Retry-After header) instead of failing the fetch outright.  The
# 500 is transient under load (a fresh retry usually renders the page); a 429
# carrying the "/sorry" CAPTCHA is handled separately by fingerprint rotation.
_RETRY_STATUSES = frozenset({429, 500, 503})
_MAX_RETRIES = 4          # attempts after the first try
_BACKOFF_BASE = 5.0       # seconds; doubles each retry (5, 10, 20, 40 …)
_BACKOFF_MAX = 90.0       # cap on a single backoff wait
# After a 429 we also permanently slow the session's steady-state pace so the
# rest of a link-following run doesn't keep tripping the limit.
_DELAY_GROWTH = 1.5
_MAX_DELAY = 30.0

# A "/sorry/index" CAPTCHA ("Our systems have detected unusual traffic") is an
# IP-level block of Scholar's *search* endpoint, not a transient 429: retrying
# in-session never clears it and each extra hit only deepens the block.  When we
# hit one we fail fast and stop touching Scholar for this long, so a
# link-following run doesn't grind through dozens of CAPTCHAs.
_BLOCK_COOLDOWN = 900.0   # seconds (15 min)

# Scholar intermittently serves a JS "Loading…" shell (page chrome with an
# absent/near-empty #gs_opinion) instead of the server-rendered opinion, mostly
# when soft-throttling.  We re-fetch a few times until the opinion materializes.
_SHELL_RETRIES = 3

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


# ---------------------------------------------------------------------------
# Browser cookie reuse
# ---------------------------------------------------------------------------
# Loading the user's real ``google.com`` cookies into the scraper makes Scholar
# treat it like that signed-in browser, which sharply cuts the "unusual
# traffic"/throttle (Loading… shell) responses.  Platform caveat: on Windows
# Chrome and Edge encrypt their cookies (DPAPI + AES-GCM, and Chrome/Edge 127+
# add App-Bound Encryption that needs *admin* to decrypt), whereas Firefox keeps
# them in plaintext SQLite.  So we try Chrome first and fall back to Firefox
# (then Edge) -- exactly the "use Chrome unless it's locked, else Firefox" flow.
# We read only google.com cookies (not the whole store), and it's purely
# best-effort: any failure just proceeds cookieless.
_COOKIE_BROWSER_ORDER = ("chrome", "firefox", "edge")

# Cookies that indicate a *signed-in* Google session (vs. a bare visitor).
_SIGNED_IN_COOKIES = frozenset({
    "SID", "HSID", "SSID", "APISID", "SAPISID", "LSID",
    "__Secure-1PSID", "__Secure-3PSID", "__Secure-1PAPISID", "__Secure-3PAPISID",
})


def _resolve_cookie_order(cookies_from: Optional[str]) -> tuple[str, ...]:
    """Turn the ``cookies_from`` setting into an ordered list of browsers.

    ``"auto"`` -> the default order; ``None``/``""``/``"off"``/``"none"`` ->
    disabled; otherwise a single name or comma-separated list (e.g. ``"firefox"``
    or ``"chrome,firefox"``).
    """
    s = (cookies_from or "").strip().lower()
    if not s or s in ("off", "none", "false", "0", "disable", "disabled"):
        return ()
    if s == "auto":
        return _COOKIE_BROWSER_ORDER
    return tuple(p.strip() for p in s.split(",") if p.strip())


def load_google_cookies(order: tuple[str, ...] = _COOKIE_BROWSER_ORDER):
    """Load ``google.com`` cookies from the user's browser(s), best-first.

    Returns ``(cookiejar, browser_name, note)``: the first browser whose cookies
    we can actually read wins.  On total failure ``cookiejar`` is None and
    ``note`` explains why (library missing, every browser locked behind
    admin-only App-Bound Encryption, none signed in, etc.).
    """
    try:
        import browser_cookie3 as bc3
    except ImportError:
        return None, "", "browser_cookie3 not installed (pip install browser-cookie3)"

    problems: list[str] = []
    for name in order:
        loader = getattr(bc3, name, None)
        if loader is None:
            problems.append(f"{name}: unsupported")
            continue
        try:
            jar = loader(domain_name="google.com")
        except Exception as exc:
            # e.g. RequiresAdminError (Chrome/Edge App-Bound Encryption) or
            # BrowserCookieError (browser not installed / no profile).
            first = (str(exc).splitlines() or [""])[0].strip()
            problems.append(f"{name}: {type(exc).__name__}{(': ' + first[:70]) if first else ''}")
            continue
        if any(c.value for c in jar):
            return jar, name, ""
        problems.append(f"{name}: no readable google.com cookies (signed in?)")
    return None, "", "; ".join(problems) or "no supported browser found"


class GoogleScholarFetcher:
    """
    Fetch and cache US case law text from Google Scholar.

    Parameters
    ----------
    cache_path:
        Path to the SQLite cache file (created on first use).
    delay:
        Minimum seconds to wait between HTTP requests.
    cookies_from:
        Where to borrow google.com cookies so Scholar treats the fetcher like
        your signed-in browser (far fewer throttle/CAPTCHA responses).
        ``"auto"`` (default) tries Chrome, then Firefox, then Edge; pass a name
        (``"firefox"``), a comma-separated list, or ``None`` to disable.  On
        Windows, Chrome/Edge need admin to decrypt (App-Bound Encryption) while
        Firefox cookies are plaintext and work without admin.
    """

    def __init__(
        self,
        cache_path: Path = _CACHE_PATH,
        delay: float = _DEFAULT_DELAY,
        cookies_from: Optional[str] = "auto",
    ) -> None:
        self._delay = delay
        self._last_request: float = 0.0
        self._warmed = False         # homepage cookie warm-up done this session?
        self._blocked_until = 0.0    # monotonic time before which Scholar is blocked

        # Prefer curl_cffi (browser TLS impersonation) over plain requests; see
        # module docstring.  ``_impersonate`` is the current curl_cffi target;
        # ``_imp_candidates`` are fallbacks we rotate through when Scholar
        # challenges a fingerprint.  None when falling back to plain requests.
        if _curl_requests is not None:
            self._imp_candidates: list[str] = _impersonation_candidates()
            self._imp_index = 0
            self._impersonate: Optional[str] = self._imp_candidates[0]
            self._session = _curl_requests.Session()
            self._session.headers.update(_HEADERS)
            print(
                f"[scholar] using curl_cffi, impersonating {self._impersonate} "
                f"(fallbacks: {', '.join(self._imp_candidates[1:]) or 'none'})"
            )
        else:
            self._impersonate = None
            self._imp_candidates = []
            self._imp_index = 0
            self._session = _std_requests.Session()
            self._session.headers.update(_FALLBACK_HEADERS)
            print(
                "[scholar] curl_cffi not installed; using plain requests "
                "(more likely to be blocked by Scholar -- "
                "`pip install curl_cffi` to fix)"
            )

        # Reuse the user's real browser cookies so Scholar treats us like their
        # signed-in browser (far fewer throttle/CAPTCHA responses).  Opt out with
        # cookies_from=None; force one with cookies_from="firefox".
        cookie_order = _resolve_cookie_order(cookies_from)
        if cookie_order:
            self._install_browser_cookies(cookie_order)

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
            html = self._get_results_html(search_url)
        except Exception as exc:
            print(f"[scholar] search request failed: {exc}")
            return None

        case_url = self._first_case_url(html)
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
            html = self._get_results_html(search_url)
        except Exception as exc:
            print(f"[scholar] search request failed: {exc}")
            return None

        case_url = self._first_case_url(html)
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
            html = self._get_results_html(url)
        except Exception as exc:
            print(f"[scholar] search request failed: {exc}")
            return []
        results = self._parse_results(html)
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
            # Markup changed?  Fall back to bare scholar_case anchors -- but
            # only opinion links (?case=<id>), never the "How cited"
            # (?about=<id>) citation-analysis links Scholar now interleaves.
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "scholar_case" not in href or "case=" not in href:
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

    @staticmethod
    def _retry_after_seconds(resp) -> Optional[float]:
        """Seconds to wait per the response's Retry-After header, or None.

        Handles both forms the header may take: a number of seconds, or an
        HTTP date.  Scholar usually omits it on a 429, but we honor it when
        present rather than guessing.
        """
        val = (resp.headers.get("Retry-After") or "").strip()
        if not val:
            return None
        if val.isdigit():
            return float(val)
        try:
            from datetime import datetime, timezone
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(val)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
        except Exception:
            pass
        return None

    @staticmethod
    def _is_block_page(resp) -> bool:
        """True if *resp* is Google's "/sorry/index" CAPTCHA interstitial.

        Scholar guards its *search* endpoint with an "unusual traffic" CAPTCHA
        served from ``www.google.com/sorry/index``; curl_cffi follows the
        redirect, so it surfaces as a 429 whose final URL is ``/sorry/`` and
        whose body mentions unusual traffic / a reCAPTCHA.  Crucially this is a
        *fingerprint* challenge, not an IP ban -- the same IP loads the page
        fine under a real browser or a different impersonation target.
        """
        if "/sorry/" in (getattr(resp, "url", "") or ""):
            return True
        if getattr(resp, "status_code", None) in _RETRY_STATUSES:
            low = (resp.text or "").lower()
            return (
                "unusual traffic" in low
                or "/sorry/index" in low
                or "g-recaptcha" in low
                or 'id="captcha"' in low
            )
        return False

    def _install_browser_cookies(self, order: tuple[str, ...]) -> None:
        """Best-effort: copy the user's real google.com cookies into the session.

        Mimics the signed-in browser so Scholar throttles far less.  When cookies
        load we also skip the homepage warm-up (we already have a cookie set)."""
        jar, source, note = load_google_cookies(order)
        if jar is None:
            print(
                f"[scholar] no browser cookies loaded ({note}); proceeding "
                "without. Tip: sign into Google Scholar in Firefox (plaintext "
                "cookies, no admin needed), or run as admin to read Chrome."
            )
            return
        n = 0
        signed_in = False
        for c in jar:
            if not c.value:
                continue
            try:
                self._session.cookies.set(
                    c.name, c.value,
                    domain=c.domain or ".google.com", path=c.path or "/",
                )
            except TypeError:                       # session jar w/o domain/path kwargs
                try:
                    self._session.cookies.set(c.name, c.value)
                except Exception:
                    continue
            except Exception:
                continue
            n += 1
            signed_in = signed_in or c.name in _SIGNED_IN_COOKIES
        if n:
            self._warmed = True   # have real cookies; homepage warm-up unneeded
            print(
                f"[scholar] loaded {n} google.com cookies from {source}"
                f"{' (signed-in session)' if signed_in else ' (visitor session)'}"
            )
        else:
            print(f"[scholar] {source} cookies present but none could be applied")

    def _warm_up(self) -> None:
        """Visit the Scholar homepage once per session to pick up Google's
        cookies (NID/GSP) before hitting the search endpoint -- a cold request
        with no cookies looks more bot-like.  Cheap hygiene; best-effort."""
        if self._warmed or self._impersonate is None:
            self._warmed = True
            return
        self._warmed = True
        try:
            self._throttle()
            self._raw_get(SCHOLAR_BASE + "/")
        except Exception:
            pass

    def _rotate_impersonation(self) -> bool:
        """Advance to the next impersonation fingerprint after a block.

        Returns True if a fresh target was selected, False once exhausted.
        Switching the JA3/header profile on the *same* session (cookies intact)
        is what actually clears Scholar's fingerprint block: a target Scholar
        trusts loads the page the challenged one couldn't.
        """
        if self._impersonate is None:
            return False
        if self._imp_index + 1 >= len(self._imp_candidates):
            return False
        self._imp_index += 1
        self._impersonate = self._imp_candidates[self._imp_index]
        print(
            f"[scholar] fingerprint challenged; rotating impersonation -> "
            f"{self._impersonate}"
        )
        return True

    def _raw_get(self, url: str):
        if self._impersonate is not None:
            # Re-assert the impersonation on every request: it drives both the
            # TLS fingerprint and the browser-matching header set.
            return self._session.get(url, timeout=20, impersonate=self._impersonate)
        return self._session.get(url, timeout=20)

    def _get(self, url: str):
        """GET *url*, handling Scholar's two distinct anti-bot responses.

        * A 429 redirecting to ``/sorry/index`` is a *fingerprint* challenge,
          not an IP ban: Scholar distrusts curl_cffi's newest Chrome JA3.  We
          rotate to the next impersonation target (older Chrome, Edge, Firefox,
          Safari) on the same session and retry immediately -- a fresh
          fingerprint loads the page the old one couldn't, with no waiting.
          Only when every fingerprint is exhausted do we cool down and fail.
        * A bare 429/503 (no CAPTCHA) is a genuine transient rate limit; we wait
          (Retry-After or jittered exponential backoff) and retry, easing the
          steady-state pace so the rest of a run is gentler.
        """
        if time.monotonic() < self._blocked_until:
            raise ScholarError(
                "Google Scholar challenged every available browser fingerprint "
                "moments ago; pausing Scholar briefly. Retry shortly, switch "
                "networks, or rely on the CourtListener results (which don't go "
                "through Scholar)."
            )
        self._warm_up()
        backoff_attempt = 0
        while True:
            self._throttle()
            resp = self._raw_get(url)
            if self._is_block_page(resp):
                if self._rotate_impersonation():
                    continue  # retry now with a fresh fingerprint, no backoff
                # Every fingerprint challenged: unusual -- may be a real
                # IP-level throttle.  Cool down so a link-following run stops
                # hammering /sorry, and fail fast with guidance.
                self._blocked_until = time.monotonic() + _BLOCK_COOLDOWN
                raise ScholarError(
                    "Google Scholar returned its 'unusual traffic' CAPTCHA for "
                    "every browser fingerprint tried "
                    f"({', '.join(self._imp_candidates)}). That's unusual for a "
                    "fingerprint block, so this IP may be genuinely throttled: "
                    "wait a few minutes, switch networks/VPN, solve the CAPTCHA "
                    "once at https://scholar.google.com/, or rely on "
                    "CourtListener results (which don't use Scholar)."
                )
            if resp.status_code in _RETRY_STATUSES:
                # Bare (non-CAPTCHA) 429/503: a real transient limit.  Ease the
                # steady-state pace and back off before retrying.
                self._delay = min(self._delay * _DELAY_GROWTH, _MAX_DELAY)
                if backoff_attempt >= _MAX_RETRIES:
                    print(
                        f"[scholar] HTTP {resp.status_code} (rate limited) after "
                        f"{_MAX_RETRIES} retries; giving up. Wait a few minutes "
                        "before retrying."
                    )
                    resp.raise_for_status()
                    return resp
                wait = self._retry_after_seconds(resp)
                if wait is None:
                    wait = min(_BACKOFF_BASE * (2 ** backoff_attempt), _BACKOFF_MAX)
                    wait += random.uniform(0, wait * 0.25)  # jitter
                print(
                    f"[scholar] HTTP {resp.status_code} (rate limited); backing "
                    f"off {wait:.0f}s then retry {backoff_attempt + 1}/{_MAX_RETRIES}"
                )
                time.sleep(wait)
                backoff_attempt += 1
                continue
            resp.raise_for_status()
            return resp

    def _get_results_html(self, url: str) -> str:
        """GET a search/results page, retrying past Scholar's "Loading…" shell.

        Like case pages, a results page sometimes comes back as the JS shell
        (page chrome, no ``gs_r`` rows) when Scholar is soft-throttling -- which
        is indistinguishable from "no hits".  We re-fetch a few times until rows
        render; a genuinely empty search (Scholar's "did not match" notice)
        short-circuits immediately so true-empty queries stay fast.
        """
        html = self._get(url).text
        for attempt in range(_SHELL_RETRIES):
            soup = BeautifulSoup(html, "html.parser")
            if soup.find("div", class_="gs_r") or "did not match" in html.lower():
                return html
            print(
                f"[scholar] results not server-rendered (Loading… shell); retry "
                f"{attempt + 1}/{_SHELL_RETRIES}"
            )
            time.sleep(_BACKOFF_BASE)
            html = self._get(url).text
        return html

    def _first_case_url(self, html: str) -> Optional[str]:
        """Return the first opinion URL (scholar_case?case=<id>) on a results page.

        Result rows now also carry "How cited" links of the form
        ``scholar_case?about=<id>`` -- a citation-analysis page, not an opinion --
        and those can appear *before* the case title link in document order.  So
        prefer the first parsed result's URL (its h3 title link, always a
        ``case=`` opinion link), and only fall back to a raw scan that requires
        ``case=`` and rejects ``about=``.
        """
        results = self._parse_results(html)
        if results:
            print(f"[scholar] found case url: {results[0].url}")
            return results[0].url
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            if "scholar_case" in href and "case=" in href and "about=" not in href:
                if href.startswith("/"):
                    href = SCHOLAR_BASE + href
                print(f"[scholar] found case url: {href}")
                return href
        return None

    @staticmethod
    def _normalize_case_url(url: str) -> str:
        """Reduce a scholar_case URL to a stable ``?case=<id>`` form.

        Scholar appends q/as_sdt/hl clutter to result links; stripping it gives
        a stable cache key and, in practice, a more reliably server-rendered
        opinion (the param-laden variants more often return the JS shell)."""
        m = re.search(r"[?&]case=(\d+)", url)
        return f"{SCHOLAR_BASE}/scholar_case?case={m.group(1)}&hl=en" if m else url

    def _fetch_case_page(self, url: str) -> Optional[tuple[str, str]]:
        """Fetch a scholar_case page and extract the opinion HTML.

        Scholar intermittently returns a JS "Loading…" shell -- the page chrome
        with an absent or near-empty ``#gs_opinion`` -- instead of the
        server-rendered opinion, especially while soft-throttling.  We re-fetch
        a few times (easing the pace) until the opinion materializes rather than
        reporting it missing.
        """
        url = self._normalize_case_url(url)
        print(f"[scholar] fetching case page {url}")
        for attempt in range(_SHELL_RETRIES + 1):
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
            if opinion_div and len(opinion_div.get_text(strip=True)) > 200:
                html = str(opinion_div)
                print(f"[scholar] extracted {len(html):,} chars of opinion HTML")
                return (url, html)
            if attempt < _SHELL_RETRIES:
                print(
                    f"[scholar] opinion not server-rendered (Loading… shell); "
                    f"retry {attempt + 1}/{_SHELL_RETRIES}"
                )
                time.sleep(_BACKOFF_BASE)
        print("[scholar] #gs_opinion not found (only the Loading… shell)")
        return None

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


if __name__ == "__main__":
    # Quick cookie-availability self-check:
    #   python google_scholar.py            # try Chrome, then Firefox, then Edge
    #   python google_scholar.py firefox    # force one browser
    import sys

    _order = _resolve_cookie_order(sys.argv[1] if len(sys.argv) > 1 else "auto")
    _jar, _src, _note = load_google_cookies(_order or _COOKIE_BROWSER_ORDER)
    if _jar is None:
        print(f"No browser cookies available: {_note}")
        print("Sign into Google Scholar in Firefox, or run this as admin for Chrome.")
    else:
        _names = sorted({c.name for c in _jar if c.value})
        _signed = [n for n in _names if n in _SIGNED_IN_COOKIES]
        print(f"Loaded {len(_names)} google.com cookies from {_src}; "
              f"signed-in session: {'yes' if _signed else 'no (visitor cookies only)'}")
