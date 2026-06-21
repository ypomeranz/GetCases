"""Fetch and parse the Federal Rules (Civil Procedure, Criminal Procedure,
Evidence, Appellate Procedure, Bankruptcy Procedure) from the Cornell
Legal Information Institute (www.law.cornell.edu), which hosts the full
text at stable per-rule URLs:

    https://www.law.cornell.edu/rules/{set}/rule_{N}

where {set} is one of frcp, frcrmp, fre, frap, frbp and {N} is the rule
number ("56", "12", "404", "32.1").

The module mirrors the contract of ``us_code`` and ``ecfr`` so the GUI's
statute viewer renders all three the same way: a tolerant citation regex
(``RULE_CITE_RE``), ``cite_spec``/``spec_label`` helpers, a
``load_section(set, rule)`` fetcher, and a ``RuleDoc`` exposing the
(kind, indent, text) paragraph stream plus ``label``/``source_name``/
``bluebook_cite``/``neighbors``.

Indentation is inferred from the rules' own enumerators
(a) -> (1) -> (A) -> (i) using the engine shared with ``us_code``.

NOTE ON THE PARSER: ``parse_rule_html`` is written against the LII page
template (a main content region of block elements, with "Notes of
Advisory Committee" sections trailing the rule text).  The detection,
spec, label and URL logic is exercised by the offline tests below; the
HTML-to-paragraph parser should be confirmed against a live page once
network access to law.cornell.edu is available (see __main__).
"""

from __future__ import annotations

import html as _html
import re
import threading
from dataclasses import dataclass, field

from us_code import infer_enum_level, _enum_value  # shared enumerator engine

# ---------------------------------------------------------------------------
# Rule sets
# ---------------------------------------------------------------------------

# key -> (Bluebook abbreviation, full name, Cornell path segment).  The key
# doubles as the Cornell path segment, but they are kept distinct so a
# future source (e.g. Supreme Court rules) can differ.
RULESETS: dict[str, tuple[str, str, str]] = {
    "frcp":   ("Fed. R. Civ. P.",   "Federal Rules of Civil Procedure",      "frcp"),
    "frcrmp": ("Fed. R. Crim. P.",  "Federal Rules of Criminal Procedure",   "frcrmp"),
    "fre":    ("Fed. R. Evid.",     "Federal Rules of Evidence",             "fre"),
    "frap":   ("Fed. R. App. P.",   "Federal Rules of Appellate Procedure",  "frap"),
    "frbp":   ("Fed. R. Bankr. P.", "Federal Rules of Bankruptcy Procedure", "frbp"),
}

# ---------------------------------------------------------------------------
# Citation recognition
#
# Tolerant of spacing, periods and capitalization per the request to catch
# "anything reasonably close to Bluebook form".  Three shapes are matched:
#
#   abbreviated   "Fed. R. Evid. 404(b)", "Fed.R.Civ.P. 56", "FRE 404",
#                 "F.R.C.P. 56", "Fed R Crim P 11(c)(1)"
#   spelled, post "Rule 404 of the Federal Rules of Evidence"
#   spelled, pre  "Federal Rule of Evidence 404(b)"
#
# A bare "Rule 56" is deliberately NOT matched — without a federal marker
# it is ambiguous with local/state rules.  A subdivision token is 1-4
# letters or 1-3 digits, so a trailing year "(2020)" is never swallowed.
# ---------------------------------------------------------------------------

_NUM = r"\d+(?:\.\d+)?"
_SUBS = r"(?:\s*\((?:[A-Za-z]{1,4}|\d{1,3})\))*"

RULE_CITE_RE = re.compile(
    r"""\b(?:
      # --- abbreviated: "Fed. R. Evid. 404", "F.R.C.P. 56", "FRE 404" ---
      (?:
         (?P<civ>   Fed\.?\s*R\.?\s*Civ\.?\s*P\.?    | F\.?\s*R\.?\s*C\.?\s*P\.? )
       | (?P<crim>  Fed\.?\s*R\.?\s*Crim\.?\s*P\.?   | F\.?\s*R\.?\s*Cr\.?\s*P\.? )
       | (?P<evid>  Fed\.?\s*R\.?\s*Evid\.?          | F\.?\s*R\.?\s*E\.? )
       | (?P<app>   Fed\.?\s*R\.?\s*App\.?\s*P\.?    | F\.?\s*R\.?\s*A\.?\s*P\.? )
       | (?P<bankr> Fed\.?\s*R\.?\s*Bankr\.?\s*P\.?  | F\.?\s*R\.?\s*B\.?\s*P\.? )
      )
      \s*(?:Rule\s+)?
      (?P<rule>""" + _NUM + r""")(?P<subs>""" + _SUBS + r""")
    |
      # --- spelled, trailing: "Rule 404 of the Federal Rules of Evidence" ---
      Rule\s+(?P<rule2>""" + _NUM + r""")(?P<subs2>""" + _SUBS + r""")
      \s+of\s+the\s+Fed(?:eral)?\.?\s+Rules?\s+of\s+
      (?:
         (?P<civ2>Civil\s+Procedure)    | (?P<crim2>Criminal\s+Procedure)
       | (?P<evid2>Evidence)            | (?P<app2>Appellate\s+Procedure)
       | (?P<bankr2>Bankruptcy\s+Procedure)
      )
    |
      # --- spelled, leading: "Federal Rule of Evidence 404(b)" ---
      Fed(?:eral)?\.?\s+Rules?\s+of\s+
      (?:
         (?P<civ3>Civil\s+Procedure)    | (?P<crim3>Criminal\s+Procedure)
       | (?P<evid3>Evidence)            | (?P<app3>Appellate\s+Procedure)
       | (?P<bankr3>Bankruptcy\s+Procedure)
      )
      \s+(?:Rule\s+)?(?P<rule3>""" + _NUM + r""")(?P<subs3>""" + _SUBS + r""")
    )""",
    re.VERBOSE | re.IGNORECASE,
)

# Which named groups identify each rule set, across the three branches.
_SET_GROUPS: dict[str, tuple[str, ...]] = {
    "frcp":   ("civ", "civ2", "civ3"),
    "frcrmp": ("crim", "crim2", "crim3"),
    "fre":    ("evid", "evid2", "evid3"),
    "frap":   ("app", "app2", "app3"),
    "frbp":   ("bankr", "bankr2", "bankr3"),
}


def _match_parts(m: re.Match) -> tuple[str, str, list[str]]:
    """(set_key, rule, [subdivisions]) from a RULE_CITE_RE match."""
    set_key = next(
        key for key, groups in _SET_GROUPS.items()
        if any(m.group(g) for g in groups)
    )
    rule = m.group("rule") or m.group("rule2") or m.group("rule3")
    subs_raw = m.group("subs") or m.group("subs2") or m.group("subs3") or ""
    subs = re.findall(r"\(([^)]+)\)", subs_raw)
    return set_key, rule, subs


def cite_spec(m: re.Match) -> str:
    """Compact "set:rule:sub,sub" spec from a RULE_CITE_RE match."""
    set_key, rule, subs = _match_parts(m)
    return f"{set_key}:{rule}:{','.join(subs)}"


# A bare "Rule 801" / "Rule 12(b)(6)" carrying no federal-rules marker.  On its
# own this is ambiguous (it could be a local or state rule), so RULE_CITE_RE
# deliberately skips it — but *inside* a federal-rules page a bare "Rule N"
# means a rule in the SAME set, and the viewer resolves it that way.
BARE_RULE_RE = re.compile(
    r"\bRule\s+(?P<rule>" + _NUM + r")(?P<subs>" + _SUBS + r")",
    re.IGNORECASE,
)


def bare_rule_spec(m: re.Match, set_key: str) -> str:
    """"set:rule:sub,sub" spec for a BARE_RULE_RE match, resolved against the
    rule set `set_key` of the page it appears on."""
    rule = m.group("rule")
    subs = re.findall(r"\(([^)]+)\)", m.group("subs") or "")
    return f"{set_key}:{rule}:{','.join(subs)}"


def spec_label(spec: str) -> str:
    """Display form of a spec: 'Fed. R. Evid. 404(b)(1)'."""
    set_key, rule, subs = spec.split(":", 2)
    abbr = RULESETS[set_key][0]
    tail = "".join(f"({s})" for s in subs.split(",") if s)
    return f"{abbr} {rule}{tail}"


def parse_query(query: str) -> tuple[str, str] | None:
    """Parse a hand-typed rule citation into ("rule", spec), or None.
    Accepts the same loose forms as RULE_CITE_RE but anchored to the whole
    string ("fre 404(b)", "Fed. R. Civ. P. 56")."""
    q = (query or "").strip()
    m = RULE_CITE_RE.match(q)
    if not m or m.end() < len(q.rstrip(". ")):
        return None
    return "rule", cite_spec(m)


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}

_HOST = "law.cornell.edu"


def rule_url(set_key: str, rule: str) -> str:
    path = RULESETS[set_key][2]
    return f"https://www.law.cornell.edu/rules/{path}/rule_{rule}"


@dataclass
class RuleDoc:
    set_key: str
    rule: str
    url: str
    # (kind, indent, text); kind in {"sechead", "head", "body", "credit",
    # "note-head", "note-body"} — same contract as us_code / ecfr.
    paras: list[tuple[str, int, str]] = field(default_factory=list)

    @property
    def kind(self) -> str:
        return "rule"

    # The "title"/"section" pair the GUI threads through specs and
    # neighbors() — for rules these are the set key and rule number.
    @property
    def title(self) -> str:
        return self.set_key

    @property
    def section(self) -> str:
        return self.rule

    @property
    def label(self) -> str:
        return f"{RULESETS[self.set_key][0]} {self.rule}"

    @property
    def heading(self) -> str:
        for kind, _i, text in self.paras:
            if kind == "sechead":
                return text
        return self.label

    @property
    def source_name(self) -> str:
        return "Cornell LII"

    @property
    def source_note(self) -> str:
        return f"Cornell Legal Information Institute — {RULESETS[self.set_key][1]}"

    def bluebook_cite(self, subs: tuple = ()) -> str:
        """Bluebook citation (rule 12.9.3): the current federal rules carry
        no date — 'Fed. R. Evid. 404(b)(1)'."""
        tail = "".join(f"({s})" for s in subs)
        return f"{RULESETS[self.set_key][0]} {self.rule}{tail}"

    def neighbors(self) -> tuple[tuple[str, str] | None,
                                 tuple[str, str] | None]:
        """Adjacent rules in the set, from the (cached) table of contents.
        Network failures simply yield (None, None)."""
        try:
            order = _set_order(self.set_key)
            i = order.index(self.rule)
        except Exception:
            return None, None
        prev = (self.set_key, order[i - 1]) if i > 0 else None
        nxt = (self.set_key, order[i + 1]) if i + 1 < len(order) else None
        return prev, nxt


_cache: dict[tuple[str, str], RuleDoc] = {}
_lock = threading.Lock()


def load_section(set_key: str, rule: str) -> RuleDoc:
    """Fetch and parse one rule, with an in-memory cache.  Raises
    RuntimeError with a readable message on failure."""
    set_key, rule = str(set_key).strip().lower(), str(rule).strip()
    if set_key not in RULESETS:
        raise RuntimeError(f"unknown rule set {set_key!r}")
    key = (set_key, rule)
    with _lock:
        if key in _cache:
            return _cache[key]

    import requests

    url = rule_url(set_key, rule)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        if resp.status_code == 404:
            raise RuntimeError(
                f"no such rule: {RULESETS[set_key][0]} {rule}")
        resp.raise_for_status()
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"law.cornell.edu: {exc}") from exc

    paras = parse_rule_html(resp.content.decode("utf-8", "replace"))
    if not paras:
        raise RuntimeError(
            f"no rule text found for {RULESETS[set_key][0]} {rule}")
    doc = RuleDoc(set_key=set_key, rule=rule, url=url, paras=paras)
    with _lock:
        _cache[key] = doc
    return doc


# Table of contents per set, for prev/next navigation (cached per process).
_order_cache: dict[str, list[str]] = {}
_TOC_RULE_RE = re.compile(
    r"/rules/(?:frcp|frcrmp|fre|frap|frbp)/rule_(\d+(?:\.\d+)?)\b"
)


def _set_order(set_key: str) -> list[str]:
    """Ordered rule numbers in a set, scraped from its Cornell index page."""
    with _lock:
        if set_key in _order_cache:
            return _order_cache[set_key]

    import requests

    path = RULESETS[set_key][2]
    resp = requests.get(
        f"https://www.law.cornell.edu/rules/{path}",
        headers=_HEADERS, timeout=30,
    )
    resp.raise_for_status()
    order: list[str] = []
    for m in _TOC_RULE_RE.finditer(resp.content.decode("utf-8", "replace")):
        r = m.group(1)
        if r not in order:
            order.append(r)
    if order:
        with _lock:
            _order_cache[set_key] = order
    return order


# ---------------------------------------------------------------------------
# HTML parsing
#
# LII rule pages render the rule text as block elements inside a main
# content region, with the rule heading first and "Notes of Advisory
# Committee" / "Committee Notes" sections trailing.  Indentation is not
# marked, so it is inferred from the leading enumerators per the rules'
# (a) -> (1) -> (A) -> (i) drafting hierarchy.
# ---------------------------------------------------------------------------

RULES_HIERARCHY = ("a", "1", "A", "i", "I")

# Heading *element* text that begins the notes that follow a rule (some LII
# pages wrap the committee notes under an <h2>Notes</h2> / similar heading).
_NOTE_HEAD_RE = re.compile(
    r"(advisory\s+committee|committee\s+note|notes?\s+of\s+decisions|"
    r"\bnotes\b|amendment|effective\s+date)",
    re.IGNORECASE,
)

# On most LII rule pages the committee notes are NOT wrapped in heading
# elements — the boundary and the per-era dividers are plain <p> paragraphs.
# These two patterns are anchored to the start of a paragraph so they detect
# the notes without misfiring on operative rule text.

# The amendment-history credit line that trails the operative text, e.g.
# "(As amended Dec. 27, 1946, eff. ...)", "(As added Apr. 12, 2006, ...)",
# "(Pub. L. 93-595, ... 88 Stat. 1932; ...)".  Its appearance both yields a
# "credit" paragraph and marks the start of the trailing notes.
# (No trailing \b: "Pub. L." ends in a period, so a word boundary there fails;
# the alternatives are distinctive enough at the head of a parenthetical.)
_CREDIT_RE = re.compile(
    r"^\(\s*(?:As\s+(?:amended|added)|Amended|Added|Pub\.\s*L\.)",
    re.IGNORECASE,
)

# A per-era committee-note divider rendered as a <p>, e.g. "Notes of Advisory
# Committee on Rules—1937", "Committee Notes on Rules—2010 Amendment", "Notes
# of Committee on the Judiciary, House Report No. 93-650".
_NOTE_SECTION_RE = re.compile(
    r"^(?:Notes?\s+of\s+(?:the\s+)?(?:Advisory\s+)?Committee\b"
    r"|Committee\s+Notes?\s+on\s+Rules?\b"
    r"|Advisory\s+Committee\s+Notes?\b)",
    re.IGNORECASE,
)

# Block elements carrying rule/note text, in document order.
_BLOCK_RE = re.compile(
    r"<(h[1-6]|p|li)\b([^>]*)>(.*?)</\1>",
    re.IGNORECASE | re.DOTALL,
)
_ENUM_LEAD_RE = re.compile(r"^((?:\((?:\d{1,3}|[a-zA-Z]{1,5})\)\s*)+)")

# LII encodes each subdivision's nesting purely in CSS: a paragraph's depth is
# carried by its class ("statutory-body-2em", "statutory-body-block-3em"),
# while top-level subdivisions are plain <p> elements (no such class).  When a
# page uses these classes they are authoritative — they disambiguate cases the
# leading enumerator alone cannot, e.g. a nested "(i)" (roman numeral, 3em)
# vs. a top-level subsection "(i)" (plain), which otherwise both look like the
# letter after "(h)".
_STAT_BODY_LEVEL_RE = re.compile(r"statutory-body(?:-block)?-(\d+)em")

# Common LII / Drupal content containers, tried in order.  The rule node is
# rendered as a single self-closing <article> that holds the rule body AND the
# trailing committee notes, with the "Toolbox" <aside> and the site <footer>
# OUTSIDE it — so the bounded <article> match is both the cleanest and the
# safest (verified across frcp/frcrmp/fre/frap/frbp).  The remaining patterns
# are fallbacks for any page that lacks the node article.
_CONTENT_RE = (
    re.compile(r'<article\b[^>]*>(.*?)</article>', re.IGNORECASE | re.DOTALL),
    re.compile(r'<div[^>]+id="?main-content"?[^>]*>(.*)',
               re.IGNORECASE | re.DOTALL),
    re.compile(r'<main\b[^>]*>(.*?)</main>', re.IGNORECASE | re.DOTALL),
    re.compile(r'<div[^>]+id="?content"?[^>]*>(.*)', re.IGNORECASE | re.DOTALL),
)

# The rule's descriptive heading ("Rule 404. Character Evidence ...") is
# rendered in the page <h1 id="page-title">, which sits OUTSIDE the node
# <article> used as the content region — so it is pulled from the whole page.
_PAGE_TITLE_RE = re.compile(
    r'<h1\b[^>]*\bid="?page-title"?[^>]*>(.*?)</h1>',
    re.IGNORECASE | re.DOTALL,
)


def _clean(fragment: str) -> str:
    text = re.sub(r"(?is)<(script|style)\b.*?</\1>", "", fragment)
    text = re.sub(r"<[^>]+>", "", text)
    text = _html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _content_region(page_html: str) -> str:
    for rx in _CONTENT_RE:
        m = rx.search(page_html)
        if m:
            return m.group(1)
    return page_html


def _page_title(page_html: str) -> str:
    """The rule's descriptive heading from <h1 id="page-title">, e.g.
    'Rule 404. Character Evidence; Other Crimes, Wrongs, or Acts'."""
    m = _PAGE_TITLE_RE.search(page_html)
    return _clean(m.group(1)) if m else ""


def _sync_enum_stack(stack: list[tuple[str, str]], level: int,
                     enums: list[str], hierarchy: tuple[str, ...]) -> None:
    """Force the open-levels `stack` to reflect a paragraph whose depth came
    from its CSS class rather than from enumerator inference, so a following
    *plain* enumerated paragraph (one LII left unclassed) infers its level
    correctly.  Mirrors infer_enum_level's stack bookkeeping."""
    del stack[level:]
    while len(stack) < level:  # pad if the class skipped an intermediate level
        k = hierarchy[min(len(stack), len(hierarchy) - 1)]
        stack.append((k, ""))
    k = hierarchy[min(level, len(hierarchy) - 1)]
    stack.append((k, enums[0] if enums else ""))
    for extra in enums[1:]:
        k = hierarchy[min(len(stack), len(hierarchy) - 1)]
        stack.append((k, extra))


def parse_rule_html(page_html: str) -> list[tuple[str, int, str]]:
    """Parse an LII rule page into the (kind, indent, text) stream used by
    the statute viewer (same contract as us_code.parse_section)."""
    region = _content_region(page_html)
    # Drop obvious chrome that can sit inside (or, on fallback paths, after)
    # the content region: scripts/styles, the prev/up/next pager <nav> that
    # trails the rule body, the "Toolbox" <aside>, and the site <footer>.
    region = re.sub(
        r"(?is)<(script|style|nav|aside|footer|form|figure)\b.*?</\1>", "",
        region)
    paras: list[tuple[str, int, str]] = []
    stack: list[tuple[str, str]] = []
    in_notes = False
    # Trust LII's CSS indent classes when the page carries them (see above).
    uses_em = bool(_STAT_BODY_LEVEL_RE.search(region))
    # Seed the stream with the rule's descriptive heading (it lives in the
    # page <h1>, outside the article region); fall back to deriving the head
    # from the first in-article heading when that <h1> is absent.
    title = _page_title(page_html)
    seen_head = bool(title)
    if title:
        paras.append(("sechead", 0, title))
    for em in _BLOCK_RE.finditer(region):
        tag = em.group(1).lower()
        attrs = em.group(2)
        text = _clean(em.group(3))
        if not text:
            continue
        is_heading = tag in ("h1", "h2", "h3", "h4", "h5", "h6")
        if is_heading:
            # The first heading is the rule title; later headings that look
            # like committee notes switch the stream into note mode.
            if not seen_head:
                seen_head = True
                paras.append(("sechead", 0, text))
                continue
            if _NOTE_HEAD_RE.search(text):
                in_notes = True
                stack.clear()
            paras.append(("note-head" if in_notes else "head", 0, text))
            continue
        if not seen_head:
            # Body text before any heading: the section head is seeded from
            # the page <h1> above, so treat this as body.
            seen_head = True
        # The amendment-history credit line ends the operative rule text and
        # opens the trailing committee notes (LII renders it as a bare <p>).
        if _CREDIT_RE.match(text):
            in_notes = True
            stack.clear()
            paras.append(("credit", 0, text))
            continue
        # Per-era note dividers are <p> paragraphs (not headings) on most LII
        # pages; promote them to note heads, flipping into note mode when the
        # credit line happens to be absent.
        if _NOTE_SECTION_RE.match(text):
            in_notes = True
            stack.clear()
            paras.append(("note-head", 0, text))
            continue
        if in_notes:
            paras.append(("note-body", 0, text))
            continue
        # Rule body: when LII's CSS indent classes are present they are
        # authoritative for depth.  But LII is not fully consistent — it
        # sometimes drops the class on a sub-item (e.g. a "(ii)" right after a
        # classed "(i)" renders as a plain <p>).  So keep the enumerator stack
        # in sync with the class-derived depths and, for a *plain* enumerated
        # paragraph, infer its depth from that stack: that nests the stray
        # "(ii)" under its "(i)" while still letting a genuine top-level
        # subsection "(i)" sit flush left.
        lead = _ENUM_LEAD_RE.match(text)
        enums = re.findall(r"\(([^)]+)\)", lead.group(1)) if lead else []
        if uses_em:
            mlvl = _STAT_BODY_LEVEL_RE.search(attrs)
            if mlvl:
                level = min(int(mlvl.group(1)), 6)
                # LII's em-class is normally authoritative, but it occasionally
                # flattens a whole subtree to a single class — Rule 23(e) tags
                # every nested item "statutory-body-1em", which would collapse
                # (1)/(A)/(i) all to one indent.  Trust the class only when the
                # leading enumerator is a valid token for that level's kind
                # ((1)=digit, (A)=upper letter, (i)=lower roman); otherwise the
                # class is wrong here, so infer the depth from the enumerator
                # hierarchy as on a class-less page.
                hk = RULES_HIERARCHY[min(level, len(RULES_HIERARCHY) - 1)]
                if enums and not _enum_value(enums[0], hk):
                    lvl = infer_enum_level(enums, stack, RULES_HIERARCHY)
                    if lvl is not None:
                        level = lvl
                    else:
                        _sync_enum_stack(stack, level, enums, RULES_HIERARCHY)
                else:
                    _sync_enum_stack(stack, level, enums, RULES_HIERARCHY)
            elif enums:
                lvl = infer_enum_level(enums, stack, RULES_HIERARCHY)
                level = lvl if lvl is not None else 0
            else:  # plain continuation paragraph
                level = max(len(stack) - 1, 0)
        else:
            level = max(len(stack) - 1, 0)
            if enums:
                lvl = infer_enum_level(enums, stack, RULES_HIERARCHY)
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

    # --- citation regex: tolerant of spacing / periods / case ---
    cases = [
        ("Fed. R. Civ. P. 56", "frcp:56:"),
        ("Fed.R.Civ.P. 56(a)", "frcp:56:a"),
        ("FRCP 12(b)(6)", "frcp:12:b,6"),
        ("F.R.C.P. 23", "frcp:23:"),
        ("fed r civ p 26", "frcp:26:"),
        ("Fed. R. Crim. P. 11(c)(1)(C)", "frcrmp:11:c,1,C"),
        ("FRCrP 41", "frcrmp:41:"),
        ("Fed. R. Evid. 404(b)(1)", "fre:404:b,1"),
        ("F.R.E. 801(d)(2)(E)", "fre:801:d,2,E"),
        ("FRE 702", "fre:702:"),
        ("Fed. R. App. P. 4(a)(1)", "frap:4:a,1"),
        ("F.R.A.P. 32.1", "frap:32.1:"),
        ("FRAP 28", "frap:28:"),
        ("Fed. R. Bankr. P. 9011", "frbp:9011:"),
        ("Rule 404 of the Federal Rules of Evidence", "fre:404:"),
        ("Rule 56(c) of the Federal Rules of Civil Procedure", "frcp:56:c"),
        ("Federal Rule of Evidence 403", "fre:403:"),
        ("Federal Rule of Civil Procedure 12(b)(6)", "frcp:12:b,6"),
        ("see Fed. R. Evid. 404(b) (2020)", "fre:404:b"),  # year not swallowed
    ]
    for text, want in cases:
        m = RULE_CITE_RE.search(text)
        got = cite_spec(m) if m else None
        check(got == want, f"{text!r} -> {got!r}")

    # things that must NOT match (bare/ambiguous or unrelated)
    for text in ("Rule 56", "Rule 12(b)(6)", "42 U.S.C. § 1983",
                 "29 C.F.R. § 1614.105", "the firefighters",  # 'fre' inside word
                 "California Rule of Court 3.1110"):
        m = RULE_CITE_RE.search(text)
        check(m is None, f"no match in {text!r} (got {m.group(0) if m else None!r})")

    # spec_label round-trips
    check(spec_label("fre:404:b,1") == "Fed. R. Evid. 404(b)(1)", "label fre")
    check(spec_label("frcp:56:") == "Fed. R. Civ. P. 56", "label frcp plain")
    check(spec_label("frcrmp:11:c,1,C") == "Fed. R. Crim. P. 11(c)(1)(C)",
          "label frcrmp")

    # hand-typed query parsing
    check(parse_query("fre 404(b)") == ("rule", "fre:404:b"), "query fre")
    check(parse_query("Fed. R. Civ. P. 56") == ("rule", "frcp:56:"),
          "query frcp")
    check(parse_query("42 USC 1983") is None, "query rejects usc")
    check(parse_query("hello") is None, "query rejects prose")

    # --- boundary patterns: credit line + <p> note dividers ---
    # (Confirmed against live LII markup for frcp/frcrmp/fre/frap/frbp.)
    check(bool(_CREDIT_RE.match("(As amended Dec. 27, 1946, eff. Mar. 19, "
                                "1948; Jan. 21, 1963.)")), "credit: As amended")
    check(bool(_CREDIT_RE.match("(As added Apr. 12, 2006, eff. Dec. 1, 2006.)")),
          "credit: As added")
    # "Pub. L." ends in a period — the pattern must not require a \b there.
    check(bool(_CREDIT_RE.match("(Pub. L. 93–595, §1, Jan. 2, 1975, "
                                "88 Stat. 1932.)")), "credit: Pub. L.")
    check(not _CREDIT_RE.match("(a) Motion for Summary Judgment."),
          "credit: not an operative subdivision")
    check(not _CREDIT_RE.match("(2) the complaint may be amended."),
          "credit: not a body para that mentions amendment")
    check(bool(_NOTE_SECTION_RE.match(
        "Notes of Advisory Committee on Rules—1937")), "note div: advisory")
    check(bool(_NOTE_SECTION_RE.match("Committee Notes on Rules—2010 "
                                      "Amendment")), "note div: committee notes")
    check(bool(_NOTE_SECTION_RE.match("Notes of Committee on the Judiciary, "
                                      "House Report No. 93–650")),
          "note div: judiciary")
    check(not _NOTE_SECTION_RE.match("The committee notes that this rule..."),
          "note div: not prose mentioning the committee")

    # --- parser on a page that mirrors real LII structure ---
    # The descriptive title lives in <h1 id="page-title"> OUTSIDE the node
    # <article>; the rule body, the "(As amended ...)" credit, and the per-era
    # note dividers are <p> elements INSIDE it; a prev/next pager <nav> trails
    # the body; the "Toolbox" <aside> and the site <footer> (with <li>/<a>
    # link text that would otherwise be scraped) sit AFTER the article.
    sample = """<html><head>
    <title>Rule 404. Character Evidence | Federal Rules of Evidence | LII</title>
    </head><body>
    <nav id="sitenav"><a href="/rules/fre">FRE</a></nav>
    <main id="main"><div id="content" class="col-sm-8"><div id="main-content">
      <h1 class="title" id="page-title">Rule 404. Character Evidence; Other
         Crimes, Wrongs, or Acts</h1>
      <article data-history-node-id="42"><div><div>
        <p>(a) <b>Character Evidence.</b></p>
        <p>(1) <i>Prohibited Uses.</i> Evidence of a person&rsquo;s character
           is not admissible to prove conduct.</p>
        <p>(2) <i>Exceptions for a Defendant or Victim.</i> The following
           exceptions apply in a criminal case:</p>
        <p>(A) a defendant may offer evidence of the defendant&rsquo;s
           pertinent trait;</p>
        <p>(b) <b>Other Crimes, Wrongs, or Acts.</b></p>
        <p>(1) <i>Prohibited Uses.</i> Evidence of any other crime is not
           admissible to prove character.</p>
        <p>(As amended Pub. L. 93&ndash;595, &sect;1, Jan. 2, 1975; Apr. 26,
           2011, eff. Dec. 1, 2011.)</p>
        <p>Notes of Advisory Committee on Proposed Rules</p>
        <p>Subdivision (a). This is a note and should be collapsible.</p>
        <p>Committee Notes on Rules&mdash;2011 Amendment</p>
        <p>The language of Rule 404 has been amended as part of restyling.</p>
        <nav class="pager"><a rel="prev">Rule 403. Excluding Evidence</a>
           <a rel="next">Rule 405. Methods of Proving Character</a></nav>
      </div></div></article>
    </div></div>
    <aside><h2>Federal Rules of Evidence Toolbox</h2>
      <ul><li><a href="/wex">Wex: Evidence: Overview</a></li></ul></aside>
    </main>
    <footer id="liifooter"><ul>
      <li><a href="/about">About LII</a></li>
      <li><a href="/privacy">Privacy</a></li></ul></footer></body></html>"""
    paras = parse_rule_html(sample)
    kinds = [(k, i) for k, i, _t in paras]
    texts = [t for _k, _i, t in paras]
    # Title comes from <h1 id="page-title"> even though it sits outside <article>.
    check(paras[0][0] == "sechead" and "Rule 404" in paras[0][2]
          and "Character Evidence" in paras[0][2],
          f"section head from page <h1>: {paras[0]!r}")
    check(("body", 0) in kinds and ("body", 1) in kinds
          and ("body", 2) in kinds, f"body indents: {kinds!r}")
    # The "(As amended ...)" line is a credit and the operative/notes boundary.
    credits = [t for k, _i, t in paras if k == "credit"]
    check(len(credits) == 1 and credits[0].startswith("(As amended"),
          f"credit line captured: {credits!r}")
    # Both <p> note dividers become note heads (no heading element present).
    note_heads = [t for k, _i, t in paras if k == "note-head"]
    check(any("Advisory Committee on Proposed Rules" in t for t in note_heads)
          and any("Committee Notes on Rules" in t for t in note_heads),
          f"<p> note dividers -> note-head: {note_heads!r}")
    # The narrative under a divider is note-body; nothing after the credit is body.
    check(any(k == "note-body" and t.startswith("Subdivision (a)")
              for k, _i, t in paras), "advisory narrative -> note-body")
    check(not any(k == "body" and i == 0
                  and (t.startswith("(As ") or "Advisory Committee" in t)
                  for k, i, t in paras), "no notes mislabeled as body")
    # Chrome: the prev/next pager, the Toolbox <aside>, and the <footer> links
    # must not leak into the paragraph stream.
    for junk in ("Rule 403", "Rule 405", "Toolbox", "Wex:", "About LII",
                 "Privacy"):
        check(not any(junk in t for t in texts), f"chrome dropped: {junk!r}")
    body_a1 = next(t for k, i, t in paras if (k, i) == ("body", 1))
    check("<i>" not in body_a1 and "Prohibited" in body_a1,
          "inline tags stripped")

    # --- regression: LII flattens Rule 23(e)'s whole subtree to one em-class ---
    # Every nested item is tagged "statutory-body-1em"; the parser must still
    # nest (1) -> (A) -> (i) from the enumerators, while a correctly-classed 2em
    # item is still trusted.
    sample23 = (
        '<html><head><title>x</title></head><body>'
        '<h1 id="page-title">Rule 23. Class Actions</h1>'
        '<article><div>'
        '<p>(e) <b>Settlement.</b> The claims, issues, or defenses.</p>'
        '<p class="statutory-body-1em">(1) Notice to the Class.</p>'
        '<p class="statutory-body-1em">(A) Information parties must provide.</p>'
        '<p class="statutory-body-1em">(B) Grounds for a decision.</p>'
        '<p class="statutory-body-1em">(i) approve the proposal; and</p>'
        '<p class="statutory-body-1em">(ii) certify the class.</p>'
        '<p class="statutory-body-1em">(2) Approval of the Proposal.</p>'
        '<p class="statutory-body-2em">(A) adequately represented;</p>'
        '</div></article></body></html>'
    )
    p23 = parse_rule_html(sample23)
    seq = [(re.match(r"\(([^)]+)\)", t).group(1), i)
           for k, i, t in p23 if k == "body" and t.startswith("(")]
    check(seq == [("e", 0), ("1", 1), ("A", 2), ("B", 2), ("i", 3),
                  ("ii", 3), ("2", 1), ("A", 2)],
          f"Rule 23(e) flat-1em subtree nested by enumerator: {seq!r}")

    raise SystemExit(1 if failed else 0)
