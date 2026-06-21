"""Fetch U.S. Supreme Court case details from Oyez (oyez.org).

Oyez (a project of Cornell's LII, Justia, and Chicago-Kent) keeps the best
free, structured record of every argued Supreme Court case: a plain-English
summary, the question presented and the holding, the justice-by-justice vote
(who was in the majority, who dissented, and who wrote), and the oral-argument
audio.  The GUI's "Case details" pane uses this for SCOTUS opinions because
CourtListener's authorship data is sparse and often empty.

There is no documented API, but the oyez.org front-end (an AngularJS app) talks
to two stable public services, both used here:

  * Search  -- an Elasticsearch index:

        POST https://beta-search.oyez.org/elasticsearch_index_scotus_nodes/_search
        body {"query": {...}}

    Each hit's ``_source`` carries ``field_court_term``,
    ``field_docket_number``, ``field_citation:field_{volume,page,year}`` and a
    ``url`` pointing at the full record below.  We can therefore match a case
    *exactly* by its U.S. Reports citation (volume + page), or fall back to a
    name search.

  * Case     -- the full JSON record:

        GET https://api.oyez.org/cases/{term}/{docket}

    Fields used: ``name``, ``citation`` (volume/page/year), ``description``
    (one-line holding summary), ``facts_of_the_case``, ``question``,
    ``conclusion`` (HTML prose), ``decided_by`` (the named Court),
    ``decisions`` -> ``votes`` (per-justice ``vote`` majority/minority and
    ``opinion_type``), and ``oral_argument_audio`` (recordings).

  * Web page -- https://www.oyez.org/cases/{term}/{docket}  (the human page,
    with the audio player + synchronized transcript; used for the links).

Public entry point: :func:`lookup`, which takes one or more citation strings
(e.g. ``"410 U.S. 113"``) plus the case name/year and returns an
:class:`OyezCase`, or ``None`` when there is no confident match or the network
is unavailable.  Results are cached in-process.

This module is self-contained (``requests`` only) and degrades to ``None`` on
any error, so the caller can fall back to its previous behaviour.  Run
``python -X utf8 oyez.py`` for offline + live tests.
"""

from __future__ import annotations

import html as _html
import re
import threading
from dataclasses import dataclass, field
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Endpoints / HTTP
# ---------------------------------------------------------------------------

API_HOST = "https://api.oyez.org"
SEARCH_URL = (
    "https://beta-search.oyez.org/elasticsearch_index_scotus_nodes/_search"
)
WEB_HOST = "https://www.oyez.org"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

_TIMEOUT = 15

_session: Optional[requests.Session] = None
_session_lock = threading.Lock()


def _get_session() -> requests.Session:
    global _session
    with _session_lock:
        if _session is None:
            s = requests.Session()
            s.headers.update(_HEADERS)
            _session = s
        return _session


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

# opinion_type (per vote) -> short parenthetical shown after a justice's name.
# Within the majority group "majority" means the justice wrote the opinion of
# the Court; within the dissent group "dissent" means they wrote the dissent --
# both read naturally as "(opinion)" because the groups are labelled.
_OPINION_TAGS = {
    "majority": "opinion",
    "plurality": "plurality",
    "concurrence": "concurrence",
    "special concurrence": "concurrence",
    "dissent": "opinion",
    "none": "",
    "": "",
}

# name suffixes to drop when reducing "William J. Brennan, Jr." to "Brennan"
_NAME_SUFFIX_RE = re.compile(
    r",?\s+(?:jr|sr|ii|iii|iv)\.?$", re.IGNORECASE
)


def _last_name(full: str) -> str:
    """'William J. Brennan, Jr.' -> 'Brennan'; 'Sandra Day O'Connor' ->
    'O'Connor'.  Falls back to the whole string if it can't be reduced."""
    name = _NAME_SUFFIX_RE.sub("", (full or "").strip()).rstrip(",")
    if not name:
        return (full or "").strip()
    return name.split()[-1]


# rank used to list the opinion's author ahead of the justices who merely
# joined, within each (majority / dissent) group
_OPINION_RANK = {"majority": 0, "plurality": 0, "concurrence": 1,
                 "special concurrence": 1, "dissent": 0}


@dataclass
class Justice:
    name: str            # full name, e.g. "Harry A. Blackmun"
    vote: str            # "majority", "minority", or "" (other/recused)
    opinion_type: str    # "majority", "concurrence", "dissent", "none", ...

    @property
    def last(self) -> str:
        return _last_name(self.name)

    @property
    def _rank(self) -> int:
        return _OPINION_RANK.get((self.opinion_type or "").lower().strip(), 2)

    @property
    def label(self) -> str:
        """Last name with a parenthetical for the opinion they wrote, e.g.
        'Blackmun (opinion)', 'Douglas (concurrence)', or just 'Burger'."""
        tag = _OPINION_TAGS.get((self.opinion_type or "").lower().strip())
        if tag is None:  # unknown but non-empty type -> show it verbatim
            tag = (self.opinion_type or "").strip()
        return f"{self.last} ({tag})" if tag else self.last


# written_opinion ``type.value`` -> normalized opinion kind (None = skip, e.g.
# "syllabus", "case").  Used only as a fallback when per-justice votes are
# absent: it names who *wrote* each opinion, not everyone who joined.
_WRITTEN_KIND = {
    "majority": "majority",
    "plurality": "majority",
    "concurring": "concurrence",
    "special concurring": "concurrence",
    "dissenting": "dissent",
}


@dataclass
class Opinion:
    """An authored opinion from ``written_opinion`` (fallback line-up)."""
    kind: str            # "majority", "concurrence", or "dissent"
    author: str          # full name

    @property
    def last(self) -> str:
        return _last_name(self.author)


@dataclass
class OralArgument:
    title: str           # "Oral Argument - December 13, 1971"
    url: str             # Oyez web page carrying the player + transcript


@dataclass
class Decision:
    """One holding's vote.  A fractured case can have several, each with its own
    description and alignment (e.g. Citizens United decided the main First
    Amendment question and the disclosure question with different majorities),
    so the per-justice split is kept per-decision, never merged."""
    description: str = ""      # the specific holding this vote resolved
    decision_type: str = ""    # e.g. "majority opinion", "plurality opinion"
    winning_party: str = ""
    majority_count: Optional[int] = None
    minority_count: Optional[int] = None
    justices: list[Justice] = field(default_factory=list)

    @property
    def majority(self) -> list[Justice]:
        # author(s) of the opinion first, then the justices who joined
        return sorted((j for j in self.justices if j.vote == "majority"),
                      key=lambda j: j._rank)

    @property
    def dissent(self) -> list[Justice]:
        return sorted((j for j in self.justices if j.vote == "minority"),
                      key=lambda j: j._rank)

    @property
    def other(self) -> list[Justice]:
        """Justices who neither joined the majority nor dissented (did not
        participate / recused), where Oyez records them at all."""
        return [j for j in self.justices
                if j.vote not in ("majority", "minority")]

    @property
    def has_votes(self) -> bool:
        return any(j.vote in ("majority", "minority") for j in self.justices)

    @property
    def vote_line(self) -> str:
        """'7–2' style split, blank if the counts aren't recorded."""
        if self.majority_count is None and self.minority_count is None:
            return ""
        maj = self.majority_count if self.majority_count is not None else "?"
        minr = self.minority_count if self.minority_count is not None else "?"
        return f"{maj}–{minr}"


@dataclass
class OyezCase:
    name: str
    citation: str        # "410 U.S. 113 (1973)" (best effort)
    term: str
    docket: str
    web_url: str
    description: str = ""     # one-line holding summary
    question: str = ""        # the question presented (plain text)
    facts: str = ""           # facts of the case (plain text)
    conclusion: str = ""      # the holding/reasoning (plain text)
    court: str = ""           # e.g. "Burger Court (1972-1975)"
    # Every recorded holding, each with its own vote split (usually one).
    decisions: list[Decision] = field(default_factory=list)
    # Authors of each opinion (from written_opinion).  Used to show the line-up
    # when Oyez has no per-justice vote record (common for very recent cases).
    opinions: list[Opinion] = field(default_factory=list)
    oral_arguments: list[OralArgument] = field(default_factory=list)
    justia_url: str = ""

    # -- derived views -----------------------------------------------------

    @property
    def voted_decisions(self) -> list[Decision]:
        """Decisions for which Oyez recorded the per-justice split."""
        return [d for d in self.decisions if d.has_votes]

    @property
    def has_votes(self) -> bool:
        """True when Oyez recorded the per-justice split (who joined whom)."""
        return any(d.has_votes for d in self.decisions)

    @property
    def is_substantive(self) -> bool:
        """True when the record carries something worth showing over the
        caller's existing data (a summary, a line-up, or audio) -- a bare
        name+citation stub returns False so the caller can fall back."""
        return bool(self.has_votes or self.opinions or self.description
                    or self.question or self.oral_arguments)

    def opinions_of(self, kind: str) -> list[Opinion]:
        return [o for o in self.opinions if o.kind == kind]


# ---------------------------------------------------------------------------
# HTML / citation helpers
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]+")


def _html_to_text(value: str) -> str:
    """Oyez prose fields are small HTML fragments (<p>…</p>).  Reduce to plain
    text: block tags become paragraph breaks, other tags are dropped, entities
    decoded, runs of whitespace collapsed."""
    if not value:
        return ""
    text = re.sub(r"(?i)</(?:p|div|li|h[1-6]|blockquote|tr)>", "\n\n", value)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = _TAG_RE.sub("", text)
    text = _html.unescape(text)
    text = _WS_RE.sub(" ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# "410 U.S. 113", "384 U. S. 436", "143 S. Ct. 1322" -> (vol, reporter, page)
_CITE_RE = re.compile(r"(\d+)\s+([A-Za-z.][A-Za-z. ]*?[A-Za-z.])\s+(\d+)")


def _parse_us_citation(cite: str) -> Optional[tuple[str, str]]:
    """Return (volume, page) when *cite* is a U.S. Reports citation, else None.
    Only the official reporter (``U.S.``) keys the Oyez citation index; parallel
    reporters (S. Ct., L. Ed.) are skipped so name search handles those."""
    if not cite:
        return None
    m = _CITE_RE.search(cite)
    if not m:
        return None
    vol, reporter, page = m.group(1), m.group(2), m.group(3)
    norm = re.sub(r"[ .]", "", reporter).upper()
    if norm == "US":
        return vol, page
    return None


_PARTY_STOP = {
    "the", "of", "v", "vs", "and", "a", "an", "et", "al", "in", "re",
    "ex", "rel", "on", "behalf",
}


def _name_tokens(name: str) -> set[str]:
    """Lower-cased alphabetic tokens of a case name, minus connective words and
    one-letter fragments, for fuzzy comparison."""
    raw = re.sub(r"[^a-z ]", " ", (name or "").lower())
    return {
        t for t in raw.split()
        if len(t) > 1 and t not in _PARTY_STOP
    }


# ---------------------------------------------------------------------------
# Search (Elasticsearch) + record fetch
# ---------------------------------------------------------------------------

def _get_json(url: str, post_body: Optional[dict] = None):
    """GET (or POST when *post_body* is given) and return parsed JSON.  Raises
    on any network/HTTP/JSON error so the caller can distinguish a transient
    failure (don't cache) from a genuine no-match (do cache)."""
    session = _get_session()
    if post_body is None:
        resp = session.get(url, timeout=_TIMEOUT)
    else:
        resp = session.post(url, json=post_body, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _es_search(body: dict) -> list[dict]:
    """POST a query to the Oyez search index; return the list of ``_source``
    dicts.  Raises on transport failure (see :func:`_get_json`)."""
    data = _get_json(SEARCH_URL, post_body=body)
    hits = (data.get("hits") or {}).get("hits") or []
    return [h.get("_source") or {} for h in hits]


def _hit_citation(src: dict) -> tuple[Optional[str], Optional[str], Optional[str]]:
    return (
        src.get("field_citation:field_volume"),
        src.get("field_citation:field_page"),
        src.get("field_citation:field_year"),
    )


def _find_by_citation(vol: str, page: str) -> Optional[dict]:
    """Exact match on U.S. Reports volume + page.  The pair is unique to one
    case, so we trust a hit only when both fields equal the query."""
    body = {
        "size": 5,
        "query": {
            "bool": {
                "must": [
                    {"match": {"field_citation:field_volume": vol}},
                    {"match": {"field_citation:field_page": page}},
                ]
            }
        },
    }
    for src in _es_search(body):
        hv, hp, _ = _hit_citation(src)
        if str(hv) == str(vol) and str(hp) == str(page):
            return src
    return None


def _find_by_name(name: str, year: str) -> Optional[dict]:
    """Relevance search on the case name, validated against the year and a
    minimum name overlap so a wrong top hit is rejected (caller falls back)."""
    if not name:
        return None
    body = {
        "size": 10,
        "query": {
            "multi_match": {
                "query": name,
                "type": "cross_fields",
                "fields": [
                    "title^3",
                    "field_first_party^2",
                    "field_second_party^2",
                ],
            }
        },
    }
    want = _name_tokens(name)
    if not want:
        return None
    yr = _year_int(year)
    best: Optional[dict] = None
    best_score = 0.0
    for src in _es_search(body):
        title = src.get("title") or ""
        have = _name_tokens(title)
        if not have:
            continue
        overlap = len(want & have) / len(want)
        # year agreement (citation year, then term) is a strong tie-breaker
        _, _, hy = _hit_citation(src)
        hyr = _year_int(hy) or _year_int(src.get("field_court_term"))
        year_ok = yr is not None and hyr is not None and abs(hyr - yr) <= 1
        score = overlap + (0.5 if year_ok else 0.0)
        if score > best_score:
            best, best_score = src, score
    # require a solid name overlap; if a year is known, require it to agree
    if best is None or best_score < 0.6:
        return None
    if yr is not None:
        _, _, hy = _hit_citation(best)
        hyr = _year_int(hy) or _year_int(best.get("field_court_term"))
        if hyr is not None and abs(hyr - yr) > 2:
            return None
    return best


def _year_int(value) -> Optional[int]:
    m = re.search(r"\d{4}", str(value or ""))
    return int(m.group(0)) if m else None


# ---------------------------------------------------------------------------
# Build an OyezCase from the full JSON record
# ---------------------------------------------------------------------------

def _as_obj(value):
    """Oyez sometimes returns a single-element list where a dict is expected
    (e.g. ``decided_by``); normalize to the dict, or None."""
    if isinstance(value, list):
        value = value[0] if value else None
    return value if isinstance(value, dict) else None


def _build_citation(d: dict) -> str:
    cit = _as_obj(d.get("citation")) or {}
    vol, page, year = cit.get("volume"), cit.get("page"), cit.get("year")
    text = ""
    if vol and page:
        text = f"{vol} U.S. {page}"
    elif vol:
        text = f"{vol} U.S. ___"
    if year:
        text = f"{text} ({year})" if text else f"({year})"
    return text


def _build_justices(decision: dict) -> list[Justice]:
    out: list[Justice] = []
    for v in decision.get("votes") or []:
        if not isinstance(v, dict):
            continue
        member = _as_obj(v.get("member")) or {}
        name = (member.get("name") or "").strip()
        if not name:
            continue
        out.append(Justice(
            name=name,
            vote=(v.get("vote") or "").strip().lower(),
            opinion_type=(v.get("opinion_type") or "").strip().lower(),
        ))
    return out


def _build_decisions(d: dict) -> list[Decision]:
    """Every recorded holding with its own vote split.  Fractured cases have
    more than one, each potentially with a different majority -- they are kept
    separate so the line-up is never wrongly merged."""
    out: list[Decision] = []
    for dec in d.get("decisions") or []:
        if not isinstance(dec, dict):
            continue
        out.append(Decision(
            description=_html_to_text(dec.get("description") or ""),
            decision_type=(dec.get("decision_type") or "").strip(),
            winning_party=(dec.get("winning_party") or "").strip(),
            majority_count=_int_or_none(dec.get("majority_vote")),
            minority_count=_int_or_none(dec.get("minority_vote")),
            justices=_build_justices(dec),
        ))
    return out


def _build_opinions(d: dict) -> list[Opinion]:
    """Authored opinions from ``written_opinion`` (majority/concurrence/dissent
    only).  Each is one author; syllabus/case entries are skipped."""
    out: list[Opinion] = []
    seen: set[tuple[str, str]] = set()
    for w in d.get("written_opinion") or []:
        if not isinstance(w, dict):
            continue
        type_obj = _as_obj(w.get("type")) or {}
        kind = _WRITTEN_KIND.get((type_obj.get("value") or "").strip().lower())
        author = (w.get("judge_full_name") or "").strip()
        if not kind or not author:
            continue
        key = (kind, author)
        if key in seen:
            continue
        seen.add(key)
        out.append(Opinion(kind=kind, author=author))
    return out


def _build_oral_arguments(d: dict, web_url: str) -> list[OralArgument]:
    out: list[OralArgument] = []
    seen: set[str] = set()
    for a in d.get("oral_argument_audio") or []:
        if not isinstance(a, dict) or a.get("unavailable"):
            continue
        title = (a.get("display_title") or a.get("title") or "Oral Argument").strip()
        # Consolidated cases list the same argument once per docket; the link is
        # the case page either way, so keep one row per distinct title.
        if title in seen:
            continue
        seen.add(title)
        out.append(OralArgument(title=title, url=web_url))
    return out


def _build_case(d: dict) -> Optional[OyezCase]:
    if not isinstance(d, dict):
        return None
    term = str(d.get("term") or "").strip()
    docket = str(d.get("docket_number") or "").strip()
    web_url = (
        f"{WEB_HOST}/cases/{term}/{docket}" if term and docket
        else d.get("justia_url") or WEB_HOST
    )
    decided_by = _as_obj(d.get("decided_by")) or {}

    return OyezCase(
        name=(d.get("name") or "").strip(),
        citation=_build_citation(d),
        term=term,
        docket=docket,
        web_url=web_url,
        description=_html_to_text(d.get("description") or ""),
        question=_html_to_text(d.get("question") or ""),
        facts=_html_to_text(d.get("facts_of_the_case") or ""),
        conclusion=_html_to_text(d.get("conclusion") or ""),
        court=(decided_by.get("name") or "").strip(),
        decisions=_build_decisions(d),
        opinions=_build_opinions(d),
        oral_arguments=_build_oral_arguments(d, web_url),
        justia_url=(d.get("justia_url") or "").strip(),
    )


def _int_or_none(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Public entry point (with caching)
# ---------------------------------------------------------------------------

_CACHE: dict[str, Optional[OyezCase]] = {}
_CACHE_LOCK = threading.Lock()


def _cache_key(cites, name: str, year: str) -> str:
    cites_part = "|".join(cites) if cites else ""
    return f"{cites_part}#{(name or '').lower().strip()}#{year or ''}"


def lookup(
    cites=None,
    name: str = "",
    year: str = "",
) -> Optional[OyezCase]:
    """Find a Supreme Court case on Oyez and return an :class:`OyezCase`.

    *cites* is a citation string or an iterable of them (the caller may pass
    every parallel reporter from the opinion header); the first that parses as
    a ``U.S.`` citation drives an exact lookup.  When no U.S. citation matches,
    a name + year search is tried.  Returns ``None`` if nothing matches
    confidently or the network is unavailable.  Results (including misses) are
    cached for the process lifetime.
    """
    if isinstance(cites, str):
        cites = [cites]
    cites = [c for c in (cites or []) if c]

    key = _cache_key(cites, name, year)
    with _CACHE_LOCK:
        if key in _CACHE:
            return _CACHE[key]

    try:
        result = _lookup_uncached(cites, name, year)
    except Exception as exc:
        # Transient failure (network/HTTP/JSON) -- return None but do NOT cache,
        # so a later attempt can succeed once connectivity is back.
        print(f"[oyez] lookup failed: {exc}")
        return None

    with _CACHE_LOCK:
        _CACHE[key] = result
    return result


def _lookup_uncached(cites: list[str], name: str, year: str) -> Optional[OyezCase]:
    hit: Optional[dict] = None

    # 1) Exact match by U.S. Reports citation (most reliable).
    for cite in cites:
        parsed = _parse_us_citation(cite)
        if parsed:
            hit = _find_by_citation(*parsed)
            if hit:
                break

    # 2) Fall back to a validated name + year search.
    if hit is None:
        hit = _find_by_name(name, year)

    if hit is None:
        return None  # genuine no-match -- safe to cache

    # The index hands us the api case URL directly; else build it from
    # term + docket.
    url = hit.get("url") or hit.get("search_api_url")
    if not url:
        term = str(hit.get("field_court_term") or "").strip()
        docket = str(hit.get("field_docket_number") or "").strip()
        if not (term and docket):
            return None
        url = f"{API_HOST}/cases/{term}/{docket}"
    return _build_case(_get_json(url))


# ---------------------------------------------------------------------------
# Tests:  python -X utf8 oyez.py   (offline checks + a few live lookups)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    # --- offline: citation parsing ---
    check(_parse_us_citation("410 U.S. 113") == ("410", "113"), "parse 410 U.S. 113")
    check(_parse_us_citation("384 U. S. 436") == ("384", "436"), "parse spaced U. S.")
    check(_parse_us_citation("Roe v. Wade, 410 U.S. 113") == ("410", "113"),
          "parse cite with name prefix")
    check(_parse_us_citation("143 S. Ct. 1322") is None, "S. Ct. is not U.S.")
    check(_parse_us_citation("576 U.S. 644 (2015)") == ("576", "644"),
          "parse with trailing year")
    check(_parse_us_citation("") is None, "empty cite")

    # --- offline: name reduction ---
    check(_last_name("Harry A. Blackmun") == "Blackmun", "last name plain")
    check(_last_name("William J. Brennan, Jr.") == "Brennan", "last name Jr.")
    check(_last_name("Sandra Day O'Connor") == "O'Connor", "last name O'Connor")

    # --- offline: HTML to text ---
    check(_html_to_text("<p>Hello &amp; bye</p>") == "Hello & bye", "html unescape")
    check("\n\n" in _html_to_text("<p>One</p><p>Two</p>"), "html paragraph break")

    # --- offline: justice label ---
    check(Justice("Harry A. Blackmun", "majority", "majority").label
          == "Blackmun (opinion)", "label majority author")
    check(Justice("Byron R. White", "minority", "dissent").label
          == "White (opinion)", "label dissent author")
    check(Justice("Warren E. Burger", "majority", "none").label == "Burger",
          "label joiner (no parenthetical)")

    if "--offline" in sys.argv:
        print(f"\n{'all offline tests passed' if not failed else str(failed)+' FAILED'}")
        raise SystemExit(failed)

    # --- live lookups (require network) ---
    print("\n--- live ---")

    def show(case: Optional[OyezCase], label: str) -> None:
        if case is None:
            print(f"FAIL {label}: no match")
            return
        print(f"ok   {label}: {case.name} | {case.citation} | term {case.term} "
              f"docket {case.docket}")
        print(f"        court: {case.court}")
        if case.has_votes:
            multi = len(case.voted_decisions) > 1
            for dec in case.voted_decisions:
                if multi:
                    print(f"        holding: {dec.description[:90]}")
                print(f"        decision: {dec.vote_line} {dec.decision_type}"
                      f"  winner: {dec.winning_party}")
                print(f"          majority: {', '.join(j.label for j in dec.majority)}")
                print(f"          dissent : {', '.join(j.label for j in dec.dissent) or '—'}")
                if dec.other:
                    print(f"          other  : {', '.join(j.label for j in dec.other)}")
        else:
            print("        (no per-justice votes; opinion authors from "
                  "written_opinion)")
            for kind in ("majority", "concurrence", "dissent"):
                ops = case.opinions_of(kind)
                if ops:
                    print(f"        {kind:9}: "
                          f"{', '.join(o.last for o in ops)}")
        print(f"        about  : {case.description[:120]}")
        for oa in case.oral_arguments:
            print(f"        audio  : {oa.title} -> {oa.url}")
        print(f"        web    : {case.web_url}")

    # Roe: reargued (decision year 1973 != term 1971), 7-2, two arguments.
    roe = lookup("410 U.S. 113", "Roe v. Wade", "1973")
    check(roe is not None and roe.docket == "70-18", "Roe by citation")
    show(roe, "Roe v. Wade")

    # Miranda: companion-docketed, unanimous-ish split.
    show(lookup("384 U.S. 436", "Miranda v. Arizona", "1966"), "Miranda")

    # Citizens United: fractured -- TWO decisions with different majorities.
    # Each must keep its own line-up (the bug that prompted per-decision votes).
    cu = lookup("558 U.S. 310", "Citizens United v. FEC", "2010")
    check(cu is not None and len(cu.voted_decisions) >= 2,
          "Citizens United has multiple decisions")
    if cu is not None:
        # The main-holding decision (corporate speech) must be Kennedy-led with
        # the conservative majority, not the liberal bloc.
        main = next((d for d in cu.voted_decisions
                     if "corporate" in d.description.lower()
                     or "free speech" in d.description.lower()), None)
        if main is not None:
            maj_last = {j.last for j in main.majority}
            check({"Kennedy", "Roberts", "Scalia", "Thomas", "Alito"} <= maj_last,
                  "Citizens United main holding: conservative 5 in majority")
            check("Stevens" in {j.last for j in main.dissent},
                  "Citizens United main holding: Stevens dissents")
    show(cu, "Citizens United")

    # Dobbs: recent -- page may be null in the index, exercising name fallback.
    dobbs = lookup(["597 U.S. 215"], "Dobbs v. Jackson Women's Health Org.", "2022")
    check(dobbs is not None, "Dobbs (citation or name fallback)")
    show(dobbs, "Dobbs")

    # Name-only path (no citation supplied).
    show(lookup(name="Brown v. Board of Education", year="1954"), "Brown (name only)")

    # Negative: a citation that isn't a real SCOTUS case should not match.
    bogus = lookup("999 U.S. 99999", "Nonexistent v. Nobody", "1700")
    check(bogus is None, "bogus citation -> None")

    print(f"\n{'all tests passed' if not failed else str(failed)+' checks FAILED'}")
    raise SystemExit(1 if failed else 0)
