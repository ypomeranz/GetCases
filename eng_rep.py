"""Detect English Reports citations ("156 Eng. Rep. 145", "95 E.R. 807") and
resolve them to the free CommonLII scan of the case.

The English Reports is the standard reprint of the old English nominate reports
(1220-1873, 178 volumes); U.S. opinions cite it as "<vol> Eng. Rep. <page>"
(Bluebook) or "<vol> E.R. <page>".  CommonLII hosts a scanned PDF of every one
of the ~124,000 cases at

    https://www.commonlii.org/uk/cases/EngR/<year>/<num>.pdf

keyed by its own medium-neutral number ("[<year>] EngR <num>").  This module
ships an index (``eng_rep_index.tsv.gz``) mapping each "<vol> E.R. <page>"
citation to that case -- built once from CommonLII's A-Z browse pages, so
detection and resolution are entirely offline; only the actual PDF fetch (done
by the caller) touches the network.

The index is a gzipped TSV, one row per citation, sorted by (vol, page, letter):

    vol <tab> page <tab> letter <tab> year <tab> num <tab> name

``letter`` is the "(A)".."(O)" sub-page marker CommonLII uses when several short
cases share one reprint page -- so a single "<vol> E.R. <page>" can resolve to
more than one case (28% of pages), and :func:`resolve` returns a *list*.

A second shipped index (``eng_rep_nominate.tsv.gz``) maps the original
*nominate-report* citations — the parallel cites the English Reports reprint,
"9 Exch. 341" (Hadley), "5 East 10", "Cro. Jac. 489" — to the same cases, one
row per citation:

    reporter <tab> volume <tab> page <tab> year <tab> num

``reporter`` is CommonLII's printed form ("Exch", "Cro Jac", "M & W"); the
detection regex derives period/spacing-tolerant patterns from it so the dotted
Bluebook forms briefs use ("M. & W.", "Q.B.", "Bro. C.C.") match too.
``volume`` is 0 for one-volume reporters cited without one — a citation that
spells the volume anyway ("1 Swa. 96", "1 Lush. 553") still resolves there.
An old-style "<vol> id. <page>" ("The Girolamo, 3 id. 169" after "1 Hagg.
Adm. 109") continues the reporter last cited.  Nominate matching is
*resolution-gated*: only a citation whose exact (reporter, volume, page) is
an indexed start page ever becomes a link, so U.S. citations that share an
abbreviation (New York's volumed "5 Johns. 37" vs the volumeless English
Johnson) are never claimed.

Only the citation/name/URL facts are shipped here (an index, like a citator);
the PDFs themselves stay on CommonLII and are fetched on demand, with
attribution, by the viewer.

To regenerate both indexes: ``python _engr_build/fetch_toc.py`` (downloads
CommonLII's A-Z browse pages) then ``python _engr_build/build_nominate.py``;
the main index was built the same way from those pages.

This module has no third-party dependencies and is import-safe even if the index
file is missing (it simply resolves nothing).  Run ``python -X utf8 eng_rep.py``
for offline self-tests.
"""

from __future__ import annotations

import gzip
import os
import re
import threading
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

# The reporter token: "Eng. Rep." (and Eng Rep / Eng.Rep.) or "E.R." (E. R. / ER).
_REPORTER = r"(?:Eng\.?\s?Rep\.?|E\.?\s?R\.?)"

# "<vol> Eng. Rep. <page>" -- volume 1-176, page up to five digits.  The leading
# year sometimes written before it ("(1854) 156 Eng. Rep. 145") is not needed:
# volume + page identify the case uniquely (modulo same-page collisions).
ER_CITE_RE = re.compile(r"\b(\d{1,3})\s+" + _REPORTER + r"\s+(\d{1,5})\b")

INDEX_FILENAME = "eng_rep_index.tsv.gz"
NOMINATE_FILENAME = "eng_rep_nominate.tsv.gz"
BASE_URL = "https://www.commonlii.org/uk/cases/EngR"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ERCase:
    """One English Reports case as catalogued by CommonLII."""
    vol: int           # E.R. volume (the citation's reporter volume)
    page: int          # E.R. start page (the citation's page)
    letter: str        # "(A)".."(O)" sub-page marker, or "" when the only case
    year: int          # CommonLII neutral-cite year ("[<year>] EngR <num>")
    num: int           # CommonLII neutral-cite number
    name: str          # case name

    @property
    def pdf_url(self) -> str:
        return f"{BASE_URL}/{self.year}/{self.num}.pdf"

    @property
    def web_url(self) -> str:
        """The human CommonLII case page (used for the CloudFlare hand-off)."""
        return f"{BASE_URL}/{self.year}/{self.num}.html"

    @property
    def neutral(self) -> str:
        return f"[{self.year}] EngR {self.num}"

    @property
    def er_cite(self) -> str:
        return f"{self.vol} E.R. {self.page}"

    @property
    def label(self) -> str:
        """'Hadley v Baxendale, 156 E.R. 145' -- name + citation for menus."""
        return f"{self.name}, {self.er_cite}" if self.name else self.er_cite


# ---------------------------------------------------------------------------
# Index (lazy-loaded once, in-process)
# ---------------------------------------------------------------------------

_INDEX: dict[tuple[int, int], list[ERCase]] | None = None
_VOL_PAGES: dict[int, list[int]] | None = None
_BY_NEUTRAL: dict[tuple[int, int], ERCase] | None = None
_LOCK = threading.Lock()


def _index_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), INDEX_FILENAME)


def _load() -> None:
    """Populate the in-memory index from the gzipped TSV (idempotent).  Any
    failure (missing/corrupt file) leaves an empty index so the app still runs;
    E.R. citations just won't resolve."""
    global _INDEX, _VOL_PAGES, _BY_NEUTRAL
    if _INDEX is not None:
        return
    with _LOCK:
        if _INDEX is not None:
            return
        idx: dict[tuple[int, int], list[ERCase]] = {}
        try:
            with gzip.open(_index_path(), "rt", encoding="utf-8") as fh:
                for line in fh:
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) < 5:
                        continue
                    vol, page, letter, year, num = parts[:5]
                    name = parts[5] if len(parts) > 5 else ""
                    try:
                        case = ERCase(int(vol), int(page), letter,
                                      int(year), int(num), name)
                    except ValueError:
                        continue
                    idx.setdefault((case.vol, case.page), []).append(case)
        except FileNotFoundError:
            print(f"[eng_rep] index not found: {_index_path()}")
        except Exception as exc:  # pragma: no cover - corrupt file
            print(f"[eng_rep] failed to load index: {exc}")
        # order candidates within a page by sub-page letter then neutral cite
        for cases in idx.values():
            cases.sort(key=lambda c: (c.letter, c.year, c.num))
        vol_pages: dict[int, set] = {}
        for vol, page in idx:
            vol_pages.setdefault(vol, set()).add(page)
        _VOL_PAGES = {v: sorted(p) for v, p in vol_pages.items()}
        # neutral cite -> case, joining the nominate index to the case records
        by_neutral: dict[tuple[int, int], ERCase] = {}
        for cases in idx.values():
            for c in cases:
                by_neutral.setdefault((c.year, c.num), c)
        _BY_NEUTRAL = by_neutral
        _INDEX = idx


def warm() -> None:
    """Load the indexes in a background thread (call at GUI start so the first
    click is instant).  Best-effort."""

    def run() -> None:
        _load()
        _load_nominate()

    threading.Thread(target=run, daemon=True).start()


def is_available() -> bool:
    """True when the index file is present (so callers can skip E.R. linking)."""
    return os.path.exists(_index_path())


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------

def lookup(vol: int, page: int) -> list[ERCase]:
    """Every case reported starting at ``<vol> E.R. <page>`` (usually one,
    sometimes several short cases sharing a reprint page).  Empty if unknown."""
    _load()
    assert _INDEX is not None
    return list(_INDEX.get((int(vol), int(page)), []))


def lookup_nearest(vol: int, page: int) -> list[ERCase]:
    """Exact match if there is one; otherwise the case(s) at the greatest start
    page <= ``page`` in the same volume -- i.e. the report that *contains* a
    pinpoint page that isn't itself a start page.  Empty if none at/below."""
    exact = lookup(vol, page)
    if exact:
        return exact
    assert _VOL_PAGES is not None
    pages = _VOL_PAGES.get(int(vol))
    if not pages:
        return []
    import bisect
    i = bisect.bisect_right(pages, int(page)) - 1
    return lookup(vol, pages[i]) if i >= 0 else []


# ---------------------------------------------------------------------------
# Nominate-report citations ("9 Exch. 341", "5 East 10", "Cro. Jac. 489") --
# the original citations the English Reports reprint.  Detected with a regex
# built from the reporter vocabulary actually in the shipped index, and
# resolution-gated: a match is only reported when its exact (reporter, volume,
# page) is an indexed start page, so shared abbreviations can't misfire.
# ---------------------------------------------------------------------------

_NOM_INDEX: "dict[tuple[str, int, int], list[tuple[int, int]]] | None" = None
_NOM_RE: "re.Pattern | None" = None
# First pages per (reporter key, volume) — pin-cite resolution.
_NOM_PAGES: "dict[tuple[str, int], list[int]] | None" = None
# Volumes each reporter key appears with — the volumeless-cite fallback.
_NOM_VOLS: "dict[str, tuple[int, ...]]" = {}
# Normalized alias key -> canonical reporter keys present in the index.
_NOM_ALIAS_KEYS: "dict[str, tuple[str, ...]]" = {}
# Reporters the index knows only volumeless (every row has volume 0) — the
# one-volume reporters.  U.S. opinions still give them the explicit volume
# ("1 Swa. 96", "1 Lush. 553" in The Scotland, 105 U.S. 24), so a cite with
# volume 1 to such a reporter falls back to its volume-0 rows.  Reporters
# that mix volume 0 with real volumes stay exact: their volume-0 rows could
# belong to any volume.
_NOM_VOLUMELESS: "frozenset[str]" = frozenset()

# Reporter keys never matched in text, because in a U.S. document the bare
# abbreviation means something else: "Curt." is Curtis' Circuit Court reports
# (English Curteis is cited "Curt. Ecc."), a capitalized "And" is prose
# ("... 2 And 45 ..." in a title-case heading), not Anderson's Common Pleas,
# "West" is geography ("the arid lands of the West 11 years later" in
# California v. United States), not West's Chancery Reports, and "Lit." is
# a journal ("54 J. Econ. Lit. 3" in SFFA v. Harvard), not Littleton —
# Littleton reaches U.S. opinions as the treatise "Co. Litt.", not by page.
_NOMINATE_DENY = frozenset({"curt", "and", "west", "lit"})

# American opinions cite several nominate reporters by shorter forms than
# the English Reports index uses — Dartmouth College alone has "1 Ves. 462"
# (Vesey Senior), "13 Ves. 519" (Vesey Junior), "10 Co. 23" (Coke),
# "1 Show. 360" (Shower), and Johnson v. M'Intosh has "1 Bl. Rep. 665"
# (Wm. Blackstone).  Each alias is detected like a native form and resolved
# against its canonical key(s); volume/page gating keeps ambiguous aliases
# honest ("13 Ves." exists only in Vesey Junior; a company's "Co. 45" has
# no volume and resolves nowhere).  U.S. Black (66-67 U.S., cited "Black.")
# is deliberately NOT aliased: its two volumes collide with Wm. Blackstone's.
# The admiralty forms are the ones the limitation-of-liability line of
# SCOTUS cases uses (The Scotland, 105 U.S. 24: "1 Dod. 290", "1 Hagg.
# Adm. 109", "1 Swa. 96", "4 Kay & J. 367", "1 John. & H. 180").  Bare
# "Hagg." (The Nestor's "The William Money, 2 Hagg. 136") is aliased to
# every court Haggard reported (Adm/Ecc/Con and the Ecc appendix); their
# volume/page ranges overlap, so resolution collects the hits across all
# four — a page indexed in only one series places the cite outright, and
# a genuine collision surfaces every candidate for the same-page chooser.
_NOM_ALIASES: "dict[str, tuple[str, ...]]" = {
    "Ves": ("Ves Sen", "Ves Jun"),
    "Co": ("Co Rep",),
    "Show": ("Show KB",),
    "Wils": ("Wils KB",),
    "Bl": ("Black W",),
    "Bl Rep": ("Black W",),
    "W Bl": ("Black W",),
    "Stra": ("Str",),
    "Term Rep": ("TR",),
    "Dougl": ("Doug",),
    "Dod": ("Dods",),
    "Hagg": ("Hag Adm", "Hag Ecc", "Hag Con", "Hag Ecc App"),
    # Late-19th-century U.S. opinions (The John G. Stevens, 170 U.S. 113;
    # Ramsay v. Allegre, 25 U.S. 611) spell several reporters out or use
    # older short forms: "7 Moore P.C. 267" (The Bold Buccleugh), "1 Dodson,
    # 37", "The Europa, Brown. & Lush. 89" — which must never fall through
    # to volumeless "Lush." (a different case) — "2 Lord Raym. 805",
    # "3 Levinz. 353", "1 Vesey, 155".  Bare "Rob." (The John, 3 Rob. 288)
    # can be Christopher or William Robinson's admiralty reports, so like
    # bare "Hagg." it aliases to both and resolution decides.
    "Moore PC": ("Moo PC",),
    "Dodson": ("Dods",),
    "Brown & Lush": ("Br & Lush",),
    "Lord Raym": ("Ld Raym",),
    "Levinz": ("Lev",),
    "Vesey": ("Ves Sen", "Ves Jun"),
    "Cro Ch": ("Cro Car",),
    "Cro Charles": ("Cro Car",),
    "Rob": ("C Rob", "W Rob"),
    "Hagg Adm": ("Hag Adm",),
    "Hagg Ecc": ("Hag Ecc",),
    "Hagg Con": ("Hag Con",),
    "Swa": ("Swab",),
    "Kay & J": ("K & J",),
    "John & H": ("J & H",),
    "Johns & H": ("J & H",),
}


def _nom_key(rep: str) -> str:
    """Lookup key for a reporter abbreviation, ignoring case, periods and
    spacing ('Cro Jac' == 'Cro. Jac.' == 'cro jac' -> 'crojac')."""
    return re.sub(r"[^a-z0-9]+", "", (rep or "").lower())


def _nom_token_pattern(tok: str) -> str:
    """Period/spacing-tolerant pattern for one reporter token.  An all-caps
    token allows periods between the letters ('TR' -> 'T.R.', 'CC' -> 'C.C.'),
    since briefs write the dotted Bluebook forms; other tokens take an optional
    trailing period ('Exch' -> 'Exch.', 'Cro' -> 'Cro.')."""
    if tok == "&":
        return r"&"
    if re.fullmatch(r"[A-Z]{2,5}", tok):
        return r"\.?\s?".join(tok) + r"\.?"
    esc = re.escape(tok).replace("'", "['’]").replace("’", "['’]")
    return esc + r"\.?"


def _nom_form_pattern(form: str) -> str:
    """Pattern for a whole reporter form ('M & W' -> M\\.?\\s*&\\s*W\\.?)."""
    toks = form.split()
    parts: list[str] = []
    for i, tok in enumerate(toks):
        if i:
            parts.append(r"\s*" if "&" in (tok, toks[i - 1]) else r"\s+")
        parts.append(_nom_token_pattern(tok))
    return "".join(parts)


def _load_nominate() -> None:
    """Populate the nominate index and its detection regex (idempotent).  Any
    failure leaves them empty so the app still runs; nominate citations just
    won't resolve.  Loads the main index first (case records join by neutral
    cite)."""
    global _NOM_INDEX, _NOM_RE, _NOM_PAGES, _NOM_ALIAS_KEYS, _NOM_VOLUMELESS
    global _NOM_VOLS
    if _NOM_INDEX is not None:
        return
    _load()  # outside _LOCK -- it takes the same (non-reentrant) lock
    with _LOCK:
        if _NOM_INDEX is not None:
            return
        idx: dict[tuple[str, int, int], list[tuple[int, int]]] = {}
        forms: set[str] = set()
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            NOMINATE_FILENAME)
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                for line in fh:
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) < 5:
                        continue
                    rep, vol, page, year, num = parts[:5]
                    key = _nom_key(rep)
                    if not key or key in _NOMINATE_DENY:
                        continue
                    try:
                        entry = (key, int(vol), int(page))
                        target = (int(year), int(num))
                    except ValueError:
                        continue
                    idx.setdefault(entry, []).append(target)
                    forms.add(rep)
        except FileNotFoundError:
            print(f"[eng_rep] nominate index not found: {path}")
        except Exception as exc:  # pragma: no cover - corrupt file
            print(f"[eng_rep] failed to load nominate index: {exc}")
        for targets in idx.values():
            targets.sort()
        # First pages per (reporter, volume), sorted — a pin cite ("3 Burr.
        # 1663" into Rex v. Vice-Chancellor at 3 Burr. 1656) resolves to the
        # case beginning at the nearest preceding indexed page.
        pages: dict[tuple[str, int], list[int]] = {}
        vols_by_key: dict[str, set] = {}
        for (key, vol, page) in idx:
            pages.setdefault((key, vol), []).append(page)
            vols_by_key.setdefault(key, set()).add(vol)
        for plist in pages.values():
            plist.sort()
        _NOM_VOLUMELESS = frozenset(
            k for k, vols in vols_by_key.items() if vols == {0})
        _NOM_VOLS = {k: tuple(sorted(v)) for k, v in vols_by_key.items()}
        present = {key for (key, _v, _p) in idx}
        alias_keys = {}
        for form, canon in _NOM_ALIASES.items():
            targets_present = tuple(
                _nom_key(c) for c in canon if _nom_key(c) in present)
            if targets_present:
                forms.add(form)
                alias_keys[_nom_key(form)] = targets_present
        if forms:
            # Longest form first, so 'Ves Jun Supp' outranks 'Ves Jun' and
            # 'CB NS' outranks 'CB' (regex alternation is first-match).
            # A comma may sit between reporter and page, as older U.S.
            # opinions print ("1 Dodson, 37"; "Owen, 122").
            alt = "|".join(_nom_form_pattern(f)
                           for f in sorted(forms, key=len, reverse=True))
            _NOM_RE = re.compile(
                r"\b(?:(\d{1,3})\s+)?(" + alt
                + r"),?\s+(\d{1,5})(?:\s*[ab])?\b")
        _NOM_PAGES = pages
        _NOM_ALIAS_KEYS = alias_keys
        _NOM_INDEX = idx


# A pin page further than this from the case's first page is treated as an
# index gap, not a pin: "Holt 715" is Philips v. Bury (not indexed), and
# pinning it 58 pages back into Hardman v. Clegg would mislink.  The longest
# genuine pins seen run ~40 pages ("1 Burr. 200" into Rex v. St John's
# College at 158).
_NOM_PIN_SPAN = 50


def _vols_for(key: str, vol: int) -> "tuple[int, ...]":
    """Index volumes to try for a cite giving *vol*: the cite's own volume,
    plus volume 0 when the cite says "1" but the index knows the reporter
    only volumeless (a one-volume reporter — "1 Swa. 96" is Swabey's single
    volume, indexed as "Swab 96")."""
    if vol == 1 and key in _NOM_VOLUMELESS:
        return (1, 0)
    return (vol,)


def _nom_resolve(key: str, vol: int, page: int, dotted: bool = True
                 ) -> "tuple[str, int, int, list] | None":
    """``(canonical_key, volume, start_page, targets)`` for a matched
    nominate cite, or None — volume and start page as *indexed*, so the spec
    built from them round-trips through :func:`resolve`.  Tries the matched
    reporter and its aliases at the exact page first; a miss then resolves
    as a pin cite — the case beginning at the nearest preceding indexed page
    of the same reporter volume ("3 Burr. 1663" pins into Rex v.
    Vice-Chancellor, 3 Burr. 1647).

    A *volumeless* cite to a multi-volume reporter ("Cowp. 636" — Cowper's
    two volumes are continuously paged) resolves when exactly one volume
    starts a case at that page, and surfaces every candidate when several
    do — but only for a dotted abbreviation of some substance: a word-shaped
    form without its period ("East 426" in prose) proves nothing, and a
    short generic one ("Insurance Co. 45") is company boilerplate, not
    Coke."""
    keys = (key,) + _NOM_ALIAS_KEYS.get(key, ())
    scan = not vol and dotted and len(key) >= 4
    hits: "list[tuple[str, int, list]]" = []
    for k in keys:
        vols: "tuple[int, ...]" = _vols_for(k, vol)
        if scan and k not in _NOM_VOLUMELESS:
            vols += tuple(v for v in _NOM_VOLS.get(k, ()) if v != 0)
        for v in vols:
            targets = _NOM_INDEX.get((k, v, page))
            if targets:
                hits.append((k, v, targets))
    if hits:
        if len(hits) == 1:
            k, v, targets = hits[0]
            return k, v, page, targets
        # Several reporters share this exact start page — an ambiguous alias
        # (bare "Hagg." can be any court Haggard reported).  Keep the matched
        # alias key in the spec and merge the candidates, so resolve() finds
        # the same union and the viewer can offer a chooser.
        merged: list = []
        for _k, _v, targets in hits:
            for t in targets:
                if t not in merged:
                    merged.append(t)
        return key, vol, page, merged
    if not vol:
        # Volumeless single-volume reporters carry word-like names ("Holt",
        # "Style"); a bare "Word NN" is also the shape of prose ("the West
        # 11 years later"), so only an exact first page is trusted.
        return None
    from bisect import bisect_right
    pin_hits: "list[tuple[str, int, int, list]]" = []
    for k in keys:
        for v in _vols_for(k, vol):
            pages = (_NOM_PAGES or {}).get((k, v))
            if pages:
                i = bisect_right(pages, page) - 1
                if i >= 0 and page - pages[i] <= _NOM_PIN_SPAN:
                    start = pages[i]
                    targets = _NOM_INDEX.get((k, v, start))
                    if targets:
                        pin_hits.append((k, v, start, targets))
    # A pin cite must place uniquely: when an ambiguous alias (bare "Hagg.")
    # pins into reports in more than one of its reporters, the candidates
    # start at *different* pages, so no one spec can carry them — leave the
    # cite unlinked rather than guess.
    if len({k for k, _v, _s, _t in pin_hits}) == 1:
        return pin_hits[0]
    return None


# Old-style "id." to the reporter just cited: "The Dundee, 1 Hagg. Adm. 109;
# The Girolamo, 3 id. 169" means 3 Hagg. Adm. 169 (a *volume* of the same
# reporter, unlike the modern pin-only "Id. at 152", which has no volume and
# is never matched here).  Traced only across a short gap with no intervening
# citation, and resolution-gated like every nominate match.
_NOM_ID_RE = re.compile(r"\b(\d{1,3})\s+[Ii]d\.\s+(\d{1,5})\b")
_NOM_ID_GAP = 160
# A digit-then-capital run in the gap is another citation's tail ("105 U.S.
# 24") — the "id." refers to *that* reporter, so the English one is dropped.
_NOM_ID_BREAK_RE = re.compile(r"\d\s+[A-Z]")


def _cases_for(targets: "list[tuple[int, int]]") -> "list[ERCase]":
    return [c for c in (_BY_NEUTRAL.get(t) for t in targets) if c is not None]


def iter_nominate_cites(text: str) -> "list[tuple[int, int, str, list[ERCase]]]":
    """Nominate-report citations in *text* that resolve to indexed cases, as
    ``(start, end, spec, cases)`` in document order.  ``spec`` ('n:exch:9:341')
    round-trips through :func:`resolve` — for an alias or pin-cite match it
    carries the canonical reporter key and the case's indexed volume and
    first page.  A following old-style "<vol> id. <page>" continues the
    reporter last cited.  Unresolvable look-alikes (a U.S. "5 Johns. 37",
    prose) are simply not reported."""
    if not text:
        return []
    _load_nominate()
    if _NOM_RE is None or not _NOM_INDEX:
        return []
    assert _BY_NEUTRAL is not None
    out: list[tuple[int, int, str, list[ERCase]]] = []
    nom = list(_NOM_RE.finditer(text))
    ids = list(_NOM_ID_RE.finditer(text))
    last: "tuple[int, str] | None" = None   # (end, canonical key) last resolved
    i = j = 0
    while i < len(nom) or j < len(ids):
        if j >= len(ids) or (i < len(nom)
                             and nom[i].start() <= ids[j].start()):
            m, i = nom[i], i + 1
            vol = int(m.group(1) or 0)
            key = _nom_key(m.group(2))
            page = int(m.group(3))
            dotted = bool(re.search(r"[.&]", m.group(2)))
        else:
            m, j = ids[j], j + 1
            if last is None or m.start() < last[0]:
                continue
            gap = text[last[0]:m.start()]
            if len(gap) > _NOM_ID_GAP or _NOM_ID_BREAK_RE.search(gap):
                last = None     # chain broken; later id.s must not reach back
                continue
            vol = int(m.group(1))
            key = last[1]
            page = int(m.group(2))
            dotted = True       # the id-form always carries a volume
        hit = _nom_resolve(key, vol, page, dotted)
        if hit is None:
            # An unresolved citation-shaped match still stands between a
            # resolved cite and a later "id." — the id. refers to *it* (the
            # "Holt 715" the gap regex cannot see), so the chain breaks.
            last = None
            continue
        ckey, rvol, start, targets = hit
        cases = _cases_for(targets)
        if cases:
            out.append((m.start(), m.end(), f"n:{ckey}:{rvol}:{start}", cases))
            last = (m.end(), ckey)
    return out


# ---------------------------------------------------------------------------
# Name search -- best-effort match of a typed case name to the index, so the
# GUI's case-name search can surface the English Reports case alongside its
# Google Scholar / CourtListener results.
# ---------------------------------------------------------------------------

# Connector / role words that carry no identifying weight in a case name, so
# they're ignored when deciding whether a typed name matches an indexed one
# ("Entick v Carrington" should still match "John Entick, Clerk, versus Nathan
# Carrington and Three Others").  "versus"/"against" are the old long forms of
# "v"; the party-role words ("plaintiff", "respondent", ...) are 18th-century
# reporting boilerplate.
_NAME_STOPWORDS = frozenset({
    "v", "vs", "versus", "against", "and", "the", "of", "in", "on", "an", "a",
    "re", "ex", "parte", "or", "to", "for", "his", "her", "wife", "al", "ux",
    "anor", "another", "others", "co", "ltd", "limited", "esq", "esqrs",
    "gent", "clerk", "knt", "bart", "appellant", "appellants", "respondent",
    "respondents", "plaintiff", "plaintiffs", "defendant", "defendants",
    "executor", "executors", "administratrix", "administrator", "deceased",
    "same", "case",
})

_NAME_NORM_RE = re.compile(r"[^a-z0-9 ]+")

# Built lazily from the citation index: (normalised name, token set, case).
_NAME_INDEX: "list[tuple[str, frozenset, ERCase]] | None" = None


def _norm_name(s: str) -> str:
    """Fold a case name to a comparison key: lowercase, '&'->'and', punctuation
    dropped, spaces collapsed -- so 'Hadley v. Baxendale' and 'Hadley v
    Baxendale' compare equal."""
    s = s.lower().replace("&", " and ")
    s = _NAME_NORM_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def _distinctive(tokens: "list[str]") -> "list[str]":
    """The identifying tokens of a name: not connector/role words, not bare
    numbers, at least three letters (drops 'v', 'and', years, single initials)."""
    return [t for t in tokens
            if t not in _NAME_STOPWORDS and len(t) >= 3 and not t.isdigit()]


def _tok_match(qt: str, name_tokens: frozenset) -> bool:
    """A query token matches a name when an indexed token equals it or extends
    it by a short suffix -- so 'pinnel' matches the possessive 'pinnels' (as in
    "Pinnel's Case") without 'bury' bleeding into 'canterbury'."""
    if qt in name_tokens:
        return True
    for nt in name_tokens:
        if nt.startswith(qt) and 0 < len(nt) - len(qt) <= 2:
            return True
    return False


def _load_names() -> None:
    """Build the name-search index once from the already-loaded citation index
    (idempotent, thread-safe).  Best-effort: leaves an empty index on failure."""
    global _NAME_INDEX
    if _NAME_INDEX is not None:
        return
    _load()
    with _LOCK:
        if _NAME_INDEX is not None:
            return
        idx: list[tuple[str, frozenset, ERCase]] = []
        seen: set[tuple[int, int]] = set()
        for cases in (_INDEX or {}).values():
            for c in cases:
                if not c.name:
                    continue
                key = (c.year, c.num)   # one row per case, not per page-letter
                if key in seen:
                    continue
                seen.add(key)
                nn = _norm_name(c.name)
                if nn:
                    idx.append((nn, frozenset(nn.split()), c))
        _NAME_INDEX = idx


def search_by_name(query: str, limit: int = 1) -> "list[ERCase]":
    """Best match(es) for a typed case name ("Entick v Carrington") among the
    indexed English Reports cases -- best first, up to *limit*, or [] when
    nothing matches well.

    Matching is deliberately strict: every identifying word of the query must
    appear in the case name.  So a U.S. or post-1865 case name (which the
    English Reports don't contain) yields nothing rather than a misleading
    near-miss -- the caller can show the result unconditionally."""
    qn = _norm_name(query)
    if not qn:
        return []
    qdist = _distinctive(qn.split())
    if not qdist:
        return []
    _load_names()
    assert _NAME_INDEX is not None
    padded_q = f" {qn} "
    scored: list[tuple[float, int, ERCase]] = []
    for nn, nset, case in _NAME_INDEX:
        if not all(_tok_match(t, nset) for t in qdist):
            continue
        if nn == qn:
            score = 1.0                              # exact name
        elif padded_q in f" {nn} ":
            score = 0.9                              # query is a contiguous run
        else:
            score = 0.7                              # all words present, scattered
        scored.append((score, len(nn), case))
    # Best score first; among equals prefer the shorter (more precise) name.
    scored.sort(key=lambda t: (-t[0], t[1]))
    return [c for _s, _ln, c in scored[:limit]]


# ---------------------------------------------------------------------------
# Helpers for the citation-detection / link-dispatch plumbing (mirrors the
# other source modules: a regex match -> a compact spec string the GUI stores
# on the link and hands back on click).
# ---------------------------------------------------------------------------

def cite_spec(m: "re.Match") -> str:
    """'<vol>:<page>' spec for an ``ER_CITE_RE`` match (the link's action value)."""
    return f"{m.group(1)}:{m.group(2)}"


def parse_spec(spec: str) -> tuple[int, int] | None:
    """Inverse of :func:`cite_spec`."""
    m = re.fullmatch(r"(\d+):(\d+)", spec.strip())
    return (int(m.group(1)), int(m.group(2))) if m else None


def cite_label(m: "re.Match") -> str:
    """Canonical 'Eng. Rep.' label for a match (normalises 'E.R.'/'ER')."""
    return f"{m.group(1)} Eng. Rep. {m.group(2)}"


def resolve(spec: str) -> list[ERCase]:
    """Candidates for a spec: '<vol>:<page>' (E.R. start page) or
    'n:<reporter-key>:<vol>:<page>' (a nominate citation, exact)."""
    s = (spec or "").strip()
    nm = re.fullmatch(r"n:([a-z0-9]+):(\d+):(\d+)", s)
    if nm:
        _load_nominate()
        if not _NOM_INDEX or _BY_NEUTRAL is None:
            return []
        key, vol, page = nm.group(1), int(nm.group(2)), int(nm.group(3))
        # An ambiguous alias spec ("n:hagg:2:136") carries the alias key
        # itself; collect the union across its canonical reporters, exactly
        # as _nom_resolve matched it — including the all-volumes scan a
        # volumeless spec ("n:cowp:0:636") implies.  Canonical specs hit
        # the index direct.
        targets: list = []
        for k in (key,) + _NOM_ALIAS_KEYS.get(key, ()):
            vols = _vols_for(k, vol)
            if not vol and len(key) >= 4 and k not in _NOM_VOLUMELESS:
                vols += tuple(v for v in _NOM_VOLS.get(k, ()) if v != 0)
            for v in vols:
                for t in _NOM_INDEX.get((k, v, page)) or []:
                    if t not in targets:
                        targets.append(t)
        return [c for c in (_BY_NEUTRAL.get(t) for t in targets)
                if c is not None]
    vp = parse_spec(s)
    return lookup(*vp) if vp else []


def search_url(vol: int, page: int) -> str:
    """A CommonLII full-text search for the citation -- the graceful fallback
    when a pattern-matched cite isn't in the index (a handful aren't)."""
    import urllib.parse
    q = urllib.parse.quote(f"{vol} ER {page}")
    return f"{BASE_URL}/cgi-bin/sinosrch.cgi?query={q}&mask_path=uk/cases/EngR"


# ---------------------------------------------------------------------------
# Offline self-test:  python -X utf8 eng_rep.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    # --- detection ---
    def first(text):
        return ER_CITE_RE.search(text)

    for text, vol, page in [
        ("156 Eng. Rep. 145", "156", "145"),
        ("see 95 E.R. 807 (KB)", "95", "807"),
        ("131 ER 305", "131", "305"),
        ("(1854) 156 Eng. Rep. 145, 151", "156", "145"),
        ("77 Eng.Rep. 237", "77", "237"),
        ("1 E. R. 1", "1", "1"),
    ]:
        m = first(text)
        got = (m.group(1), m.group(2)) if m else None
        check(got == (vol, page), f"detect {text!r} -> {got!r}")

    # things that must NOT match as E.R.
    for text in ["410 U.S. 113", "5 F.3d 12", "see para. 5", "E.R. Jones"]:
        check(first(text) is None, f"no E.R. match in {text!r}")

    check(cite_label(first("131 ER 305")) == "131 Eng. Rep. 305", "label normalises")
    check(cite_spec(first("156 Eng. Rep. 145")) == "156:145", "cite_spec")
    check(parse_spec("156:145") == (156, 145), "parse_spec")

    if "--offline" in sys.argv:
        print(f"\n{'all offline tests passed' if not failed else str(failed)+' FAILED'}")
        raise SystemExit(failed)

    # --- index-backed lookups (needs eng_rep_index.tsv.gz) ---
    print(f"\nindex present: {is_available()}  ({_index_path()})")

    def one(vol, page, expect_name_substr):
        cases = lookup(vol, page)
        ok = any(expect_name_substr.lower() in c.name.lower() for c in cases)
        check(ok, f"{vol} E.R. {page} -> {expect_name_substr!r} "
                  f"(got {[c.name[:30] for c in cases[:3]]})")
        if cases:
            c = cases[0]
            print(f"        {c.label[:60]}  {c.neutral}  -> {c.pdf_url}")

    one(156, 145, "Hadley")          # Hadley v Baxendale
    one(95, 807, "Entick")           # Entick v Carrington
    one(77, 237, "Pinnel")           # Pinnel's Case
    one(131, 305, "Planche")         # Planche v Colburn

    # same-page collision: 34 E.R. 1100 holds many cases
    many = lookup(34, 1100)
    check(len(many) > 5, f"34 E.R. 1100 has many cases (got {len(many)})")

    # nearest-page fallback: a pinpoint inside Hadley's report (145..) resolves
    near = lookup_nearest(156, 147)
    check(any("Hadley" in c.name for c in near),
          f"nearest 156 E.R. 147 -> Hadley (got {[c.name[:20] for c in near[:2]]})")

    # a bogus citation resolves to nothing
    check(lookup(999, 99999) == [], "bogus cite -> no cases")

    # --- nominate-report citations (needs eng_rep_nominate.tsv.gz) ---
    def nom_one(text, expect_spec, expect_name_substr):
        hits = iter_nominate_cites(text)
        ok = any(spec == expect_spec
                 and any(expect_name_substr.lower() in c.name.lower()
                         for c in cases)
                 for _s, _e, spec, cases in hits)
        check(ok, f"nominate {text!r} -> {expect_spec} "
                  f"({expect_name_substr!r}); got {[(s, [c.name[:24] for c in cs]) for _a, _b, s, cs in hits]!r}")

    nom_one("Hadley v. Baxendale, 9 Exch. 341 (1854)", "n:exch:9:341", "Hadley")
    nom_one("Wain v. Warlters, 5 East 10", "n:east:5:10", "Wain")
    # dotted all-caps forms, as briefs write them ("3 T.R. 557" = "3 TR 557")
    nom_one("Rawlinson v. Shaw, 3 T.R. 557", "n:tr:3:557", "Rawlinson")
    nom_one("Abell v. Heathcote, 4 Bro. C.C. 278", "n:brocc:4:278", "Abell")
    # a volumeless one-volume reporter ("Cro. Jac. 3")
    check(any(spec == "n:crojac:0:3"
              for _s, _e, spec, _c in iter_nominate_cites("see Cro. Jac. 3")),
          "volumeless 'Cro. Jac. 3' resolves")
    # nominate specs round-trip through resolve()
    check(any("Hadley" in c.name for c in resolve("n:exch:9:341")),
          "resolve('n:exch:9:341') -> Hadley")
    # American alias forms (Dartmouth College's citations): bare "Ves."
    # resolves against Vesey Senior or Junior by volume; "Co." is Coke;
    # "Show." is Shower; "Bl. Rep."/"W. Bl." is Wm. Blackstone.
    nom_one("Green v. Rutherforth, 1 Ves. 462", "n:vessen:1:462", "Green")
    nom_one("Attorney-General v. Clarendon, 17 Ves. 491",
            "n:vesjun:17:491", "Clarendon")
    nom_one("Philips v. Bury, 1 Show. 360", "n:showkb:1:360", "Philips")
    nom_one("Sutton v. Bishop, 1 Bl. Rep. 665", "n:blackw:1:665", "Sutton")
    # A pin cite resolves to the case beginning at the nearest preceding
    # indexed page ("10 Co. 33" pins into Sutton's Hospital at 10 Co. 23) —
    # never across more than _NOM_PIN_SPAN pages, and never for a
    # volumeless cite (the "Holt 715" trap: Philips v. Bury is unindexed
    # there and must stay unlinked, not mislink to its neighbor).
    nom_one("The Case of Sutton's Hospital, 10 Co. 33", "n:corep:10:23",
            "Sutton")
    nom_one("Rex v. Vice-Chancellor of Cambridge, 3 Burr. 1656",
            "n:burr:3:1647", "Vice-Chancellor")
    check(iter_nominate_cites("Philips v. Bury, Holt 715") == [],
          "volumeless pin ('Holt 715') stays unlinked")
    check(iter_nominate_cites("the arid lands of the West 11 years") == [],
          "prose 'West 11' is never West's Chancery Reports")
    # A company's volumeless "Co. <number>" must never be claimed for Coke.
    check(iter_nominate_cites("the Insurance Co. 45 filing") == [],
          "no nominate claim on 'Insurance Co. 45'")
    # U.S. citations sharing an abbreviation must never be claimed: New York's
    # volumed Johnson ("5 Johns. 37") vs the volumeless English Johnson, a U.S.
    # reporter cite, and the denied "Curt." (Curtis' U.S. circuit reports).
    for us_text in ["Kilbourn v. Woodworth, 5 Johns. 37",
                    "Roe v. Wade, 410 U.S. 113", "306 Md. 556",
                    "In re X, 1 Curt. 344"]:
        check(iter_nominate_cites(us_text) == [],
              f"no nominate claim on {us_text!r}")
    # The Scotland, 105 U.S. 24, 31 (1882) cites the English limitation-of-
    # liability cases in the American short forms: "Dod." (Dods), "Hagg.
    # Adm." (Hag Adm), "Swa."/"Lush." with an explicit volume 1 the index
    # stores volumeless, "Kay & J." (K & J), "John. & H." (J & H) — and
    # "3 id. 169" continuing the reporter last cited (3 Hagg. Adm. 169).
    scotland = ("See The Nostra Signora de los Dolores, 1 Dod. 290; "
                "The Carl Johan, cited in The Dundee, 1 Hagg. Adm. 109, 113; "
                "The Girolamo, 3 id. 169, 186; The Zollverein, 1 Swa. 96; "
                "Cope v. Doherty, 4 Kay & J. 367; S.C. 2 De G. & J. 614; "
                "The General Iron Screw Collier Co. v. Schurmanns, "
                "1 John. & H. 180; The Wild Ranger, 1 Lush. 553.")
    # Bare "Hagg." (any of Haggard's courts) is placed by resolution: The
    # Nestor's "The William Money, 2 Hagg. 136" exists only in Haggard's
    # Admiralty, so it resolves there outright; a page shared by two series
    # ("1 Hagg. 22" is in both Adm and Ecc) keeps the alias key and yields
    # every candidate, for the same-page chooser.
    nom_one("The William Money, 2 Hagg. 136", "n:hagadm:2:136",
            "William Money")
    check(len(resolve("n:hagg:1:22")) >= 2
          and any(spec == "n:hagg:1:22"
                  for _s, _e, spec, _c in iter_nominate_cites("1 Hagg. 22")),
          "colliding bare 'Hagg.' page yields every candidate")
    # The John G. Stevens (170 U.S. 113) forms: "Moore P.C." is Moore's
    # Privy Council; "1 Dodson, 37" tolerates the comma; "Brown. & Lush. 89"
    # (The Europa) must resolve as Browning & Lushington and never fall
    # through to volumeless "Lush." (The Alpha, a different case).
    # (CommonLII captions The Bold Buccleugh as "Daniel Harmer, Appellant
    # …" and spells the Madonna D'Idra "D'Indra".)
    nom_one("The Bold Buccleugh, 7 Moore P.C. 267", "n:moopc:7:267",
            "Harmer")
    nom_one("The Madonna D'Idra, 1 Dodson, 37, 40", "n:dods:1:37", "Madonna")
    europa = iter_nominate_cites("The Europa, Brown. & Lush. 89, 91, 97")
    check(any(spec == "n:brlush:0:89" for _s, _e, spec, _c in europa)
          and not any("lush:0:89" in spec and "brlush" not in spec
                      for _s, _e, spec, _c in europa),
          f"Brown. & Lush. 89 is the Europa, not the Alpha (got "
          f"{[s for _a, _b, s, _c in europa]})")
    # Ramsay v. Allegre (25 U.S. 611) forms: "Lord Raym." for Ld Raym,
    # comma'd volumeless "Owen, 122", and the volumeless "Cowp. 636" —
    # Cowper's volumes are continuously paged, so page 636 places it in
    # volume 2 (Rich v. Coe) — while a volumeless *word* form in prose
    # ("East 426") stays unlinked.
    nom_one("Justen v. Ballam, 2 Lord Raym. 805", "n:ldraym:2:805", "Ballam")
    nom_one("Leigh against Burleigh, Owen, 122", "n:owen:0:122", "Leigh")
    nom_one("Rich v. Coe, Cowp. 636", "n:cowp:2:636", "Rich")
    check(iter_nominate_cites("travelling East 426 miles") == [],
          "volumeless word-form 'East 426' in prose stays unlinked")
    # Bare "Rob." can be Christopher or William Robinson; resolution decides
    # ("The John, 3 Rob. 288" exists only in C Rob).
    nom_one("The John, 3 Rob. 288", "n:crob:3:288", "John")
    nom_one(scotland, "n:dods:1:290", "Nostra Signora")
    nom_one(scotland, "n:hagadm:1:109", "Dundee")
    nom_one(scotland, "n:hagadm:3:169", "Girolamo")
    nom_one(scotland, "n:swab:0:96", "Zollverein")
    nom_one(scotland, "n:kj:4:367", "Cope")
    nom_one(scotland, "n:degj:2:614", "Cope")
    nom_one(scotland, "n:jh:1:180", "Schurman")
    nom_one(scotland, "n:lush:0:553", "Wild Ranger")
    check(len(iter_nominate_cites(scotland)) == 8,
          "the Scotland passage yields exactly its 8 citations")
    # Every emitted spec must round-trip through resolve().
    check(all(resolve(spec) for _s, _e, spec, _c in
              iter_nominate_cites(scotland)),
          "Scotland specs round-trip through resolve()")
    # An intervening citation breaks the id. chain: after "105 U.S. 24" the
    # "3 id. 169" means 3 U.S. 169, not 3 Hagg. Adm. 169.
    check(not any(spec.startswith("n:hagadm:3:")
                  for _s, _e, spec, _c in iter_nominate_cites(
                      "The Dundee, 1 Hagg. Adm. 109; The Scotland, "
                      "105 U.S. 24; The Girolamo, 3 id. 169")),
          "id. after an intervening citation stays unlinked")
    # A bare "id." with no preceding English cite is never claimed.
    check(iter_nominate_cites("See 3 id. 169.") == [],
          "id. with no antecedent stays unlinked")
    # An unresolved citation-shaped match ("Holt 715" is not indexed, and
    # the digit-then-capital gap regex cannot see a volumeless cite) still
    # breaks the chain: the id. refers to Holt, not Haggard.
    check(len(iter_nominate_cites(
        "The Dundee, 1 Hagg. Adm. 109; Philips v. Bury, Holt 715; "
        "The Girolamo, 3 id. 169")) == 1,
          "id. after an unresolved nominate look-alike stays unlinked")
    # Volume 1 to a truly multi-volume reporter never falls back to the
    # volume-0 rows ("1 Vern. 100": Vernon has real volumes 1-2, so its
    # stray volumeless rows prove nothing about volume 1).
    check(all(not spec.startswith("n:vern:0:")
              for _s, _e, spec, _c in iter_nominate_cites("1 Vern. 100")),
          "no volume-0 fallback for a multi-volume reporter")

    # --- name search ---
    def name_one(query, expect_substr):
        hits = search_by_name(query, limit=1)
        ok = bool(hits) and expect_substr.lower() in hits[0].name.lower()
        check(ok, f"name {query!r} -> {expect_substr!r} "
                  f"(got {hits[0].name[:40] if hits else None!r})")

    name_one("Hadley v Baxendale", "Hadley")        # "...v Baxendale and Others"
    name_one("Entick v Carrington", "Entick")       # "...versus Nathan Carrington..."
    name_one("Pinnel", "Pinnel")                    # possessive: "Pinnel's Case"
    name_one("Planche v Colburn", "Planche")
    # A U.S. case name (not in the English Reports) must not match anything.
    check(search_by_name("Monroe v Pape") == [], "U.S. name -> no match")
    check(search_by_name("") == [], "empty name -> no match")

    total = sum(len(v) for v in (_INDEX or {}).values())
    print(f"\nindex: {len(_INDEX or {}):,} pages, {total:,} cases")
    print(f"\n{'all tests passed' if not failed else str(failed)+' checks FAILED'}")
    raise SystemExit(1 if failed else 0)
