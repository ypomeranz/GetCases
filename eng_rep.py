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

Only the citation/name/URL facts are shipped here (an index, like a citator);
the PDFs themselves stay on CommonLII and are fetched on demand, with
attribution, by the viewer.

To regenerate the index: see ``_engr_build/parse_engr.py`` then
``_engr_build/build_index.py``.

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
_LOCK = threading.Lock()


def _index_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), INDEX_FILENAME)


def _load() -> None:
    """Populate the in-memory index from the gzipped TSV (idempotent).  Any
    failure (missing/corrupt file) leaves an empty index so the app still runs;
    E.R. citations just won't resolve."""
    global _INDEX, _VOL_PAGES
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
        _INDEX = idx


def warm() -> None:
    """Load the index in a background thread (call at GUI start so the first
    click is instant).  Best-effort."""
    threading.Thread(target=_load, daemon=True).start()


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
    """Candidates for a '<vol>:<page>' spec (exact start page)."""
    vp = parse_spec(spec)
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

    total = sum(len(v) for v in (_INDEX or {}).values())
    print(f"\nindex: {len(_INDEX or {}):,} pages, {total:,} cases")
    print(f"\n{'all tests passed' if not failed else str(failed)+' checks FAILED'}")
    raise SystemExit(1 if failed else 0)
