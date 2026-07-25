"""Recent Supreme Court decisions and slip-opinion archives.

The Court's homepage carries a "Recent Decisions" panel: for each recently
decided case it lists the case name, docket number, decision date, a plain
English one-paragraph description, and a link to the slip-opinion PDF.
This module fetches and parses that panel into :class:`RecentDecision` records,
with a short-lived on-disk cache so the panel opens instantly and the site is
polled at most a few times a day.  It also parses the Court's per-Term
slip-opinion tables and safely matches a case by docket, reporter citation, or
caption plus the exact decision date printed in the opinion.

Headless and dependency-light (``requests`` + ``beautifulsoup4``, both already
required by the app); no tkinter.  Run ``python -X utf8 scotus_recent.py`` for
an offline self-test (parses a bundled fixture) plus, with ``--live``, a real
fetch.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "scotus_recent requires 'requests' and 'beautifulsoup4'."
    ) from exc

HOME_URL = "https://www.supremecourt.gov/"
SLIP_INDEX_URL = (
    "https://www.supremecourt.gov/opinions/slipopinion/{term}"
)
_BASE = "https://www.supremecourt.gov/"
_CACHE_PATH = Path.home() / ".cache" / "courtlistener_scotus_recent.json"
_CACHE_VERSION = 3
_CACHE_TTL = 6 * 3600  # seconds; the homepage updates on decision days only
_SLIP_CACHE_VERSION = 1
_SLIP_CURRENT_TTL = 6 * 3600
_SLIP_PAST_TTL = 30 * 24 * 3600
_TIMEOUT = 25
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) "
       "Gecko/20100101 Firefox/144.0")

# "National Republican Senatorial Committee v. FEC (24-621)" — split the
# trailing "(docket)" off the case name.
_DOCKET_RE = re.compile(r"\s*\((\d{1,3}[-‐-―]\d{1,5}(?:,[^)]*)?)\)\s*$")


@dataclass
class RecentDecision:
    """One case from the homepage's Recent Decisions panel."""
    name: str            # "National Republican Senatorial Committee v. FEC"
    docket: str          # "24-621"
    date: str            # "June 30, 2026" (as printed)
    description: str     # the plain-English summary paragraph
    opinion_url: str     # absolute URL of the slip-opinion PDF ("" if none)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SlipOpinion:
    """One row from an October Term slip-opinion archive page."""

    term: str            # two-digit October Term slug, e.g. "24"
    release: str         # release number in the Court's table
    date: str            # ISO decision date when parseable
    docket: str          # normalized ASCII-hyphen docket
    name: str
    opinion_url: str
    citation: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _abs(href: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith("#"):
        return ""
    return urljoin(_BASE, href)


def _clean(text: str) -> str:
    # The site uses a soft hyphen (U+00AD, often rendered as a box) at line
    # wraps and non-breaking spaces; normalize whitespace and drop soft hyphens.
    text = (text or "").replace("­", "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


_DASH_RE = re.compile(r"[\u2010-\u2015\u2212]")
_DOCKET_TOKEN_RE = re.compile(
    r"\b(?:\d{1,3}-\d{1,5}|\d{1,3}[A-Z]\d{1,5})\b", re.IGNORECASE,
)
_US_CITE_RE = re.compile(
    r"\b(\d{1,3})\s+U\.?\s*S\.?\s+(\d{1,5})\b", re.IGNORECASE,
)


def _normalize_docket(value: str) -> str:
    return re.sub(r"\s+", "", _DASH_RE.sub("-", value or "")).upper()


def _docket_tokens(value: str) -> set[str]:
    normalized = _normalize_docket(value)
    return {
        _normalize_docket(m.group(0))
        for m in _DOCKET_TOKEN_RE.finditer(normalized)
    }


def _date_iso(value: str) -> str:
    text = _clean(value).rstrip(".")
    if not text:
        return ""
    text = re.sub(
        r"^(?:Decided|Filed|Released)\s+", "", text, flags=re.IGNORECASE,
    )
    for fmt in (
        "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y",
        "%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y",
    ):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    m = re.search(r"\b(20\d{2})\b", text)
    return m.group(1) if m else ""


def _cite_key(value: str) -> tuple[str, str] | None:
    m = _US_CITE_RE.search(value or "")
    return (m.group(1), m.group(2)) if m else None


def parse_slip_opinions(html: str, term: str) -> list[SlipOpinion]:
    """Parse an October Term archive table from ``slipopinion/<term>``.

    The Court occasionally adds a second PDF link for a revision notice.
    The case-name link is selected instead of those date-labelled links.
    """
    term = re.sub(r"\D", "", str(term or ""))[-2:]
    soup = BeautifulSoup(html or "", "html.parser")
    out: list[SlipOpinion] = []
    seen: set[str] = set()
    for row in soup.find_all("tr"):
        pdf_links = [
            a for a in row.find_all("a", href=True)
            if re.search(
                r"/opinions/\d{2}pdf/[^?#]+\.pdf(?:[?#].*)?$",
                _abs(a.get("href")), re.IGNORECASE,
            )
        ]
        if not pdf_links:
            continue
        opinion_link = next(
            (
                a for a in pdf_links
                if _clean(a.get_text())
                and not re.fullmatch(
                    r"\d{1,2}/\d{1,2}/\d{2,4}", _clean(a.get_text())
                )
            ),
            pdf_links[0],
        )
        url = _abs(opinion_link.get("href"))
        if not url or url in seen:
            continue
        cells = row.find_all(["td", "th"])
        name_cell = opinion_link.find_parent(["td", "th"])
        if name_cell is None or name_cell not in cells:
            continue
        name_ix = cells.index(name_cell)
        if name_ix < 2:
            continue
        texts = [_clean(cell.get_text(" ", strip=True)) for cell in cells]
        name = _clean(opinion_link.get_text(" ", strip=True))
        if not name:
            continue
        docket = texts[name_ix - 1]
        decision_date = texts[name_ix - 2]
        release = texts[name_ix - 3] if name_ix >= 3 else ""
        citation = texts[-1] if len(texts) > name_ix + 2 else ""
        seen.add(url)
        out.append(SlipOpinion(
            term=term,
            release=release,
            date=_date_iso(decision_date),
            docket=_normalize_docket(docket),
            name=name,
            opinion_url=url,
            citation=citation,
        ))
    return out


def _term_slug(value: str | int) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) >= 4:
        digits = digits[-2:]
    return digits.zfill(2) if digits else ""


def _current_term_year() -> int:
    today = date.today()
    return today.year if today.month >= 10 else today.year - 1


def _slip_cache_path(term: str) -> Path:
    return (
        Path.home() / ".cache"
        / f"courtlistener_scotus_slip_{_term_slug(term)}.json"
    )


def _read_slip_cache(
    term: str, *, allow_stale: bool = False,
) -> "Optional[list[SlipOpinion]]":
    try:
        blob = json.loads(
            _slip_cache_path(term).read_text(encoding="utf-8")
        )
        if blob.get("version") != _SLIP_CACHE_VERSION:
            return None
        term_year = 2000 + int(_term_slug(term))
        ttl = (
            _SLIP_CURRENT_TTL
            if term_year >= _current_term_year()
            else _SLIP_PAST_TTL
        )
        if (
            not allow_stale
            and time.time() - float(blob.get("fetched_at") or 0) > ttl
        ):
            return None
        return [SlipOpinion(**item) for item in (blob.get("items") or [])]
    except Exception:
        return None


def _write_slip_cache(term: str, items: list[SlipOpinion]) -> None:
    try:
        path = _slip_cache_path(term)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "version": _SLIP_CACHE_VERSION,
            "fetched_at": time.time(),
            "term": _term_slug(term),
            "items": [item.to_dict() for item in items],
        }), encoding="utf-8")
    except Exception:
        pass


def fetch_slip_opinions(
    term: str | int, *, force: bool = False, session=None,
) -> list[SlipOpinion]:
    """All opinions on one October Term archive page.

    A current-term index is cached for six hours; completed terms are cached
    for thirty days.  A stale cache is used when supremecourt.gov is
    temporarily unavailable.
    """
    slug = _term_slug(term)
    if not slug:
        return []
    if not force:
        cached = _read_slip_cache(slug)
        if cached is not None:
            return cached
    url = SLIP_INDEX_URL.format(term=slug)
    try:
        get = session.get if session is not None else requests.get
        resp = get(url, headers={"User-Agent": _UA}, timeout=_TIMEOUT)
        resp.raise_for_status()
        items = parse_slip_opinions(resp.text, slug)
    except Exception as exc:
        print(f"[scotus] slip-opinion index {slug} failed: {exc}")
        return _read_slip_cache(slug, allow_stale=True) or []
    if items:
        _write_slip_cache(slug, items)
    return items


def _default_name_score(query: str, candidate: str) -> float:
    def tokens(value: str) -> set[str]:
        return {
            word for word in re.findall(r"[a-z0-9]+", (value or "").lower())
            if len(word) > 1 and word not in {
                "the", "of", "and", "a", "an", "v", "vs", "inc", "llc",
                "corp", "company", "co", "et", "al",
            }
        }

    wanted = tokens(query)
    found = tokens(candidate)
    return (len(wanted & found) / len(wanted)) if wanted else 0.0


def _candidate_terms(date_filed: str, docket: str) -> list[str]:
    """Likely archive slugs, strongest first, from date/year and docket."""
    out: list[int] = []

    def add(year: int) -> None:
        if 2018 <= year <= _current_term_year() and year not in out:
            out.append(year)

    parsed = _date_iso(date_filed)
    year = int(parsed[:4]) if parsed[:4].isdigit() else 0
    if len(parsed) == 10:
        month = int(parsed[5:7])
        add(year if month >= 10 else year - 1)
    if year:
        # A saved Scholar record may know only the decision year and represent
        # it as January 1.  Try both October Terms that can issue in that year.
        add(year - 1)
        add(year)
    docket_terms = {
        int(token[:2]) + 2000
        for token in _docket_tokens(docket)
        if token[:2].isdigit()
    }
    for docket_year in sorted(docket_terms, reverse=True):
        add(docket_year)
        add(docket_year + 1)
    return [_term_slug(year) for year in out]


def find_slip_opinion(
    *,
    name: str = "",
    docket: str = "",
    date_filed: str = "",
    citations: tuple[str, ...] | list[str] = (),
    session=None,
    name_scorer=None,
) -> Optional[SlipOpinion]:
    """Match a case to the Court's archived opinion PDF.

    The exact decision date is required even when the docket or U.S.-Reports
    citation matches.  A merits opinion and a later order or separate writing
    can share the same docket, so identity metadata alone is not conclusive.
    Within that date, docket, citation, or a strong caption match identifies
    the archive row.
    """
    parsed_date = _date_iso(date_filed)
    if len(parsed_date) != 10:
        return None
    terms = _candidate_terms(date_filed, docket)
    if not terms:
        return None
    wanted_dockets = _docket_tokens(docket)
    wanted_cites = {
        key for key in (_cite_key(str(c)) for c in citations) if key
    }
    scorer = name_scorer or _default_name_score
    rated: list[tuple[float, SlipOpinion]] = []
    for term in terms:
        for item in fetch_slip_opinions(term, session=session):
            if item.date != parsed_date:
                continue
            docket_match = bool(
                wanted_dockets & _docket_tokens(item.docket)
            )
            cite_key = _cite_key(item.citation)
            cite_match = bool(cite_key and cite_key in wanted_cites)
            name_score = (
                float(scorer(name, item.name)) if name and item.name else 0.0
            )
            if not (docket_match or cite_match or name_score >= 0.65):
                continue
            score = (
                (1000.0 if docket_match else 0.0)
                + (900.0 if cite_match else 0.0)
                + name_score * 30.0
            )
            rated.append((score, item))
    if not rated:
        return None
    rated.sort(key=lambda pair: pair[0], reverse=True)
    return rated[0][1]


def parse_recent_decisions(html: str) -> list[RecentDecision]:
    """Parse the homepage HTML into recent-decision records, in the order the
    site lists them (newest first).  Only cases with an opinion PDF link are
    returned — the panel also carries order-list items, which have no opinion."""
    soup = BeautifulSoup(html or "", "html.parser")
    panels = soup.find_all(id="opinionsbyday")
    if not panels:
        panel = (soup.find(id=lambda v: v and "RecentDecisions" in v)
                 or soup)
        panels = [panel]
    out: list[RecentDecision] = []
    seen: set[tuple[str, str]] = set()
    current_date = ""
    # Walk the panel in document order: date headers (span.soday) set the
    # running date; each case is a casenamerow + following casedetail, with the
    # opinion PDF in the preceding buttonrow.
    nodes = (node for panel in panels for node in panel.find_all(["span", "div"]))
    for el in nodes:
        classes = el.get("class") or []
        if "soday" in classes:
            current_date = _clean(el.get_text())
            continue
        if "casenamerow" not in classes:
            continue
        name_raw = _clean(el.get_text())
        if not name_raw:
            continue
        docket = ""
        m = _DOCKET_RE.search(name_raw)
        if m:
            docket = re.sub(r"[‐-―]", "-", m.group(1)).strip()
            name = name_raw[: m.start()].strip()
        else:
            name = name_raw
        # The description is the next casedetail sibling.
        description = ""
        sib = el.find_next_sibling()
        while sib is not None:
            sc = sib.get("class") or []
            if "casedetail" in sc:
                description = _clean(sib.get_text())
                break
            if "casenamerow" in sc or "buttonrow" in sc:
                break  # ran into the next case — no detail for this one
            sib = sib.find_next_sibling()
        # The opinion PDF link lives in the preceding buttonrow.
        opinion_url = ""
        prev = el.find_previous_sibling()
        while prev is not None:
            pc = prev.get("class") or []
            if "buttonrow" in pc:
                a = prev.find("a", href=re.compile(r"opinions/.*\.pdf$",
                                                    re.IGNORECASE))
                if a:
                    opinion_url = _abs(a.get("href"))
                break
            if "casenamerow" in pc:
                break
            prev = prev.find_previous_sibling()
        if not opinion_url:
            continue  # an order or a case without a released opinion
        key = (docket or name, opinion_url)
        if key in seen:
            continue
        seen.add(key)
        out.append(RecentDecision(
            name=name, docket=docket, date=current_date,
            description=description, opinion_url=opinion_url,
        ))
    return out


def _read_cache() -> "Optional[list[RecentDecision]]":
    try:
        blob = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
        if blob.get("version") != _CACHE_VERSION:
            return None
        if time.time() - blob.get("fetched_at", 0) > _CACHE_TTL:
            return None
        items = blob.get("items") or []
        return [RecentDecision(**it) for it in items]
    except Exception:
        return None


def _write_cache(items: list[RecentDecision]) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(json.dumps({
            "version": _CACHE_VERSION,
            "fetched_at": time.time(),
            "items": [it.to_dict() for it in items],
        }), encoding="utf-8")
    except Exception:
        pass


def fetch_recent_decisions(
    *, force: bool = False, session=None,
) -> list[RecentDecision]:
    """Recent decisions, from the cache when fresh, else the live homepage.
    Returns ``[]`` on any failure (the caller shows nothing / links out)."""
    if not force:
        cached = _read_cache()
        if cached is not None:
            return cached
    try:
        get = session.get if session is not None else requests.get
        resp = get(HOME_URL, headers={"User-Agent": _UA}, timeout=_TIMEOUT)
        resp.raise_for_status()
        items = parse_recent_decisions(resp.text)
    except Exception as exc:
        print(f"[scotus] recent-decisions fetch failed: {exc}")
        stale = _read_cache_stale()
        return stale or []
    if items:
        _write_cache(items)
    return items


def _read_cache_stale() -> "Optional[list[RecentDecision]]":
    """Cache contents ignoring the TTL — a fetch failure is better served by
    a day-old list than by nothing."""
    try:
        blob = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
        if blob.get("version") != _CACHE_VERSION:
            return None
        return [RecentDecision(**it) for it in (blob.get("items") or [])]
    except Exception:
        return None


if __name__ == "__main__":  # pragma: no cover - offline smoke test
    import sys

    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        if not cond:
            failures.append(msg)
        print(("ok   " if cond else "FAIL ") + msg)

    FIXTURE = """
    <div id="opinionsbyday">
      <span class="soday">June 30, 2026</span>
      <div class="buttonrow">
        <a href="opinions/25pdf/24-43_2b35.pdf" target="_blank">op</a>
        <a href='#' onclick="x()">docket</a>
      </div>
      <div class="casenamerow"><span>West Virginia v. B.&nbsp;P.&nbsp;J. (24-43)</span></div>
      <div class="casedetail"><span>Title IX allows schools to provide sepa&shy;rate teams.</span></div>
      <div class="buttonrow">
        <a href="opinions/25pdf/24-621_h315.pdf">op</a>
      </div>
      <div class="casenamerow"><span>NRSC v. FEC (24-621)</span></div>
      <div class="casedetail"><span>FECA limits violate the First Amendment.</span></div>
      <div class="buttonrow"></div>
      <div class="casenamerow"><span>Some Order (25-100)</span></div>
      <div class="casedetail"><span>No opinion released.</span></div>
    </div>
    """
    items = parse_recent_decisions(FIXTURE)
    check(len(items) == 2, f"two opinions parsed (orders skipped): {len(items)}")
    check(items[0].name == "West Virginia v. B. P. J.",
          f"name split from docket + soft-hyphen cleaned: {items[0].name!r}")
    check(items[0].docket == "24-43", f"docket: {items[0].docket!r}")
    check(items[0].date == "June 30, 2026", f"date: {items[0].date!r}")
    check("separate teams" in items[0].description,
          f"soft hyphen removed in detail: {items[0].description!r}")
    check(items[0].opinion_url ==
          "https://www.supremecourt.gov/opinions/25pdf/24-43_2b35.pdf",
          f"opinion url absolutized: {items[0].opinion_url!r}")
    check(items[1].docket == "24-621", f"second docket: {items[1].docket!r}")
    check(all("Order" not in it.name for it in items),
          "the order without an opinion PDF is dropped")

    if "--live" in sys.argv:
        print("\n--- live fetch ---")
        live = fetch_recent_decisions(force=True)
        print(f"fetched {len(live)} decisions")
        for it in live[:5]:
            print(f"  {it.date} | {it.name} ({it.docket})")
            print(f"      {it.description[:90]}")
            print(f"      {it.opinion_url}")
        check(len(live) > 0, "live homepage returned at least one decision")

    if failures:
        print(f"\n{len(failures)} FAILED")
        sys.exit(1)
    print("\nOK: scotus_recent smoke test passed")
