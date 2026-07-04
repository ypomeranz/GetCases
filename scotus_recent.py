"""Recent Supreme Court decisions from supremecourt.gov's homepage.

The Court's homepage carries a "Recent Decisions" panel: for each recently
decided case it lists the case name, docket number, decision date, a plain
plain-English one-paragraph description, and a link to the slip-opinion PDF.
This module fetches and parses that panel into :class:`RecentDecision` records,
with a short-lived on-disk cache so the panel opens instantly and the site is
polled at most a few times a day.

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
from pathlib import Path
from typing import Optional

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "scotus_recent requires 'requests' and 'beautifulsoup4'."
    ) from exc

HOME_URL = "https://www.supremecourt.gov/"
_BASE = "https://www.supremecourt.gov/"
_CACHE_PATH = Path.home() / ".cache" / "courtlistener_scotus_recent.json"
_CACHE_VERSION = 2
_CACHE_TTL = 6 * 3600  # seconds; the homepage updates on decision days only
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


def _abs(href: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith("#"):
        return ""
    if href.startswith("http"):
        return href
    return _BASE + href.lstrip("/")


def _clean(text: str) -> str:
    # The site uses a soft hyphen (U+00AD, often rendered as a box) at line
    # wraps and non-breaking spaces; normalize whitespace and drop soft hyphens.
    text = (text or "").replace("­", "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def parse_recent_decisions(html: str) -> list[RecentDecision]:
    """Parse the homepage HTML into recent-decision records, in the order the
    site lists them (newest first).  Only cases with an opinion PDF link are
    returned — the panel also carries order-list items, which have no opinion."""
    soup = BeautifulSoup(html or "", "html.parser")
    panel = (soup.find(id="opinionsbyday")
             or soup.find(id=lambda v: v and "RecentDecisions" in v)
             or soup)
    out: list[RecentDecision] = []
    seen: set[tuple[str, str]] = set()
    current_date = ""
    # Walk the panel in document order: date headers (span.soday) set the
    # running date; each case is a casenamerow + following casedetail, with the
    # opinion PDF in the preceding buttonrow.
    for el in panel.find_all(["span", "div"]):
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
