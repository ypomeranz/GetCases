"""
Google Scholar Opinion Fetcher
==============================
Fetches US case law opinion text from Google Scholar.

Strategy:
  1. Search scholar.google.com/scholar?q="citation"&as_sdt=4 for the citation.
  2. Pull the first scholar_case link from results.
  3. Scrape the #gs_opinion div from that page.
  4. Cache everything in a local SQLite database to avoid re-fetching.

Requires:
    pip install requests beautifulsoup4
"""

from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

try:
    import requests
    from bs4 import BeautifulSoup
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
        self._db.commit()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_by_citation(self, citation: str) -> Optional[tuple[str, str]]:
        """
        Fetch opinion text by citation string (e.g. "410 U.S. 113").

        Returns (scholar_url, plain_text) or None if not found / blocked.
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
        Fetch opinion text by case name, optionally scoped to a year.

        Returns (scholar_url, plain_text) or None.
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
        """Fetch a scholar_case page and extract plain opinion text."""
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

        text = opinion_div.get_text(separator="\n\n")
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        print(f"[scholar] extracted {len(text):,} chars of opinion text")
        return (url, text)

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def _cache_get(self, key: str) -> Optional[tuple[str, str]]:
        row = self._db.execute(
            "SELECT case_url, text FROM opinions WHERE cache_key=?", (key,)
        ).fetchone()
        return row  # (url, text) or None

    def _cache_put(self, key: str, url: str, text: str) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO opinions (cache_key, case_url, text, fetched_at) "
            "VALUES (?, ?, ?, ?)",
            (key, url, text, time.time()),
        )
        self._db.commit()
