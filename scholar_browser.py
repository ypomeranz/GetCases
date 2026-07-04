"""Fetch Google Scholar pages through the user's real Firefox (Selenium).

Google Scholar's search endpoint validates the TLS/HTTP2 handshake, not just
cookies or the User-Agent: a real browser passes even from an IP that Scholar
has flagged, while every curl_cffi impersonation profile gets the "/sorry/
unusual traffic" challenge (verified 2026-07-03 — same IP, same google.com
cookies, real Firefox passed a fresh search and all curl_cffi fingerprints
429'd).  So when the impersonation layer in :mod:`google_scholar` is blocked,
it falls back here and drives the actual Firefox, whose genuine Gecko network
stack Scholar trusts.

Design:
  * A **dedicated** headless Firefox profile (under the app cache) — never the
    user's live profile, which Firefox locks while it is open.  Headless uses
    the same NSS/TLS stack as a headed Firefox, so the fingerprint Scholar
    sees is identical; only rendering is skipped.
  * Selenium 4's built-in driver manager downloads geckodriver on demand, so
    there is no manual setup beyond ``pip install selenium`` and Firefox.
  * The driver is started lazily and kept alive for reuse (start-up is slow);
    :func:`ScholarBrowser.close` / atexit tear it down.

Everything here is optional: :func:`available` is False when Selenium or
Firefox is missing, and the caller degrades to the local opinion DB /
CourtListener exactly as before.
"""

from __future__ import annotations

import atexit
import threading
import time
from pathlib import Path
from typing import Optional

_PROFILE_DIR = Path.home() / ".cache" / "courtlistener_scholar_ff"
_PAGE_LOAD_TIMEOUT = 40
_CONTENT_TIMEOUT = 20  # seconds to wait for results / opinion to appear

# Substrings that mark Google's bot challenge, checked against the URL + source.
_BLOCK_MARKERS = (
    "/sorry/", "unusual traffic", "not a robot",
    "id=\"recaptcha\"", "g-recaptcha",
)


class BrowserUnavailable(Exception):
    """Selenium or Firefox isn't usable here — caller links out / falls back."""


class BrowserBlocked(Exception):
    """Even the real browser hit Scholar's challenge (rare — usually a CAPTCHA
    the user must solve once in their normal Firefox)."""


def _firefox_binary() -> "Optional[str]":
    try:
        import eng_rep_pdf
        return eng_rep_pdf._firefox_exe()
    except Exception:
        return None


def _google_cookie_rows() -> list:
    """Full google.com cookie rows from the user's Firefox profile(s), as
    Selenium ``add_cookie`` dicts.  These carry the account/reputation state
    (chiefly ``NID``) that — together with a real Gecko fingerprint — is what
    lets a search pass; a fresh profile has the fingerprint but not the
    standing, so it is challenged.  Host-only ``__Host-*`` cookies are skipped
    (they can't be re-scoped)."""
    import os
    import shutil
    import sqlite3
    import tempfile
    try:
        import eng_rep_pdf
        dbs = eng_rep_pdf._firefox_cookie_dbs()
    except Exception:
        return []
    seen: set = set()
    rows: list = []
    for db in dbs:
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(suffix=".sqlite")
            os.close(fd)
            shutil.copy2(db, tmp)
            con = sqlite3.connect(tmp)
            for name, value, host, path, is_secure in con.execute(
                "SELECT name, value, host, path, isSecure FROM moz_cookies "
                "WHERE host LIKE '%google.com'"
            ):
                if name.startswith("__Host-") or name in seen:
                    continue
                seen.add(name)
                rows.append({
                    "name": name, "value": value,
                    "domain": host if host.startswith(".") else "." + host,
                    "path": path or "/",
                    "secure": bool(is_secure) or name.startswith("__Secure-"),
                })
            con.close()
        except Exception:
            continue
        finally:
            if tmp:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
    return rows


def available() -> bool:
    """True when a Scholar fetch through Firefox is possible here."""
    try:
        import selenium  # noqa: F401
    except ImportError:
        return False
    return _firefox_binary() is not None


class ScholarBrowser:
    """A lazily-started headless Firefox that fetches Scholar pages.  Not
    safe for concurrent use — the caller serializes fetches with a lock."""

    def __init__(self) -> None:
        self._driver = None
        self._lock = threading.Lock()
        atexit.register(self.close)

    # -- lifecycle -----------------------------------------------------------

    def _ensure_driver(self):
        if self._driver is not None:
            return self._driver
        try:
            from selenium import webdriver
            from selenium.webdriver.firefox.options import Options
            from selenium.webdriver.firefox.service import Service
        except ImportError as exc:  # pragma: no cover
            raise BrowserUnavailable(f"selenium missing: {exc}")

        binary = _firefox_binary()
        if not binary:
            raise BrowserUnavailable("Firefox not found")

        _PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        opts = Options()
        opts.add_argument("-headless")
        # A dedicated, persistent profile (accumulates its own good standing;
        # never touches the user's live, locked profile).
        opts.add_argument("-profile")
        opts.add_argument(str(_PROFILE_DIR))
        try:
            opts.binary_location = binary
        except Exception:
            pass
        # Trim obvious automation tells (harmless for the TLS-level block, but
        # cheap insurance against JS-level checks).
        for pref, val in (
            ("dom.webdriver.enabled", False),
            ("useAutomationExtension", False),
            ("general.useragent.override", None),  # keep Firefox's real UA
        ):
            if val is not None:
                opts.set_preference(pref, val)
        opts.set_preference("intl.accept_languages", "en-US, en")

        try:
            # Selenium Manager (4.6+) resolves geckodriver automatically.
            self._driver = webdriver.Firefox(options=opts, service=Service())
        except Exception as exc:
            raise BrowserUnavailable(f"could not start Firefox: {exc}")
        self._driver.set_page_load_timeout(_PAGE_LOAD_TIMEOUT)
        self._seed_cookies(self._driver)
        return self._driver

    def _seed_cookies(self, driver) -> None:
        """Copy the user's google.com cookies into this session, so the driven
        Firefox carries the same standing as their normal one.  Best-effort:
        cookies can only be set while on the domain, so we load the (laxly
        guarded) Scholar homepage first."""
        rows = _google_cookie_rows()
        if not rows:
            return
        try:
            driver.get("https://scholar.google.com/")
        except Exception:
            return
        added = 0
        for row in rows:
            try:
                driver.add_cookie(row)
                added += 1
            except Exception:
                # Retry without the domain (Selenium is strict about matching
                # the current host); Firefox then scopes it to google.com.
                try:
                    driver.add_cookie({k: v for k, v in row.items()
                                       if k != "domain"})
                    added += 1
                except Exception:
                    pass
        if added:
            print(f"[scholar-browser] seeded {added} google.com cookies")

    def close(self) -> None:
        drv, self._driver = self._driver, None
        if drv is not None:
            try:
                drv.quit()
            except Exception:
                pass

    def restart(self) -> None:
        self.close()
        self._ensure_driver()

    # -- fetch ---------------------------------------------------------------

    def fetch(self, url: str, wait_for: str = "") -> "tuple[str, str]":
        """Navigate to *url* and return ``(page_source, final_url)`` once the
        page's content has loaded.  ``wait_for`` is a CSS selector to wait for
        (the results container or the opinion div); when empty a sensible one
        is chosen from the URL.  Raises :class:`BrowserBlocked` on a challenge,
        :class:`BrowserUnavailable` when Firefox can't be driven."""
        from selenium.common.exceptions import (
            TimeoutException, WebDriverException,
        )

        with self._lock:
            driver = self._ensure_driver()
            if not wait_for:
                wait_for = ("#gs_opinion" if "scholar_case" in url
                            else "#gs_res_ccl, .gs_r")
            try:
                driver.get(url)
            except TimeoutException:
                pass  # partial load is fine — we inspect what arrived
            except WebDriverException as exc:
                # A dead driver (crash / profile hiccup): restart once.
                self.restart()
                driver = self._driver
                try:
                    driver.get(url)
                except Exception as exc2:
                    raise BrowserUnavailable(f"navigation failed: {exc2}")

            self._await_content(driver, wait_for)
            source = driver.page_source or ""
            final_url = driver.current_url or url
            low = source.lower()
            if ("/sorry/" in final_url.lower()
                    or any(m in low for m in _BLOCK_MARKERS)):
                raise BrowserBlocked()
            return source, final_url

    @staticmethod
    def _await_content(driver, wait_for: str) -> None:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        def ready(d) -> bool:
            # Any target selector present, or Scholar's "no results" notice,
            # or the challenge page — all terminal states.
            for sel in wait_for.split(","):
                sel = sel.strip()
                if sel and d.find_elements(By.CSS_SELECTOR, sel):
                    return True
            src = (d.page_source or "").lower()
            return ("did not match any" in src or "/sorry/" in d.current_url
                    or "unusual traffic" in src)

        try:
            WebDriverWait(driver, _CONTENT_TIMEOUT).until(ready)
        except Exception:
            pass  # timed out — return whatever is present; caller handles it


if __name__ == "__main__":  # pragma: no cover - live smoke test
    import sys

    print("available:", available())
    if not available():
        print("  (needs `pip install selenium` and Firefox)")
        sys.exit(0)
    q = sys.argv[1] if len(sys.argv) > 1 else '"347 U.S. 483"'
    from urllib.parse import quote_plus
    br = ScholarBrowser()
    try:
        t0 = time.perf_counter()
        html, final = br.fetch(
            f"https://scholar.google.com/scholar?q={quote_plus(q)}&as_sdt=4")
        dt = time.perf_counter() - t0
        has_rows = "gs_r" in html or "did not match" in html
        print(f"fetched {len(html):,} chars in {dt:.1f}s; final={final[:80]}")
        print("results present:", has_rows)
    except BrowserBlocked:
        print("BLOCKED even in real Firefox — solve the CAPTCHA in your normal "
              "Firefox once, then retry.")
    except BrowserUnavailable as exc:
        print("unavailable:", exc)
    finally:
        br.close()
