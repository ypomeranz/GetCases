"""Fetch and cache English Reports PDFs from CommonLII, which is behind
CloudFlare.

CommonLII tar-pits every scripted request: a plain ``requests`` GET (even with a
valid ``cf_clearance`` cookie and a matching User-Agent) is rejected because
CloudFlare fingerprints the TLS handshake (JA3).  The combination that actually
works -- discovered empirically -- is:

  1. The user clears the "Just a moment..." check once **in Firefox** (whose
     cookies, unlike Chrome's app-bound-encrypted store, are readable without
     admin).  This is the only manual step, and only when there is no valid
     clearance yet.
  2. We read Firefox's ``cf_clearance`` (+ ``__cf_bm``/``__cflb``) straight from
     the profile's ``cookies.sqlite`` (Firefox stores cookies unencrypted).  We
     search every profile location ourselves -- standard, Microsoft Store,
     custom ``profiles.ini`` paths, Snap/Flatpak -- so this works where
     ``browser_cookie3``'s single hard-coded path fails ("Could not find firefox
     profile directory"); ``browser_cookie3`` remains a last-ditch fallback.
  3. We GET the PDF with ``curl_cffi`` impersonating Firefox's TLS fingerprint,
     sending the user's real Firefox User-Agent (so it matches the cookie) and a
     ``Referer`` to the case's ``.html`` page -- the origin Apache hotlink-blocks
     PDFs requested without it.
  4. The bytes are cached on disk, so the same case never needs the network (or
     the captcha) again.

All of this is optional: ``curl_cffi`` and Firefox may be absent, in which case
:func:`can_fetch` is False and the caller falls back to simply opening the
citation in the user's browser.

No tkinter here -- the GUI drives the user-facing hand-off/retry; this module is
the headless fetch+cache engine.  Run ``python -X utf8 eng_rep_pdf.py`` to fetch
a sample PDF live.
"""

from __future__ import annotations

import configparser
import glob
import os
import re
import subprocess
import sys
import threading
import webbrowser
from pathlib import Path
from typing import Optional

# CommonLII case PDFs land here; keyed by the neutral cite (year-num), which is
# unique per case.  Sits next to the app's existing config file.
CACHE_DIR = Path.home() / ".config" / "courtlistener" / "engr_cache"

_TIMEOUT = 45


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------

class FetchUnavailable(Exception):
    """In-app fetching isn't possible here (Firefox or a dependency missing);
    the caller should just open the citation in the browser."""


class CloudflareChallenge(Exception):
    """CommonLII served a CloudFlare challenge -- the user must clear it in
    Firefox.  ``web_url`` is the page to open for them to do so."""

    def __init__(self, web_url: str):
        super().__init__("CloudFlare challenge")
        self.web_url = web_url


class OriginError(Exception):
    """CloudFlare was passed but the origin returned an error (status code)."""

    def __init__(self, status: int):
        super().__init__(f"origin returned HTTP {status}")
        self.status = status


# ---------------------------------------------------------------------------
# Firefox discovery + User-Agent
# ---------------------------------------------------------------------------

def _firefox_exe() -> Optional[str]:
    """Path to firefox.exe / firefox, or None when Firefox isn't installed."""
    import shutil
    found = shutil.which("firefox")
    if found:
        return found
    cands = [
        r"C:\Program Files\Mozilla Firefox\firefox.exe",
        r"C:\Program Files (x86)\Mozilla Firefox\firefox.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Mozilla Firefox\firefox.exe"),
        "/Applications/Firefox.app/Contents/MacOS/firefox",
        "/usr/bin/firefox",
        "/usr/local/bin/firefox",
    ]
    for pat in cands:
        for hit in glob.glob(pat):
            if os.path.exists(hit):
                return hit
    return None


_UA_CACHE: Optional[str] = None


def firefox_user_agent() -> str:
    """The user's real Firefox UA, derived from the install's version so it
    matches the UA Firefox used to obtain ``cf_clearance``.  Falls back to a
    recent UA if the version can't be read."""
    global _UA_CACHE
    if _UA_CACHE:
        return _UA_CACHE
    major = "128"
    exe = _firefox_exe()
    if exe:
        ini = os.path.join(os.path.dirname(exe), "application.ini")
        try:
            cp = configparser.ConfigParser()
            with open(ini, encoding="utf-8") as fh:
                cp.read_file(fh)
            ver = cp.get("App", "Version", fallback="")
            m = re.match(r"(\d+)", ver)
            if m:
                major = m.group(1)
        except Exception:
            pass
    _UA_CACHE = (f"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{major}.0) "
                 f"Gecko/20100101 Firefox/{major}.0")
    return _UA_CACHE


def firefox_available() -> bool:
    return _firefox_exe() is not None


def open_in_firefox(url: str) -> bool:
    """Open *url* in Firefox so the user can clear CloudFlare.  Returns False if
    Firefox couldn't be launched."""
    exe = _firefox_exe()
    if not exe:
        return False
    try:
        subprocess.Popen([exe, url])
        return True
    except Exception:
        return False


def open_in_browser(url: str) -> None:
    """Open *url* in the user's default browser (the no-Firefox fallback)."""
    try:
        webbrowser.open(url)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Optional dependencies (imported lazily so the app runs without them)
# ---------------------------------------------------------------------------

def _have(mod: str) -> bool:
    import importlib.util
    return importlib.util.find_spec(mod) is not None


def deps_present() -> bool:
    # curl_cffi does the TLS-impersonating fetch.  The Firefox clearance cookie
    # is read straight from cookies.sqlite (Firefox stores cookies unencrypted),
    # so browser_cookie3 is only an optional fallback now, not a hard requirement.
    return _have("curl_cffi")


def can_fetch() -> bool:
    """True when in-app fetching is possible: Firefox installed and both
    optional packages importable.  When False the caller links out instead."""
    return firefox_available() and deps_present()


_IMPERSONATE_CACHE: Optional[str] = None


def _impersonate_target() -> str:
    """The newest Firefox TLS-fingerprint target curl_cffi offers (its JA3 is
    stable across Firefox versions, so the latest available matches a newer
    installed Firefox closely enough to pass CloudFlare)."""
    global _IMPERSONATE_CACHE
    if _IMPERSONATE_CACHE:
        return _IMPERSONATE_CACHE
    target = "firefox"
    try:
        import typing
        from curl_cffi.requests.impersonate import BrowserTypeLiteral
        ffs = []
        for t in typing.get_args(BrowserTypeLiteral):
            m = re.fullmatch(r"firefox(\d+)", t)
            if m:
                ffs.append((int(m.group(1)), t))
        if ffs:
            target = max(ffs)[1]
    except Exception:
        pass
    _IMPERSONATE_CACHE = target
    return target


def _safe_mtime(p: Path) -> float:
    try:
        return p.stat().st_mtime
    except OSError:
        return 0.0


def _firefox_data_roots() -> "list[Path]":
    """Every directory a Firefox profile tree might live under -- far more
    thorough than browser_cookie3's single hard-coded location, which is why it
    raises "Could not find firefox profile directory" for these layouts:

      * Windows standard      %APPDATA%/%LOCALAPPDATA%\\Mozilla\\Firefox
      * Windows Microsoft Store %LOCALAPPDATA%\\Packages\\Mozilla.Firefox_*\\...
      * macOS                 ~/Library/Application Support/Firefox
      * Linux native / Snap / Flatpak
    """
    roots: list[Path] = []
    home = Path.home()
    if sys.platform == "win32":
        for env in ("APPDATA", "LOCALAPPDATA"):
            base = os.environ.get(env)
            if base:
                roots.append(Path(base) / "Mozilla" / "Firefox")
        local = os.environ.get("LOCALAPPDATA")
        if local:
            # Microsoft Store package keeps its own Roaming tree.
            roots += [Path(p) for p in glob.glob(os.path.join(
                local, "Packages", "Mozilla.Firefox_*", "LocalCache",
                "Roaming", "Mozilla", "Firefox"))]
    elif sys.platform == "darwin":
        roots.append(home / "Library" / "Application Support" / "Firefox")
    else:
        roots += [
            home / ".mozilla" / "firefox",
            home / "snap" / "firefox" / "common" / ".mozilla" / "firefox",
            home / ".var" / "app" / "org.mozilla.firefox" / ".mozilla" / "firefox",
        ]
    seen: set[str] = set()
    out: list[Path] = []
    for r in roots:
        key = os.path.normcase(str(r))
        if key not in seen and r.is_dir():
            seen.add(key)
            out.append(r)
    return out


def _profiles_ini_cookie_dbs(root: Path) -> "list[Path]":
    """cookies.sqlite for every profile named in *root*/profiles.ini -- the
    authoritative list, and the only way to find profiles stored outside the
    usual ``Profiles`` folder (a custom ``IsRelative=0`` path)."""
    ini = root / "profiles.ini"
    if not ini.is_file():
        return []
    cp = configparser.ConfigParser()
    try:
        cp.read(str(ini), encoding="utf-8")
    except Exception:
        return []
    out: list[Path] = []
    for sec in cp.sections():
        path = cp.get(sec, "Path", fallback="").strip()
        if not path:
            continue
        relative = cp.get(sec, "IsRelative", fallback="1").strip() != "0"
        prof = (root / path) if relative else Path(path)
        out.append(prof / "cookies.sqlite")
    return out


def _firefox_cookie_dbs() -> "list[Path]":
    """All Firefox cookies.sqlite files we can find, newest first (the profile
    the user just cleared CloudFlare in has the freshest mtime)."""
    dbs: list[Path] = []
    seen: set[str] = set()
    for root in _firefox_data_roots():
        cands = _profiles_ini_cookie_dbs(root)
        for pat in ("Profiles/*/cookies.sqlite", "*/cookies.sqlite",
                    "cookies.sqlite"):
            cands += [Path(p) for p in glob.glob(str(root / pat))]
        for db in cands:
            key = os.path.normcase(str(db))
            if key not in seen and db.is_file():
                seen.add(key)
                dbs.append(db)
    dbs.sort(key=_safe_mtime, reverse=True)
    return dbs


def _read_cookies_sqlite(db: Path, domain_substr: str) -> dict:
    """{name: value} for hosts containing *domain_substr*, read straight from a
    Firefox cookies.sqlite.  Firefox stores cookies unencrypted, so no
    browser_cookie3/decryption is needed.  The DB (and any -wal/-shm) is copied
    first so a running Firefox can't block the read and recent writes (the
    just-obtained clearance) are visible."""
    import shutil
    import sqlite3
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix="engr_ff_")
    try:
        tmp = os.path.join(tmpdir, "cookies.sqlite")
        shutil.copyfile(str(db), tmp)
        for ext in ("-wal", "-shm"):
            side = Path(str(db) + ext)
            if side.is_file():
                try:
                    shutil.copyfile(str(side), tmp + ext)
                except OSError:
                    pass
        con = sqlite3.connect(tmp)
        try:
            rows = con.execute(
                "SELECT name, value FROM moz_cookies WHERE host LIKE ?",
                (f"%{domain_substr}%",),
            ).fetchall()
        finally:
            con.close()
        return {name: value for name, value in rows}
    except Exception as exc:
        print(f"[eng_rep_pdf] reading {db} failed: {exc}")
        return {}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _firefox_cookies() -> Optional[dict]:
    """{name: value} of commonlii.org cookies from Firefox, or None.  Must
    include ``cf_clearance`` to be useful.

    Searches every known Firefox profile location (newest first) and reads
    cookies.sqlite directly -- so a Microsoft Store install or a non-default
    profile path, which trip browser_cookie3's "Could not find firefox profile
    directory", are handled.  Falls back to browser_cookie3 for any exotic
    layout the direct search misses."""
    best: Optional[dict] = None
    dbs = _firefox_cookie_dbs()
    for db in dbs:
        cookies = _read_cookies_sqlite(db, "commonlii")
        if not cookies:
            continue
        if "cf_clearance" in cookies:
            return cookies          # the profile that holds the clearance
        best = best or cookies      # commonlii cookies but no clearance yet
    if not dbs:
        print("[eng_rep_pdf] no Firefox cookies.sqlite found in: "
              + ", ".join(str(r) for r in _firefox_data_roots()))
    # Fallback: browser_cookie3 (covers any layout our globs/profiles.ini miss).
    try:
        import browser_cookie3
        cj = browser_cookie3.firefox(domain_name="commonlii.org")
        cookies = {c.name: c.value for c in cj if "commonlii" in (c.domain or "")}
        if "cf_clearance" in cookies:
            return cookies
        best = best or (cookies or None)
    except Exception as exc:
        print(f"[eng_rep_pdf] browser_cookie3 fallback failed: {exc}")
    return best


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def cache_path(year: int, num: int) -> Path:
    return CACHE_DIR / f"{year}-{num}.pdf"


def get_cached(year: int, num: int) -> Optional[bytes]:
    p = cache_path(year, num)
    try:
        if p.is_file() and p.stat().st_size > 0:
            data = p.read_bytes()
            if data[:4] == b"%PDF":
                return data
    except Exception:
        pass
    return None


def _store(year: int, num: int, data: bytes) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = cache_path(year, num).with_suffix(".pdf.part")
        tmp.write_bytes(data)
        tmp.replace(cache_path(year, num))
    except Exception as exc:
        print(f"[eng_rep_pdf] cache write failed: {exc}")


def is_cached(year: int, num: int) -> bool:
    return get_cached(year, num) is not None


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

_CHALLENGE_MARKERS = (b"Just a moment", b"challenge-platform",
                      b"cf-browser-verification", b"__cf_chl")


def fetch_pdf(year: int, num: int, web_url: str) -> bytes:
    """Return the PDF bytes for a CommonLII case, from cache or the network.

    Raises :class:`FetchUnavailable` when in-app fetching isn't possible (the
    caller links out), :class:`CloudflareChallenge` when the user must clear the
    check in Firefox, or :class:`OriginError` on an origin HTTP error.
    Successful fetches are cached.
    """
    cached = get_cached(year, num)
    if cached is not None:
        return cached

    if not can_fetch():
        raise FetchUnavailable()

    cookies = _firefox_cookies()
    if not cookies or "cf_clearance" not in cookies:
        # No clearance yet -- the user has to pass the check in Firefox first.
        raise CloudflareChallenge(web_url)

    from curl_cffi import requests as creq

    pdf_url = re.sub(r"\.html?$", ".pdf", web_url)
    headers = {
        "User-Agent": firefox_user_agent(),
        "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                   "image/avif,image/webp,*/*;q=0.8"),
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": web_url,  # origin Apache hotlink-blocks PDFs without this
    }
    try:
        resp = creq.get(pdf_url, headers=headers, cookies=cookies,
                        impersonate=_impersonate_target(), timeout=_TIMEOUT)
    except Exception as exc:
        raise OriginError(0) from exc

    data = resp.content or b""
    if resp.status_code == 200 and data[:4] == b"%PDF":
        _store(year, num, data)
        return data

    # CloudFlare re-challenge (cookie expired / fingerprint mismatch) -> user
    # must re-clear in Firefox; a bare origin error -> surface the status.
    if resp.status_code in (403, 503) and any(mk in data for mk in _CHALLENGE_MARKERS):
        raise CloudflareChallenge(web_url)
    raise OriginError(resp.status_code)


# ---------------------------------------------------------------------------
# Live test:  python -X utf8 eng_rep_pdf.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("firefox exe       :", _firefox_exe())
    print("firefox UA        :", firefox_user_agent())
    print("deps present      :", deps_present())
    print("can_fetch         :", can_fetch())
    print("impersonate target:", _impersonate_target())
    print("cache dir         :", CACHE_DIR)

    # Hadley v Baxendale, 156 E.R. 145  ->  [1854] EngR 296
    year, num = 1854, 296
    web = f"https://www.commonlii.org/uk/cases/EngR/{year}/{num}.html"
    print(f"\nfetching {year}/{num} (Hadley v Baxendale)...")
    try:
        data = fetch_pdf(year, num, web)
        print(f"  OK: {len(data):,} bytes, head={data[:8]!r}")
        print(f"  cached at: {cache_path(year, num)} "
              f"(exists={cache_path(year, num).exists()})")
        # second call should be served from cache
        again = fetch_pdf(year, num, web)
        print(f"  cache hit on 2nd call: {again == data}")
    except CloudflareChallenge as exc:
        print(f"  needs CloudFlare clearance in Firefox: {exc.web_url}")
        if "--open" in sys.argv:
            open_in_firefox(exc.web_url)
            print("  opened Firefox -- solve the check and re-run.")
    except FetchUnavailable:
        print("  in-app fetch unavailable (install Firefox + curl_cffi + "
              "browser_cookie3); would link out.")
    except OriginError as exc:
        print(f"  origin error: {exc}")
