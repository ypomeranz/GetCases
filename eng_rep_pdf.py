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
import json
import os
import re
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Optional

# CommonLII case PDFs land here; keyed by the neutral cite (year-num), which is
# unique per case.  Sits next to the app's existing config file.
_CONFIG_DIR = Path.home() / ".config" / "courtlistener"
CACHE_DIR = _CONFIG_DIR / "engr_cache"
# Playwright's persistent browser profile (so a passed CloudFlare check carries
# over between cases) and the clearance we lift from it for the curl_cffi path.
_PW_PROFILE_DIR = _CONFIG_DIR / "pw_profile"
_PW_STATE_FILE = _CONFIG_DIR / "pw_clearance.json"

_TIMEOUT = 45


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------

class FetchUnavailable(Exception):
    """In-app fetching isn't possible here (no clearance path available);
    the caller should just open the citation in the browser."""


class PlaywrightUnavailable(Exception):
    """Playwright is installed but couldn't drive a browser (no Chrome/Edge and
    no bundled Chromium).  The caller should fall back to the other options."""


class CloudflareChallenge(Exception):
    """CommonLII served a CloudFlare challenge -- the user must clear it (in
    Firefox, or in their own browser via Playwright).  ``web_url`` is the page
    to open for them to do so."""

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


def _major_from_ini(path: str, section: str, option: str) -> Optional[str]:
    """The leading version number of *option* in an INI file, or None."""
    try:
        cp = configparser.ConfigParser()
        with open(path, encoding="utf-8") as fh:
            cp.read_file(fh)
        m = re.match(r"(\d+)", cp.get(section, option, fallback=""))
        return m.group(1) if m else None
    except Exception:
        return None


def _detect_firefox_major() -> Optional[str]:
    """The installed Firefox's major version.  Prefer ``application.ini`` next to
    a real firefox.exe; for a Microsoft Store install the exe is a stub alias
    with no application.ini beside it, so fall back to the active profile's
    ``compatibility.ini`` (which records the exact version Firefox last ran)."""
    exe = _firefox_exe()
    if exe:
        major = _major_from_ini(
            os.path.join(os.path.dirname(exe), "application.ini"),
            "App", "Version")
        if major:
            return major
    for db in _firefox_cookie_dbs():  # newest profile first
        major = _major_from_ini(
            str(db.parent / "compatibility.ini"), "Compatibility", "LastVersion")
        if major:
            return major
    return None


def firefox_user_agent() -> str:
    """The user's real Firefox UA, derived from the install/profile version so it
    matches the UA Firefox used to obtain ``cf_clearance`` (CloudFlare ties the
    clearance to the exact UA).  Falls back to a recent UA if unreadable."""
    global _UA_CACHE
    if _UA_CACHE:
        return _UA_CACHE
    major = _detect_firefox_major() or "128"
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


def firefox_path_available() -> bool:
    """The Firefox clearance path: read Firefox's cf_clearance cookie and fetch
    with curl_cffi.  Needs Firefox installed and curl_cffi importable."""
    return firefox_available() and _have("curl_cffi")


def playwright_available() -> bool:
    """The Playwright path: let the user pass the CloudFlare check in their own
    browser (Chrome/Edge) and fetch the PDF through it.  Needs the playwright
    package importable (system Chrome/Edge is used via channels, so the browser
    binaries needn't be downloaded)."""
    return _have("playwright")


def can_fetch() -> bool:
    """True when in-app fetching is possible by some clearance path -- the
    Firefox cookie path or the Playwright path.  When False the caller links
    out to the browser instead."""
    return firefox_path_available() or playwright_available()


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


_CHROMIUM_IMPERSONATE_CACHE: Optional[str] = None


def _chromium_impersonate_target() -> str:
    """The newest Chrome TLS-fingerprint target curl_cffi offers.  Chromium's
    fingerprint covers Edge too (Edge is Chromium), so this matches a clearance
    obtained in either browser via Playwright."""
    global _CHROMIUM_IMPERSONATE_CACHE
    if _CHROMIUM_IMPERSONATE_CACHE:
        return _CHROMIUM_IMPERSONATE_CACHE
    target = "chrome"
    try:
        import typing
        from curl_cffi.requests.impersonate import BrowserTypeLiteral
        chromes = []
        for t in typing.get_args(BrowserTypeLiteral):
            m = re.fullmatch(r"chrome(\d+)", t)
            if m:
                chromes.append((int(m.group(1)), t))
        if chromes:
            target = max(chromes)[1]
    except Exception:
        pass
    _CHROMIUM_IMPERSONATE_CACHE = target
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


def _find_cookie_sqlites(base: Path, limit: int = 40) -> "list[Path]":
    """Recursively locate cookies.sqlite under *base*, pruning Firefox's large
    cache/storage/AppContainer subtrees so the walk stays fast.  Used for a
    Microsoft Store install, whose internal profile path varies by build."""
    skip = {
        "cache2", "startupcache", "storage", "thumbnails", "crashes",
        "datareporting", "minidumps", "saved-telemetry-pings", "gmp",
        "gmp-gmpopenh264", "ac", "temp", "tempstate", "inetcache",
        "settingsbackup", "doh-rollout", "weave",
    }
    found: list[Path] = []
    try:
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d.lower() not in skip]
            if "cookies.sqlite" in filenames:
                found.append(Path(dirpath) / "cookies.sqlite")
                if len(found) >= limit:
                    break
    except Exception:
        pass
    return found


def _windows_store_cookie_dbs() -> "list[Path]":
    """cookies.sqlite for a Microsoft Store Firefox (its firefox.EXE resolves to
    the WindowsApps alias).  The profile lives somewhere under
    ``%LOCALAPPDATA%\\Packages\\Mozilla.Firefox_*`` — redirected to a LocalCache
    subpath whose exact shape varies between builds — so search the package tree
    rather than guess the path."""
    if sys.platform != "win32":
        return []
    local = os.environ.get("LOCALAPPDATA")
    if not local:
        return []
    pkgs = Path(local) / "Packages"
    out: list[Path] = []
    seen: set[str] = set()
    for pat in ("Mozilla.Firefox_*", "*Firefox*", "*Mozilla*"):
        for pkg in pkgs.glob(pat):
            key = os.path.normcase(str(pkg))
            if key in seen or not pkg.is_dir():
                continue
            seen.add(key)
            out += _find_cookie_sqlites(pkg)
    return out


def _firefox_cookie_dbs() -> "list[Path]":
    """All Firefox cookies.sqlite files we can find, newest first (the profile
    the user just cleared CloudFlare in has the freshest mtime)."""
    cands: list[Path] = []
    for root in _firefox_data_roots():
        cands += _profiles_ini_cookie_dbs(root)
        for pat in ("Profiles/*/cookies.sqlite", "*/cookies.sqlite",
                    "cookies.sqlite"):
            cands += [Path(p) for p in glob.glob(str(root / pat))]
    # Microsoft Store install: the profile is buried in the package tree.
    cands += _windows_store_cookie_dbs()
    dbs: list[Path] = []
    seen: set[str] = set()
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


def _fetch_pdf_with(cookies: dict, ua: str, impersonate: str,
                    year: int, num: int, web_url: str) -> bytes:
    """Fetch and cache the PDF with curl_cffi given a clearance cookie set, a
    matching User-Agent and a browser TLS-impersonation target.  Raises
    :class:`CloudflareChallenge` on a re-challenge (cookie expired) or
    :class:`OriginError` on any other origin error."""
    from curl_cffi import requests as creq

    pdf_url = re.sub(r"\.html?$", ".pdf", web_url)
    headers = {
        "User-Agent": ua,
        "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                   "image/avif,image/webp,*/*;q=0.8"),
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": web_url,  # origin Apache hotlink-blocks PDFs without this
    }
    try:
        resp = creq.get(pdf_url, headers=headers, cookies=cookies,
                        impersonate=impersonate, timeout=_TIMEOUT)
    except Exception as exc:
        raise OriginError(0) from exc

    data = resp.content or b""
    if resp.status_code == 200 and data[:4] == b"%PDF":
        _store(year, num, data)
        return data
    if resp.status_code in (403, 503) and any(mk in data for mk in _CHALLENGE_MARKERS):
        raise CloudflareChallenge(web_url)
    raise OriginError(resp.status_code)


def fetch_pdf(year: int, num: int, web_url: str) -> bytes:
    """Return the PDF bytes for a CommonLII case, from cache or the network,
    using any clearance we can get without bothering the user.

    Tries the disk cache, then Firefox's cf_clearance cookie, then a clearance
    saved from an earlier Playwright solve.  Raises :class:`CloudflareChallenge`
    when none is available (the caller offers the interactive options),
    :class:`FetchUnavailable` when no clearance path exists at all, or
    :class:`OriginError` on an origin HTTP error.  Successful fetches are cached.
    """
    cached = get_cached(year, num)
    if cached is not None:
        return cached

    if not can_fetch():
        raise FetchUnavailable()

    # 1. Firefox's clearance cookie (read from cookies.sqlite) + curl_cffi.
    if firefox_path_available():
        cookies = _firefox_cookies()
        if cookies and "cf_clearance" in cookies:
            return _fetch_pdf_with(cookies, firefox_user_agent(),
                                   _impersonate_target(), year, num, web_url)

    # 2. Clearance saved from a previous Playwright solve, reused via curl_cffi
    #    (no browser launch) until it expires.
    if _have("curl_cffi"):
        saved = _load_clearance()
        if saved and (saved.get("cookies") or {}).get("cf_clearance"):
            try:
                return _fetch_pdf_with(saved["cookies"], saved["ua"],
                                       saved.get("impersonate", "chrome"),
                                       year, num, web_url)
            except CloudflareChallenge:
                _clear_saved_clearance()  # expired -- fall through to re-solve

    # No silent clearance -- the caller must solve interactively (Firefox or
    # Playwright).
    raise CloudflareChallenge(web_url)


# ---------------------------------------------------------------------------
# Playwright path: pass the CloudFlare check in the user's own browser
# ---------------------------------------------------------------------------

def _save_clearance(cookies: dict, ua: str, impersonate: str) -> None:
    """Persist a CloudFlare clearance (cookies + UA + TLS target) so later cases
    reuse it via curl_cffi without launching a browser again."""
    try:
        _PW_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _PW_STATE_FILE.write_text(
            json.dumps({"cookies": cookies, "ua": ua, "impersonate": impersonate}),
            encoding="utf-8")
    except Exception as exc:
        print(f"[eng_rep_pdf] saving clearance failed: {exc}")


def _load_clearance() -> Optional[dict]:
    try:
        return json.loads(_PW_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


def _clear_saved_clearance() -> None:
    try:
        _PW_STATE_FILE.unlink()
    except Exception:
        pass


def _launch_user_browser(p):
    """A persistent Playwright context driving the user's installed browser:
    Google Chrome, then Microsoft Edge (via channels -- no browser download),
    then a bundled Chromium.  Persistent so a passed CloudFlare check carries
    over between cases.  Returns (context, channel) or (None, "")."""
    _PW_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    args = ["--no-first-run", "--no-default-browser-check"]
    for channel in ("chrome", "msedge"):
        try:
            ctx = p.chromium.launch_persistent_context(
                str(_PW_PROFILE_DIR), channel=channel, headless=False, args=args)
            return ctx, channel
        except Exception as exc:
            print(f"[eng_rep_pdf] playwright channel {channel!r}: {exc}")
    try:  # bundled Chromium (needs `playwright install chromium`)
        ctx = p.chromium.launch_persistent_context(
            str(_PW_PROFILE_DIR), headless=False, args=args)
        return ctx, "chromium"
    except Exception as exc:
        print(f"[eng_rep_pdf] playwright bundled chromium: {exc}")
        return None, ""


def _wait_for_clearance(ctx, page, timeout: int) -> dict:
    """Poll until the commonlii.org cf_clearance cookie appears (the user passed
    the check) or *timeout* seconds elapse / the window is closed.  Returns the
    commonlii cookies (with cf_clearance) or {} on timeout/abort."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            cookies = {c["name"]: c["value"] for c in ctx.cookies()
                       if "commonlii" in (c.get("domain") or "")}
        except Exception:
            return {}  # context closed
        if "cf_clearance" in cookies:
            return cookies
        try:
            if page.is_closed():
                return {}
            page.wait_for_timeout(1000)
        except Exception:
            return {}  # page/window closed by the user
    return {}


def _pw_fetch_pdf(page, pdf_url: str) -> bytes:
    """Fetch the PDF from inside the (cleared) page with a same-origin fetch, so
    it goes through the real browser's network, cookies and TLS.  Base64 is done
    via FileReader (no String.fromCharCode argument limit on large scans).
    Returns the bytes, or b"" on failure."""
    import base64
    result = page.evaluate(
        """async (url) => {
            try {
                const r = await fetch(url, {credentials: 'include'});
                if (!r.ok) return 'ERR:status ' + r.status;
                const blob = await r.blob();
                const b64 = await new Promise((resolve) => {
                    const fr = new FileReader();
                    fr.onloadend = () => resolve(
                        String(fr.result).split(',')[1] || '');
                    fr.onerror = () => resolve('');
                    fr.readAsDataURL(blob);
                });
                return b64;
            } catch (e) { return 'ERR:' + (e && e.message || e); }
        }""", pdf_url)
    if isinstance(result, str) and result.startswith("ERR:"):
        print(f"[eng_rep_pdf] in-page PDF fetch failed: {result[4:]}")
        return b""
    try:
        return base64.b64decode(result) if result else b""
    except Exception:
        return b""


def fetch_pdf_via_playwright(year: int, num: int, web_url: str,
                             on_status=lambda _s: None,
                             timeout: int = 180) -> bytes:
    """Open the case in the user's own browser (driven by Playwright), let them
    pass the CloudFlare check, then fetch the PDF through that browser.  The
    clearance is saved so subsequent cases skip the browser.

    Raises :class:`FetchUnavailable` (playwright not importable),
    :class:`PlaywrightUnavailable` (no browser to drive),
    :class:`CloudflareChallenge` (check not completed in time), or
    :class:`OriginError`.  Successful fetches are cached."""
    cached = get_cached(year, num)
    if cached is not None:
        return cached
    try:
        from playwright.sync_api import sync_playwright
        from playwright.sync_api import TimeoutError as _PWTimeout
    except Exception:
        raise FetchUnavailable()

    pdf_url = re.sub(r"\.html?$", ".pdf", web_url)
    cookies: dict = {}
    ua = ""
    data = b""
    impersonate = _chromium_impersonate_target()
    with sync_playwright() as p:
        ctx, channel = _launch_user_browser(p)
        if ctx is None:
            raise PlaywrightUnavailable()
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            on_status("Opening your browser — pass the “Just a moment…” check…")
            try:
                page.goto(web_url, wait_until="domcontentloaded", timeout=60000)
            except _PWTimeout:
                pass  # the challenge page can stall load; we poll cookies anyway
            cookies = _wait_for_clearance(ctx, page, timeout)
            if "cf_clearance" not in cookies:
                raise CloudflareChallenge(web_url)
            on_status("Check passed — downloading the scan…")
            ua = page.evaluate("() => navigator.userAgent")
            # Primary: fetch with curl_cffi using the fresh clearance — the same
            # proven mechanism as the Firefox path (cookie + Referer + a
            # browser-matching TLS fingerprint), just with Edge/Chrome's cookie.
            if _have("curl_cffi"):
                try:
                    data = _fetch_pdf_with(cookies, ua, impersonate,
                                           year, num, web_url)
                except (CloudflareChallenge, OriginError) as exc:
                    print(f"[eng_rep_pdf] playwright+curl_cffi fetch failed: {exc}")
                    data = b""
            # Fallback (e.g. no curl_cffi): fetch inside the browser itself.
            if not data:
                try:
                    page.goto(web_url, wait_until="domcontentloaded", timeout=30000)
                except _PWTimeout:
                    pass
                raw = _pw_fetch_pdf(page, pdf_url)
                if raw[:4] == b"%PDF":
                    _store(year, num, raw)
                    data = raw
        finally:
            try:
                ctx.close()
            except Exception:
                pass

    # Save the clearance so later cases reuse it via curl_cffi without a browser.
    if cookies.get("cf_clearance"):
        _save_clearance(cookies, ua, impersonate)
    if not data or data[:4] != b"%PDF":
        raise OriginError(0)
    return data


# ---------------------------------------------------------------------------
# Live test:  python -X utf8 eng_rep_pdf.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("firefox exe       :", _firefox_exe())
    print("firefox UA        :", firefox_user_agent())
    print("curl_cffi present :", _have("curl_cffi"))
    print("firefox path      :", firefox_path_available())
    print("playwright path   :", playwright_available())
    print("can_fetch         :", can_fetch())
    print("impersonate target:", _impersonate_target())
    print("saved clearance   :", "yes" if _load_clearance() else "no")
    print("cache dir         :", CACHE_DIR)

    if "--playwright" in sys.argv:
        # Force the interactive Playwright solve for a quick manual check.
        yr, nm = 1854, 296
        wb = f"https://www.commonlii.org/uk/cases/EngR/{yr}/{nm}.html"
        print("\nsolving via playwright (a browser window will open)...")
        try:
            d = fetch_pdf_via_playwright(yr, nm, wb, on_status=print)
            print(f"  OK: {len(d):,} bytes, head={d[:8]!r}")
        except Exception as exc:
            print(f"  {type(exc).__name__}: {exc}")
        raise SystemExit(0)

    # Profile search diagnostics: shows where we looked and whether the
    # clearance cookie is actually on disk (the two distinct failure modes are
    # "no profile/cookie found" vs "found but rejected at fetch").
    print("\nfirefox profile search:")
    print("  data roots      :",
          [str(r) for r in _firefox_data_roots()] or "(none)")
    dbs = _firefox_cookie_dbs()
    if not dbs:
        print("  cookies.sqlite  : (none found)")
    for db in dbs:
        ck = _read_cookies_sqlite(db, "commonlii")
        flag = "cf_clearance=YES" if "cf_clearance" in ck else "cf_clearance=no"
        print(f"  - {db}\n      commonlii cookies {sorted(ck)}  {flag}")

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
        print("  in-app fetch unavailable (needs Firefox + curl_cffi); "
              "would link out.")
    except OriginError as exc:
        print(f"  origin error: {exc}")
