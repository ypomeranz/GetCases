"""Searchable opinion database for GetCases.

Unlike ``google_scholar``'s opaque, query-keyed cache (which stores the same
opinion several times under ``cite:`` / ``name:`` / ``url:`` keys and can't be
searched), this module maintains a real database of opinions keyed on their
**true identity** — the Google Scholar opinion number, the ``case=`` value in
every ``scholar_case?case=<number>`` URL — and searchable by:

  * the Scholar number,
  * any of the opinion's reporter citations (``410 U.S. 113``), and
  * the names of the parties.

Storage is two files (see the project plan):

  * ``opinions.jsonl`` — the **source of truth**, one opinion per line, committed
    to Git so it can be diffed, merged, and synced via GitHub by hand.  The
    opinion HTML is gzip+base64 packed into ``html_gz`` to keep lines compact.
  * ``opinions.index.db`` — a **local SQLite index** rebuilt from the JSONL
    (and therefore *gitignored*).  It materializes the plain text and the
    ``citations`` / ``parties`` lookup tables for fast, collision-aware search.

Because two different cases can share party names *or* begin on the same
reporter page, the citation/party lookups deliberately return a **list** of
candidate opinions; only :func:`scholar_id_from_url` identifies one uniquely.

This module is free of any ``tkinter`` dependency and can be exercised
headlessly with ``python opinion_db.py`` (offline self-test, exit 0 = pass),
mirroring ``citations.py`` / ``fed_rules.py``.  The Scholar-HTML parsing helpers
(``parse_opinion_blocks`` / ``blocks_to_text``) are imported lazily so the store
and its search/merge keep working even where ``beautifulsoup4`` is absent.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import re
import sqlite3
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import citations
from bluebook_names import (
    abbreviate_case_name,
    normal_case_caption,
    strip_related_case_note,
)

_SCHEMA_VERSION = 2

# Word tokens too generic to help narrow a party search.
_PARTY_STOP = {
    "the", "of", "and", "in", "re", "v", "vs", "et", "al", "a", "an",
    "for", "on", "ex", "parte", "matter", "state", "people",
}

# Entity initialisms kept upper-case when normal-casing an all-caps caption.
_ENTITY_KEEP = {
    "llc", "llp", "pllc", "lp", "lllp", "plc", "pc", "pa", "na", "sa",
    "ag", "nv", "co", "corp", "inc", "ltd",
}
# Caption small words lower-cased except in leading position.
_CAPTION_SMALL = {
    "of", "the", "and", "v", "vs", "in", "re", "for", "on", "a", "an",
    "to", "by", "at", "as", "or",
}


# ---------------------------------------------------------------------------
# Scholar-HTML helpers (imported lazily — only fresh extraction needs bs4)
# ---------------------------------------------------------------------------

def _blocks(html: str) -> list:
    if not html:
        return []
    try:
        from google_scholar import parse_opinion_blocks
        return parse_opinion_blocks(html)
    except Exception:
        return []


def _blocks_text(blocks: list) -> str:
    if not blocks:
        return ""
    try:
        from google_scholar import blocks_to_text
        return blocks_to_text(blocks)
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def scholar_id_from_url(url: str) -> Optional[str]:
    """The Google Scholar opinion number (the ``case=`` value) in a
    ``scholar_case`` URL, or ``None``.  This is the database's primary key."""
    if not url:
        return None
    try:
        q = urllib.parse.urlparse(url).query
        vals = urllib.parse.parse_qs(q).get("case")
        if vals and vals[0].strip():
            return vals[0].strip()
    except Exception:
        pass
    m = re.search(r"[?&]case=(\d+)", url)
    return m.group(1) if m else None


def _gz_pack(s: str) -> str:
    return base64.b64encode(gzip.compress((s or "").encode("utf-8"))).decode("ascii")


def _gz_unpack(packed: str) -> str:
    if not packed:
        return ""
    try:
        return gzip.decompress(base64.b64decode(packed.encode("ascii"))).decode(
            "utf-8", "replace"
        )
    except Exception:
        return ""


def _cite_key(cite: str) -> Optional[tuple[int, str, int]]:
    """(volume, normalized-reporter, page) for a reporter citation, or None."""
    m = citations.CITE_CAPTURE_RE.search(cite or "")
    if not m:
        return None
    try:
        return (int(m.group(1)), citations.norm_reporter(m.group(2)), int(m.group(3)))
    except (TypeError, ValueError):
        return None


def _dedupe_cites(cites: list[str]) -> list[str]:
    """De-duplicate citation strings by (vol, reporter, page), keeping order."""
    seen: set = set()
    out: list[str] = []
    for c in cites:
        c = re.sub(r"\s+", " ", c or "").strip().replace("U. S.", "U.S.")
        if not c:
            continue
        key = _cite_key(c)
        dedupe = key if key is not None else c.lower()
        if dedupe in seen:
            continue
        seen.add(dedupe)
        out.append(c)
    return out


def _header_cites(blocks: list) -> list[str]:
    """Reporter citations printed in the case's own caption — the centered /
    heading blocks at the top.  The opinion *body* is deliberately not scanned:
    that would pull in every other case the opinion cites."""
    out: list[str] = []
    for b in blocks[:10]:
        if getattr(b, "kind", None) not in ("center", "heading"):
            continue
        t = re.sub(r"\s+", " ", b.text()).strip()
        t = re.sub(r"\bU\.\s+S\.", "U.S.", t)
        t = re.sub(r"\b(\d{1,4})\s+US\s+(\d{1,5})\b", r"\1 U.S. \2", t)
        for m in citations.CITE_CAPTURE_RE.finditer(t):
            out.append(re.sub(r"\s+", " ", m.group(0)).strip())
    return out


def _caption_name(blocks: list) -> str:
    """Raw case name from the Scholar caption (a centered block with a 'v.'
    separator, or an 'In re'/'Ex parte' form).  Light, ``tkinter``-free cousin
    of the GUI's ``_scholar_caption_name``; the result is Bluebook-abbreviated
    by the caller."""
    for b in blocks[:10]:
        if getattr(b, "kind", None) not in ("center", "heading"):
            continue
        t = re.sub(r"\s+", " ", b.text()).strip()
        # An Alabama-style "(Re <underlying case>)" cross-reference carries
        # its own " v. " and would masquerade as this case's caption.
        t = strip_related_case_note(t)
        if not t or t.startswith(("No.", "Nos.")):
            continue
        if citations.TEXT_CITE_RE.match(t):
            continue  # a bare citation line, not the caption
        sides = re.split(r"\s+vs?\.\s+", t, maxsplit=1)
        if len(sides) != 2:
            sides = re.split(r"\s+[vV]s?\.\s+", t, maxsplit=1)
        if len(sides) == 2 and sides[0].strip() and sides[1].strip():
            return t
        if re.match(
            r"(?:IN\s+RE|EX\s+PARTE|(?:IN\s+THE\s+)?MATTER\s+OF)\b", t, re.IGNORECASE
        ):
            return t.split(",")[0].strip()
    return ""


def _smart_titlecase(s: str) -> str:
    """Normal-case an ALL-CAPS Scholar caption ('ROE v. WADE' → 'Roe v. Wade',
    'MERCY HOSPITAL, INC.' → 'Mercy Hospital, Inc.') so the Bluebook
    abbreviator doesn't mistake an all-caps party for an initialism (which would
    turn 'ROE' into 'R.O.E.').  A ``tkinter``-free cousin of the GUI's
    ``_titlecase_caps``; mixed-case words and dotted initialisms pass through."""
    return normal_case_caption(s)


def _norm_party_side(s: str) -> str:
    s = re.sub(r"[^a-z0-9 ]+", " ", (s or "").lower())
    return re.sub(r"\s+", " ", s).strip()


def parties_from_name(name: str) -> list[str]:
    """Normalized party strings for a case name: ``["roe", "wade"]`` for
    "Roe v. Wade".  A name with no 'v.' (e.g. "In re Grand Jury") yields a
    single party string."""
    if not name:
        return []
    sides = re.split(r"(?i)\s+vs?\.\s+", name, maxsplit=1)
    return [p for p in (_norm_party_side(s) for s in sides) if p]


def _party_tokens(text: str) -> list[str]:
    """Significant lower-case word tokens for party matching, stop-words and
    1-character fragments dropped."""
    seen: set = set()
    out: list[str] = []
    for w in re.findall(r"[a-z0-9]+", (text or "").lower()):
        if len(w) < 2 or w in _PARTY_STOP or w in seen:
            continue
        seen.add(w)
        out.append(w)
    return out


def _court_from_cites(cites: list[str]) -> str:
    """Best-effort court id from the reporters alone (HTML-only fallback used
    when no CourtListener metadata is available): the SCOTUS reporters imply
    ``scotus``; otherwise leave it blank."""
    for c in cites:
        key = _cite_key(c)
        if key and key[1] in ("u.s.", "s.ct.", "l.ed.", "l.ed.2d"):
            return "scotus"
    return ""


def extract_record(
    url: str, html: str, item: Optional[dict] = None
) -> Optional[dict]:
    """Build a JSONL record from a fetched Scholar opinion.

    ``item`` (a CourtListener result dict) enriches the name/court/year when
    present, but the record is derivable from ``(url, html)`` alone.  Returns
    ``None`` when the URL carries no Scholar id (nothing to key on)."""
    sid = scholar_id_from_url(url)
    if not sid:
        return None
    item = item or {}
    blocks = _blocks(html)

    raw_name = re.sub(
        r"<[^>]+>", "", item.get("caseName") or item.get("case_name") or ""
    ).strip()
    if not raw_name:
        raw_name = _caption_name(blocks)
        if raw_name:
            raw_name = _smart_titlecase(raw_name)  # caption is often ALL CAPS
    name = abbreviate_case_name(raw_name) if raw_name else ""

    cites = _header_cites(blocks)
    for c in item.get("citation", []) or []:
        cites.append(str(c))
    cites = _dedupe_cites(cites)

    parties = parties_from_name(name or raw_name)

    date_filed = item.get("dateFiled") or item.get("date_filed") or ""
    year = date_filed[:4] if len(str(date_filed)) >= 4 else ""
    if not year:
        head = " ".join(
            b.text() for b in blocks[:10]
            if getattr(b, "kind", None) in ("center", "heading")
        )
        years = re.findall(r"\b(1[6-9]\d{2}|20\d{2})\b", head)
        if years:
            year = years[-1]

    court = str(item.get("court_id") or "").strip().lower()
    if not court:
        court = _court_from_cites(cites)

    return {
        "v": _SCHEMA_VERSION,
        "scholar_id": sid,
        "url": url,
        "name": name,
        "parties": parties,
        "cites": cites,
        "court": court,
        "year": year,
        "date_filed": date_filed,
        "added_at": time.time(),
        "source": item.get("source") or "scholar",
        "html_gz": _gz_pack(html),
    }


# ---------------------------------------------------------------------------
# The database
# ---------------------------------------------------------------------------

def _default_dir() -> Path:
    env = os.environ.get("GETCASES_DB_DIR")
    if env:
        return Path(env)
    return Path(__file__).resolve().parent / "data"


def data_dir() -> Path:
    """Directory holding ``opinions.jsonl`` / ``opinions.index.db`` — the same
    location :class:`OpinionDB` uses by default.  Public so the self-updater can
    back up and restore the opinions file without opening the database."""
    return _default_dir()


class OpinionDB:
    """JSONL store of opinions plus a derived SQLite search index.

    All access is serialized with a re-entrant lock so the fetcher's worker
    threads can read and write safely (the connection is opened with
    ``check_same_thread=False``).
    """

    def __init__(
        self,
        jsonl_path: Optional[os.PathLike | str] = None,
        index_path: Optional[os.PathLike | str] = None,
    ) -> None:
        base = _default_dir()
        self.jsonl_path = Path(jsonl_path) if jsonl_path else base / "opinions.jsonl"
        self.index_path = (
            Path(index_path) if index_path else base / "opinions.index.db"
        )
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(self.index_path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._init_schema()
        self._sync_from_jsonl()

    # -- schema -------------------------------------------------------------

    def _init_schema(self) -> None:
        with self._lock:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS opinions (
                    scholar_id   TEXT PRIMARY KEY,
                    url          TEXT,
                    name         TEXT,
                    court        TEXT,
                    year         TEXT,
                    date_filed   TEXT,
                    html         TEXT,
                    text         TEXT,
                    cites_json   TEXT,
                    parties_json TEXT,
                    added_at     REAL,
                    source       TEXT
                );
                CREATE TABLE IF NOT EXISTS citations (
                    scholar_id TEXT,
                    vol        INTEGER,
                    reporter   TEXT,
                    page       INTEGER,
                    raw        TEXT
                );
                CREATE TABLE IF NOT EXISTS parties (
                    scholar_id TEXT,
                    token      TEXT
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
                CREATE INDEX IF NOT EXISTS ix_cite ON citations(reporter, vol, page);
                CREATE INDEX IF NOT EXISTS ix_cite_sid ON citations(scholar_id);
                CREATE INDEX IF NOT EXISTS ix_party ON parties(token);
                CREATE INDEX IF NOT EXISTS ix_party_sid ON parties(scholar_id);
                """
            )
            if self._get_meta("schema_version") is None:
                self._set_meta("schema_version", str(_SCHEMA_VERSION))
            self._db.commit()

    # -- meta ---------------------------------------------------------------

    def _get_meta(self, key: str) -> Optional[str]:
        row = self._db.execute(
            "SELECT value FROM meta WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value)
        )

    # -- JSONL <-> index sync ----------------------------------------------

    def _sync_from_jsonl(self) -> None:
        """Bring the index in line with the JSONL, which a ``git pull`` or an
        out-of-process write may have changed.  A clean append (the file only
        grew, on a line boundary) is ingested incrementally; anything else
        triggers a full rebuild."""
        with self._lock:
            if not self.jsonl_path.exists():
                return
            cur_size = self.jsonl_path.stat().st_size
            try:
                stored_size = int(self._get_meta("jsonl_size") or "-1")
            except ValueError:
                stored_size = -1
            stored_ver = self._get_meta("schema_version")
            if stored_ver != str(_SCHEMA_VERSION):
                self.rebuild_index()
                return
            if cur_size == stored_size:
                return
            if 0 <= stored_size < cur_size and self._appended_cleanly(stored_size):
                try:
                    self._ingest_tail(stored_size)
                    return
                except Exception:
                    pass  # fall back to a full rebuild
            self.rebuild_index()

    def _appended_cleanly(self, offset: int) -> bool:
        """True when byte ``offset`` sits just after a newline, so reading from
        there yields whole JSON lines (the only way ``add`` ever grows the
        file)."""
        if offset == 0:
            return True
        try:
            with open(self.jsonl_path, "rb") as f:
                f.seek(offset - 1)
                return f.read(1) == b"\n"
        except OSError:
            return False

    def _ingest_tail(self, offset: int) -> None:
        added = 0
        with open(self.jsonl_path, "r", encoding="utf-8") as f:
            f.seek(offset)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    self._index_record(json.loads(line))
                    added += 1
                except Exception:
                    continue
        prev = int(self._get_meta("jsonl_lines") or "0")
        self._set_meta("jsonl_lines", str(prev + added))
        self._set_meta("jsonl_size", str(self.jsonl_path.stat().st_size))
        self._db.commit()

    def rebuild_index(self) -> None:
        """Drop and repopulate the SQLite index from the JSONL source of truth."""
        with self._lock:
            self._db.executescript(
                "DELETE FROM opinions; DELETE FROM citations; DELETE FROM parties;"
            )
            lines = 0
            if self.jsonl_path.exists():
                with open(self.jsonl_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            self._index_record(json.loads(line))
                            lines += 1
                        except Exception:
                            continue
            size = self.jsonl_path.stat().st_size if self.jsonl_path.exists() else 0
            self._set_meta("schema_version", str(_SCHEMA_VERSION))
            self._set_meta("jsonl_lines", str(lines))
            self._set_meta("jsonl_size", str(size))
            self._db.commit()

    # -- indexing a single record ------------------------------------------

    def _index_record(self, rec: dict) -> None:
        """Upsert one record into the SQLite index (no JSONL write)."""
        sid = rec.get("scholar_id")
        if not sid:
            return
        html = _gz_unpack(rec.get("html_gz", ""))
        text = _blocks_text(_blocks(html)) if html else ""
        cites = rec.get("cites")
        if cites is None:
            cites = _header_cites(_blocks(html))
        parties = rec.get("parties")
        if parties is None:
            parties = parties_from_name(rec.get("name", ""))

        self._db.execute(
            "INSERT OR REPLACE INTO opinions (scholar_id, url, name, court, year, "
            "date_filed, html, text, cites_json, parties_json, added_at, source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                sid, rec.get("url", ""), rec.get("name", ""), rec.get("court", ""),
                rec.get("year", ""), rec.get("date_filed", ""), html, text,
                json.dumps(cites, ensure_ascii=False),
                json.dumps(parties, ensure_ascii=False),
                rec.get("added_at") or time.time(), rec.get("source", "scholar"),
            ),
        )
        self._db.execute("DELETE FROM citations WHERE scholar_id=?", (sid,))
        self._db.execute("DELETE FROM parties WHERE scholar_id=?", (sid,))
        for c in cites:
            key = _cite_key(c)
            if key:
                self._db.execute(
                    "INSERT INTO citations (scholar_id, vol, reporter, page, raw) "
                    "VALUES (?,?,?,?,?)",
                    (sid, key[0], key[1], key[2], re.sub(r"\s+", " ", c).strip()),
                )
        for tok in _party_tokens(" ".join(parties)):
            self._db.execute(
                "INSERT INTO parties (scholar_id, token) VALUES (?, ?)", (sid, tok)
            )

    # -- writes -------------------------------------------------------------

    def add(self, record: dict) -> bool:
        """Add a record (append to the JSONL and index it).  De-duped by
        Scholar id — an id already present is left untouched.  Returns whether
        a new opinion was stored."""
        sid = record.get("scholar_id")
        if not sid:
            return False
        with self._lock:
            if self._exists(sid):
                return False
            line = json.dumps(record, ensure_ascii=False)
            with open(self.jsonl_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            self._index_record(record)
            prev = int(self._get_meta("jsonl_lines") or "0")
            self._set_meta("jsonl_lines", str(prev + 1))
            self._set_meta("jsonl_size", str(self.jsonl_path.stat().st_size))
            self._db.commit()
            return True

    def add_opinion(
        self, url: str, html: str, item: Optional[dict] = None
    ) -> bool:
        """Extract a record from a fetched Scholar opinion and add it."""
        rec = extract_record(url, html, item)
        if rec is None:
            return False
        return self.add(rec)

    def _rewrite_jsonl(self, sid: str, new_record: Optional[dict]) -> bool:
        """Rewrite ``opinions.jsonl`` with the record for *sid* removed
        (``new_record=None``) or replaced in place (diff-friendly for the
        Git-synced file).  A replacement whose id isn't present yet is
        appended.  Atomic (temp file + rename); the index is rebuilt from
        the rewritten file.  Returns whether anything changed."""
        with self._lock:
            lines: list[str] = []
            found = False
            if self.jsonl_path.exists():
                with open(self.jsonl_path, "r", encoding="utf-8") as f:
                    for line in f:
                        stripped = line.strip()
                        if not stripped:
                            continue
                        try:
                            rec_sid = json.loads(stripped).get("scholar_id")
                        except Exception:
                            lines.append(stripped)  # keep unreadable lines as-is
                            continue
                        if rec_sid == sid:
                            found = True
                            if new_record is not None:
                                lines.append(
                                    json.dumps(new_record, ensure_ascii=False))
                            continue  # removed (or just replaced)
                        lines.append(stripped)
            if not found:
                if new_record is None:
                    return False
                lines.append(json.dumps(new_record, ensure_ascii=False))
            tmp = self.jsonl_path.with_suffix(f".{os.getpid()}.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                for line in lines:
                    f.write(line + "\n")
            tmp.replace(self.jsonl_path)
            self.rebuild_index()
            return True

    def delete(self, sid: str) -> bool:
        """Remove the opinion with Scholar id *sid* from the JSONL store and
        the index.  Returns whether a record was removed."""
        if not sid:
            return False
        return self._rewrite_jsonl(str(sid).strip(), None)

    def replace(self, record: dict) -> bool:
        """Store *record* in place of the existing record sharing its Scholar
        id (appending when absent) — used to refresh an opinion with a newer
        Google Scholar version."""
        sid = record.get("scholar_id")
        if not sid:
            return False
        return self._rewrite_jsonl(str(sid).strip(), record)

    def merge_from(self, other_jsonl: os.PathLike | str) -> dict:
        """Merge another ``opinions.jsonl`` into this store.  Opinions whose
        Scholar id is already present are skipped (existing copy kept).  Returns
        ``{"added", "skipped", "errors"}``."""
        added = skipped = errors = 0
        path = Path(other_jsonl)
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    errors += 1
                    continue
                if not rec.get("scholar_id"):
                    errors += 1
                    continue
                if self.add(rec):
                    added += 1
                else:
                    skipped += 1
        return {"added": added, "skipped": skipped, "errors": errors}

    # -- reads --------------------------------------------------------------

    def _exists(self, sid: str) -> bool:
        return (
            self._db.execute(
                "SELECT 1 FROM opinions WHERE scholar_id=?", (sid,)
            ).fetchone()
            is not None
        )

    def count(self) -> int:
        with self._lock:
            return self._db.execute("SELECT COUNT(*) FROM opinions").fetchone()[0]

    def get_by_scholar_id(self, sid: str) -> Optional[dict]:
        """Full stored record (including opinion ``html`` and plain ``text``),
        or ``None``."""
        if not sid:
            return None
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM opinions WHERE scholar_id=?", (str(sid).strip(),)
            ).fetchone()
        return self._row_to_record(row) if row else None

    def get_by_url(self, url: str) -> Optional[dict]:
        """Full stored record for a ``scholar_case`` URL (matched on its
        ``case=`` id), or ``None``."""
        return self.get_by_scholar_id(scholar_id_from_url(url) or "")

    def find_by_citation(self, vol, reporter: str, page) -> list[dict]:
        """Every opinion bearing the reporter citation ``vol reporter page`` —
        possibly more than one (two cases can start on the same page)."""
        try:
            vol_i, page_i = int(vol), int(page)
        except (TypeError, ValueError):
            return []
        rep = citations.norm_reporter(reporter)
        with self._lock:
            rows = self._db.execute(
                "SELECT o.* FROM citations c JOIN opinions o "
                "ON o.scholar_id=c.scholar_id "
                "WHERE c.reporter=? AND c.vol=? AND c.page=? "
                "ORDER BY o.year, o.name",
                (rep, vol_i, page_i),
            ).fetchall()
        return [self._summary(r) for r in rows]

    def find_by_party(self, query: str) -> list[dict]:
        """Opinions whose parties contain *all* the significant tokens in
        ``query`` (so "wade" and "roe wade" both find Roe v. Wade, while many
        "United States v. Smith" cases all surface for "smith")."""
        toks = _party_tokens(query)
        if not toks:
            return []
        placeholders = ",".join("?" * len(toks))
        with self._lock:
            rows = self._db.execute(
                f"SELECT o.* FROM parties p JOIN opinions o "
                f"ON o.scholar_id=p.scholar_id "
                f"WHERE p.token IN ({placeholders}) "
                f"GROUP BY o.scholar_id "
                f"HAVING COUNT(DISTINCT p.token) >= ? "
                f"ORDER BY o.year, o.name",
                (*toks, len(toks)),
            ).fetchall()
        return [self._summary(r) for r in rows]

    def search_names(self, query: str, limit: int = 40) -> list[dict]:
        """Candidate opinions sharing *any* significant party token with
        ``query``, ranked by how many distinct query tokens they match (most
        first).  Unlike :meth:`find_by_party` — which requires *every* token —
        this casts a wide net so a fuzzy name matcher can judge the candidates:
        it surfaces "Brown v. Board of Ed." for "Brown v. Board of Education",
        which the all-tokens match would miss on the abbreviated word.  The
        ranking only orders the candidate pool; the caller does the real
        name-closeness scoring."""
        toks = _party_tokens(query)
        if not toks:
            return []
        placeholders = ",".join("?" * len(toks))
        with self._lock:
            rows = self._db.execute(
                f"SELECT o.*, COUNT(DISTINCT p.token) AS _n "
                f"FROM parties p JOIN opinions o ON o.scholar_id=p.scholar_id "
                f"WHERE p.token IN ({placeholders}) "
                f"GROUP BY o.scholar_id "
                f"ORDER BY _n DESC, o.year, o.name "
                f"LIMIT ?",
                (*toks, int(limit)),
            ).fetchall()
        return [self._summary(r) for r in rows]

    def find(self, query: str) -> list[dict]:
        """Search the database, dispatching on the shape of ``query``: an
        all-digit run is a Scholar id; a reporter citation is matched as such;
        anything else is treated as party names.  Returns candidate summaries
        (use :meth:`get_by_scholar_id` to load the chosen opinion's text)."""
        q = (query or "").strip()
        if not q:
            return []
        if q.isdigit():  # a bare number is a Google Scholar opinion id
            rec = self.get_by_scholar_id(q)
            return [self._summary_from_record(rec)] if rec else []
        m = citations.CITE_CAPTURE_RE.search(q)
        if m:
            return self.find_by_citation(m.group(1), m.group(2), m.group(3))
        return self.find_by_party(q)

    # -- row helpers --------------------------------------------------------

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> dict:
        return {
            "scholar_id": row["scholar_id"],
            "url": row["url"],
            "name": row["name"],
            "court": row["court"],
            "year": row["year"],
            "date_filed": row["date_filed"],
            "html": row["html"],
            "text": row["text"],
            "cites": json.loads(row["cites_json"] or "[]"),
            "parties": json.loads(row["parties_json"] or "[]"),
            "added_at": row["added_at"],
            "source": row["source"],
        }

    @classmethod
    def _summary(cls, row: sqlite3.Row) -> dict:
        cites = json.loads(row["cites_json"] or "[]")
        return {
            "scholar_id": row["scholar_id"],
            "name": row["name"],
            "cite": cites[0] if cites else "",
            "cites": cites,
            "court": row["court"],
            "year": row["year"],
            "url": row["url"],
        }

    @staticmethod
    def _summary_from_record(rec: dict) -> dict:
        cites = rec.get("cites") or []
        return {
            "scholar_id": rec.get("scholar_id", ""),
            "name": rec.get("name", ""),
            "cite": cites[0] if cites else "",
            "cites": cites,
            "court": rec.get("court", ""),
            "year": rec.get("year", ""),
            "url": rec.get("url", ""),
        }

    def close(self) -> None:
        with self._lock:
            try:
                self._db.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Offline self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover - offline smoke test
    import sys
    import tempfile

    def opinion_html(caption: str, cite_lines: list[str], body: str) -> str:
        head = "".join(f"<center>{c}</center>" for c in cite_lines)
        return (
            '<div id="gs_opinion">'
            f"{head}<center>{caption}</center><p>{body}</p></div>"
        )

    roe_url = "https://scholar.google.com/scholar_case?case=12345678901234567890&q=roe"
    roe_html = opinion_html(
        "ROE v. WADE", ["410 U.S. 113", "93 S. Ct. 705"],
        "MR. JUSTICE BLACKMUN delivered the opinion of the Court. " * 30,
    )
    # A different case that happens to begin on the same reporter page.
    twin_url = "https://scholar.google.com/scholar_case?case=99999999999999999999"
    twin_html = opinion_html(
        "SMITH v. JONES", ["410 U.S. 113"], "The judgment is affirmed. " * 30,
    )

    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        if not cond:
            failures.append(msg)
            print("FAIL:", msg)

    # scholar_id_from_url
    check(scholar_id_from_url(roe_url) == "12345678901234567890", "id parse (q tail)")
    check(scholar_id_from_url(twin_url) == "99999999999999999999", "id parse (bare)")
    check(scholar_id_from_url("https://x/scholar_case?cluster=5") is None, "id absent")

    # extract_record
    rec = extract_record(roe_url, roe_html)
    check(rec is not None, "extract returns a record")
    check(rec["scholar_id"] == "12345678901234567890", "record id")
    check("Roe" in rec["name"] and "Wade" in rec["name"], f"name: {rec['name']!r}")
    check("410 U.S. 113" in rec["cites"], f"cites: {rec['cites']!r}")
    check("93 S. Ct. 705" in rec["cites"], "parallel cite captured")
    check("roe" in rec["parties"] and "wade" in rec["parties"],
          f"parties: {rec['parties']!r}")
    check(rec["court"] == "scotus", f"court from cite: {rec['court']!r}")

    with tempfile.TemporaryDirectory() as d:
        jsonl = Path(d) / "opinions.jsonl"
        index = Path(d) / "opinions.index.db"
        db = OpinionDB(jsonl, index)

        check(db.add(rec) is True, "add new -> True")
        check(db.add(rec) is False, "add dup -> False (dedupe by id)")
        check(db.count() == 1, "count after dedupe == 1")
        check(jsonl.read_text(encoding="utf-8").count("\n") == 1, "one JSONL line")

        got = db.get_by_scholar_id("12345678901234567890")
        check(got is not None and "delivered the opinion" in (got["html"] or ""),
              "html round-trips")
        check(bool(got and got["text"]), "plain text materialized for search")

        # Citation collision: add the twin, then both share 410 U.S. 113.
        check(db.add_opinion(twin_url, twin_html) is True, "add twin opinion")
        hits = db.find_by_citation(410, "U.S.", 113)
        check(len(hits) == 2, f"citation collision returns both ({len(hits)})")

        # Party search (tolerant, all-tokens-must-match).
        check(len(db.find_by_party("wade")) == 1, "party single token")
        check(len(db.find_by_party("Roe v. Wade")) == 1, "party full name")
        check(len(db.find_by_party("jones")) == 1, "twin party token")

        # find() dispatch.
        check(len(db.find("12345678901234567890")) == 1, "find by scholar id")
        check(len(db.find("410 U.S. 113")) == 2, "find by citation")
        check(len(db.find("roe wade")) == 1, "find by party")
        check(db.find("nonesuch xyzzy") == [], "find miss -> empty")

        # Index rebuild from JSONL keeps everything.
        db.rebuild_index()
        check(db.count() == 2, "count survives rebuild")
        check(len(db.find("410 U.S. 113")) == 2, "citation survives rebuild")

        # Reopen (fresh index file would rebuild; same file should be in sync).
        db.close()
        db2 = OpinionDB(jsonl, index)
        check(db2.count() == 2, "count after reopen")

        # Merge: a third opinion from another file; re-merge is a no-op.
        other = Path(d) / "other.jsonl"
        third = extract_record(
            "https://scholar.google.com/scholar_case?case=55555555555555555555",
            opinion_html("DOE v. ROE", ["500 U.S. 1"], "Reversed. " * 20),
        )
        other.write_text(json.dumps(third) + "\n", encoding="utf-8")
        stats = db2.merge_from(other)
        check(stats["added"] == 1 and stats["skipped"] == 0, f"merge add: {stats}")
        stats2 = db2.merge_from(other)
        check(stats2["added"] == 0 and stats2["skipped"] == 1, f"re-merge: {stats2}")
        check(db2.count() == 3, "count after merge")

        # search_names casts a wider net than find_by_party: it returns every
        # opinion sharing *any* party token (so a fuzzy matcher can rank them),
        # ordered by how many distinct query tokens each matches.
        names = [h["name"] for h in db2.search_names("roe wade")]
        check("Roe v. Wade" in names and "Doe v. Roe" in names,
              f"search_names returns any-token matches: {names}")
        check(names and names[0] == "Roe v. Wade",
              f"search_names ranks the fuller token match first: {names}")
        check(db2.search_names("xyzzy nonesuch") == [],
              "search_names with no shared token -> empty")
        db2.close()

    if failures:
        print(f"\n{len(failures)} FAILED")
        sys.exit(1)
    print("\nOK: opinion_db self-test passed")
