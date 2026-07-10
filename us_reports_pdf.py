"""Extract single opinions from US Reports volume PDFs, fetching the volumes
from the Supreme Court's website on first use.

The official per-opinion scans stop at vol 542 (LOC CDN) / vol 583 (GovInfo).
For later volumes the Supreme Court publishes whole bound volumes (vols
502-587 today) and preliminary prints (588+, split in two halves, each with
its own roman-numbered front matter) at predictable URLs:

  https://www.supremecourt.gov/opinions/boundvolumes/584BV.pdf
  https://www.supremecourt.gov/opinions/preliminaryprint/592US2PP.pdf
                                    (suffix varies: _final / _web / bare)

Given a citation like "584 U.S. 79" this module downloads the volume into
the "US Reports" folder next to this module — once; every later citation
into that volume reuses the saved file — then carves the cited opinion out
of it and caches that as a standalone PDF.  Only volumes past GovInfo's
per-opinion coverage (584+) are ever fetched; volumes already sitting in
the folder (hand-downloaded) are used as-is, no network involved.

How the carving works — all of these PDFs carry proper page *labels*
("III", "XI", "1", "29", …), so the printed reporter page maps straight to
a PDF index with no text scanning.  Where an opinion *ends* is read from
the running heads, which are rigidly structured:

  opinion first page   "28 OCTOBER TERM, 2017"  /  "OCTOBER TERM, 2017 79"
  interior recto       "Cite as: 584 U. S. 28 (2018) 29"   (28 = opinion start)
  interior verso       "30 AYESTAS v. DAVIS"
  orders section       "902 OCTOBER TERM, 2018"  /  "ORDERS 903"  (every page)

So: start at the cited page and scan forward; the opinion ends just before
the first page whose head says OCTOBER TERM (a new opinion's first page),
ORDERS, a "Cite as:" start page other than ours, a new section title, or a
gap in the printed page numbers (the label "runs" separate the opinions,
orders, in-chambers and rules-amendment sections).  A citation into the
orders section therefore extracts that single page — orders share pages and
their running heads carry no per-order boundary — while argued opinions and
per curiams come out whole (syllabus through last dissent).

Headless (no tkinter); requires pypdfium2.  Run
``python -X utf8 us_reports_pdf.py 584 79`` to test an extraction.
"""

from __future__ import annotations

import ctypes
import os
import re
import threading
from pathlib import Path
from typing import Optional

import requests

# The volume PDFs live next to the app; downloads land here too, so a volume
# is fetched from supremecourt.gov at most once.
US_REPORTS_DIR = Path(__file__).resolve().parent / "US Reports"

# Extracted per-opinion PDFs land here, next to the app's other caches.
CACHE_DIR = Path.home() / ".config" / "courtlistener" / "usrep_cache"

# Where the Court hosts the volume PDFs (bound volumes, else preliminary
# prints whose filename suffix varies with revision state — probe in order).
# Only volumes past GovInfo's per-opinion coverage (through 583) are worth
# fetching; the site does host bound volumes back to 502, but GovInfo already
# serves those as per-opinion PDFs.
_SC_BASE = "https://www.supremecourt.gov/opinions/"
_SC_DOWNLOAD_MIN = 584
_PP_SUFFIXES = ("_final", "_web", "")
_MAX_PP_PARTS = 4  # currently 2 halves; tolerate more

_DOWNLOAD_TIMEOUT = 120  # whole-volume PDFs run 2-15 MB

# "584 U.S. 79", "584 U. S. 79" (the reports themselves put a space in U. S.)
CITATION_RE = re.compile(r"(\d+)\s+U\.\s?S\.\s+(\d+)")

# No opinion (with appendices) comes close to this; guards a runaway scan.
_MAX_OPINION_PAGES = 300

# Running-head shapes that mark "this page starts something new".
_TERM_HEAD_RE = re.compile(r"\bOCTOBER TERM, \d{4}\b")
_CITE_AS_RE = re.compile(r"Cite as:\s*(\d+)\s*U\.\s?S\.\s*(\d+)")
_ORDERS_HEAD_RE = re.compile(r"^ORDERS(?:\s+\d+|\s+FOR\b)")
_SECTION_TITLE_RE = re.compile(
    r"^(?:REPORTER['’]S NOTE|AMENDMENTS? TO|OPINIONS? OF INDIVIDUAL JUSTICES)",
)

# Preliminary prints watermark every page; it shows up as a text line that
# would otherwise be mistaken for the running head.
_WATERMARK = "Page Proof Pending Publication"

_lock = threading.Lock()
# path -> (mtime, size, [(pdf_index, printed_page), …] for arabic labels)
_label_cache: dict[Path, tuple[float, int, list[tuple[int, int]]]] = {}

_dl_lock = threading.Lock()
# Volumes that supremecourt.gov didn't have this session — don't re-probe on
# every citation into them.
_dl_missing: set[int] = set()

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """Lazy session with browser-like headers; supremecourt.gov serves PDFs
    to plain clients, but a real User-Agent keeps us clear of bot filters."""
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        _session = s
    return _session


def available() -> bool:
    """True when pypdfium2 is importable (volumes are fetched on demand)."""
    try:
        import pypdfium2  # noqa: F401
    except ImportError:
        return False
    return True


def volume_files(vol: int) -> list[Path]:
    """The local PDF file(s) holding *vol*, e.g. 584BV.pdf or the two
    588US1PP/588US2PP halves.  Empty when the volume isn't in the folder."""
    if not US_REPORTS_DIR.is_dir():
        return []
    out = []
    for p in sorted(US_REPORTS_DIR.glob("*.pdf")):
        m = re.match(r"(\d+)\D", p.name)
        if m and int(m.group(1)) == vol:
            out.append(p)
    return out


def has_volume(vol: int) -> bool:
    return bool(volume_files(vol))


def _download(url: str, dest: Path) -> bool:
    """Stream *url* into *dest* (atomically, via a temp file).  False on any
    HTTP error or when the body isn't a PDF; True once *dest* is in place."""
    try:
        resp = _get_session().get(url, stream=True, timeout=_DOWNLOAD_TIMEOUT)
    except Exception as exc:
        print(f"[usrep] download failed ({exc}): {url}")
        return False
    with resp:
        if resp.status_code != 200:
            return False
        tmp = dest.with_suffix(f".{os.getpid()}.part")
        try:
            first = b""
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 16):
                    if not first:
                        first = chunk[:8]
                        if not first.startswith(b"%PDF"):
                            print(f"[usrep] not a PDF: {url}")
                            return False
                    f.write(chunk)
            if not first:
                return False
            tmp.replace(dest)
            print(f"[usrep] downloaded {dest.name} "
                  f"({dest.stat().st_size // 1024} KB) from {url}")
            return True
        except Exception as exc:
            print(f"[usrep] download failed ({exc}): {url}")
            return False
        finally:
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass


def ensure_volume(vol: int) -> list[Path]:
    """The local file(s) for *vol*, downloading them from supremecourt.gov
    into the "US Reports" folder the first time.  Only volumes past
    GovInfo's coverage (584+) are fetched.  Tries the bound volume (single
    complete file), then the preliminary-print halves, probing each half's
    filename suffix (_final / _web / bare).  Empty when the Court hasn't
    published the volume (or the network is down); a volume found missing
    isn't re-probed until the app restarts."""
    files = volume_files(vol)
    if files or vol < _SC_DOWNLOAD_MIN or vol in _dl_missing:
        return files
    with _dl_lock:
        files = volume_files(vol)  # another thread may have fetched it
        if files or vol in _dl_missing:
            return files
        US_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        bv = f"{vol}BV.pdf"
        if _download(_SC_BASE + f"boundvolumes/{bv}", US_REPORTS_DIR / bv):
            return volume_files(vol)
        for part in range(1, _MAX_PP_PARTS + 1):
            for suffix in _PP_SUFFIXES:
                name = f"{vol}US{part}PP{suffix}.pdf"
                if _download(_SC_BASE + f"preliminaryprint/{name}",
                             US_REPORTS_DIR / name):
                    break
            else:
                break  # no variant of this half → no further halves either
        files = volume_files(vol)
        if not files:
            _dl_missing.add(vol)
            print(f"[usrep] volume {vol} not on supremecourt.gov yet")
        return files


def _page_label(pdf, raw_mod, index: int) -> Optional[str]:
    """The printed page label of page *index* ("XI", "29"), None if absent."""
    buflen = raw_mod.FPDF_GetPageLabel(pdf.raw, index, None, 0)
    if buflen <= 0:
        return None
    buf = ctypes.create_string_buffer(buflen)
    raw_mod.FPDF_GetPageLabel(pdf.raw, index, buf, buflen)
    return buf.raw[: buflen - 2].decode("utf-16-le", errors="replace")


def _arabic_labels(path: Path, pdf) -> list[tuple[int, int]]:
    """All (pdf_index, printed_page) pairs with a plain-number label, cached
    per file (keyed on mtime+size)."""
    st = path.stat()
    with _lock:
        hit = _label_cache.get(path)
        if hit and hit[0] == st.st_mtime and hit[1] == st.st_size:
            return hit[2]
    import pypdfium2.raw as C

    pairs: list[tuple[int, int]] = []
    for i in range(len(pdf)):
        lab = _page_label(pdf, C, i)
        if lab and lab.isdigit():
            pairs.append((i, int(lab)))
    with _lock:
        _label_cache[path] = (st.st_mtime, st.st_size, pairs)
    return pairs


def _runs(pairs: list[tuple[int, int]]) -> list[tuple[int, int, int, int]]:
    """Contiguous stretches of sequential printed pages:
    (idx_start, page_start, idx_end, page_end).  The gaps between runs are
    the volume's section boundaries (opinions | orders | in-chambers | rules)."""
    runs: list[list[int]] = []
    for idx, num in pairs:
        if runs and idx == runs[-1][2] + 1 and num == runs[-1][3] + 1:
            runs[-1][2], runs[-1][3] = idx, num
        else:
            runs.append([idx, num, idx, num])
    return [tuple(r) for r in runs]


def _head_lines(pdf, index: int, n: int = 2) -> list[str]:
    """The first *n* real text lines of a page — the running head region —
    with the preliminary-print watermark and blank lines dropped."""
    try:
        page = pdf[index]
        text = page.get_textpage().get_text_bounded()
    except Exception:
        return []
    lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line or _WATERMARK in line:
            continue
        lines.append(line)
        if len(lines) >= n:
            break
    return lines


def _page_is_blank(pdf, index: int) -> bool:
    try:
        return not pdf[index].get_textpage().get_text_bounded().strip()
    except Exception:
        return False


def _starts_new_item(head: list[str], start_page: int, printed: int) -> bool:
    """True when a page's running head says it begins the *next* opinion,
    an orders page, or a new back-matter section — i.e. ours ended."""
    for line in head:
        m = _CITE_AS_RE.search(line)
        if m:
            # Interior recto of some opinion, giving that opinion's start
            # page.  Ours when it equals start_page — but the volumes carry
            # the odd misprint (584BV says "Cite as: 584 U. S. 544" on
            # p. 563 of Upper Skagit, which starts at 554), so only treat it
            # as the next opinion when the start is *plausible*: after ours
            # and not beyond the page it is printed on.
            x = int(m.group(2))
            return start_page < x <= printed
        if _TERM_HEAD_RE.search(line):
            return True
        if _ORDERS_HEAD_RE.match(line) or _SECTION_TITLE_RE.match(line):
            return True
    return False


def _locate(vol: int, page: int) -> Optional[tuple[Path, "object", int, int]]:
    """Find the cited page and the end of its opinion.

    Returns (path, open PdfDocument, first_index, last_index) — the caller
    must close the document — or None when the volume/page isn't held locally.
    """
    import pypdfium2 as pdfium

    for path in volume_files(vol):
        pdf = pdfium.PdfDocument(path)
        try:
            pairs = _arabic_labels(path, pdf)
        except Exception:
            pdf.close()
            raise
        # Prefer the longest run holding the page — 587BV has a stray "1"
        # label on its cover that would otherwise shadow the real page 1.
        holding = [r for r in _runs(pairs) if r[1] <= page <= r[3]]
        if not holding:
            pdf.close()
            continue
        run = max(holding, key=lambda r: r[3] - r[1])
        start = run[0] + (page - run[1])

        end = start
        limit = min(run[2], start + _MAX_OPINION_PAGES - 1)
        for idx in range(start + 1, limit + 1):
            printed = run[1] + (idx - run[0])
            if _starts_new_item(_head_lines(pdf, idx), page, printed):
                break
            end = idx
        # An opinion ending on a recto leaves a blank labelled verso before
        # the next one; don't ship trailing blanks.
        while end > start and _page_is_blank(pdf, end):
            end -= 1
        return path, pdf, start, end
    return None


def extract(vol: int, page: int) -> Optional[Path]:
    """Carve the opinion starting at ``vol U.S. page`` out of the volume PDF
    into a standalone cached PDF, downloading the volume from
    supremecourt.gov if it isn't in the folder yet; None when the volume
    can't be had or the page isn't in it."""
    import pypdfium2 as pdfium

    out = CACHE_DIR / f"usrep{vol:03d}{page:04d}.pdf"
    if out.is_file() and out.stat().st_size > 0:
        # Stale when a volume file changed since — e.g. a "_web" preliminary
        # print replaced by the final print or the bound volume.  With the
        # volume files gone entirely, the cached opinion is still good (and
        # much cheaper than re-fetching the whole volume).
        sources = volume_files(vol)
        if not sources or out.stat().st_mtime >= max(
                p.stat().st_mtime for p in sources):
            return out
    ensure_volume(vol)
    located = _locate(vol, page)
    if located is None:
        return None
    path, pdf, start, end = located
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        dst = pdfium.PdfDocument.new()
        dst.import_pages(pdf, pages=list(range(start, end + 1)))
        tmp = out.with_suffix(".tmp")
        dst.save(tmp)
        dst.close()
        tmp.replace(out)
        print(f"[usrep] extracted {vol} U.S. {page} from {path.name} "
              f"(pdf pages {start + 1}-{end + 1})")
        return out
    finally:
        pdf.close()


def extract_citation(cite: str) -> Optional[Path]:
    """extract() from a citation string ("584 U.S. 79"); None when it isn't
    a US Reports cite or the volume is neither local nor on the Court's
    site."""
    m = CITATION_RE.search(cite or "")
    if not m:
        return None
    vol, page = int(m.group(1)), int(m.group(2))
    if not has_volume(vol) and vol < _SC_DOWNLOAD_MIN:
        return None
    try:
        return extract(vol, page)
    except Exception as exc:
        print(f"[usrep] extraction failed for {vol} U.S. {page}: {exc}")
        return None


if __name__ == "__main__":
    import sys

    v, p = int(sys.argv[1]), int(sys.argv[2])
    result = extract(v, p)
    print(result if result else f"no local copy of {v} U.S. {p}")
