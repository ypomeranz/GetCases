"""In-app Florida statutes from the official source, the Florida Senate
(``flsenate.gov``).

A section is fetched from the per-year display URL

    https://www.flsenate.gov/Laws/Statutes/<year>/<section>

e.g. ``/Laws/Statutes/2025/776.012`` for Fla. Stat. § 776.012.  The site
publishes one edition per year; the latest complete edition is discovered at
runtime (``_latest_year``) and cached.

Florida renders a section as nested ``<div>`` elements whose class names give
the hierarchy — ``Subsection`` (1) > ``Paragraph`` (a) > ``SubParagraph`` 1. >
``SubSubParagraph`` a. — each holding a ``<span class="Number">`` designator
and ``<span class="Text…">`` body, with a descriptive ``Catchline`` title and a
trailing ``<div class="History">``.  Because the divs nest, this module walks
the markup with the stdlib HTML parser (rather than regex) and assigns the
indent from the class.

Mirrors the source-module contract of ``us_code`` / ``state_ca`` so the GUI's
statute viewer renders Florida the same way.  ``state_statutes`` owns citation
*detection* (key ``fl``); this module owns the Florida *fetch + parse*.
"""

from __future__ import annotations

import datetime
import re
import threading
from dataclasses import dataclass, field
from html.parser import HTMLParser

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}

_HOST = "flsenate.gov"

# Subdivision class -> indent depth (matches Florida's drafting hierarchy).
_CLASS_INDENT = {
    "subsection": 0,
    "paragraph": 1,
    "subparagraph": 2,
    "subsubparagraph": 3,
    "subsubsubparagraph": 4,
}


def section_url(year: int, section: str) -> str:
    return f"https://www.flsenate.gov/Laws/Statutes/{year}/{section}"


class _SectionParser(HTMLParser):
    """Walk a Florida section page into a (indent, text) body stream plus the
    catchline (section title) and history (credit)."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.body: list[tuple[int, str]] = []
        self.catchline = ""
        self.history = ""
        self._buf: list[str] = []
        self._indent = 0
        self._mode: str | None = None     # 'body' | 'catchline' | 'history'
        self._in_section = False
        self._depth = 0                   # tag depth since <... class="Section">
        self._section_depth = 0

    def handle_starttag(self, tag, attrs):
        cls = (dict(attrs).get("class") or "").strip().lower()
        first = cls.split()[0] if cls else ""
        if not self._in_section:
            if "section" == first:
                self._in_section = True
                self._section_depth = self._depth
            self._depth += 1
            return
        self._depth += 1
        if first in _CLASS_INDENT:
            self._flush()
            self._mode = "body"
            self._indent = _CLASS_INDENT[first]
        elif first == "sectionbody":
            self._flush()
            self._mode = "body"
            self._indent = 0
        elif first == "catchline":
            self._flush()
            self._mode = "catchline"
        elif first == "history":
            self._flush()
            self._mode = "history"

    def handle_endtag(self, tag):
        if self._in_section:
            self._depth -= 1
            if self._depth <= self._section_depth:
                self._in_section = False
                self._flush()
        else:
            self._depth = max(0, self._depth - 1)

    def handle_data(self, data):
        if self._in_section and self._mode:
            self._buf.append(data)

    def _flush(self) -> None:
        text = re.sub(r"\s+", " ", "".join(self._buf)).strip()
        self._buf = []
        mode, self._mode = self._mode, None
        if not text:
            return
        if mode == "catchline":
            self.catchline = text.rstrip(" —-—")
        elif mode == "history":
            self.history = (self.history + " " + text).strip()
        elif mode == "body":
            self.body.append((min(self._indent, 6), text))


def parse_section_html(page_html: str, label: str) -> tuple[list, str]:
    """Return (paras, catchline) for a Florida section page.  `paras` is the
    (kind, indent, text) stream; `catchline` is the section's descriptive
    title (for the heading)."""
    p = _SectionParser()
    p.feed(page_html)
    p.close()
    head = f"{label} — {p.catchline}" if p.catchline else label
    paras: list[tuple[str, int, str]] = [("sechead", 0, head)]
    for indent, text in p.body:
        paras.append(("body", indent, text))
    if p.history:
        paras.append(("credit", 0, p.history))
    return paras, p.catchline


@dataclass
class FlStatuteDoc:
    sec: str
    year: int
    url: str
    catchline: str = ""
    paras: list[tuple[str, int, str]] = field(default_factory=list)

    @property
    def kind(self) -> str:
        return "statestat"

    @property
    def title(self) -> str:
        return "fl"

    @property
    def section(self) -> str:
        return self.sec

    @property
    def label(self) -> str:
        return f"Fla. Stat. § {self.sec}"

    @property
    def heading(self) -> str:
        return f"{self.label} — {self.catchline}" if self.catchline else self.label

    @property
    def source_name(self) -> str:
        return "The Florida Senate"

    @property
    def source_note(self) -> str:
        return (f"{self.year} Florida Statutes — official text "
                f"(flsenate.gov)")

    def bluebook_cite(self, subs: tuple = ()) -> str:
        tail = "".join(f"({s})" for s in subs)
        return f"Fla. Stat. § {self.sec}{tail}"

    def neighbors(self):
        # Florida's adjacent-section numbers would require parsing the chapter
        # table of contents; prev/next is left disabled rather than guessed.
        return None, None


_cache: dict[str, FlStatuteDoc] = {}
_year_cache: list[int] = []
_lock = threading.Lock()


def _is_real_section(page_html: str, section: str) -> bool:
    """A real section page titles "Chapter X Section Y"; a missing one falls
    back to a year landing page."""
    m = re.search(r"<title>(.*?)</title>", page_html, re.IGNORECASE | re.DOTALL)
    return bool(m and re.search(r"Chapter\s+\d+\s+Section", m.group(1), re.I))


def _fetch(year: int, section: str):
    import requests
    resp = requests.get(section_url(year, section), headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.content.decode("utf-8", "replace")


def load(key: str, section: str) -> FlStatuteDoc:
    """Fetch and parse one Florida section.  `key` is "fl"; `section` is the
    chapter.section number ("776.012").  Raises RuntimeError on failure."""
    section = str(section).strip().rstrip(".")
    with _lock:
        if section in _cache:
            return _cache[section]
        years = list(_year_cache)

    # Try the cached good year first, else probe the current edition downward.
    if not years:
        this_year = datetime.datetime.now().year
        years = [this_year, this_year - 1, this_year - 2]

    page = None
    used_year = None
    last_exc = None
    for y in years:
        try:
            html = _fetch(y, section)
        except Exception as exc:           # network / HTTP error — try older
            last_exc = exc
            continue
        if _is_real_section(html, section):
            page, used_year = html, y
            break

    if page is None:
        if last_exc is not None:
            raise RuntimeError(f"flsenate.gov: {last_exc}") from last_exc
        raise RuntimeError(f"no such section: Fla. Stat. § {section}")

    label = f"Fla. Stat. § {section}"
    paras, catchline = parse_section_html(page, label)
    if not any(k == "body" for k, _i, _t in paras):
        raise RuntimeError(f"no text found for {label}")
    doc = FlStatuteDoc(sec=section, year=used_year,
                       url=section_url(used_year, section),
                       catchline=catchline, paras=paras)
    with _lock:
        _cache[section] = doc
        if used_year not in _year_cache:
            _year_cache.insert(0, used_year)
    return doc


if __name__ == "__main__":
    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    # A page mirroring flsenate's real nesting (Section > SectionBody >
    # Subsection/Paragraph/SubParagraph divs, each Number + Text; History).
    sample = """<html><body>
    <div class="Section">
      <span class="SectionNumber">810.02&#x2003;</span>
      <span class="Catchline">Burglary.</span><span class="EmDash">&#x2014;</span>
      <span class="SectionBody">
        <div class="Subsection"><span class="Number">(1)&#x2003;</span>
          <span class="Text Intro Justify">As used in this chapter, the term means:</span>
          <div class="Paragraph"><span class="Number">(a)&#x2003;</span>
            <span class="Text">Entering a dwelling with intent to commit an offense.</span>
            <div class="SubParagraph"><span class="Number">1.&#x2003;</span>
              <span class="Text">Unless the premises are open to the public.</span></div>
          </div>
        </div>
        <div class="Subsection"><span class="Number">(2)&#x2003;</span>
          <span class="Text">Burglary is a felony of the first degree.</span></div>
      </span>
      <div class="History"><span class="HistoryTitle">History.</span>
        <span class="EmDash">&#x2014;</span>
        <span class="HistoryText">s. 13, ch. 74-383; s. 2, ch. 2005-27.</span></div>
    </div>
    <div class="grid-20">Senators Session Bills Calendars (site chrome)</div>
    <p>Privacy Statement|Accessibility</p>
    </body></html>"""
    paras, catchline = parse_section_html(sample, "Fla. Stat. § 810.02")
    kinds = [(k, i) for k, i, _t in paras]
    check(catchline == "Burglary.", f"catchline: {catchline!r}")
    check(paras[0] == ("sechead", 0, "Fla. Stat. § 810.02 — Burglary."),
          f"sechead with catchline: {paras[0]!r}")
    check(("body", 0) in kinds and ("body", 1) in kinds and ("body", 2) in kinds,
          f"nested indents from class: {kinds!r}")
    # designator + text merged into one para per subdivision
    b0 = next(t for k, i, t in paras if (k, i) == ("body", 0))
    check(b0.startswith("(1)") and "means:" in b0, f"subsection (1): {b0!r}")
    b1 = next(t for k, i, t in paras if (k, i) == ("body", 1))
    check(b1.startswith("(a)") and "dwelling" in b1, f"paragraph (a): {b1!r}")
    b2 = next(t for k, i, t in paras if (k, i) == ("body", 2))
    check(b2.startswith("1.") and "public" in b2, f"subparagraph 1.: {b2!r}")
    check(paras[-1][0] == "credit" and "ch. 74-383" in paras[-1][2],
          f"history -> credit: {paras[-1]!r}")
    check(not any("site chrome" in t or "Privacy" in t for _k, _i, t in paras),
          "site chrome excluded")

    # missing section -> no body
    empty, _ = parse_section_html("<html><body><div class='grid-20'>x</div></body></html>",
                                  "Fla. Stat. § 999.999")
    check(not any(k == "body" for k, _i, _t in empty), "no Section -> no body")

    # _is_real_section discriminates section pages from landing pages
    check(_is_real_section("<title>Chapter 776 Section 012 - 2025 Florida Statutes</title>", "776.012"),
          "real section title recognized")
    check(not _is_real_section("<title>2025 Florida Statutes - The Florida Senate</title>", "776.012"),
          "landing page rejected")

    raise SystemExit(1 if failed else 0)
