"""Compile every authority a brief cites into a single ZIP of downloaded cases,
statutes and rules.

Used by GetCases' brief reader: "Download Cited Cases…" scans the open brief for
citations (via :mod:`citations`), resolves each one to a file, and writes them
all — with proper Bluebook file names — into one ``.zip`` the user chooses.

Design
------
The *ordering* of where to look for a case, the de-duplication of citations, and
the Bluebook file naming all live here so they can be unit-tested without Tk or
the network (run ``python3 brief_compiler.py`` for the offline self-test).  The
actual fetching — HEAD-checking static.case.law, downloading PDFs, the
CourtListener API, the Google Scholar cache/search — is injected as a
``resolver`` object built by the GUI from the helpers it already owns.

Per the product spec, each cited **case** is resolved in this strict order, so
Google Scholar is only ever *searched* (a network request that can get the app
throttled) when nothing else can find the case:

  1. **static.case.law** — built straight from the cited reporter.
  2. **Google Scholar cache** — an opinion already downloaded (no network).
  3. **CourtListener** — resolve the citation through the API; use the parallel
     citations it returns to retry static.case.law, then fall back to the
     CourtListener opinion text itself.
  4. **Google Scholar search** — last resort only.

Statutes, rules, regulations and the Constitution are saved as their text; the
Statutes at Large are saved as the official PDF.
"""

from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass, field
from typing import Callable, Optional

import bluebook_names
import citations

__all__ = [
    "Authority",
    "AuthorityResult",
    "CompileSummary",
    "collect_authorities",
    "compile_to_zip",
    "case_file_stem",
    "safe_filename",
    "html_to_text",
]


# ---------------------------------------------------------------------------
# What a brief cites
# ---------------------------------------------------------------------------

@dataclass
class Authority:
    """One thing a brief cites, de-duplicated across every place it appears.

    ``kind`` is the citation action kind from :func:`citations.detect_links`
    ("cite" for a reporter case, "usc"/"cfr"/"rule"/"const"/"statestat" for a
    statute-like source, "statpdf" for a Statutes at Large scan, plus
    "recap"/"engrep"/"browse" we can only note).  ``value`` is that action's
    payload (a "vol reporter page" citation for a case — pin cite stripped — or
    the source spec).  ``name``/``year`` are the case caption and decision year
    scraped from the brief text near the citation, used for the file name."""

    kind: str
    value: str
    name: str = ""
    year: str = ""

    @property
    def is_case(self) -> bool:
        return self.kind == "cite"

    def label(self) -> str:
        """A short human label for progress/manifest lines."""
        if self.is_case:
            return ", ".join(p for p in (self.name, self.value) if p) \
                or self.value
        return self.value


# Statute-like sources whose text we can render into the zip.
_TEXT_STATUTE_KINDS = frozenset({"usc", "cfr", "rule", "const", "statestat"})
# Kinds we recognise but cannot bundle (link-only / needs a bespoke fetch).
_NOTE_ONLY = {
    "browse": "state statute — official source is link-only",
    "engrep": "English Reports — CommonLII scan not bundled",
    "recap": "unpublished opinion — RECAP/PACER document not bundled",
    "url": "external link",
}


def collect_authorities(text: str) -> list[Authority]:
    """Every distinct authority a brief cites, in first-appearance order.

    Reporter-case citations are keyed by their base citation (pin cite dropped)
    so a case cited a dozen times — including its short forms and ``Id.``s,
    which :func:`citations.detect_links` resolves to the same base — is compiled
    once.  Statute-like sources are keyed by their spec.  The case name and year
    are taken from the fullest occurrence (the one that actually prints them)."""
    out: list[Authority] = []
    by_key: dict[tuple, Authority] = {}
    for start, end, action in citations.detect_links(text or ""):
        kind, value = action
        if kind == "cite":
            base = str(value).split("@", 1)[0].strip()
            if not base:
                continue
            key = ("cite", base)
            name = _name_before(text, start)
            year = _year_after(text, end)
            existing = by_key.get(key)
            if existing is None:
                by_key[key] = Authority("cite", base, name=name, year=year)
                out.append(by_key[key])
            else:
                # Fill in a caption/year from a richer later occurrence.
                if not existing.name and name:
                    existing.name = name
                if not existing.year and year:
                    existing.year = year
            continue
        # Statute-like sources: one file per section, so the same section cited
        # with different pin-cited subsections ("§ 1983" and "§ 1983(b)") isn't
        # downloaded twice.  The spec is "title:section:subsections"; key on the
        # first two and keep the first (fullest) spec seen.
        if kind in _TEXT_STATUTE_KINDS and str(value).count(":") >= 2:
            title, section = str(value).split(":", 2)[:2]
            key = (kind, title, section)
        else:
            key = (kind, str(value))
        if key in by_key:
            continue
        auth = Authority(kind, str(value))
        by_key[key] = auth
        out.append(auth)
    return out


# Lowercase words that legitimately sit inside a party name ("Board of
# Education", "Jones & Laughlin", "State ex rel. Doe") — so walking outward from
# the "v." doesn't stop on them.  Prepositions that double as prose connectors
# ("relied on", "held in") are deliberately absent, which is what stops the
# scrape from swallowing "The Court relied on Roe".
_NAME_PARTICLES = frozenset({
    "of", "the", "and", "ex", "rel", "rel.", "et", "al", "al.", "&",
    "de", "la", "le", "van", "von", "der", "den", "del", "dos", "du", "da",
})
# Capitalized words that lead *into* a citation (a signal or a sentence start)
# but are never the first word of a party name — stripped off the front of the
# scraped first party so "In Miranda v. Arizona" / "Cf. Nat'l Fed'n v. …" don't
# keep the "In" / "Cf.".
_NAME_LEAD_INS = frozenset({
    "in", "see", "cf", "cf.", "e.g.", "eg", "but", "accord", "compare",
    "contra", "citing", "quoting", "following", "also", "cited", "under",
    "quoted", "id.", "id", "here", "thus",
})
_INRE_RE = re.compile(r"(?i)\b(in re|ex parte|matter of)\b")


def _looks_like_name_token(tok: str) -> bool:
    if not tok:
        return False
    if tok[0].isupper() or tok[0].isdigit():
        return True
    return tok.strip(".,").lower() in _NAME_PARTICLES


def _name_before(text: str, start: int) -> str:
    """The case caption printed immediately before the citation at *start*, or
    "" — scraped from the brief so a static.case.law-only download still gets a
    proper Bluebook file name.

    Works outward from the "v." nearest the citation: party names are built from
    the capitalized (and in-name particle) tokens on either side, stopping at
    the first prose word, so "The Court relied on Roe v. Wade, 410 U.S. 113"
    yields "Roe v. Wade", not the whole clause."""
    window = text[max(0, start - 160):start].replace("\n", " ")
    window = re.sub(r"[\s,;]+$", "", window)          # trailing scaffolding
    tokens = window.split()
    if not tokens:
        return ""

    # "In re X" / "Ex parte X" / "Matter of X" captions (no "v.").
    inre = list(_INRE_RE.finditer(window))
    if inre:
        tail = window[inre[-1].start():].split()
        prefix_len = len(inre[-1].group(0).split())
        name_toks = [tail[0].title()] + list(tail[1:prefix_len])  # the label
        for tok in tail[prefix_len:]:
            if _looks_like_name_token(tok):
                name_toks.append(tok)
            else:
                break
        name = re.sub(r"\s+", " ", " ".join(name_toks)).strip(" ,;.")
        if len(name) >= 6:
            return name

    # Otherwise find the "v." nearest the citation (rightmost) and grow both
    # party names outward from it.
    sep = None
    for i in range(len(tokens) - 1, -1, -1):
        if tokens[i].strip(".,").lower() in ("v", "vs"):
            sep = i
            break
    if sep is None or sep == 0 or sep == len(tokens) - 1:
        return ""

    left: list[str] = []
    for tok in reversed(tokens[:sep]):
        if _looks_like_name_token(tok) and len(left) < 8:
            left.append(tok)
        else:
            break
    left.reverse()
    # Drop a leading citation signal / sentence-starter ("In", "Cf.", "But
    # see", …) that isn't part of the first party's actual name.
    while left and left[0].strip(".,").lower() in _NAME_LEAD_INS:
        left.pop(0)
    right: list[str] = []
    for tok in tokens[sep + 1:]:
        if _looks_like_name_token(tok) and len(right) < 8:
            right.append(tok)
        else:
            break
    if not left or not right:
        return ""
    name = re.sub(r"\s+", " ",
                  f"{' '.join(left)} v. {' '.join(right)}").strip(" ,;.")
    return name if len(name) >= 4 else ""


def _year_after(text: str, end: int) -> str:
    """The decision year in the parenthetical after a citation ("… 113 (1973)"
    → "1973"), or ""."""
    m = re.search(r"^[^.()]{0,40}?\((?:[^)]*?\s)?(\d{4})[a-z]?\)",
                  text[end:end + 80])
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# File naming
# ---------------------------------------------------------------------------

_UNSAFE_FILENAME_RE = re.compile(r'[\\/:*?"<>|\x00-\x1f]+')


def safe_filename(label: str, *, max_len: int = 180) -> str:
    """A filesystem-safe version of *label* that still reads like a citation:
    the Bluebook periods, commas, ``§`` and parentheses are kept; only the
    characters no file system allows are stripped."""
    name = _UNSAFE_FILENAME_RE.sub(" ", label or "")
    name = re.sub(r"\s+", " ", name).strip(" .")
    if len(name) > max_len:
        name = name[:max_len].rstrip(" .")
    return name or "authority"


def case_file_stem(name: str, cite: str, year: str = "") -> str:
    """The Bluebook file stem for a case: ``Abbrev. Name, vol Rep page (year)``.

    The party names are abbreviated with :mod:`bluebook_names` (so "Roe v. Wade,
    410 U.S. 113 (1973)"), falling back to the bare citation when no caption was
    found in the brief."""
    parts: list[str] = []
    if name:
        try:
            abbr = bluebook_names.abbreviate_case_name(name)
        except Exception:
            abbr = name
        abbr = (abbr or name).strip()
        if abbr:
            parts.append(abbr)
    if cite:
        parts.append(cite)
    stem = ", ".join(parts) if parts else (cite or "case")
    if year:
        stem += f" ({year})"
    return safe_filename(stem)


# ---------------------------------------------------------------------------
# HTML -> readable text (for the Scholar cache / CourtListener text copies)
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]+")


def html_to_text(html: str) -> str:
    """Best-effort plain text for a Scholar/CourtListener opinion page.

    Uses the app's own opinion parser when it's importable (the same rendering
    the reader shows), falling back to a tag strip so the module has no hard
    dependency on the network stack for its offline test."""
    if not html:
        return ""
    try:  # the reader's parser: paragraph structure, footnotes, star pages
        from google_scholar import blocks_to_text, parse_opinion_blocks
        text = blocks_to_text(parse_opinion_blocks(html))
        if text and text.strip():
            return text.strip()
    except Exception:
        pass
    # Fallback: drop scripts/styles, strip tags, unescape entities.
    import html as _html

    body = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    body = re.sub(r"(?i)<br\s*/?>", "\n", body)
    body = re.sub(r"(?i)</p\s*>", "\n\n", body)
    body = _TAG_RE.sub("", body)
    body = _html.unescape(body)
    body = _WS_RE.sub(" ", body)
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


# ---------------------------------------------------------------------------
# Resolution — the injected resolver does the fetching, we own the order
# ---------------------------------------------------------------------------

@dataclass
class AuthorityResult:
    """The outcome of compiling one authority."""

    authority: Authority
    ok: bool
    source: str = ""          # where it came from (for the manifest)
    filename: str = ""        # name inside the zip ("" when not saved)
    detail: str = ""          # a note when it couldn't be bundled


@dataclass
class CompileSummary:
    results: list[AuthorityResult] = field(default_factory=list)
    zip_path: str = ""
    cancelled: bool = False

    @property
    def saved(self) -> int:
        return sum(1 for r in self.results if r.ok)

    @property
    def missing(self) -> int:
        return sum(1 for r in self.results if not r.ok)


def _resolve_case(resolver, auth: Authority) -> tuple[Optional[bytes], str, str, str]:
    """Resolve one reporter-cited case to (bytes, extension, source, note).

    Follows the strict spec order (static.case.law → Scholar cache →
    CourtListener [alt-citation static.case.law, then the CL text] → Scholar
    search).  ``bytes`` is None when the case was found nowhere; ``note`` then
    explains.  ``auth.name`` may be upgraded to CourtListener's caption along
    the way so the file name is authoritative."""
    cite = auth.value

    # 1) static.case.law, straight from the cited reporter.
    hit = resolver.case_law_pdf([cite])
    if hit:
        url, _matched = hit
        data = resolver.pdf_bytes(url)
        if data:
            return data, ".pdf", "static.case.law", ""

    # 2) Google Scholar cache — an opinion already downloaded (no network).
    html = resolver.scholar_cached(cite, auth.name)
    if html:
        text = html_to_text(html)
        if text:
            return text.encode("utf-8"), ".txt", "Google Scholar (cache)", ""

    # 3) CourtListener: resolve the citation, then (a) retry static.case.law
    #    with the parallel citations it knows, else (b) take the CL opinion text.
    item = resolver.cl_resolve(cite, auth.name)
    if item is not None:
        cl_name = resolver.cl_case_name(item)
        if cl_name:
            auth.name = cl_name
        alt = [c for c in resolver.cl_all_cites(item) if c and c != cite]
        if alt:
            hit = resolver.case_law_pdf(alt)
            if hit:
                url, matched = hit
                data = resolver.pdf_bytes(url)
                if data:
                    return (data, ".pdf",
                            f"static.case.law (parallel cite {matched})", "")
        text = resolver.cl_opinion_text(item)
        if text and text.strip():
            return text.encode("utf-8"), ".txt", "CourtListener", ""

    # 4) Last resort: search Google Scholar over the network.
    html = resolver.scholar_search(cite, auth.name)
    if html:
        text = html_to_text(html)
        if text:
            return text.encode("utf-8"), ".txt", "Google Scholar (search)", ""

    return None, "", "", "not found on static.case.law, the Scholar cache, " \
                         "CourtListener, or Google Scholar"


def _resolve_statute(resolver, auth: Authority) -> tuple[Optional[bytes], str, str, str]:
    """Resolve a statute/rule/regulation/Constitution or Statutes at Large
    citation to (bytes, extension, source, note)."""
    if auth.kind == "statpdf":
        data = resolver.statute_pdf_bytes(auth.value)
        if data:
            return data, ".pdf", "Statutes at Large (GovInfo)", ""
        return None, "", "", "Statutes at Large PDF could not be downloaded"
    if auth.kind in _TEXT_STATUTE_KINDS:
        loaded = resolver.statute_text(auth.kind, auth.value)
        if loaded:
            label, text = loaded
            if text and text.strip():
                auth.name = label or auth.name
                header = f"{label}\n{'=' * len(label)}\n\n" if label else ""
                return (header + text).encode("utf-8"), ".txt", "official source", ""
        return None, "", "", "could not load the section text"
    note = _NOTE_ONLY.get(auth.kind, "not a downloadable source")
    return None, "", "", note


def _statute_file_stem(auth: Authority, resolver) -> str:
    """Bluebook file stem for a non-case authority — its citation label."""
    label = auth.name
    if not label:
        try:
            label = resolver.authority_label(auth.kind, auth.value)
        except Exception:
            label = ""
    return safe_filename(label or auth.value or auth.kind)


def compile_to_zip(
    authorities: list[Authority],
    resolver,
    zip_path: str,
    *,
    progress: Optional[Callable[[int, int, str], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> CompileSummary:
    """Resolve every *authority* and write the downloads into *zip_path*.

    ``progress(done, total, message)`` is called before each item.
    ``should_cancel()`` is polled between items; when it returns True the zip is
    closed with whatever was gathered so far and ``summary.cancelled`` is set.
    A ``_MANIFEST.txt`` describing every authority is always added."""
    summary = CompileSummary(zip_path=zip_path)
    total = len(authorities)
    used_names: set[str] = set()

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for i, auth in enumerate(authorities):
            if should_cancel is not None and should_cancel():
                summary.cancelled = True
                break
            if progress is not None:
                progress(i, total, auth.label())
            try:
                if auth.is_case:
                    data, ext, source, note = _resolve_case(resolver, auth)
                    stem = case_file_stem(auth.name, auth.value, auth.year)
                else:
                    data, ext, source, note = _resolve_statute(resolver, auth)
                    stem = _statute_file_stem(auth, resolver)
            except Exception as exc:  # a resolver failure must not sink the run
                data, ext, source, note = None, "", "", f"error: {exc}"
                stem = case_file_stem(auth.name, auth.value, auth.year) \
                    if auth.is_case else _statute_file_stem(auth, resolver)

            if data:
                fname = _unique_name(used_names, stem, ext)
                try:
                    zf.writestr(fname, data)
                    summary.results.append(
                        AuthorityResult(auth, True, source=source,
                                        filename=fname))
                except Exception as exc:
                    summary.results.append(
                        AuthorityResult(auth, False,
                                        detail=f"could not write file: {exc}"))
            else:
                summary.results.append(
                    AuthorityResult(auth, False, detail=note))

        if progress is not None:
            progress(min(len(summary.results), total), total, "Writing manifest…")
        zf.writestr("_MANIFEST.txt", _manifest_text(summary))
    return summary


def _unique_name(used: set[str], stem: str, ext: str) -> str:
    """A zip member name that hasn't been used yet (append " (2)", " (3)", …)."""
    base = f"{stem}{ext}"
    if base.lower() not in used:
        used.add(base.lower())
        return base
    n = 2
    while True:
        cand = f"{stem} ({n}){ext}"
        if cand.lower() not in used:
            used.add(cand.lower())
            return cand
        n += 1


def _manifest_text(summary: CompileSummary) -> str:
    lines = [
        "Cited authorities compiled by GetCases",
        "=" * 40,
        f"Saved: {summary.saved}    Not found: {summary.missing}"
        + ("    (cancelled early)" if summary.cancelled else ""),
        "",
    ]
    saved = [r for r in summary.results if r.ok]
    missing = [r for r in summary.results if not r.ok]
    if saved:
        lines.append("Downloaded")
        lines.append("-" * 40)
        for r in saved:
            lines.append(f"  {r.filename}")
            lines.append(f"      {r.authority.label()}  —  {r.source}")
        lines.append("")
    if missing:
        lines.append("Not downloaded")
        lines.append("-" * 40)
        for r in missing:
            lines.append(f"  {r.authority.label()}")
            if r.detail:
                lines.append(f"      {r.detail}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Offline self-test  (no Tk, no network — a fake resolver drives the ordering)
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover - offline smoke test
    import os
    import sys
    import tempfile

    failures = 0

    # --- collection & de-duplication ---
    brief = (
        "The Court relied on Roe v. Wade, 410 U.S. 113, 152 (1973), and again "
        "on 410 U.S. at 164.  See also In re Gault, 387 U.S. 1 (1967); "
        "42 U.S.C. § 1983; Fed. R. Civ. P. 56."
    )
    auths = collect_authorities(brief)
    cases = [a for a in auths if a.is_case]
    if not any(a.value == "410 U.S. 113" for a in cases):
        print("collect: Roe base cite missing", cases); failures += 1
    roe = next((a for a in cases if a.value == "410 U.S. 113"), None)
    if roe and roe.name != "Roe v. Wade":
        print("collect: Roe name wrong ->", repr(roe.name)); failures += 1
    if roe and roe.year != "1973":
        print("collect: Roe year wrong ->", repr(roe.year)); failures += 1
    # The short form "410 U.S. at 164" must not create a second case entry.
    if sum(1 for a in cases if a.value == "410 U.S. 113") != 1:
        print("collect: short form duplicated the case"); failures += 1
    gault = next((a for a in cases if a.value == "387 U.S. 1"), None)
    if gault and gault.name != "In re Gault":
        print("collect: Gault name wrong ->", repr(gault.name)); failures += 1

    # --- Bluebook file stems ---
    stem = case_file_stem("Roe v. Wade", "410 U.S. 113", "1973")
    if stem != "Roe v. Wade, 410 U.S. 113 (1973)":
        print("stem: Roe ->", repr(stem)); failures += 1
    if safe_filename('a/b:c*d?"e<f>g|h') == "":
        print("safe_filename stripped everything"); failures += 1
    if "/" in case_file_stem("A/B v. C", "1 F.2d 2", ""):
        print("stem: slash leaked into file name"); failures += 1

    # --- resolution order: a fake resolver records which lever was pulled ---
    class FakeResolver:
        def __init__(self, **avail):
            self.avail = avail          # which stages "have" the case
            self.calls: list[str] = []

        def case_law_pdf(self, cites):
            self.calls.append(f"case_law_pdf:{cites}")
            if self.avail.get("static") and any(
                    c in self.avail["static"] for c in cites):
                return ("https://static.case.law/x.pdf", cites[0])
            return None

        def pdf_bytes(self, url):
            self.calls.append("pdf_bytes")
            return b"%PDF-1.4 fake"

        def scholar_cached(self, cite, name):
            self.calls.append("scholar_cached")
            return "<p>cached opinion</p>" if self.avail.get("cache") else None

        def cl_resolve(self, cite, name):
            self.calls.append("cl_resolve")
            return {"cluster_id": 1} if self.avail.get("cl") else None

        def cl_case_name(self, item):
            return "Real v. Name"

        def cl_all_cites(self, item):
            return self.avail.get("cl_alts", [])

        def cl_opinion_text(self, item):
            self.calls.append("cl_opinion_text")
            return "CL opinion body" if self.avail.get("cl_text") else ""

        def scholar_search(self, cite, name):
            self.calls.append("scholar_search")
            return "<p>searched</p>" if self.avail.get("search") else None

    # static.case.law wins first — Scholar is never searched.
    r = FakeResolver(static={"999 F.2d 1"})
    data, ext, source, note = _resolve_case(
        r, Authority("cite", "999 F.2d 1", name="A v. B"))
    if not (data and ext == ".pdf" and source == "static.case.law"):
        print("order: static.case.law not preferred", source); failures += 1
    if "scholar_search" in r.calls:
        print("order: searched Scholar despite static hit!", r.calls); failures += 1

    # No static, no cache, no CL -> Scholar search is the last resort.
    r = FakeResolver(search=True)
    data, ext, source, note = _resolve_case(
        r, Authority("cite", "1 X 1", name="A v. B"))
    if source != "Google Scholar (search)":
        print("order: search not reached", source, r.calls); failures += 1
    if r.calls.index("scholar_cached") > r.calls.index("scholar_search"):
        print("order: cache checked after search", r.calls); failures += 1

    # CL parallel citation locates a static.case.law PDF (no Scholar search).
    r = FakeResolver(cl=True, cl_alts=["525 U.S. 500"], static={"525 U.S. 500"})
    data, ext, source, note = _resolve_case(
        r, Authority("cite", "119 S. Ct. 1", name="A v. B"))
    if not (data and ext == ".pdf" and "parallel cite" in source):
        print("order: CL alt-cite static retry failed", source, r.calls); failures += 1
    if "scholar_search" in r.calls:
        print("order: searched Scholar despite CL alt hit", r.calls); failures += 1

    # Cache beats CourtListener.
    r = FakeResolver(cache=True, cl=True, cl_text=True)
    data, ext, source, note = _resolve_case(
        r, Authority("cite", "1 X 1"))
    if source != "Google Scholar (cache)":
        print("order: cache not preferred over CL", source); failures += 1
    if "cl_resolve" in r.calls:
        print("order: hit CL despite a cache copy", r.calls); failures += 1

    # --- end-to-end zip with the fake resolver ---
    class ZipResolver(FakeResolver):
        def statute_text(self, kind, spec):
            return ("42 U.S.C. § 1983", "Every person who ...")

        def statute_pdf_bytes(self, url):
            return b"%PDF-1.4 stat"

        def authority_label(self, kind, spec):
            return "42 U.S.C. § 1983"

    tmp = tempfile.mkdtemp()
    zpath = os.path.join(tmp, "out.zip")
    test_auths = [
        Authority("cite", "999 F.2d 1", name="Alpha v. Beta", year="1990"),
        Authority("usc", "usc:42:1983:"),
        Authority("engrep", "some-eng-rep"),  # note-only
    ]
    zres = ZipResolver(static={"999 F.2d 1"})
    seen: list[tuple[int, int, str]] = []
    summ = compile_to_zip(test_auths, zres, zpath,
                          progress=lambda d, t, m: seen.append((d, t, m)))
    with zipfile.ZipFile(zpath) as zf:
        names = zf.namelist()
    if not any(n.startswith("Alpha v. Beta, 999 F.2d 1 (1990)") for n in names):
        print("zip: case file misnamed ->", names); failures += 1
    if "_MANIFEST.txt" not in names:
        print("zip: no manifest", names); failures += 1
    if summ.saved != 2 or summ.missing != 1:
        print("zip: wrong counts", summ.saved, summ.missing); failures += 1
    if not seen:
        print("zip: progress never reported"); failures += 1

    if failures:
        print(f"\n{failures} check(s) failed")
        sys.exit(1)
    print("OK: brief_compiler self-test passed")
