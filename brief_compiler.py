"""Compile every authority a brief cites into a single ZIP of downloaded cases,
statutes and rules — each file named with its proper Bluebook citation.

Used by GetCases' brief reader ("Download Cited Cases…"): it scans the open
brief for citations (:mod:`citations`), resolves each to a file, and writes them
all into one ``.zip`` the user chooses.

The ordering of where to look, the de-duplication of citations, and the Bluebook
file naming live here so they can be unit-tested without Tk or the network (run
``python3 brief_compiler.py`` for the offline self-test).  The actual fetching —
static.case.law's JSON, the Google Scholar cache, the CourtListener API, RECAP —
is injected as a ``resolver`` built by the GUI from helpers it already owns.

Per the product spec, files are text/RTF rather than PDFs (opinion PDFs turn out
too big), and names come from authoritative data, never the brief's own prose:

  * **Federal Appendix** cases -> static.case.law's per-case ``.json`` parsed to
    a ``.txt`` (the scans have no good text layer otherwise).
  * **Unpublished** opinions (cited by docket / WL number) -> the RECAP/PACER
    document, via the app's existing RECAP downloader.
  * **All other cases** -> saved Google Scholar copy, else CourtListener, else
    (only if it must) a fresh Google Scholar search — exported as ``.rtf`` with
    the app's RTF export, the Bluebook citation taken from the opinion and
    supplemented by the CourtListener API.
  * **Statutes / rules / regulations / Constitution** -> their text, named with
    the Bluebook citation of the provision (not whatever the brief called it).
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
]


# ---------------------------------------------------------------------------
# What a brief cites
# ---------------------------------------------------------------------------

@dataclass
class Authority:
    """One thing a brief cites, de-duplicated across every place it appears.

    ``kind`` is the citation action kind from :func:`citations.detect_links`:
    "cite" for a reporter case, "recap" for an unpublished opinion cited by
    docket / WL number, "usc"/"cfr"/"rule"/"const"/"statestat" for a statute
    source, "statpdf" for a Statutes at Large scan.  ``value`` is that action's
    payload.  ``name``/``year`` are scraped from the brief only as a last-resort
    fallback for the file name."""

    kind: str
    value: str
    name: str = ""
    year: str = ""

    @property
    def is_case(self) -> bool:
        return self.kind in ("cite", "recap")

    def label(self) -> str:
        """A short human label for progress / manifest lines."""
        if self.kind == "cite":
            return ", ".join(p for p in (self.name, self.value) if p) \
                or self.value
        if self.kind == "recap":
            return "unpublished opinion"
        return self.value


# Statute-like sources whose text we can render into the zip.
_TEXT_STATUTE_KINDS = frozenset({"usc", "cfr", "rule", "const", "statestat"})
# Kinds we recognise but cannot bundle (link-only / no text form).
_NOTE_ONLY = {
    "browse": "state statute — official source is link-only",
    "engrep": "English Reports — CommonLII scan not bundled",
    "url": "external link",
}


def collect_authorities(text: str) -> list[Authority]:
    """Every distinct authority a brief cites, in first-appearance order.

    Reporter-case citations are keyed by their base citation (pin cite dropped)
    so a case cited a dozen times — including short forms and ``Id.``s, which
    :func:`citations.detect_links` resolves to the same base — is compiled once.
    Unpublished (RECAP) citations key on their spec, statute-like sources on
    ``(kind, title, section)`` so different pin cites collapse to one file."""
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
                if not existing.name and name:
                    existing.name = name
                if not existing.year and year:
                    existing.year = year
            continue
        if kind == "recap":
            key = ("recap", str(value))
            if key not in by_key:
                by_key[key] = Authority("recap", str(value),
                                        name=_name_before(text, start))
                out.append(by_key[key])
            continue
        # Statute-like sources: one file per section, so the same section cited
        # with different pin-cited subsections isn't downloaded twice.  The spec
        # is "title:section:subsections"; key on the first two.
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
# Education", "Jones & Laughlin", "State ex rel. Doe").
_NAME_PARTICLES = frozenset({
    "of", "the", "and", "ex", "rel", "rel.", "et", "al", "al.", "&",
    "de", "la", "le", "van", "von", "der", "den", "del", "dos", "du", "da",
})
# Capitalized words that lead *into* a citation (a signal or sentence start)
# but are never the first word of a party name.
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
    "" — a last-resort file-name fallback, used only when neither the opinion
    nor CourtListener yields the caption.  Works outward from the "v." nearest
    the citation so "The Court relied on Roe v. Wade, 410 U.S. 113" yields
    "Roe v. Wade"."""
    window = text[max(0, start - 160):start].replace("\n", " ")
    window = re.sub(r"[\s,;]+$", "", window)
    tokens = window.split()
    if not tokens:
        return ""

    inre = list(_INRE_RE.finditer(window))
    if inre:
        tail = window[inre[-1].start():].split()
        prefix_len = len(inre[-1].group(0).split())
        name_toks = [tail[0].title()] + list(tail[1:prefix_len])
        for tok in tail[prefix_len:]:
            if _looks_like_name_token(tok):
                name_toks.append(tok)
            else:
                break
        name = re.sub(r"\s+", " ", " ".join(name_toks)).strip(" ,;.")
        if len(name) >= 6:
            return name

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
    """A Bluebook file stem for a case from brief-scraped parts —
    ``Abbrev. Name, vol Rep page (year)`` — used only as a fallback when the
    resolver couldn't supply an authoritative citation."""
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
# Resolution — the injected resolver does the fetching, we own the order
# ---------------------------------------------------------------------------

@dataclass
class _Resolved:
    """What resolving one authority produced."""
    data: Optional[bytes] = None
    ext: str = ""             # ".rtf" / ".txt" / ".pdf"
    source: str = ""          # where it came from (manifest)
    stem: str = ""            # authoritative Bluebook file stem (no extension)
    note: str = ""            # why nothing was saved


@dataclass
class AuthorityResult:
    authority: Authority
    ok: bool
    source: str = ""
    filename: str = ""
    detail: str = ""


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


def _resolve_case(resolver, auth: Authority) -> _Resolved:
    """A reporter-cited case.  Federal Appendix cites are pulled from
    static.case.law's JSON as text; everything else is exported as RTF from the
    Scholar cache, CourtListener, or (last) a Scholar search."""
    cite = auth.value
    # Federal Appendix: parse static.case.law's per-case JSON to text.
    if resolver.is_fed_appx(cite):
        got = resolver.fed_appx_text(cite)
        if got:
            text, stem = got
            return _Resolved(text.encode("utf-8"), ".txt",
                             "static.case.law (JSON)", stem=stem)
        # No JSON there — fall through to the ordinary RTF path.
    got = resolver.case_rtf(cite, auth.name)
    if got:
        rtf, stem, source = got
        return _Resolved(rtf.encode("ascii", "replace"), ".rtf", source,
                         stem=stem)
    return _Resolved(note="not found in the Google Scholar cache, on "
                          "CourtListener, or by a Google Scholar search")


def _resolve_recap(resolver, auth: Authority) -> _Resolved:
    """An unpublished opinion cited by docket / WL number — fetched from the
    RECAP/PACER archive."""
    got = resolver.recap_pdf(auth.value)
    if got:
        data, stem = got
        return _Resolved(data, ".pdf", "RECAP/PACER", stem=stem)
    return _Resolved(note="unpublished opinion not available on RECAP")


def _resolve_statute(resolver, auth: Authority) -> _Resolved:
    """A statute / rule / regulation / Constitution section (as text) or a
    Statutes at Large scan (PDF), named with the Bluebook citation of the
    provision itself."""
    label = ""
    try:
        label = resolver.authority_label(auth.kind, auth.value) or ""
    except Exception:
        label = ""
    if auth.kind == "statpdf":
        data = resolver.statute_pdf_bytes(auth.value)
        if data:
            return _Resolved(data, ".pdf", "Statutes at Large (GovInfo)",
                             stem=label or auth.value)
        return _Resolved(note="Statutes at Large PDF could not be downloaded")
    if auth.kind in _TEXT_STATUTE_KINDS:
        loaded = resolver.statute_text(auth.kind, auth.value)
        if loaded:
            title, body = loaded
            if body and body.strip():
                heading = title or label
                header = f"{heading}\n{'=' * len(heading)}\n\n" if heading else ""
                return _Resolved((header + body).encode("utf-8"), ".txt",
                                 "official source", stem=label or title)
        return _Resolved(note="could not load the section text")
    return _Resolved(note=_NOTE_ONLY.get(auth.kind, "not a downloadable source"))


def _resolve(resolver, auth: Authority) -> _Resolved:
    if auth.kind == "cite":
        return _resolve_case(resolver, auth)
    if auth.kind == "recap":
        return _resolve_recap(resolver, auth)
    return _resolve_statute(resolver, auth)


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
    ``should_cancel()`` is polled between items.  A ``_MANIFEST.txt`` describing
    every authority is always added.  File names come from the resolver's
    authoritative Bluebook citation, falling back to the brief-scraped caption
    only when the resolver supplied none."""
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
                res = _resolve(resolver, auth)
            except Exception as exc:   # a resolver failure must not sink the run
                res = _Resolved(note=f"error: {exc}")

            if res.data:
                stem = res.stem.strip() if res.stem else ""
                if not stem:
                    stem = (case_file_stem(auth.name, auth.value, auth.year)
                            if auth.kind == "cite" else auth.value)
                fname = _unique_name(used_names, safe_filename(stem), res.ext)
                try:
                    zf.writestr(fname, res.data)
                    summary.results.append(
                        AuthorityResult(auth, True, source=res.source,
                                        filename=fname))
                except Exception as exc:
                    summary.results.append(
                        AuthorityResult(auth, False,
                                        detail=f"could not write file: {exc}"))
            else:
                summary.results.append(
                    AuthorityResult(auth, False, detail=res.note))

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
        "42 U.S.C. § 1983; 42 U.S.C. § 1983(b); Fed. R. Civ. P. 56."
    )
    auths = collect_authorities(brief)
    cases = [a for a in auths if a.kind == "cite"]
    if not any(a.value == "410 U.S. 113" for a in cases):
        print("collect: Roe base cite missing", cases); failures += 1
    roe = next((a for a in cases if a.value == "410 U.S. 113"), None)
    if roe and (roe.name, roe.year) != ("Roe v. Wade", "1973"):
        print("collect: Roe caption/year ->", roe.name, roe.year); failures += 1
    if sum(1 for a in cases if a.value == "410 U.S. 113") != 1:
        print("collect: short form duplicated the case"); failures += 1
    if sum(1 for a in auths if a.kind == "usc") != 1:
        print("collect: statute pin-cite not de-duped"); failures += 1

    # --- Bluebook file stems ---
    if case_file_stem("Roe v. Wade", "410 U.S. 113", "1973") \
            != "Roe v. Wade, 410 U.S. 113 (1973)":
        print("stem: Roe wrong"); failures += 1
    if "/" in case_file_stem("A/B v. C", "1 F.2d 2", ""):
        print("stem: slash leaked into file name"); failures += 1

    # --- resolution routing & ordering, via a fake resolver ---
    class FakeResolver:
        def __init__(self, **kw):
            self.kw = kw
            self.calls: list[str] = []

        def is_fed_appx(self, cite):
            return "App'x" in cite or "App' x" in cite

        def fed_appx_text(self, cite):
            self.calls.append("fed_appx_text")
            if self.kw.get("appx"):
                return ("PER CURIAM. Affirmed.", "Ebrahimi v. Duffy, 1 F. App'x 2 (D.C. Cir. 2001)")
            return None

        def case_rtf(self, cite, name):
            self.calls.append("case_rtf")
            src = self.kw.get("rtf_source")
            if src:
                return ("{\\rtf1 body}", "Roe v. Wade, 410 U.S. 113 (1973)", src)
            return None

        def recap_pdf(self, spec):
            self.calls.append("recap_pdf")
            if self.kw.get("recap"):
                return (b"%PDF-1.4 recap", "Doe v. Roe, No. 21-1 (2d Cir. 2021)")
            return None

        def statute_text(self, kind, spec):
            return ("42 U.S.C. § 1983", "Every person who ...")

        def statute_pdf_bytes(self, url):
            return b"%PDF stat"

        def authority_label(self, kind, value):
            return {"usc": "42 U.S.C. § 1983"}.get(kind, value)

    # Fed. App'x -> JSON text, never the RTF path.
    r = FakeResolver(appx=True, rtf_source="should-not-be-used")
    res = _resolve_case(r, Authority("cite", "1 F. App'x 2"))
    if not (res.data and res.ext == ".txt" and "JSON" in res.source):
        print("route: Fed App'x not JSON->txt", res); failures += 1
    if "case_rtf" in r.calls:
        print("route: Fed App'x fell through to RTF", r.calls); failures += 1
    if not res.stem.startswith("Ebrahimi v. Duffy, 1 F. App'x 2"):
        print("route: Fed App'x stem wrong ->", res.stem); failures += 1

    # Regular case -> RTF from the resolver's chosen source.
    r = FakeResolver(rtf_source="CourtListener")
    res = _resolve_case(r, Authority("cite", "410 U.S. 113", name="Roe v. Wade"))
    if not (res.data and res.ext == ".rtf" and res.source == "CourtListener"):
        print("route: regular case not RTF", res); failures += 1
    if res.stem != "Roe v. Wade, 410 U.S. 113 (1973)":
        print("route: regular stem wrong ->", res.stem); failures += 1

    # Unpublished -> RECAP pdf.
    res = _resolve_recap(FakeResolver(recap=True), Authority("recap", "{}"))
    if not (res.data and res.ext == ".pdf" and res.source == "RECAP/PACER"):
        print("route: recap not pdf", res); failures += 1

    # Nothing found -> a note, no data.
    res = _resolve_case(FakeResolver(), Authority("cite", "1 X 1"))
    if res.data is not None or not res.note:
        print("route: missing case should note", res); failures += 1

    # --- end-to-end zip: names come from the resolver, not the brief ---
    tmp = tempfile.mkdtemp()
    zpath = os.path.join(tmp, "out.zip")
    test_auths = [
        # brief calls it "Wrong Name" but the resolver's citation must win:
        Authority("cite", "410 U.S. 113", name="Wrong Name", year="1999"),
        Authority("usc", "usc:42:1983:"),
        Authority("recap", "{}"),
        Authority("engrep", "x"),  # note-only
    ]
    zres = FakeResolver(rtf_source="Google Scholar (cache)", recap=True)
    seen: list = []
    summ = compile_to_zip(test_auths, zres, zpath,
                          progress=lambda d, t, m: seen.append((d, t, m)))
    with zipfile.ZipFile(zpath) as zf:
        names = zf.namelist()
    if not any(n == "Roe v. Wade, 410 U.S. 113 (1973).rtf" for n in names):
        print("zip: case not named from resolver citation ->", names); failures += 1
    if any("Wrong Name" in n for n in names):
        print("zip: brief's wrong name leaked into a file name", names); failures += 1
    if not any(n.startswith("42 U.S.C. § 1983") and n.endswith(".txt") for n in names):
        print("zip: statute misnamed ->", names); failures += 1
    if "_MANIFEST.txt" not in names:
        print("zip: no manifest", names); failures += 1
    if summ.saved != 3 or summ.missing != 1:
        print("zip: wrong counts", summ.saved, summ.missing); failures += 1

    if failures:
        print(f"\n{failures} check(s) failed")
        sys.exit(1)
    print("OK: brief_compiler self-test passed")
