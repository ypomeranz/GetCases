"""In-app California statutes from the official source, California Legislative
Information (``leginfo.legislature.ca.gov``).

A section is fetched from the stable display URL

    https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml
        ?lawCode=<CODE>&sectionNum=<section>.

where <CODE> is one of California's 29 code abbreviations (PEN, CIV, CCP, ...).
The section text lives in ``<div id="codeLawSectionNoHead">``: a breadcrumb of
``<h4>`` Code / Part / Title / Chapter headings, the section number in an
``<h6>``, the operative text as ``<p style="...margin-left: N em">`` blocks
(the left margin gives the indent level), and a trailing ``<i>(Amended by
Stats. ...)</i>`` history line.

This module mirrors the contract of ``us_code`` / ``fed_rules`` so the GUI's
statute viewer renders California the same way: a ``CaStatuteDoc`` exposing the
``(kind, indent, text)`` paragraph stream plus ``label`` / ``source_name`` /
``bluebook_cite`` / ``neighbors``.  ``state_statutes`` owns the citation
*detection*; this module owns the California *fetch + parse*.
"""

from __future__ import annotations

import html as _html
import re
import threading
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# California's 29 codes
#
# (lawCode, Bluebook subject abbreviation, [recognition variants]).  The
# Bluebook label is "Cal. <subject> Code"; the variants are matched against a
# normalized form of the subject captured from the citation (see
# ``code_for_subject``) so spelling/abbreviation differences resolve to the
# same code.
# ---------------------------------------------------------------------------
CA_CODES: list[tuple[str, str, list[str]]] = [
    ("BPC",  "Bus. & Prof.",   ["businessprofessions", "busprof", "bpc"]),
    ("CIV",  "Civ.",           ["civil", "civ"]),
    ("CCP",  "Civ. Proc.",     ["civilprocedure", "civproc", "codeofcivilprocedure", "ccp"]),
    ("COM",  "Com.",           ["commercial", "comm", "com"]),
    ("CORP", "Corp.",          ["corporations", "corp"]),
    ("EDC",  "Educ.",          ["education", "educ"]),
    ("ELEC", "Elec.",          ["elections", "elec"]),
    ("EVID", "Evid.",          ["evidence", "evid"]),
    ("FAM",  "Fam.",           ["family", "fam"]),
    ("FIN",  "Fin.",           ["financial", "fin"]),
    ("FGC",  "Fish & G.",      ["fishgame", "fishg", "fgc"]),
    ("FAC",  "Food & Agric.",  ["foodagricultural", "foodagric", "fac"]),
    ("GOV",  "Gov't",          ["government", "govt", "gov"]),
    ("HNC",  "Harb. & Nav.",   ["harborsnavigation", "harbnav", "hnc"]),
    ("HSC",  "Health & Safety", ["healthsafety", "hs", "hsc"]),
    ("INS",  "Ins.",           ["insurance", "ins"]),
    ("LAB",  "Lab.",           ["labor", "lab"]),
    ("MVC",  "Mil. & Vet.",    ["militaryveterans", "milvet", "mvc"]),
    ("PEN",  "Penal",          ["penal", "pen"]),
    ("PROB", "Prob.",          ["probate", "prob"]),
    ("PCC",  "Pub. Cont.",     ["publiccontract", "pubcont", "pcc"]),
    ("PRC",  "Pub. Res.",      ["publicresources", "pubres", "prc"]),
    ("PUC",  "Pub. Util.",     ["publicutilities", "pubutil", "puc"]),
    ("RTC",  "Rev. & Tax.",    ["revenuetaxation", "revtax", "rtc"]),
    ("SHC",  "Sts. & High.",   ["streetshighways", "stshigh", "shc"]),
    ("UIC",  "Unemp. Ins.",    ["unemploymentinsurance", "unempins", "uic"]),
    ("VEH",  "Veh.",           ["vehicle", "veh"]),
    ("WAT",  "Water",          ["water", "wat"]),
    ("WIC",  "Welf. & Inst.",  ["welfareinstitutions", "welfinst", "wic"]),
]

# lawCode -> Bluebook subject abbreviation ("PEN" -> "Penal").
SUBJECT: dict[str, str] = {code: subj for code, subj, _v in CA_CODES}
# normalized variant -> lawCode.
_VARIANT_TO_CODE: dict[str, str] = {}
for _code, _subj, _variants in CA_CODES:
    for _v in _variants:
        _VARIANT_TO_CODE[_v] = _code


def _canon(subject: str) -> str:
    """Normalize a captured subject for variant lookup: lowercase, drop the
    word 'and', strip non-alphanumerics. 'Health & Safety' / 'Health and
    Safety' -> 'healthsafety'; 'Civ. Proc.' -> 'civproc'."""
    s = re.sub(r"\band\b", " ", subject.lower())
    return re.sub(r"[^a-z0-9]+", "", s)


def code_for_subject(subject: str) -> str | None:
    """California lawCode for a captured subject string, or None if unknown."""
    return _VARIANT_TO_CODE.get(_canon(subject))


def label_for_code(code: str) -> str:
    """Bluebook label prefix for a lawCode ('PEN' -> 'Cal. Penal Code')."""
    return f"Cal. {SUBJECT.get(code, code)} Code"


def spec_key(code: str) -> str:
    """state_statutes spec key for a lawCode ('PEN' -> 'ca-pen')."""
    return f"ca-{code.lower()}"


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}

_HOST = "leginfo.legislature.ca.gov"


def section_url(code: str, section: str) -> str:
    sec = section if section.endswith(".") else section + "."
    return ("https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml"
            f"?lawCode={code}&sectionNum={sec}")


@dataclass
class CaStatuteDoc:
    code: str          # "PEN"
    sec: str           # "187"
    url: str
    paras: list[tuple[str, int, str]] = field(default_factory=list)

    @property
    def kind(self) -> str:
        return "statestat"

    @property
    def title(self) -> str:
        # The state_statutes spec key — threaded through specs and neighbors.
        return spec_key(self.code)

    @property
    def section(self) -> str:
        return self.sec

    @property
    def label(self) -> str:
        return f"{label_for_code(self.code)} § {self.sec}"

    @property
    def heading(self) -> str:
        return self.label

    @property
    def source_name(self) -> str:
        return "California Legislative Information"

    @property
    def source_note(self) -> str:
        return (f"{label_for_code(self.code)} — official text "
                f"(leginfo.legislature.ca.gov)")

    def bluebook_cite(self, subs: tuple = ()) -> str:
        tail = "".join(f"({s})" for s in subs)
        return f"{label_for_code(self.code)} § {self.sec}{tail}"

    def neighbors(self):
        # California's adjacent-section numbers are not derivable from the
        # section page alone (they need the code's table of contents); prev/next
        # is left disabled rather than guessed.
        return None, None


_cache: dict[tuple[str, str], CaStatuteDoc] = {}
_lock = threading.Lock()


def load(key: str, section: str) -> CaStatuteDoc:
    """Fetch and parse one California section.  `key` is a state_statutes spec
    key ("ca-pen"); `section` is the bare number ("187").  Raises RuntimeError
    with a readable message on failure."""
    code = key.split("-", 1)[1].upper() if "-" in key else key.upper()
    if code not in SUBJECT:
        raise RuntimeError(f"unknown California code {code!r}")
    section = str(section).strip().rstrip(".")
    ck = (code, section)
    with _lock:
        if ck in _cache:
            return _cache[ck]

    import requests

    url = section_url(code, section)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"leginfo.legislature.ca.gov: {exc}") from exc

    label = f"{label_for_code(code)} § {section}"
    paras = parse_section_html(resp.content.decode("utf-8", "replace"), label)
    # A missing section returns the chrome with an empty body (just the head).
    if not any(k == "body" for k, _i, _t in paras):
        raise RuntimeError(f"no text found for {label}")
    doc = CaStatuteDoc(code=code, sec=section, url=url, paras=paras)
    with _lock:
        _cache[ck] = doc
    return doc


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def _clean(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", "", fragment or "")
    text = _html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


# The section's operative text and history live in this container.
_REGION_RE = re.compile(
    r'<div[^>]*id="codeLawSectionNoHead"[^>]*>(.*?)</div>\s*</(?:form|body)',
    re.IGNORECASE | re.DOTALL,
)
_H4_RE = re.compile(r"<h4[^>]*>(.*?)</h4>\s*(?:<i>(.*?)</i>)?",
                    re.IGNORECASE | re.DOTALL)
_P_RE = re.compile(r"<p\b([^>]*)>(.*?)</p>", re.IGNORECASE | re.DOTALL)
_MARGIN_RE = re.compile(r"margin-left:\s*([\d.]+)\s*em", re.IGNORECASE)
# Trailing parenthesized enactment/amendment history.
_CREDIT_RE = re.compile(
    r"<i>\s*(\((?:Amended|Added|Repealed|Renumbered|Enacted|Reenacted)\b[^<]*)</i>",
    re.IGNORECASE | re.DOTALL,
)


def parse_section_html(page_html: str, label: str) -> list[tuple[str, int, str]]:
    """Parse a leginfo section page into the (kind, indent, text) stream used
    by the statute viewer (same contract as us_code.parse_section)."""
    rm = _REGION_RE.search(page_html)
    if not rm:
        # Fall back to an open-ended grab from the container start.
        rm = re.search(r'id="codeLawSectionNoHead"[^>]*>(.*)', page_html,
                       re.IGNORECASE | re.DOTALL)
    region = rm.group(1) if rm else ""
    if not region.strip():
        return []

    paras: list[tuple[str, int, str]] = [("sechead", 0, label)]

    # Breadcrumb headings (Code / Part / Title / Chapter / Article).  The first
    # <h4> is the code name, already conveyed by the label, so it is dropped.
    for i, hm in enumerate(_H4_RE.finditer(region)):
        if i == 0:
            continue
        head = _clean(hm.group(1))
        note = _clean(hm.group(2) or "")
        if note:
            head = f"{head} {note}".strip()
        if head:
            paras.append(("head", 0, head))

    # Operative text: each <p> is a subdivision; its left margin (in em) is the
    # indent depth.
    for pm in _P_RE.finditer(region):
        text = _clean(pm.group(2))
        if not text:
            continue
        mm = _MARGIN_RE.search(pm.group(1))
        indent = min(int(round(float(mm.group(1)))), 6) if mm else 0
        paras.append(("body", indent, text))

    # Trailing amendment/enactment history.
    cm = _CREDIT_RE.search(region)
    if cm:
        paras.append(("credit", 0, _clean(cm.group(1))))

    return paras


if __name__ == "__main__":
    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    # --- code mapping: spelling/abbreviation variants resolve correctly ---
    check(code_for_subject("Penal") == "PEN", "subject Penal -> PEN")
    check(code_for_subject("Civ.") == "CIV", "subject Civ. -> CIV")
    check(code_for_subject("Civ. Proc.") == "CCP", "subject Civ. Proc. -> CCP")
    check(code_for_subject("Health & Safety") == "HSC", "H&S -> HSC")
    check(code_for_subject("Health and Safety") == "HSC", "H and S -> HSC")
    check(code_for_subject("Welf. & Inst.") == "WIC", "W&I -> WIC")
    check(code_for_subject("Vehicle") == "VEH", "Vehicle -> VEH")
    check(code_for_subject("Bus. & Prof.") == "BPC", "B&P -> BPC")
    check(code_for_subject("Nonexistent") is None, "unknown subject -> None")
    check(len(CA_CODES) == 29, f"all 29 codes present ({len(CA_CODES)})")
    check(label_for_code("CCP") == "Cal. Civ. Proc. Code", "CCP label")
    check(spec_key("PEN") == "ca-pen", "spec key PEN")

    # --- parser on a page mirroring leginfo's real structure ---
    sample = """<html><body>
    <form><div id="codeLawSectionNoHead"><font face="Times New Roman">
      <div align="left" style="text-transform: uppercase"><h4><b>Penal Code - PEN</b></h4></div>
      <div style="float:left;text-indent: 0.25in;"><h4 style="display:inline;"><b>PART 1. OF CRIMES AND PUNISHMENTS [25 - 680.4]</b></h4><i> ( Part 1 enacted 1872. )</i></div>
      <div style="float:left;text-indent: 0.5in;"><h4 style="display:inline;"><b>TITLE 8. OF CRIMES AGAINST THE PERSON [187 - 248]</b></h4><i> ( Title 8 enacted 1872. )</i></div>
      <div><h6 style="float:left;"><b>189.  </b></h6>
      <p style="margin:0 0 0.5em 0;">(a) All murder that is willful, deliberate is murder of the first degree.</p>
      <p style="margin:0 0 0.5em 0;">(b) All other kinds of murders are of the second degree.</p>
      <p style="margin:0 0 0.5em 0;">(c) As used in this section, the following definitions apply:</p>
      <p style="margin:0 0 1em 0;margin-left: 1em;">(1) &#8220;Destructive device&#8221; has the same meaning as in Section 16460.</p>
      <p style="margin:0 0 1em 0;margin-left: 2em;">(A) a nested item for indent depth.</p>
      </div>
      <i>(Amended by Stats. 2019, Ch. 497, Sec. 192. (AB 991) Effective January 1, 2020.)</i>
    </font></div></form></body></html>"""
    paras = parse_section_html(sample, "Cal. Penal Code § 189")
    kinds = [(k, i) for k, i, _t in paras]
    check(paras[0] == ("sechead", 0, "Cal. Penal Code § 189"), f"sechead: {paras[0]!r}")
    check(("head", 0) in kinds, "breadcrumb heads captured")
    check(not any("Penal Code - PEN" in t for _k, _i, t in paras),
          "code-name h4 dropped (label covers it)")
    check(("body", 0) in kinds and ("body", 1) in kinds and ("body", 2) in kinds,
          f"indent depths from margin-left: {kinds!r}")
    body0 = next(t for k, i, t in paras if (k, i) == ("body", 0))
    check(body0.startswith("(a)"), f"first body is (a): {body0[:20]!r}")
    body1 = next(t for k, i, t in paras if (k, i) == ("body", 1))
    check("“" in body1 or "Destructive" in body1, "entities unescaped + nested text")
    check(paras[-1][0] == "credit" and paras[-1][2].startswith("(Amended by Stats."),
          f"history -> credit: {paras[-1]!r}")
    # missing-section page (no <p> body) -> no body paras
    empty = parse_section_html(
        '<div id="codeLawSectionNoHead"><h4>Penal Code - PEN</h4></div></form>',
        "Cal. Penal Code § 999999")
    check(not any(k == "body" for k, _i, _t in empty), "empty section -> no body")

    raise SystemExit(1 if failed else 0)
