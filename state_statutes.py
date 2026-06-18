"""Detect citations to **state statutes** in opinion text and turn them into
actions for the GUI: either an in-app statute view (for the user's priority
states, once a per-state parser exists) or a browser link-out (for the rest).

This module owns the *detection* half of the contract shared with
``us_code`` / ``ecfr`` / ``fed_rules``:

    iter_cites(text)      -> yields Cite records (start, end, key, section, ...)
    cite_spec(cite)       -> compact "jkey:section:subs" spec
    spec_label(spec)      -> human Bluebook label
    action_for(cite)      -> ("statestat", spec) | ("browse", url)
    parse_query(text)     -> ("statestat", spec) | None   (hand-typed lookup)

State statutes are *not* hosted uniformly anywhere (Cornell links out; the
official sites differ per state), so in-app rendering is added one state at a
time via ``load_section`` + a ``StatuteDoc`` (not yet wired — ``IN_APP`` is
empty, so every detected citation currently link-outs).  Detection, however,
covers all 50 states + D.C. now, because that is HTML-independent.

Citation forms are taken from Bluebook Table T1 and are matched tolerantly
(optional "Ann.", flexible spacing, optional periods, any case).  The big
families:

    single compilation   Fla. Stat. § 776.012   Wash. Rev. Code § 9A.32.030
    subject-matter codes  Cal. Penal Code § 187  N.Y. Penal Law § 125.25
                          Tex. Penal Code Ann. § 19.02
    subject + comma       Md. Code Ann., Crim. Law § 2-201
    title + section        Del. Code Ann. tit. 11, § 636   Okla. Stat. tit. 21, § 701.7
    title prefix          42 Pa. Cons. Stat. § 9711  (a.k.a. 42 Pa.C.S. § 9711)
    chapter / act / sec   720 ILCS 5/9-1
    chapter + section      Mass. Gen. Laws ch. 265, § 1   (M.G.L. c. 265, § 1)
    colon section          N.J. Stat. Ann. § 2C:11-3   La. Rev. Stat. § 14:30
    article-based          La. Civ. Code art. 2315
"""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass, field

import state_ca  # California: canonical code table + in-app fetch/parse
import state_fl  # Florida: in-app fetch/parse (single compilation)

# ---------------------------------------------------------------------------
# Shared sub-patterns
# ---------------------------------------------------------------------------

# A section number: starts with a digit, then digits / letters / . : -
# (covers "776.012", "9A.32.030", "16-5-1", "2C:11-3", "18.2-32", "701.7").
_SEC = r"\d[0-9A-Za-z.:\-]*"

# The section sign / word: §, §§, "sec.", "section(s)".
_SS = r"(?:§{1,2}|[Ss]ec(?:tion)?s?\.?)\s*"

# Trailing subdivisions: "(a)(1)(B)".  Each token is 1-4 letters or 1-3 digits
# so a trailing year "(2020)" is never swallowed.
_SUBS = r"(?:\s*\((?:[A-Za-z]{1,4}|\d{1,3})\))*"

# A run of capitalized subject words before "Code"/"Law" ("Penal",
# "Civ. Proc.", "Health & Safety", "Veh. & Traf.").
_SUBJ = r"(?:(?:[A-Z][A-Za-z'.]*|&)\s+){1,6}"


@dataclass
class Cite:
    """One detected state-statute citation."""
    start: int
    end: int
    key: str            # jurisdiction spec key ("fl", "ca-pen", "ny-pen", ...)
    section: str        # normalized section ("776.012", "2C:11-3", "5/9-1")
    label: str          # Bluebook label ("Fla. Stat. § 776.012")
    text: str           # the matched source text
    subs: tuple = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Jurisdiction table
#
# Each entry is a compiled regex plus a callable turning a match into a Cite.
# Builders below keep the common shapes terse; the irregular states get bespoke
# patterns.  Detection runs every pattern (see iter_cites) — the abbreviations
# are distinctive enough that they do not collide.
# ---------------------------------------------------------------------------

_PATTERNS: list[tuple[re.Pattern, "callable"]] = []


def _add(rx: str, build) -> None:
    _PATTERNS.append((re.compile(rx, re.IGNORECASE), build))


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _subj_slug(subj: str) -> str:
    """A short, collision-resistant key fragment for a subject code, e.g.
    "Civ. Proc." -> "civproc" (distinct from "Civ." -> "civ")."""
    return re.sub(r"[^a-z0-9]+", "", subj.lower())[:16] or "x"


def _subs_of(m: re.Match) -> tuple:
    try:
        raw = m.group("subs") or ""
    except IndexError:
        raw = ""  # pattern has no 'subs' group (e.g. Illinois)
    return tuple(re.findall(r"\(([^)]+)\)", raw))


# --- family 1: single compilation, "<Abbr> § <sec>" ------------------------
# (key, full state name, abbr-regex up to the section sign, canonical label abbr)
_SIMPLE = [
    ("al", "Alabama",        r"Ala\.?\s*Code",                          "Ala. Code"),
    ("ak", "Alaska",         r"Alaska\s*Stat\.?(?:\s*Ann\.?)?",         "Alaska Stat."),
    ("az", "Arizona",        r"Ariz\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?", "Ariz. Rev. Stat."),
    ("ar", "Arkansas",       r"Ark\.?\s*Code(?:\s*Ann\.?)?",            "Ark. Code Ann."),
    ("co", "Colorado",       r"Colo\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?", "Colo. Rev. Stat."),
    ("ct", "Connecticut",    r"Conn\.?\s*Gen\.?\s*Stat\.?(?:\s*Ann\.?)?", "Conn. Gen. Stat."),
    ("fl", "Florida",        r"Fla\.?\s*Stat\.?(?:\s*Ann\.?)?",         "Fla. Stat."),
    ("ga", "Georgia",        r"(?:Ga\.?\s*Code\s*Ann\.?|O\.?C\.?G\.?A\.?)", "Ga. Code Ann."),
    ("hi", "Hawaii",         r"Haw\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?", "Haw. Rev. Stat."),
    ("id", "Idaho",          r"Idaho\s*Code(?:\s*Ann\.?)?",             "Idaho Code"),
    ("in", "Indiana",        r"Ind\.?\s*Code(?:\s*Ann\.?)?",            "Ind. Code"),
    ("ia", "Iowa",           r"Iowa\s*Code(?:\s*Ann\.?)?",              "Iowa Code"),
    ("ks", "Kansas",         r"Kan\.?\s*Stat\.?(?:\s*Ann\.?)?",         "Kan. Stat. Ann."),
    ("ky", "Kentucky",       r"(?:Ky\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?|K\.?R\.?S\.?)", "Ky. Rev. Stat. Ann."),
    ("mi", "Michigan",       r"(?:Mich\.?\s*Comp\.?\s*Laws(?:\s*Ann\.?)?|M\.?C\.?L\.?A?)", "Mich. Comp. Laws"),
    ("mn", "Minnesota",      r"Minn\.?\s*Stat\.?(?:\s*Ann\.?)?",        "Minn. Stat."),
    ("ms", "Mississippi",    r"Miss\.?\s*Code(?:\s*Ann\.?)?",           "Miss. Code Ann."),
    ("mo", "Missouri",       r"Mo\.?\s*(?:Ann\.?\s*)?Rev\.?\s*Stat\.?(?:\s*Ann\.?)?", "Mo. Rev. Stat."),
    ("mt", "Montana",        r"Mont\.?\s*Code(?:\s*Ann\.?)?",           "Mont. Code Ann."),
    ("ne", "Nebraska",       r"Neb\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?", "Neb. Rev. Stat."),
    ("nv", "Nevada",         r"(?:Nev\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?|N\.?R\.?S\.?)", "Nev. Rev. Stat."),
    ("nh", "New Hampshire",  r"(?:N\.?H\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?|R\.?S\.?A\.?)", "N.H. Rev. Stat. Ann."),
    ("nm", "New Mexico",     r"N\.?M\.?\s*Stat\.?(?:\s*Ann\.?)?",       "N.M. Stat. Ann."),
    ("nc", "North Carolina", r"N\.?C\.?\s*Gen\.?\s*Stat\.?(?:\s*Ann\.?)?", "N.C. Gen. Stat."),
    ("nd", "North Dakota",   r"N\.?D\.?\s*Cent\.?\s*Code(?:\s*Ann\.?)?", "N.D. Cent. Code"),
    ("oh", "Ohio",           r"Ohio\s*Rev\.?\s*Code(?:\s*Ann\.?)?",     "Ohio Rev. Code Ann."),
    ("or", "Oregon",         r"(?:Or\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?|O\.?R\.?S\.?)", "Or. Rev. Stat."),
    ("ri", "Rhode Island",   r"R\.?I\.?\s*Gen\.?\s*Laws(?:\s*Ann\.?)?", "R.I. Gen. Laws"),
    ("sc", "South Carolina", r"S\.?C\.?\s*Code(?:\s*Ann\.?)?",          "S.C. Code Ann."),
    ("sd", "South Dakota",   r"S\.?D\.?\s*Codified\s*Laws",             "S.D. Codified Laws"),
    ("tn", "Tennessee",      r"Tenn\.?\s*Code(?:\s*Ann\.?)?",           "Tenn. Code Ann."),
    ("ut", "Utah",           r"Utah\s*Code(?:\s*Ann\.?)?",              "Utah Code Ann."),
    ("va", "Virginia",       r"Va\.?\s*Code(?:\s*Ann\.?)?",             "Va. Code Ann."),
    ("wa", "Washington",     r"(?:Wash\.?\s*Rev\.?\s*Code(?:\s*Ann\.?)?|R\.?C\.?W\.?)", "Wash. Rev. Code"),
    ("wv", "West Virginia",  r"W\.?\s*Va\.?\s*Code(?:\s*Ann\.?)?",      "W. Va. Code"),
    ("wi", "Wisconsin",      r"Wis\.?\s*Stat\.?(?:\s*Ann\.?)?",         "Wis. Stat."),
    ("wy", "Wyoming",        r"Wyo\.?\s*Stat\.?(?:\s*Ann\.?)?",         "Wyo. Stat. Ann."),
    ("dc", "District of Columbia", r"D\.?C\.?\s*Code(?:\s*Ann\.?)?",    "D.C. Code"),
]

KEY_NAME: dict[str, str] = {}     # spec key -> jurisdiction display name
KEY_ABBR: dict[str, str] = {}     # spec key -> label abbreviation


def _register(key: str, name: str, abbr: str) -> None:
    KEY_NAME.setdefault(key, name)
    KEY_ABBR.setdefault(key, abbr)


def _make_simple(key, name, abbr_rx, abbr):
    _register(key, name, abbr)
    rx = rf"\b{abbr_rx}\s*{_SS}(?P<sec>{_SEC})(?P<subs>{_SUBS})"

    def build(m: re.Match) -> Cite:
        sec = m.group("sec")
        return Cite(m.start(), m.end(), key, sec,
                    f"{abbr} § {sec}", m.group(0), _subs_of(m))
    _add(rx, build)


for _k, _n, _rx, _a in _SIMPLE:
    _make_simple(_k, _n, _rx, _a)


# --- family 2: title + section ("Del. Code Ann. tit. 11, § 636") ------------
_TITLE = [
    ("de", "Delaware",  r"Del\.?\s*Code(?:\s*Ann\.?)?", "Del. Code Ann."),
    ("me", "Maine",     r"Me\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?", "Me. Rev. Stat."),
    ("ok", "Oklahoma",  r"Okla\.?\s*Stat\.?(?:\s*Ann\.?)?", "Okla. Stat."),
    ("vt", "Vermont",   r"Vt\.?\s*Stat\.?(?:\s*Ann\.?)?", "Vt. Stat. Ann."),
]


def _make_title(key, name, abbr_rx, abbr):
    _register(key, name, abbr)
    rx = (rf"\b{abbr_rx}\s*tit\.?\s*(?P<title>\d+[A-Za-z]?)\s*,?\s*"
          rf"{_SS}(?P<sec>{_SEC})(?P<subs>{_SUBS})")

    def build(m: re.Match) -> Cite:
        title, sec = m.group("title"), m.group("sec")
        return Cite(m.start(), m.end(), key, f"{title}-{sec}",
                    f"{abbr} tit. {title}, § {sec}", m.group(0), _subs_of(m))
    _add(rx, build)


for _k, _n, _rx, _a in _TITLE:
    _make_title(_k, _n, _rx, _a)


# --- family 3: subject-matter codes ----------------------------------------
# California "Cal. <Subject> Code § X"; Texas "Tex. <Subject> Code (Ann.) § X";
# New York "N.Y. <Subject> Law § X".  The subject is captured generically so
# every code is detected without enumerating all of them.
def _subject_build(key_prefix, name, label_state, code_word):
    def build(m: re.Match) -> Cite:
        subj = _norm(m.group("subj"))
        sec = m.group("sec")
        key = f"{key_prefix}-{_subj_slug(subj)}"
        abbr = f"{label_state} {subj} {code_word}"
        _register(key, name, abbr)
        return Cite(m.start(), m.end(), key, sec,
                    f"{abbr} § {sec}", m.group(0), _subs_of(m))
    return build


# California: "Cal. Penal Code § 187", "Cal. Health & Safety Code § 11350".
# CA has a fixed set of 29 named codes, so the captured subject is canonicalized
# to an official lawCode (key "ca-pen", label "Cal. Penal Code") — which both
# normalizes spelling variants and makes the citation renderable in-app.
def _ca_build(m: re.Match) -> Cite:
    subj = _norm(m.group("subj"))
    sec = m.group("sec")
    code = state_ca.code_for_subject(subj)
    if code:
        key, abbr = state_ca.spec_key(code), state_ca.label_for_code(code)
    else:  # not one of the 29 CA codes — keep generic and link-out
        key, abbr = f"ca-{_subj_slug(subj)}", f"Cal. {subj} Code"
    _register(key, "California", abbr)
    return Cite(m.start(), m.end(), key, sec, f"{abbr} § {sec}",
                m.group(0), _subs_of(m))


_add(rf"\bCal\.?\s+(?P<subj>{_SUBJ})Code\s*(?:Ann\.?)?\s*{_SS}"
     rf"(?P<sec>{_SEC})(?P<subs>{_SUBS})", _ca_build)

# Texas: "Tex. Penal Code Ann. § 19.02" — but NOT "Tex. Admin. Code" (regs).
# Texas codes are cited *with* "Ann." (unlike Cal./N.Y.), so normalize to it.
_add(rf"\bTex\.?\s+(?P<subj>(?!Admin)(?:{_SUBJ}))Code\s*(?:Ann\.?)?\s*{_SS}"
     rf"(?P<sec>{_SEC})(?P<subs>{_SUBS})",
     _subject_build("tx", "Texas", "Tex.", "Code Ann."))

# New York: "N.Y. Penal Law § 125.25", "N.Y. Veh. & Traf. Law § 1192".
_add(rf"\bN\.?\s*Y\.?\s+(?P<subj>{_SUBJ})Law\s*{_SS}"
     rf"(?P<sec>{_SEC})(?P<subs>{_SUBS})",
     _subject_build("ny", "New York", "N.Y.", "Law"))


# --- family 4: Maryland "Md. Code Ann., <Subject> § 2-201" ------------------
_register("md", "Maryland", "Md. Code Ann.")


def _md_build(m: re.Match) -> Cite:
    subj = _norm(m.group("subj"))
    sec = m.group("sec")
    key = f"md-{_subj_slug(subj)}"
    abbr = f"Md. Code Ann., {subj}"
    _register(key, "Maryland", abbr)
    return Cite(m.start(), m.end(), key, sec, f"{abbr} § {sec}",
                m.group(0), _subs_of(m))


_add(rf"\bMd\.?\s*Code\s*(?:Ann\.?)?\s*,\s*(?P<subj>{_SUBJ})"
     rf"{_SS}(?P<sec>{_SEC})(?P<subs>{_SUBS})", _md_build)


# --- family 5: Pennsylvania "42 Pa. Cons. Stat. § 9711" / "42 Pa.C.S. § ..." -
_register("pa", "Pennsylvania", "Pa. Cons. Stat.")


def _pa_build(m: re.Match) -> Cite:
    title, sec = m.group("title"), m.group("sec")
    return Cite(m.start(), m.end(), "pa", f"{title}-{sec}",
                f"{title} Pa. Cons. Stat. § {sec}", m.group(0), _subs_of(m))


_add(rf"\b(?P<title>\d+)\s*Pa\.?\s*(?:Cons\.?\s*Stat\.?|C\.?S\.?)(?:\s*Ann\.?)?\s*"
     rf"{_SS}(?P<sec>{_SEC})(?P<subs>{_SUBS})", _pa_build)


# --- family 6: Illinois "720 ILCS 5/9-1" -----------------------------------
_register("il", "Illinois", "ILCS")


def _il_build(m: re.Match) -> Cite:
    ch, act, sec = m.group("ch"), m.group("act"), m.group("sec")
    return Cite(m.start(), m.end(), "il", f"{ch}/{act}/{sec}",
                f"{ch} ILCS {act}/{sec}", m.group(0))


_add(r"\b(?P<ch>\d+)\s*ILCS\s*(?P<act>\d+)\s*/\s*(?P<sec>[0-9A-Za-z.\-]+)",
     _il_build)


# --- family 7: Massachusetts "Mass. Gen. Laws ch. 265, § 1" -----------------
_register("ma", "Massachusetts", "Mass. Gen. Laws")


def _ma_build(m: re.Match) -> Cite:
    ch, sec = m.group("ch"), m.group("sec")
    return Cite(m.start(), m.end(), "ma", f"{ch}/{sec}",
                f"Mass. Gen. Laws ch. {ch}, § {sec}", m.group(0), _subs_of(m))


_add(rf"\b(?:Mass\.?\s*Gen\.?\s*Laws(?:\s*Ann\.?)?|M\.?G\.?L\.?(?:\s*A\.?)?)\s*"
     rf"(?:ch\.?|c\.?)\s*(?P<ch>\d+[A-Za-z]?)\s*,?\s*"
     rf"{_SS}(?P<sec>{_SEC})(?P<subs>{_SUBS})", _ma_build)


# --- family 8: Louisiana "La. Rev. Stat. § 14:30" + "La. Civ. Code art. 2315"
_register("la", "Louisiana", "La. Rev. Stat.")
_register("la-civ", "Louisiana", "La. Civ. Code")


def _la_rs_build(m: re.Match) -> Cite:
    sec = m.group("sec")
    return Cite(m.start(), m.end(), "la", sec, f"La. Rev. Stat. § {sec}",
                m.group(0), _subs_of(m))


def _la_art_build(m: re.Match) -> Cite:
    art = m.group("art")
    code = _norm(m.group("code"))
    abbr = f"La. {code} Code" if code else "La. Civ. Code"
    return Cite(m.start(), m.end(), "la-civ", art, f"{abbr} art. {art}",
                m.group(0))


_add(rf"\b(?:La\.?\s*Rev\.?\s*Stat\.?(?:\s*Ann\.?)?|La\.?\s*R\.?S\.?)\s*"
     rf"{_SS}(?P<sec>\d[0-9A-Za-z.:\-]*)(?P<subs>{_SUBS})", _la_rs_build)
_add(r"\bLa\.?\s*(?P<code>Civ\.?|Civil|Crim\.?|Code\s*Crim\.?\s*Proc\.?|"
     r"Code\s*Civ\.?\s*Proc\.?|Child(?:ren'?s)?)\s*Code\s*art(?:icle|\.)?\s*"
     r"(?P<art>\d+[0-9A-Za-z.\-]*)", _la_art_build)


# --- family 9: New Jersey "N.J. Stat. Ann. § 2C:11-3" ----------------------
_register("nj", "New Jersey", "N.J. Stat. Ann.")


def _nj_build(m: re.Match) -> Cite:
    sec = m.group("sec")
    return Cite(m.start(), m.end(), "nj", sec, f"N.J. Stat. Ann. § {sec}",
                m.group(0), _subs_of(m))


_add(rf"\bN\.?\s*J\.?\s*(?:Stat\.?\s*Ann\.?|Rev\.?\s*Stat\.?|S\.?A\.?)\s*"
     rf"{_SS}(?P<sec>\d[0-9A-Za-z.:\-]*)(?P<subs>{_SUBS})", _nj_build)


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def iter_cites(text: str):
    """Yield non-overlapping Cite records found in `text`, left to right.
    Overlaps are resolved first-start / longest-match (as the GUI also does
    across sources)."""
    found: list[Cite] = []
    for rx, build in _PATTERNS:
        for m in rx.finditer(text):
            if m.group(0).strip():
                found.append(build(m))
    found.sort(key=lambda c: (c.start, -(c.end - c.start)))
    pos = -1
    for c in found:
        if c.start >= pos:
            yield c
            pos = c.end


# ---------------------------------------------------------------------------
# Spec / label / action helpers
# ---------------------------------------------------------------------------

def cite_spec(c: Cite) -> str:
    """Compact "jkey:section:subs" spec (jkey may carry a subject: "ca-pen")."""
    return f"{c.key}:{c.section}:{','.join(c.subs)}"


def spec_label(spec: str) -> str:
    """Human Bluebook label for a spec (best-effort from the key table).
    Parsed key-first / subs-last so a section containing colons (N.J.
    "2C:11-3", La. "14:30") survives the round trip."""
    key, rest = spec.split(":", 1)
    section, _, subs = rest.rpartition(":")
    abbr = KEY_ABBR.get(key, key)
    tail = "".join(f"({s})" for s in subs.split(",") if s)
    if key == "il":
        ch, act, sec = (section.split("/") + ["", ""])[:3]
        return f"{ch} ILCS {act}/{sec}{tail}"
    if key == "ma":
        ch, _, sec = section.partition("/")
        return f"Mass. Gen. Laws ch. {ch}, § {sec}{tail}"
    if key == "pa":
        title, _, sec = section.partition("-")
        return f"{title} Pa. Cons. Stat. § {sec}{tail}"
    if key in ("de", "me", "ok", "vt"):
        title, _, sec = section.partition("-")
        return f"{abbr} tit. {title}, § {sec}{tail}"
    if key == "la-civ":
        return f"{abbr} art. {section}"
    return f"{abbr} § {section}{tail}"


# Spec keys we can render in-app (everything else link-outs).  Populated from
# the per-state modules as parsers land.  California: all 29 codes.
_RENDERABLE_KEYS: set[str] = set()

for _code in state_ca.SUBJECT:                       # noqa: SIM118
    _k = state_ca.spec_key(_code)
    KEY_ABBR.setdefault(_k, state_ca.label_for_code(_code))
    KEY_NAME.setdefault(_k, "California")
    _RENDERABLE_KEYS.add(_k)
_RENDERABLE_KEYS.add("fl")          # Florida: single compilation, in-app


def _renderable(key: str) -> bool:
    return key in _RENDERABLE_KEYS


def load_section(key: str, section: str):
    """Fetch & parse one section for an in-app state (dispatched by the spec
    key's state prefix).  Matches the source-module contract used by the GUI's
    _fetch_statute_window / _STATUTE_SOURCES registry."""
    state = key.split("-", 1)[0]
    if state == "ca":
        return state_ca.load(key, section)
    if state == "fl":
        return state_fl.load(key, section)
    raise RuntimeError(f"no in-app source for {KEY_NAME.get(key, key)}")


def link_url(c: Cite) -> str:
    """Browser link-out target for a detected citation.  Defaults to a web
    search for the (normalized) citation, which reliably surfaces the official
    text for any jurisdiction without per-state URL schemes.  Per-state deep
    links can override this as they are added."""
    q = urllib.parse.quote_plus(_norm(c.text))
    return f"https://www.google.com/search?q={q}"


def action_for(c: Cite) -> tuple[str, str]:
    """The (kind, value) link action for a detected citation: an in-app view
    for renderable (priority-state) cites, else a browser link-out."""
    if _renderable(c.key):
        return ("statestat", cite_spec(c))
    return ("browse", link_url(c))


# ---------------------------------------------------------------------------
# Hand-typed query parsing (Quick Look Up / Spotlight)
# ---------------------------------------------------------------------------

def parse_query(query: str) -> tuple[str, str] | None:
    """Parse a whole hand-typed citation into ("statestat", spec), or None.
    Only resolves to an in-app spec for states with a parser; otherwise the
    GUI should fall back to opening the link-out."""
    q = (query or "").strip()
    cites = [c for c in iter_cites(q)]
    if not cites:
        return None
    c = cites[0]
    # Require the match to span essentially the whole query (a lookup, not a
    # citation buried in prose).
    if c.start > 0 or c.end < len(q.rstrip(". ")):
        return None
    if not _renderable(c.key):
        return None
    return ("statestat", cite_spec(c))


# ---------------------------------------------------------------------------
# Offline tests
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    def first(text: str):
        cs = list(iter_cites(text))
        return cs[0] if cs else None

    # (citation text, expected key, expected section, expected label)
    cases = [
        ("N.Y. Penal Law § 125.25", "ny-penal", "125.25", "N.Y. Penal Law § 125.25"),
        ("Cal. Penal Code § 187", "ca-pen", "187", "Cal. Penal Code § 187"),
        ("Tex. Penal Code Ann. § 19.02", "tx-penal", "19.02", "Tex. Penal Code Ann. § 19.02"),
        ("Md. Code Ann., Crim. Law § 2-201", "md-crimlaw", "2-201", "Md. Code Ann., Crim. Law § 2-201"),
        ("Fla. Stat. § 776.012", "fl", "776.012", "Fla. Stat. § 776.012"),
        ("Va. Code Ann. § 18.2-32", "va", "18.2-32", "Va. Code Ann. § 18.2-32"),
        ("Ohio Rev. Code Ann. § 2903.01", "oh", "2903.01", "Ohio Rev. Code Ann. § 2903.01"),
        ("Ga. Code Ann. § 16-5-1", "ga", "16-5-1", "Ga. Code Ann. § 16-5-1"),
        ("720 ILCS 5/9-1", "il", "720/5/9-1", "720 ILCS 5/9-1"),
        ("Mass. Gen. Laws ch. 265, § 1", "ma", "265/1", "Mass. Gen. Laws ch. 265, § 1"),
        ("42 Pa. Cons. Stat. § 9711", "pa", "42-9711", "42 Pa. Cons. Stat. § 9711"),
        ("N.J. Stat. Ann. § 2C:11-3", "nj", "2C:11-3", "N.J. Stat. Ann. § 2C:11-3"),
        ("La. Rev. Stat. § 14:30", "la", "14:30", "La. Rev. Stat. § 14:30"),
        ("Wash. Rev. Code § 9A.32.030", "wa", "9A.32.030", "Wash. Rev. Code § 9A.32.030"),
        ("Mich. Comp. Laws § 750.316", "mi", "750.316", "Mich. Comp. Laws § 750.316"),
        ("La. Civ. Code art. 2315", "la-civ", "2315", "La. Civ. Code art. 2315"),
        ("Del. Code Ann. tit. 11, § 636", "de", "11-636", "Del. Code Ann. tit. 11, § 636"),
        ("Okla. Stat. tit. 21, § 701.7", "ok", "21-701.7", "Okla. Stat. tit. 21, § 701.7"),
        # abbreviated / spacing / period variants
        ("M.G.L. c. 265, § 1", "ma", "265/1", "Mass. Gen. Laws ch. 265, § 1"),
        ("42 Pa.C.S. § 9711", "pa", "42-9711", "42 Pa. Cons. Stat. § 9711"),
        ("O.C.G.A. § 16-5-1", "ga", "16-5-1", "Ga. Code Ann. § 16-5-1"),
        ("Cal. Health & Safety Code § 11350", "ca-hsc", "11350", "Cal. Health & Safety Code § 11350"),
        ("N.Y. Veh. & Traf. Law § 1192", "ny-vehtraf", "1192", "N.Y. Veh. & Traf. Law § 1192"),
        ("Tex. Civ. Prac. & Rem. Code § 16.003", "tx-civpracrem", "16.003", "Tex. Civ. Prac. & Rem. Code Ann. § 16.003"),
        ("fla. stat. ann. § 776.012", "fl", "776.012", "Fla. Stat. § 776.012"),
    ]
    for text, key, section, label in cases:
        c = first(text)
        got = (c.key, c.section, c.label) if c else None
        check(got == (key, section, label), f"{text!r} -> {got!r}")

    # California subjects canonicalize to official lawCodes (distinct keys for
    # Civil Code vs Code of Civil Procedure).
    check(first("Cal. Civ. Code § 1714").key == "ca-civ"
          and first("Cal. Civ. Proc. Code § 425.16").key == "ca-ccp",
          "Cal. Civ. Code -> ca-civ, Civ. Proc. Code -> ca-ccp")

    # subdivisions captured, year not swallowed
    c = first("Cal. Penal Code § 187(a)")
    check(c and c.subs == ("a",), f"subs: {c.subs if c else None}")
    c = first("Fla. Stat. § 776.012 (2023)")
    check(c and c.section == "776.012" and c.subs == (),
          f"year not swallowed: {(c.section, c.subs) if c else None}")

    # spec round-trips
    check(spec_label("fl:776.012:") == "Fla. Stat. § 776.012", "spec fl")
    check(spec_label("il:720/5/9-1:") == "720 ILCS 5/9-1", "spec il")
    check(spec_label("nj:2C:11-3:") == "N.J. Stat. Ann. § 2C:11-3", "spec nj")
    check(spec_label("de:11-636:") == "Del. Code Ann. tit. 11, § 636", "spec de")
    check(spec_label("pa:42-9711:") == "42 Pa. Cons. Stat. § 9711", "spec pa")

    # Priority states (CA, FL) render in-app; non-priority states link-out.
    kind, val = action_for(first("Cal. Penal Code § 187"))
    check(kind == "statestat" and val == "ca-pen:187:",
          f"CA in-app action: {(kind, val)}")
    kind, val = action_for(first("Fla. Stat. § 776.012"))
    check(kind == "statestat" and val == "fl:776.012:",
          f"FL in-app action: {(kind, val)}")
    kind, val = action_for(first("Ga. Code Ann. § 16-5-1"))
    check(kind == "browse" and val.startswith("https://www.google.com/search?"),
          f"link-out action: {(kind, val[:40])}")
    check(parse_query("Cal. Penal Code § 187") == ("statestat", "ca-pen:187:"),
          "parse_query CA -> in-app spec")
    check(parse_query("Fla. Stat. § 776.012") == ("statestat", "fl:776.012:"),
          "parse_query FL -> in-app spec")
    check(parse_query("Ga. Code Ann. § 16-5-1") is None,
          "parse_query non-priority -> None (link-out handled elsewhere)")
    check(spec_label("ca-pen:187:") == "Cal. Penal Code § 187",
          "spec_label CA works cold (eager registration)")

    # things that must NOT be detected as state statutes
    negatives = [
        "42 U.S.C. § 1983",           # federal code
        "29 C.F.R. § 1614.105",       # federal regs
        "Fed. R. Evid. 404(b)",       # federal rule
        "Cal. Code Regs. tit. 22, § 51303",   # CA regulations, not statute
        "Tex. Admin. Code § 1.1",     # TX regulations, not statute
        "N.Y. Comp. Codes R. & Regs. tit. 18, § 505.2",  # NY regs
        "512 U.S. 477 (1994)",        # reporter
        "5 Cal. App. 4th 1289",       # reporter
    ]
    for text in negatives:
        c = first(text)
        check(c is None, f"no state-statute in {text!r} (got {c.label if c else None!r})")

    # detection inside running prose, with a pincite-y context
    prose = ("The court applied Fla. Stat. § 776.012 and also cited "
             "Cal. Penal Code § 187 before turning to Ga. Code Ann. § 16-5-1.")
    keys = [c.key for c in iter_cites(prose)]
    check(keys == ["fl", "ca-pen", "ga"], f"prose scan: {keys}")

    raise SystemExit(1 if failed else 0)
