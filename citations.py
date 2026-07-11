"""Citation detection shared by the opinion reader and the brief viewer.

This module is deliberately free of any ``tkinter`` dependency so the citation
logic can be unit-tested headlessly (``python3 citations.py``) and reused by the
"Open Brief…" feature, which renders a user's brief and highlights every
citation it can resolve.

It owns the reporter-citation regexes (case cites, short forms, ``Id.``) that
used to live in ``courtlistener_gui`` and adds :func:`detect_links`, which scans
a whole document and returns the clickable spans — case citations plus every
statute/regulation/rule/constitution source the app already knows how to open.

The per-source modules (``us_code``, ``ecfr``, ``fed_rules``, ``constitution``,
``state_statutes``, ``statutes_at_large``) each expose their own ``*_CITE_RE``
and a ``cite_spec``/``action`` helper; :func:`detect_links` simply runs them all
over the text and reconciles overlaps the same way the opinion reader does.
"""

from __future__ import annotations

import json
import re

import constitution
import court_catalog
import ecfr
import eng_rep
import fed_rules
import state_statutes
import statutes_at_large
import us_code

# A pinpoint page following a case citation: ", 171", ", at 171", or
# ", 171-72" — but not
# the volume of a parallel citation (", 510 A.2d 562"), recognized by the
# capital letter that follows the number.
PINCITE_AFTER_RE = re.compile(
    r",\s*(?:at\s+)?\*?(\d{1,6})(?:\s*[-–—]\s*\*?\d{1,6})?(?!\d|\s*[A-Z])",
    re.IGNORECASE,
)

# Citations recognized inside running text (made clickable → Scholar lookup).
# Pattern: volume, reporter abbreviation, page.
REPORTER_ALT = (
    r"(?:U\.\s?S\.(?!\s?C)|S\.\s?Ct\.|L\.\s?Ed\.(?:\s?2d)?|"
    r"F\.\s?Supp\.(?:\s?[23]d)?|F\.\s?(?:2d|3d|4th)|F\.\s?App[’']x|Fed\.\s?Appx\.|B\.R\.|"
    r"A\.(?:2d|3d)?|P\.(?:2d|3d)?|N\.E\.(?:2d|3d)?|N\.W\.(?:2d)?|S\.E\.(?:2d)?|"
    r"S\.W\.(?:2d|3d)?|So\.(?:\s?[23]d)?|Cal\.\s?Rptr\.(?:\s?[23]d)?|"
    r"N\.Y\.S\.(?:2d|3d)?|Ohio\s?St\.\s?(?:2d|3d)?|Ill\.\s?2d|Wis\.\s?2d|Wn\.\s?(?:2d|App\.))"
)
TEXT_CITE_RE = re.compile(r"\b\d{1,4}\s+" + REPORTER_ALT + r"\s+\d{1,5}\b")

# Some citators — and Google Scholar, for old state cases — drop a court /
# jurisdiction parenthetical between the reporter and the page:
# "5 Johns. (N.Y.) 37", "15 Johns. (N.Y.) 121".  Matched optionally and never
# captured, so the reporter/page groups stay clean; :func:`_case_match_text`
# strips it back out of the matched span so the normalized cite is "5 Johns.
# 37".  Requiring a letter-led parenthetical leaves a parallel-reporter form
# ("5 U.S. (1 Cranch) 137") untouched.
_COURT_PAREN = r"(?:\s*\([A-Za-z][A-Za-z.'’ ]{0,20}\))?"

# Capturing form (volume, reporter, page) — used to index every full citation
# in a document so short forms can be resolved back to it.
CITE_CAPTURE_RE = re.compile(
    r"\b(\d{1,4})\s+(" + REPORTER_ALT + r")" + _COURT_PAREN + r"\s+(\d{1,5})\b")

# Briefs often cite official state reporters that are too numerous to list in
# REPORTER_ALT ("306 Md. 556", "100 Cal. 400", "515 Pa. 1").  This guarded
# fallback is intentionally broad but excludes statute/regulation abbreviations
# before they can become case links.
_REPORTER_TOKEN = r"(?:[A-Z][A-Za-z0-9.'’]*|\d+d|\d+th)"
BROAD_CITE_CAPTURE_RE = re.compile(
    r"\b(\d{1,4})\s+("
    + _REPORTER_TOKEN
    + r"(?:\s+"
    + _REPORTER_TOKEN
    + r"){0,5}?)"
    + _COURT_PAREN
    + r"\s+(\d{1,6})(?=[\s,;.)(]|$)"
)
_NONCASE_REPORTERS = {
    "usc", "usca", "uscs", "cfr", "fr", "fedr", "fedreg",
    # English Reports ("156 Eng. Rep. 145", "95 E.R. 807"): real case cites,
    # but ones Google Scholar / CourtListener / case.law cannot open — the
    # eng_rep pass links them to the CommonLII scan instead, so the broad case
    # regex must not claim them first (a Scholar lookup by an E.R. cite lands
    # on an unrelated case).
    "engrep", "er",
}
_PLAIN_CASE_REPORTERS = {
    "alaska", "idaho", "iowa", "ohio", "utah", "vermont", "wyoming",
    "wl", "lexis",
}

# Short-form citation: "Roe, 410 U.S., at 152" → volume, reporter, pin page.
SHORT_CITE_RE = re.compile(
    r"\b(\d{1,4})\s+(" + REPORTER_ALT + r")\s*,?\s+at\s+(\d{1,5})\b")
BROAD_SHORT_CITE_RE = re.compile(
    r"\b(\d{1,4})\s+("
    + _REPORTER_TOKEN
    + r"(?:\s+"
    + _REPORTER_TOKEN
    + r"){0,5}?)\s*,?\s+at\s+\*?(\d{1,6})\b",
    re.IGNORECASE,
)

# "Id." short form — refers to the immediately preceding citation; group 1 is
# the optional pin page ("Id. at 152").  ("Ibid." is deliberately not traced —
# it usually points at a non-case source.)
ID_CITE_RE = re.compile(r"\bid\.(?:\s*,?\s*at\s+\*?(\d{1,6}))?", re.IGNORECASE)

# Record cites in briefs commonly use "Id." too.  If one appears between an
# authority and a later "Id. at N", do not carry the authority forward.
_RECORD_CITE_RE = re.compile(
    r"\b(?:App\.|J\.?A\.|A\.R\.|R\.|Tr\.|Dkt\.|Doc\.|ECF|Ex\.|ER|SER)"
    r"\s*(?:No\.?\s*)?[\w*.-]+|\b(?:ECF|Dkt\.|Doc\.)\s+No\.?\s+\d+|¶\s*\d+",
    re.IGNORECASE,
)


def norm_reporter(rep: str) -> str:
    """Reporter key for matching, ignoring spacing/case ('U. S.' == 'U.S.')."""
    return re.sub(r"\s+", "", rep or "").lower()


def _reporter_key(rep: str) -> str:
    # Lowercase *before* stripping: the character class is lowercase-only, so
    # stripping first would delete every capital letter ("Eng. Rep." → "ngep")
    # and no key would ever match the reporter sets below.
    return re.sub(r"[^a-z0-9]+", "", (rep or "").lower())


def _valid_case_reporter(rep: str) -> bool:
    key = _reporter_key(rep)
    if not key or key in _NONCASE_REPORTERS:
        return False
    if key in _PLAIN_CASE_REPORTERS or key.endswith("lexis"):
        return True
    return "." in (rep or "")


def _case_match_text(m: re.Match) -> str:
    s = re.sub(r"\s+", " ", m.group(0)).replace("U. S.", "U.S.").replace("’", "'")
    # Drop the court/jurisdiction parenthetical the reporter regexes tolerate
    # between reporter and page ("5 Johns. (N.Y.) 37" -> "5 Johns. 37").  The
    # match ends at the page, so the only parenthetical it can contain is that.
    return re.sub(r"\s*\([^)]*\)\s*", " ", s).strip()


def _iter_case_cites(text: str) -> list[re.Match]:
    matches: list[re.Match] = list(CITE_CAPTURE_RE.finditer(text or ""))
    for m in BROAD_CITE_CAPTURE_RE.finditer(text or ""):
        if not _valid_case_reporter(m.group(2)):
            continue
        if any(m.start() < km.end() and km.start() < m.end() for km in matches):
            continue
        matches.append(m)
    matches.sort(key=lambda m: (m.start(), -(m.end() - m.start())))
    return matches


def _iter_short_cites(text: str) -> list[re.Match]:
    matches: list[re.Match] = list(SHORT_CITE_RE.finditer(text or ""))
    for m in BROAD_SHORT_CITE_RE.finditer(text or ""):
        if not _valid_case_reporter(m.group(2)):
            continue
        if any(m.start() < km.end() and km.start() < m.end() for km in matches):
            continue
        matches.append(m)
    matches.sort(key=lambda m: (m.start(), -(m.end() - m.start())))
    return matches


def _id_chain_broken(gap: str) -> bool:
    stripped = (gap or "").strip()
    return bool(stripped and (
        len(stripped) > 240
        or "\n\n" in gap
        or _RECORD_CITE_RE.search(gap)
    ))


def build_short_cite_index(text: str) -> dict[tuple[str, str], list[int]]:
    """Map (volume, reporter) → sorted first-pages of every full citation in
    `text`, so a short form ('410 U.S. at 152') can be resolved to the case's
    first page (and thence opened and pin-jumped)."""
    idx: dict[tuple[str, str], set] = {}
    for m in _iter_case_cites(text or ""):
        idx.setdefault((m.group(1), norm_reporter(m.group(2))),
                       set()).add(int(m.group(3)))
    return {k: sorted(v) for k, v in idx.items()}


def cite_target_from_text(
    text: str, index: dict[tuple[str, str], list[int]]
) -> tuple[str, str]:
    """(base cite, pin) named in `text`.  The base is "vol reporter firstpage"
    whether the cite is written in full ("8 F.4th 557, 565") or short
    ("8 F.4th at 565", resolved to its first page via `index`); the pin is the
    pincite/short page, or "".  Empty base when no reporter cite is present."""
    case_matches = _iter_case_cites(text)
    if case_matches:
        cm = case_matches[0]
        base = _case_match_text(cm)
        pm = PINCITE_AFTER_RE.match(text, cm.end())
        return base, (pm.group(1) if pm else "")
    short_matches = _iter_short_cites(text)
    if short_matches:
        sm = short_matches[0]
        rep = re.sub(r"\s+", " ", sm.group(2)).strip().replace("U. S.", "U.S.")
        pin = int(sm.group(3))
        pages = index.get((sm.group(1), norm_reporter(sm.group(2))))
        if pages:
            below = [p for p in pages if p <= pin]
            first = max(below) if below else pages[0]
        else:
            first = pin  # no full cite indexed — best effort
        return f"{sm.group(1)} {rep} {first}", str(pin)
    return "", ""


# ---------------------------------------------------------------------------
# Unpublished opinions cited by Westlaw / LEXIS number
# ---------------------------------------------------------------------------
# "Care One Mgmt., LLC v. United Healthcare Workers E., No. 12-6371, 2024 WL
# 1327972, at *7 (D.N.J. Mar. 28, 2024)" — no reporter ever prints these, but
# the docket number and opinion date locate the document in CourtListener's
# RECAP (PACER) archive.  The docket number usually appears only in the
# citation's first (or table-of-authorities) occurrence, while later short
# forms carry just the WL number — so the fields are indexed per WL number
# across the whole document and every occurrence gets the merged spec.

WL_CITE_RE = re.compile(
    r"\b(\d{4})\s+(WL|U\.\s?S\.\s?(?:Dist\.|App\.)\s?LEXIS)\s+(\d{2,10})\b")

# The docket number written immediately before the WL cite: "No. 12-6371,",
# "Nos. 12-6371, 12-6372,", "Civ. A. No. 96-3837,", "Case No. 2:13-cv-7779,".
_RECAP_DOCKET_RE = re.compile(
    r"(?:Nos?\.|Civ(?:il)?\.?\s?(?:A(?:ction)?\.?)?\s?Nos?\.?|Case\s+No\.)\s*"
    r"([A-Za-z]{0,4}\s?[\w:().-]{3,30}?)\s*,\s*$"
)

# The court/date parenthetical after the cite (an optional star pin cite in
# between): ", at *7 (D.N.J. Mar. 28, 2024)".
_RECAP_AFTER_RE = re.compile(
    r"^(?:,\s*(?:at\s+)?\*?\d{1,6}(?:\s*[-–—]\s*\*?\d{1,6})?)?"
    r"\s*\(([^()]{2,45}?)\s+"
    r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sept?|Oct|Nov|Dec)[a-z]*\.?)\s+"
    r"(\d{1,2}),\s+(\d{4})\)"
)

# The case name before the docket number, for the viewer's window title.
_RECAP_NAME_RE = re.compile(
    r"([A-Z][\w.,'’&() -]{1,80}?\sv\.\s[\w.,'’&() -]{1,60}?|"
    r"In\s+re\s+[\w.,'’&() -]{2,60}?),\s*"
    r"(?:Nos?\.|Civ|Case\s+No\.)"
)

_MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
           "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}

# Bluebook court abbreviation → CourtListener court id, federal courts only
# (RECAP is the PACER archive; state-court WL cites keep the Scholar path).
# Keyed with spacing/periods squashed so "D.N.J.", "D. N.J." both hit.
_FED_COURT_IDS: dict[str, str] = {}
for _map in (court_catalog.CIRCUIT_COURTS, court_catalog.DISTRICT_COURTS,
             court_catalog.SPECIAL_COURTS):
    for _cid, _abbr in _map.items():
        _FED_COURT_IDS[re.sub(r"[^a-z0-9]", "", _abbr.lower())] = _cid


def iter_recap_cites(text: str) -> list[tuple[int, int, "str | None"]]:
    """Every WL / LEXIS citation in *text* as ``(start, end, spec)``.

    ``spec`` is a JSON string with the fields a RECAP lookup needs —
    ``cite``, ``docket``, ``date`` and (when it resolved to a federal
    court) ``court``, plus ``name`` for the window title — or ``None``
    when the citation can't be a RECAP document (no docket/date anywhere
    in the document, or a state court), in which case the caller should
    treat it as an ordinary case citation."""
    index: dict = {}
    occurrences: list = []
    for m in WL_CITE_RE.finditer(text or ""):
        key = (m.group(1), norm_reporter(m.group(2)), m.group(3))
        info = index.setdefault(
            key, {"cite": re.sub(r"\s+", " ", m.group(0))})
        before = re.sub(r"\s+", " ", text[max(0, m.start() - 90):m.start()])
        dm = _RECAP_DOCKET_RE.search(before)
        if dm:
            info.setdefault("docket", dm.group(1).strip())
            nm = _RECAP_NAME_RE.search(before)
            if nm:
                info.setdefault("name", nm.group(1).strip(" ,"))
        am = _RECAP_AFTER_RE.match(
            re.sub(r"\s+", " ", text[m.end():m.end() + 110]))
        if am:
            info.setdefault(
                "court_raw", re.sub(r"\s+", " ", am.group(1)).strip())
            mon = _MONTHS.get(am.group(2)[:3].lower())
            if mon and "date" not in info:
                info["date"] = (f"{am.group(4)}-{mon:02d}-"
                                f"{int(am.group(3)):02d}")
        occurrences.append((m.start(), m.end(), key))

    out: list = []
    for start, end, key in occurrences:
        info = index[key]
        spec = None
        court_raw = info.get("court_raw", "")
        court_id = _FED_COURT_IDS.get(
            re.sub(r"[^a-z0-9]", "", court_raw.lower()))
        # Only a federal docket + date is worth a RECAP lookup; a court
        # named but not federal is a state court's unpublished opinion.
        if ("docket" in info and "date" in info
                and (court_id or not court_raw)):
            fields = {"cite": info["cite"], "docket": info["docket"],
                      "date": info["date"]}
            if court_id:
                fields["court"] = court_id
            if info.get("name"):
                fields["name"] = info["name"]
            spec = json.dumps(fields)
        out.append((start, end, spec))
    return out


# ---------------------------------------------------------------------------
# Whole-document detection (used by the brief viewer)
# ---------------------------------------------------------------------------

# An "Id., at N" links to the case last cited only when N is plausibly a page of
# that reporter — within this many pages of its start.  A far page ("Id. at 1450"
# pointing into the record / a joint appendix, not the reporter) falls outside the
# window and is left unlinked.  Mirrors the opinion reader's _id_pin_in_range.
ID_PIN_WINDOW = 100


def _cite_first_page(base_cite: str) -> "int | None":
    """Reporter start page of a base citation ("410 U.S. 113" → 113), ignoring
    any "@pin" suffix; ``None`` when it doesn't parse."""
    matches = _iter_case_cites((base_cite or "").split("@", 1)[0])
    m = matches[0] if matches else None
    try:
        return int(m.group(3)) if m else None
    except (TypeError, ValueError):
        return None


def _id_pin_in_range(base_cite: str, pin: str) -> bool:
    """True when an "Id., at *pin*" page falls within :data:`ID_PIN_WINDOW` pages
    of *base_cite*'s start page — i.e. a page of that reporter, not a record page."""
    start = _cite_first_page(base_cite)
    try:
        n = int(pin)
    except (TypeError, ValueError):
        return False
    return start is not None and start <= n <= start + ID_PIN_WINDOW


def detect_links(text: str) -> list[tuple[int, int, tuple[str, str]]]:
    """Scan `text` and return ``(start, end, action)`` for every citation that
    can be opened, in document order with overlaps resolved (first/longest
    wins).  ``action`` is the same ``(kind, value)`` pair the opinion reader
    hands to its link dispatch:

      * ``("cite", "410 U.S. 113@152")`` — a case (optionally pin-cited),
      * ``("usc"|"cfr"|"rule"|"const"|"statestat", spec)`` — an in-app source,
      * ``("browse", url)`` — a state statute we only link out to,
      * ``("statpdf", url)`` — a Statutes at Large scan.

    Unlike the opinion reader this works over the whole document at once, so a
    short form ("410 U.S. at 152") or an ``Id.`` resolves against citations that
    appear anywhere in the brief.
    """
    if not text:
        return []
    index = build_short_cite_index(text)
    matches: list[tuple[int, int, str, object]] = []
    # English Reports citations first — both the reprint form ("156 Eng. Rep.
    # 145") and the original nominate cites ("9 Exch. 341", resolution-gated in
    # eng_rep) — so the broad case-reporter fallback below can yield to them:
    # a Scholar lookup by an English cite lands on an unrelated case.
    engrep_spans: list[tuple[int, int]] = []
    for m in eng_rep.ER_CITE_RE.finditer(text):
        engrep_spans.append((m.start(), m.end()))
        matches.append((m.start(), m.end(), "engrep", eng_rep.cite_spec(m)))
    for start, end, spec, _cases in eng_rep.iter_nominate_cites(text):
        engrep_spans.append((start, end))
        matches.append((start, end, "engrep", spec))
    # Unpublished opinions cited by WL/LEXIS number: RECAP-resolvable ones
    # (federal docket + date found in the document) get a "recap" action;
    # the rest become ordinary case cites (Scholar), including the 7-digit
    # WL numbers the broad reporter regex's page group won't match.
    recap_spans: list[tuple[int, int]] = []
    for start, end, spec in iter_recap_cites(text):
        recap_spans.append((start, end))
        if spec is not None:
            matches.append((start, end, "recap", spec))
        else:
            cite = re.sub(r"\s+", " ", text[start:end]).strip()
            matches.append((start, end, "cite", cite))
    claimed_spans = engrep_spans + recap_spans
    for m in _iter_case_cites(text):
        if any(m.start() < e and s < m.end() for s, e in claimed_spans):
            continue
        matches.append((m.start(), m.end(), "cite", m))
    for m in us_code.USC_CITE_RE.finditer(text):
        matches.append((m.start(), m.end(), "usc", m))
    for m in ecfr.CFR_CITE_RE.finditer(text):
        matches.append((m.start(), m.end(), "cfr", m))
    for m in fed_rules.RULE_CITE_RE.finditer(text):
        matches.append((m.start(), m.end(), "rule", m))
    for m in constitution.CONST_CITE_RE.finditer(text):
        matches.append((m.start(), m.end(), "const", m))
    # Short forms ("Roe, 410 U.S. at 152") resolve to the case's full citation.
    for m in _iter_short_cites(text):
        # A WL short form ("2014 WL 1922831 at *5") overlapping a RECAP span
        # would outrank it (same start, longer) — the RECAP action wins.
        if any(m.start() < e and s < m.end() for s, e in recap_spans):
            continue
        pages = index.get((m.group(1), norm_reporter(m.group(2))))
        if not pages:
            continue
        pin = int(m.group(3))
        below = [p for p in pages if p <= pin]
        first = max(below) if below else pages[0]
        rep = re.sub(r"\s+", " ", m.group(2)).strip().replace("U. S.", "U.S.")
        cite = f"{m.group(1)} {rep} {first}"
        if pin != first:
            cite += f"@{pin}"
        matches.append((m.start(), m.end(), "shortcite", cite))
    for m in ID_CITE_RE.finditer(text):
        matches.append((m.start(), m.end(), "idcite", m))
    for c in state_statutes.iter_cites(text):
        if re.match(r"\s*id\.", c.text, re.IGNORECASE):
            continue
        matches.append((c.start, c.end, "statestat", c))
    for m in statutes_at_large.STAT_CITE_RE.finditer(text):
        if statutes_at_large.url_for(m):  # only link volumes GovInfo has
            matches.append((m.start(), m.end(), "stat", m))

    matches.sort(key=lambda t: (t[0], -t[1]))
    out: list[tuple[int, int, tuple[str, str]]] = []
    pos = 0
    last_cite_action: tuple[str, str] | None = None
    last_cite_end: int | None = None
    const_linked: set[int] = set()  # amendments already linked (prose dedup)
    for start, end, kind, m in matches:
        if start < pos:
            continue  # overlapping match — first/longest wins
        action: tuple[str, str] | None
        cite_base = ""
        if kind == "cite":
            # m is a regex match for reporter cites, a pre-normalized string
            # for the WL/LEXIS cites added by the RECAP pass.
            cite = m if isinstance(m, str) else _case_match_text(m)
            cite_base = cite
            pin_m = PINCITE_AFTER_RE.match(text, end)
            if pin_m:
                cite += "@" + pin_m.group(1)
            action = ("cite", cite)
        elif kind == "recap":
            action = ("recap", m)  # m is the pre-built JSON spec
        elif kind == "usc":
            action = ("usc", us_code.cite_spec(m))
        elif kind == "cfr":
            action = ("cfr", ecfr.cite_spec(m))
        elif kind == "rule":
            action = ("rule", fed_rules.cite_spec(m))
        elif kind == "const":
            # Link a bare prose amendment mention ("the First Amendment …", no
            # section, not a "U.S. Const." citation) only the first time that
            # amendment appears; formal citations always link.
            spec = constitution.cite_spec(m)
            ck, cnum, csec = (spec.split(":") + ["", "", ""])[:3]
            prose = "const" not in re.sub(r"\s+", " ", m.group(0)).lower()
            if ck == "amend" and cnum.isdigit():
                cn = int(cnum)
                if prose and not csec and cn in const_linked:
                    action = None
                else:
                    const_linked.add(cn)
                    action = ("const", spec)
            else:
                action = ("const", spec)
        elif kind == "shortcite":
            action = ("cite", m)  # m is the pre-built "vol rep page@pin"
            cite_base = m.split("@")[0]
        elif kind == "idcite":
            # "Id." → the last citation, but conservatively, because in a brief an
            # "Id." often points at a record document rather than the cited
            # authority.  A bare "Id." (no page) is never linked.  "Id. at N" links
            # to the previous *case* only when N is plausibly a page of its reporter
            # (within ID_PIN_WINDOW of its start); a far page is a record/appendix
            # cite, left unlinked.  "Id. at N" after a statute/rule reopens that
            # source.
            la = last_cite_action
            if la and last_cite_end is not None and _id_chain_broken(
                text[last_cite_end:start]
            ):
                la = None
            pin = m.group(1)
            if not la or pin is None:
                action = None
            elif la[0] == "cite":
                action = (("cite", f"{la[1]}@{pin}")
                          if _id_pin_in_range(la[1], pin) else None)
            else:
                action = la
        elif kind == "statestat":
            action = state_statutes.action_for(m)
        elif kind == "stat":
            action = ("statpdf", statutes_at_large.url_for(m))
        elif kind == "engrep":
            # English Reports cite — reprint ("156 Eng. Rep. 145") or nominate
            # ("9 Exch. 341") — -> CommonLII scan; m is the pre-built spec.
            action = ("engrep", m)
        else:  # pragma: no cover - defensive
            action = None
        if action is not None:
            out.append((start, end, action))
            if kind in ("cite", "shortcite"):
                last_cite_action = ("cite", cite_base)
            else:
                last_cite_action = action
            last_cite_end = end
        pos = end
    return out


if __name__ == "__main__":  # pragma: no cover - offline smoke test
    import sys

    sample = (
        "The Court relied on Roe v. Wade, 410 U.S. 113, 152 (1973), and later "
        "on 410 U.S. at 164.  See also 42 U.S.C. § 1983; Fed. R. Civ. P. 56; "
        "29 C.F.R. § 1614.105; U.S. Const. amend. XIV, § 1; Cal. Penal Code "
        "§ 187; Id. at 170."
    )
    found = detect_links(sample)
    for start, end, action in found:
        print(f"{start:4d}-{end:<4d} {action[0]:10s} {sample[start:end]!r} -> {action[1]!r}")

    kinds = {a[0] for _, _, a in found}
    expect = {"cite", "usc", "rule", "cfr", "const"}
    missing = expect - kinds
    if missing:
        print("MISSING kinds:", missing)
        sys.exit(1)
    # The short form "410 U.S. at 164" must resolve to the indexed first page.
    if not any(a == ("cite", "410 U.S. 113@164") for _, _, a in found):
        print("short form did not resolve to 410 U.S. 113@164")
        sys.exit(1)

    # "Id., at N" links to the previous case only when N is within ID_PIN_WINDOW
    # of its start page; a far page is a record/appendix cite, left unlinked.
    near = detect_links("See Roe v. Wade, 410 U.S. 113 (1973). Id. at 160.")
    if not any(a == ("cite", "410 U.S. 113@160") for _, _, a in near):
        print("in-range Id. did not link:", near)
        sys.exit(1)
    far = detect_links("See Roe v. Wade, 410 U.S. 113 (1973). Id. at 1450.")
    if any(a[0] == "cite" and "@1450" in a[1] for _, _, a in far):
        print("out-of-range Id. should not link to the case:", far)
        sys.exit(1)
    # A bare "Id." (no page) is never linked — too often a record cite — so the
    # only case link here is the full citation itself, not the trailing "Id.".
    bare = detect_links("See Roe v. Wade, 410 U.S. 113 (1973). Id.")
    if sum(1 for _, _, a in bare if a == ("cite", "410 U.S. 113")) != 1:
        print("bare Id. should not add a link:", bare)
        sys.exit(1)

    # Official state reporters, common in briefs, should be clickable and should
    # support short forms and in-range Id. references.
    state = detect_links(
        "Smith v. Jones, 306 Md. 556, 560 (1986). 306 Md. at 561. Id. at 562."
    )
    for want in (
        ("cite", "306 Md. 556@560"),
        ("cite", "306 Md. 556@561"),
        ("cite", "306 Md. 556@562"),
    ):
        if not any(a == want for _, _, a in state):
            print("state reporter/short/Id. failed:", want, state)
            sys.exit(1)

    # Do not mistake U.S.C./C.F.R. references for broad case reporters.
    statutory = detect_links("See 42 U.S.C. 1983 and 29 C.F.R. 1614.105.")
    if any(a[0] == "cite" for _, _, a in statutory):
        print("statutory citations became case cites:", statutory)
        sys.exit(1)

    # Brief record cites between an authority and Id. break the Id. chain.
    record_gap = detect_links("See Foo, 1 F.4th 1. App. 5. Id. at 6.")
    if any(a == ("cite", "1 F.4th 1@6") for _, _, a in record_gap):
        print("record Id. should not point to the case:", record_gap)
        sys.exit(1)

    star_pin = detect_links("See Foo, 1 F.4th 1. Id. at *6.")
    if not any(a == ("cite", "1 F.4th 1@6") for _, _, a in star_pin):
        print("star-page Id. did not link:", star_pin)
        sys.exit(1)

    # A court/jurisdiction parenthetical between reporter and page (as Google
    # Scholar prints old state cases) must not defeat the cite — it normalizes
    # away so the link resolves to "5 Johns. 37" (Kilburn v. Woodworth), not a
    # dead cite that dead-ends on a fuzzy name search.
    juris = detect_links("Kilbourn v. Woodworth, 5 Johns. (N.Y.) 37, was an "
                         "action of debt; see Borden v. Fitch, 15 Johns. (N.Y.) 121.")
    for want in (("cite", "5 Johns. 37"), ("cite", "15 Johns. 121")):
        if not any(a == want for _, _, a in juris):
            print("embedded jurisdiction paren cite failed:", want, juris)
            sys.exit(1)
    base, _pin = cite_target_from_text("5 Johns. (N.Y.) 37", {})
    if base != "5 Johns. 37":
        print("cite_target_from_text kept the paren:", repr(base))
        sys.exit(1)

    # English Reports cites must route to the CommonLII viewer ("engrep"), not
    # become Scholar case links (a Scholar lookup by an E.R. cite lands on an
    # unrelated case) — in the Bluebook "Eng. Rep." form, the "E.R." form, and
    # never via the short form either.
    er = detect_links(
        "Hadley v. Baxendale, 156 Eng. Rep. 145, 151 (1854); Wain v. "
        "Warlters, 102 E.R. 972.  See 156 Eng. Rep. at 151."
    )
    for want in (("engrep", "156:145"), ("engrep", "102:972")):
        if not any(a == want for _, _, a in er):
            print("Eng. Rep. cite did not route to engrep:", want, er)
            sys.exit(1)
    if any(a[0] == "cite" for _, _, a in er):
        print("Eng. Rep. cite leaked into a Scholar case link:", er)
        sys.exit(1)

    # The nominate-report parallel cites route to the same viewer (resolution-
    # gated on the shipped index): "9 Exch. 341" is Hadley, "5 East 10" is
    # Wain.  U.S. cites sharing an abbreviation stay ordinary case links —
    # New York's volumed "5 Johns. 37" must never be claimed by the volumeless
    # English Johnson.
    nom = detect_links(
        "Hadley v. Baxendale, 9 Exch. 341, 156 Eng. Rep. 145 (1854); "
        "Wain v. Warlters, 5 East 10; Kilbourn v. Woodworth, 5 Johns. "
        "(N.Y.) 37."
    )
    for want in (("engrep", "n:exch:9:341"), ("engrep", "156:145"),
                 ("engrep", "n:east:5:10"), ("cite", "5 Johns. 37")):
        if not any(a == want for _, _, a in nom):
            print("nominate detection failed:", want, nom)
            sys.exit(1)
    if any(a == ("cite", "9 Exch. 341") or a == ("cite", "5 East 10")
           for _, _, a in nom):
        print("nominate cite leaked into a Scholar case link:", nom)
        sys.exit(1)

    # Unpublished opinions: a federal WL cite with docket + court/date routes
    # to RECAP — with the docket carried from the first occurrence to later
    # short forms — while a state-court WL cite stays an ordinary case link.
    recap = detect_links(
        "Care One Mgmt., LLC v. United Healthcare Workers E., No. 12-6371, "
        "2024 WL 1327972, at *7 (D.N.J. Mar. 28, 2024).  A later short form "
        "cites 2024 WL 1327972, at *9 (D.N.J. Mar. 28, 2024).  But Foxtons, "
        "Inc. v. Cirri Germain Realty, No. A-61210-05T3, 2008 WL 465653 "
        "(N.J. Super. Ct. App. Div. Feb. 22, 2008) is a state case."
    )
    recap_actions = [a for _s, _e, a in recap if a[0] == "recap"]
    if len(recap_actions) != 2:
        print("expected 2 recap links:", recap)
        sys.exit(1)
    spec = json.loads(recap_actions[0][1])
    if not (spec.get("docket") == "12-6371" and spec.get("court") == "njd"
            and spec.get("date") == "2024-03-28"
            and spec.get("cite") == "2024 WL 1327972"
            and "Care One" in spec.get("name", "")):
        print("bad recap spec:", spec)
        sys.exit(1)
    if json.loads(recap_actions[1][1]).get("docket") != "12-6371":
        print("short-form recap did not inherit the docket:", recap_actions)
        sys.exit(1)
    if not any(a == ("cite", "2008 WL 465653") for _s, _e, a in recap):
        print("state WL cite should stay a case link:", recap)
        sys.exit(1)

    # A WL cite with no docket anywhere stays a plain case link, even when
    # its number is too long for the broad reporter regex.
    plain = detect_links("ShotSpotter Inc. v. VICE Media, LLC, 2022 WL "
                         "2373418, at *12 (Del. Super. Ct. June 30, 2022).")
    if not any(a == ("cite", "2022 WL 2373418@12") for _s, _e, a in plain):
        print("7-digit WL cite did not become a case link:", plain)
        sys.exit(1)
    if any(a[0] == "recap" for _s, _e, a in plain):
        print("state WL cite must not become recap:", plain)
        sys.exit(1)

    print("\nOK:", len(found), "links;", sorted(kinds))
