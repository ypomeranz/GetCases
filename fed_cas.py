"""Detect Federal Cases citations given by case number ("Case No. 10,126").

The Federal Cases (cited "F. Cas.") reprint every available lower federal
opinion from 1789 to 1880 in thirty volumes, arranged *alphabetically* by
case name and numbered consecutively 1-18,313.  Opinions of that era cite
them by that case number, not by volume and page —

    Cole v. The Atlantic, Case No. 2,976; The Chusan, Id. 2,717;
    Davis v. A New Brig [Case No. 3,643]

— where the old-style "Id. <number>" repeats the "Case No." of the citation
before it (not the modern pin-page "Id. at 152", which is never matched
here).  No public digital index maps case numbers to reporter citations, so
these links resolve at click time through the CourtListener API instead: the
case *name* printed just before the number keys a search, and the number
itself verifies the hit — it is printed at the head of every Federal Cases
opinion, and the volume of a candidate's "F. Cas." citation must agree with
:data:`VOLUME_RANGES`, the exact case-number span of each volume (from the
volumes' own title pages).  That resolution lives in
``courtlistener.find_fedcas_case``; this module is detection and number
arithmetic only, dependency-free and unit-testable offline
(``python -X utf8 fed_cas.py``).

The texts are OCR of 1890s typesetting, so the matching is forgiving: the
thousands separator may be a period ("Id. 7.030" is 7,030), a stray hyphen
may interrupt the digits ("Case No. 2,-976") and suffixed numbers exist
("Case No. 6,082a").  Modern docket numbers that also follow the words
"Case No." ("Case No. 2:13-cv-7779", "Case No. 12-6371") are excluded by
shape.
"""

from __future__ import annotations

import json
import re
from bisect import bisect_right

# ---------------------------------------------------------------------------
# Volume arithmetic
# ---------------------------------------------------------------------------

# First case number of each Federal Cases volume 1-30, from the volumes' own
# title pages ("BOOK 18 ... Case No. 10,121—Case No. 10,847"); the last
# volume runs through 18,313 (its appendix of late-found cases included).
_VOLUME_STARTS = (
    1, 565, 1195, 1799, 2375, 2954, 3583, 4131, 4761, 5240,
    5806, 6394, 7010, 7561, 8125, 8735, 9418, 10121, 10848, 11439,
    12137, 12806, 13390, 14078, 14692, 15244, 15820, 16426, 17060, 17747,
)
MAX_CASE_NO = 18313

#: (volume, first case number, last case number) for every volume.
VOLUME_RANGES = tuple(
    (i + 1, start,
     (_VOLUME_STARTS[i + 1] - 1 if i + 1 < len(_VOLUME_STARTS)
      else MAX_CASE_NO))
    for i, start in enumerate(_VOLUME_STARTS)
)


def number_key(no: "str | int") -> "int | None":
    """The numeric part of a case number ("6082a" -> 6082), or None when it
    isn't a possible Federal Cases number."""
    m = re.fullmatch(r"(\d{1,5})[a-z]?", str(no or "").strip())
    if not m:
        return None
    n = int(m.group(1))
    return n if 1 <= n <= MAX_CASE_NO else None


def expected_volume(no: "str | int") -> "int | None":
    """The Federal Cases volume that holds case number *no* (18 for the
    Nestor's 10,126), or None for an impossible number."""
    n = number_key(no)
    if n is None:
        return None
    return bisect_right(_VOLUME_STARTS, n)


def plausible_volume(no: "str | int", vol: "str | int") -> bool:
    """True when an "F. Cas." citation's *vol* is the volume the case-number
    table assigns to *no* — the alphabetical-order sanity check for a
    candidate found by name search."""
    try:
        v = int(str(vol).strip())
    except (TypeError, ValueError):
        return False
    return expected_volume(no) == v


def pretty_number(no: str) -> str:
    """'10126' -> '10,126' (any letter suffix kept: '6082a' -> '6,082a')."""
    m = re.fullmatch(r"(\d+)([a-z]?)", str(no or ""))
    if not m:
        return str(no or "")
    return f"{int(m.group(1)):,}{m.group(2)}"


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

# The number itself: either the thousands-separated form the reporter prints
# — where OCR may turn the comma into a period and/or push a hyphen into the
# digits ("2,976", "2,-976", "7.030", "2, 976") — or a plain run of digits
# (numbers under 1,000, and OCR that dropped the separator), plus an optional
# letter suffix ("6,082a").  The lookahead refuses a docket-number tail
# ("Case No. 2:13-cv-7779", "Case No. 12-6371"), whose next character run
# would continue with more digits.
_NO_BLOB = (r"(\d{1,2}\s?[.,]\s?-?\s?\d{3}|\d{1,5})([a-z])?\b"
            r"(?!\s?[:\-–—]\s?\d)")

# "Case No. 10,126" (OCR may slip punctuation between the words: "Case; No.
# 14.074"); "Case Nos." lists match their first number.
_CASE_NO_RE = re.compile(r"\bCase\W{0,2}Nos?\.\s*" + _NO_BLOB)

# Old-style "Id. 2,717" — the chained repetition of "Case No.".  Whether an
# "Id." belongs to this pass at all is decided by the chain logic below; the
# modern pin form ("Id. at 152") has no digits directly after the period and
# never matches.
_ID_NO_RE = re.compile(r"\b[Ii]d\.\s*" + _NO_BLOB)

# An "Id. <number>" continues the last Federal Cases citation only across a
# short gap containing no other citation: a digit-then-capital run in the gap
# is another citation's tail ("9 Wall. (76 U. S.) 136; The Maggie Hammond v.
# Morland, Id. 450" — that Id. is 9 Wall. 450, not a case number).
_CHAIN_GAP = 200
_CHAIN_BREAK_RE = re.compile(r"\d\s+[A-Z]")

# The case name printed just before the number: "Cole v. The Atlantic",
# "In re Meyer", or the in-rem vessel form "The Chusan" / "the Johns Walls,
# Jr.".  Searched right-anchored against the text preceding the match, so
# the lazy bodies stop at the nearest name-shaped run; digits are excluded
# so a body can never reach back across an earlier citation's number.
_NAME_BODY = (
    r"(?:[A-Z][A-Za-z.,'’&() -]{0,60}?\s+v\.?\s+[A-Z(][A-Za-z.,'’&() -]{0,60}?"
    r"|(?:In\s+re|Ex\s+parte)\s+[A-Z][A-Za-z.,'’&() -]{0,60}?"
    r"|[Tt]he\s+[A-Z][A-Za-z.'’ -]{0,50}?(?:,\s?[JS]r\.?)?)"
)
_NAME_BEFORE_RE = re.compile(_NAME_BODY + r"\s*[,.:]?\s*\[?\s*$")

# Citation-signal and citator boilerplate a name grab may drag along
# ("Cited in Phelps v. The Camilla", "See Davis v. A New Brig").
_NAME_SIGNAL_RE = re.compile(
    r"^(?:But\s+see|See\s+also|See|Accord|Cf\.?|Compare|Contra|Citing|"
    r"Quoting|Cited\s+in|Followed\s+in|Approved\s+in|Explained\s+in|"
    r"Distinguished\s+in|Questioned\s+in|Limited\s+in|Overruled\s+in|"
    r"Criticised\s+in|Criticized\s+in|Reversed\s+in|Affirmed\s+in|"
    r"S\.\s?P\.|E\.g\.?|And|Also)\W+\s*", re.IGNORECASE)

# A "name" that swallowed a reporter citation is search junk, not a name.
_NAME_CITE_RE = re.compile(r"\d\s+[A-Z][\w.]*\.?\s+\d")


def _name_before(text: str, start: int) -> str:
    """The case name printed just before position *start*, cleaned for a
    search query, or "" when nothing name-shaped is there."""
    window = re.sub(r"\s+", " ", text[max(0, start - 90):start])
    m = _NAME_BEFORE_RE.search(window)
    if not m:
        return ""
    name = m.group(0).strip(" ,.:[")
    while True:
        sm = _NAME_SIGNAL_RE.match(name)
        if not sm:
            break
        name = name[sm.end():]
    name = name.strip(" ,.:;[")
    if len(name) < 4 or _NAME_CITE_RE.search(name):
        return ""
    return name


def iter_cites(text: str) -> "list[tuple[int, int, str]]":
    """Every Federal Cases case-number citation in *text*, as ``(start, end,
    spec)`` in document order.  ``spec`` is a JSON string with ``no`` (the
    normalized number, "2976" / "6082a") and, when one was printed, ``name``
    — the fields ``courtlistener.find_fedcas_case`` resolves at click time.

    A "Case No. <n>" always cites; an "Id. <n>" cites only while chained to
    a preceding Federal Cases citation with no other citation between (see
    :data:`_CHAIN_BREAK_RE`), so a "Id. 450" following "9 Wall. 136" is
    left for the reporter passes."""
    if not text:
        return []
    ms = sorted(
        [(m, False) for m in _CASE_NO_RE.finditer(text)]
        + [(m, True) for m in _ID_NO_RE.finditer(text)],
        key=lambda t: t[0].start(),
    )
    out: list[tuple[int, int, str]] = []
    chain_end: "int | None" = None      # end of the last cite, chain alive
    for m, is_id in ms:
        if is_id:
            if chain_end is None or m.start() < chain_end:
                continue
            gap = text[chain_end:m.start()]
            if len(gap) > _CHAIN_GAP or _CHAIN_BREAK_RE.search(gap):
                chain_end = None
                continue
        digits = re.sub(r"\D", "", m.group(1))
        no = (str(int(digits)) if digits else "") + (m.group(2) or "")
        if number_key(no) is None:
            chain_end = None            # unreadable number breaks the chain
            continue
        fields: dict = {"no": no}
        name = _name_before(text, m.start())
        if name:
            fields["name"] = name
        out.append((m.start(), m.end(), json.dumps(fields)))
        chain_end = m.end()
    return out


# ---------------------------------------------------------------------------
# Offline self-test:  python -X utf8 fed_cas.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    # --- volume arithmetic (anchors: the Nestor itself is 18 F. Cas. 9) ---
    for no, vol in [("10126", 18), ("1", 1), ("564", 1), ("565", 2),
                    ("2976", 6), ("2717", 5), ("18313", 30), ("6082a", 11),
                    ("409", 1), ("17746", 29), ("17747", 30)]:
        check(expected_volume(no) == vol, f"No. {no} -> vol {vol} "
                                          f"(got {expected_volume(no)})")
    check(plausible_volume("10126", "18"), "plausible_volume exact")
    check(not plausible_volume("10126", 17), "wrong volume rejected")
    check(expected_volume("99999") is None, "impossible number -> None")
    check(pretty_number("6082a") == "6,082a", "pretty_number suffix")
    check(pretty_number("199") == "199", "pretty_number small")

    def cites(text):
        return [(json.loads(spec), text[s:e])
                for s, e, spec in iter_cites(text)]

    # --- detection: the exact OCR forms in The Nestor, 18 F. Cas. 9 ---
    nestor = (
        "[Cited in Cole v. The Atlantic, Case No. 2,-976: The Chnsan. "
        "Id. 2,717; Leland v. The Medora, Id. 8,237: Macy v. De Wolf, "
        "Id. 8,933; The Alida, Id. 199; The Infanta, Id. 7.030; The Young "
        "Mechanic, Id. 18.180; The Lulu, Id. 8,604; The Grapeshot v. "
        "Wal-lerstein, 9 Wall. (76 U. S.) 136; The Maggie Hammond v. "
        "Morland, Id. 450; The Avon, Case No. 680; Rodd v. Heartt, 21 "
        "Wall. (88 U. S.)-597; The Albany, Case No. 131; The General "
        "Burnside, 3 Fed. ’231; The Richard Busteed, Case No. 11,764; "
        "The, Canada, ,7 Fed. 121; Stephenson v. The Francis, 21 Fed. "
        "717.]"
    )
    got = cites(nestor)
    want = [("2976", "Cole v. The Atlantic"), ("2717", "The Chnsan"),
            ("8237", "Leland v. The Medora"), ("8933", "Macy v. De Wolf"),
            ("199", "The Alida"), ("7030", "The Infanta"),
            ("18180", "The Young Mechanic"), ("8604", "The Lulu"),
            ("680", "The Avon"), ("131", "The Albany"),
            ("11764", "The Richard Busteed")]
    check(len(got) == len(want),
          f"Nestor headnote 1 yields {len(want)} cites (got {len(got)}: "
          f"{[(f['no'], f.get('name')) for f, _t in got]})")
    for (no, name), (fields, _txt) in zip(want, got):
        check(fields["no"] == no and fields.get("name") == name,
              f"No. {no} named {name!r} "
              f"(got {fields['no']!r} {fields.get('name')!r})")
    # "Id. 450" after the intervening "9 Wall." citation must NOT be claimed
    # — it is 9 Wall. 450, and the chain restarts at the next "Case No.".
    check(not any(f["no"] == "450" for f, _t in got),
          "Id. 450 after 9 Wall. stays a reporter cite")

    # Bracketed forms, and the chain running through them.
    brig = ("See Davis v. A New Brig [Case No. 3,643]; Harper v. A New "
            "Brig [Id. 6,090]; Read v. The Hull of a New Brig [Id. "
            "11,609]; The Marion [Id. 9,087].")
    got = cites(brig)
    check([f["no"] for f, _t in got] == ["3643", "6090", "11609", "9087"],
          f"bracketed chain (got {[f['no'] for f, _t in got]})")
    check(got[3][0].get("name") == "The Marion", "vessel name in brackets")

    # OCR'd "Case; No. 14.074", suffixed "6,082a", lowercase vessel "the".
    more = cites("Cited in Todd v. The Euphrates. Case; No. 14.074. and "
                 "later in Har-ney v. The Sydney L. Wright, Case No. "
                 "6,082a; see also the Johns Walls, Jr., Case No. 7,432.")
    check([f["no"] for f, _t in more] == ["14074", "6082a", "7432"],
          f"OCR forms (got {[f['no'] for f, _t in more]})")
    check(more[0][0].get("name") == "Todd v. The Euphrates",
          f"name through OCR'd Case; No. (got {more[0][0].get('name')!r})")
    check(more[1][0].get("name") == "Har-ney v. The Sydney L. Wright",
          "hyphenated OCR name kept as printed")
    check(more[2][0].get("name") == "the Johns Walls, Jr",
          f"lowercase vessel name (got {more[2][0].get('name')!r})")

    # A followed-by-Id. name after a *different* case's number: the name
    # belongs to the Id.'s own citation, not the antecedent's.
    pratt = cites("Followed in The Chusan, Case No. 2,716, and cited in "
                  "The Ann C. Pratt. Id. 409. to the point that the lien "
                  "created by the maritime law may be waived.")
    check([(f["no"], f.get("name")) for f, _t in pratt]
          == [("2716", "The Chusan"), ("409", "The Ann C. Pratt")],
          f"Id. carries its own name (got {[(f['no'], f.get('name')) for f, _t in pratt]})")

    # An "Id." with no Federal Cases antecedent is never claimed; nor is a
    # modern pin "Id. at 152"; nor modern dockets styled "Case No.".
    check(cites("See Roe v. Wade, 410 U.S. 113. Id. 450.") == [],
          "Id. without a Case No. antecedent stays unclaimed")
    check(cites("Case No. 2,976. Id. at 152.") and
          len(cites("Case No. 2,976. Id. at 152.")) == 1,
          "modern pin Id. at N never matches")
    for docket in ("Case No. 2:13-cv-7779", "Case No. 12-6371",
                   "Case No. 21-55079, 2022 WL 123"):
        check(cites(docket) == [], f"docket {docket!r} not claimed")

    # The self-referential headnote number links too (harmlessly).
    check([f["no"] for f, _t in cites("Case No. 10,126.")] == ["10126"],
          "bare headnote number")

    print("\n" + ("all tests passed" if not failed
                  else f"{failed} checks FAILED"))
    raise SystemExit(1 if failed else 0)
