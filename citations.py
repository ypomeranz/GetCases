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

import re

import constitution
import ecfr
import fed_rules
import state_statutes
import statutes_at_large
import us_code

# A pinpoint page following a case citation: ", 171" or ", 171-72" — but not
# the volume of a parallel citation (", 510 A.2d 562"), recognized by the
# capital letter that follows the number.
PINCITE_AFTER_RE = re.compile(
    r",\s*(\d{1,5})(?:\s*[-–—]\s*\d{1,5})?(?!\d|\s*[A-Z])"
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

# Capturing form (volume, reporter, page) — used to index every full citation
# in a document so short forms can be resolved back to it.
CITE_CAPTURE_RE = re.compile(
    r"\b(\d{1,4})\s+(" + REPORTER_ALT + r")\s+(\d{1,5})\b")

# Short-form citation: "Roe, 410 U.S., at 152" → volume, reporter, pin page.
SHORT_CITE_RE = re.compile(
    r"\b(\d{1,4})\s+(" + REPORTER_ALT + r")\s*,?\s+at\s+(\d{1,5})\b")

# "Id." short form — refers to the immediately preceding citation; group 1 is
# the optional pin page ("Id. at 152").  ("Ibid." is deliberately not traced —
# it usually points at a non-case source.)
ID_CITE_RE = re.compile(r"\b[Ii]d\.(?:\s*,?\s*at\s+(\d{1,5}))?")


def norm_reporter(rep: str) -> str:
    """Reporter key for matching, ignoring spacing/case ('U. S.' == 'U.S.')."""
    return re.sub(r"\s+", "", rep or "").lower()


def build_short_cite_index(text: str) -> dict[tuple[str, str], list[int]]:
    """Map (volume, reporter) → sorted first-pages of every full citation in
    `text`, so a short form ('410 U.S. at 152') can be resolved to the case's
    first page (and thence opened and pin-jumped)."""
    idx: dict[tuple[str, str], set] = {}
    for m in CITE_CAPTURE_RE.finditer(text or ""):
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
    cm = CITE_CAPTURE_RE.search(text)
    if cm:
        base = re.sub(r"\s+", " ", cm.group(0)).replace("U. S.", "U.S.")
        pm = PINCITE_AFTER_RE.match(text, cm.end())
        return base, (pm.group(1) if pm else "")
    sm = SHORT_CITE_RE.search(text)
    if sm:
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
# Whole-document detection (used by the brief viewer)
# ---------------------------------------------------------------------------

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
    for m in TEXT_CITE_RE.finditer(text):
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
    for m in SHORT_CITE_RE.finditer(text):
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
        matches.append((c.start, c.end, "statestat", c))
    for m in statutes_at_large.STAT_CITE_RE.finditer(text):
        if statutes_at_large.url_for(m):  # only link volumes GovInfo has
            matches.append((m.start(), m.end(), "stat", m))

    matches.sort(key=lambda t: (t[0], -t[1]))
    out: list[tuple[int, int, tuple[str, str]]] = []
    pos = 0
    last_cite_action: tuple[str, str] | None = None
    for start, end, kind, m in matches:
        if start < pos:
            continue  # overlapping match — first/longest wins
        action: tuple[str, str] | None
        cite_base = ""
        if kind == "cite":
            cite = re.sub(r"\s+", " ", m.group(0)).replace("U. S.", "U.S.")
            cite = cite.replace("’", "'")
            cite_base = cite
            pin_m = PINCITE_AFTER_RE.match(text, end)
            if pin_m:
                cite += "@" + pin_m.group(1)
            action = ("cite", cite)
        elif kind == "usc":
            action = ("usc", us_code.cite_spec(m))
        elif kind == "cfr":
            action = ("cfr", ecfr.cite_spec(m))
        elif kind == "rule":
            action = ("rule", fed_rules.cite_spec(m))
        elif kind == "const":
            action = ("const", constitution.cite_spec(m))
        elif kind == "shortcite":
            action = ("cite", m)  # m is the pre-built "vol rep page@pin"
            cite_base = m.split("@")[0]
        elif kind == "idcite":
            la = last_cite_action
            if not la:
                action = None
            elif la[0] == "cite":
                pin = m.group(1)
                action = ("cite", la[1] + (f"@{pin}" if pin else ""))
            else:
                action = la
        elif kind == "statestat":
            action = state_statutes.action_for(m)
        elif kind == "stat":
            action = ("statpdf", statutes_at_large.url_for(m))
        else:  # pragma: no cover - defensive
            action = None
        if action is not None:
            out.append((start, end, action))
            if kind in ("cite", "shortcite"):
                last_cite_action = ("cite", cite_base)
            else:
                last_cite_action = action
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
    print("\nOK:", len(found), "links;", sorted(kinds))
