"""Detect citations to the United States Statutes at Large ("<vol> Stat.
<page>", e.g. "88 Stat. 1932") and link them to the free official scan.

GovInfo has digitized the **complete** Statutes at Large and exposes a link
service that resolves a volume/page straight to the PDF, opened to the cited
page:

    https://www.govinfo.gov/link/statute/<volume>/<page>
        → https://www.govinfo.gov/content/pkg/STATUTE-<v>/pdf/STATUTE-<v>-Pg<p>.pdf#page=N

Coverage runs from volume 1 (1789) up to the most recent published volume; a
citation outside that range (a not-yet-published recent volume) yields no link
rather than a dead one.  This is link-out only — the GUI opens the URL with a
``("browse", url)`` action — so there is no HTML parser here.
"""

from __future__ import annotations

import re

# GovInfo's published-volume ceiling.  Bump this as new annual volumes appear
# on GovInfo (vol 137 = 2023; 138 was not yet posted as of this writing).
STAT_MAX_VOL = 137

# "<volume> Stat. <page>" — a number immediately before "Stat." (so it can't be
# a state code like "Minn. Stat.") and a bare page number after (so it can't be
# "Stat. §" of a state code).  The period is required to avoid matching words
# like "status".
STAT_CITE_RE = re.compile(r"\b(\d{1,3})\s+Stat\.\s+(\d{1,5})\b")


def cite_label(m: re.Match) -> str:
    """Human label for a match, e.g. '88 Stat. 1932'."""
    return f"{m.group(1)} Stat. {m.group(2)}"


def url_for(m: re.Match) -> str | None:
    """GovInfo link-service URL for a Statutes at Large match, or None if the
    volume is outside GovInfo's published range (so we don't make a dead link)."""
    vol, page = int(m.group(1)), int(m.group(2))
    if not (1 <= vol <= STAT_MAX_VOL) or page < 1:
        return None
    return f"https://www.govinfo.gov/link/statute/{vol}/{page}"


if __name__ == "__main__":
    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    def first(text: str):
        return STAT_CITE_RE.search(text)

    # --- detection: real Statutes at Large cites ---
    for text, vol, page in [
        ("88 Stat. 1932", "88", "1932"),
        ("Pub. L. No. 93-595, 88 Stat. 1926", "88", "1926"),
        ("124 Stat. 119 (2010)", "124", "119"),
        ("see 1 Stat. 112", "1", "112"),
        ("ch. 20, 60 Stat. 237", "60", "237"),
        ("act of June 25, 1948, 62 Stat. 869, 928", "62", "869"),  # page, not pincite
    ]:
        m = first(text)
        got = (m.group(1), m.group(2)) if m else None
        check(got == (vol, page), f"{text!r} -> {got!r}")

    # --- must NOT match (state codes / words / federal Revised Statutes) ---
    for text in [
        "Minn. Stat. § 609.02",        # state code: word before Stat., § after
        "42 Pa. Cons. Stat. § 9711",   # "Cons. Stat." (word before), and §
        "Rev. Stat. § 1979",           # federal Revised Statutes, no volume
        "the status of the case",      # 'status', no period+digits
        "Fla. Stat. 776.012",          # state code (no number before Stat.)
    ]:
        m = first(text)
        check(m is None, f"no match in {text!r} (got {m.group(0) if m else None!r})")

    # --- URL building + coverage ceiling ---
    check(url_for(first("88 Stat. 1932"))
          == "https://www.govinfo.gov/link/statute/88/1932", "url 88/1932")
    check(url_for(first("1 Stat. 112"))
          == "https://www.govinfo.gov/link/statute/1/112", "url 1/112 (floor)")
    check(url_for(first(f"{STAT_MAX_VOL} Stat. 100")) is not None,
          "url at ceiling volume")
    check(url_for(first(f"{STAT_MAX_VOL + 1} Stat. 100")) is None,
          "no url past ceiling (not yet published)")
    check(cite_label(first("124 Stat. 119")) == "124 Stat. 119", "label")

    raise SystemExit(1 if failed else 0)
