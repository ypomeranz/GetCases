"""Detect Federal Register citations and build official GovInfo PDF links.

The Federal Register is cited by volume and page (for example, ``88 Fed. Reg.
382`` or the publication's own compact ``88 FR 382`` form). GovInfo's link
service resolves that pair to the official PDF containing the cited page:

    https://www.govinfo.gov/link/fr/<volume>/<page>?link-type=pdf

Keeping this module free of GUI dependencies lets opinion text, C.F.R. text,
PDF text layers, and Spotlight share exactly the same parser.
"""

from __future__ import annotations

import re


_PAGE = r"(?:\d{1,3}(?:,\d{3})*|\d{1,6})"
_REPORTER = r"(?:F\.?\s*R\.?|Fed(?:eral)?\.?\s*Reg(?:ister)?\.?)"

# Printed page ranges are included in the clickable span, while group 2 stays
# the first page so GovInfo opens at the beginning of the cited material.
FR_CITE_RE = re.compile(
    rf"\b(\d{{1,3}})\s+{_REPORTER}\s+({_PAGE})"
    rf"(?:\s*[-–—]\s*{_PAGE})?\b",
    re.IGNORECASE,
)


def _number(value: str) -> int:
    return int((value or "").replace(",", ""))


def cite_label(m: re.Match) -> str:
    """Canonical display label for a citation match."""
    return f"{_number(m.group(1))} Fed. Reg. {_number(m.group(2)):,}"


def url_for(m: re.Match) -> str | None:
    """Official GovInfo PDF link for *m*, or ``None`` for invalid numbers."""
    volume, page = _number(m.group(1)), _number(m.group(2))
    if volume < 1 or page < 1:
        return None
    return (
        f"https://www.govinfo.gov/link/fr/{volume}/{page}"
        "?link-type=pdf"
    )


def parse_query(query: str) -> tuple[str, str] | None:
    """Parse a standalone Spotlight query into a Federal Register action."""
    text = (query or "").strip()
    if text.endswith("."):
        text = text[:-1].rstrip()
    match = FR_CITE_RE.fullmatch(text)
    url = url_for(match) if match else None
    return ("frpdf", url) if url else None


if __name__ == "__main__":
    failed = 0

    def check(condition: bool, label: str) -> None:
        global failed
        failed += not condition
        print(("ok   " if condition else "FAIL ") + label)

    for text, volume, page in [
        ("57 FR 12146", 57, 12146),
        ("88 Fed. Reg. 382", 88, 382),
        ("77 Fed.Reg. 36,150–36,152", 77, 36150),
        ("91 Federal Register 123", 91, 123),
    ]:
        match = FR_CITE_RE.search(text)
        got = (
            (_number(match.group(1)), _number(match.group(2)))
            if match else None
        )
        check(got == (volume, page), f"{text!r} -> {got!r}")

    for text in [
        "Fed. R. Civ. P. 56",
        "123 F.R.D. 456",
        "Minn. Stat. § 609.02",
        "the Federal Register",
    ]:
        check(FR_CITE_RE.search(text) is None, f"no match in {text!r}")

    match = FR_CITE_RE.search("88 Fed. Reg. 382")
    check(
        url_for(match)
        == "https://www.govinfo.gov/link/fr/88/382?link-type=pdf",
        "GovInfo URL",
    )
    check(cite_label(match) == "88 Fed. Reg. 382", "canonical label")
    check(
        parse_query("77 FR 36,150.")
        == (
            "frpdf",
            "https://www.govinfo.gov/link/fr/77/36150?link-type=pdf",
        ),
        "Spotlight query",
    )

    raise SystemExit(1 if failed else 0)
