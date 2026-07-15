"""Pure helpers for user-edited base case citations.

The GUI persists the returned mapping in its ordinary local config file.  This
module deliberately has no tkinter dependency so identity matching and pin-cite
insertion can be regression-tested headlessly.
"""

from __future__ import annotations

import re
import urllib.parse
from typing import Iterable

import citations


def clean_base_citation(value: str) -> str:
    """Whitespace-normalized base citation with an optional final full stop
    removed.  Internal punctuation (including entity abbreviations) is kept.
    """
    value = re.sub(r"\s+", " ", value or "").strip()
    if value.endswith(")."):
        value = value[:-1]
    elif re.search(r"\d\.$", value):
        value = value[:-1]
    return value


def _reporter_key(value: str) -> str:
    match = citations.CITE_CAPTURE_RE.search(value or "")
    if not match:
        return ""
    reporter = citations.norm_reporter(match.group(2))
    return f"cite:{int(match.group(1))}:{reporter}:{int(match.group(3))}"


def citation_identity_keys(
    item: dict | None,
    primary_cite: str = "",
    parallel_cites: Iterable[str] = (),
    scholar_url: str = "",
) -> list[str]:
    """Stable local keys for one opinion, strongest first.

    Every known reporter citation is included, so an override saved while the
    regional reporter is selected is also found when the same case is later
    opened through its official or alternate reporter.
    """
    item = item or {}
    out: list[str] = []

    cluster = item.get("cluster_id") or item.get("id")
    if cluster:
        out.append(f"cl:{cluster}")

    values: list[str] = [primary_cite]
    values.extend(str(v) for v in parallel_cites or ())
    values.extend(str(v) for v in (item.get("citation") or ()))
    for value in values:
        key = _reporter_key(value)
        if key and key not in out:
            out.append(key)

    try:
        case_id = (urllib.parse.parse_qs(
            urllib.parse.urlparse(scholar_url or "").query
        ).get("case") or [""])[0].strip()
    except Exception:
        case_id = ""
    if case_id:
        out.append(f"scholar:{case_id}")
    return out


def find_override(saved: object, keys: Iterable[str]) -> str:
    """First nonempty override associated with any identity key."""
    if not isinstance(saved, dict):
        return ""
    for key in keys:
        value = clean_base_citation(str(saved.get(key) or ""))
        if value:
            return value
    return ""


def update_overrides(
    saved: object, keys: Iterable[str], base_citation: str
) -> dict[str, str]:
    """Return an updated override map.  An empty citation removes the known
    keys and restores automatic Bluebooking for the opinion.
    """
    out = dict(saved) if isinstance(saved, dict) else {}
    value = clean_base_citation(base_citation)
    for key in keys:
        if value:
            out[key] = value
        else:
            out.pop(key, None)
    return out


def add_pin_to_base(base_citation: str, pin: str | None) -> str:
    """Insert *pin* after the first reporter citation in a user-edited base.

    ``Deslandes v. McDonald's USA, LLC, 81 F.4th 699 (7th Cir. 2023)`` thus
    receives ``, 703`` before its court/year parenthetical.  A pin equal to the
    reporter's first page is omitted, matching the automatic citation path.
    """
    base = clean_base_citation(base_citation)
    pin = (pin or "").strip()
    if not pin:
        return base
    match = citations.CITE_CAPTURE_RE.search(base)
    if not match or pin == match.group(3):
        return base
    return base[:match.end()] + f", {pin}" + base[match.end():]


def split_name_from_citation(base_citation: str) -> tuple[str, str]:
    """Split a case name from the roman remainder for rich-text italics."""
    base = clean_base_citation(base_citation)
    match = citations.CITE_CAPTURE_RE.search(base)
    if not match:
        return base, ""
    prefix = base[:match.start()]
    name = prefix.rstrip(" ,")
    return name, base[len(name):]


def format_edited_citation(
    base_citation: str,
    pin: str | None = None,
    suffix_parentheticals: Iterable[str] = (),
) -> tuple[str, str]:
    """Plain citation and its name/rest split after pin and writer notes."""
    value = add_pin_to_base(base_citation, pin)
    for parenthetical in suffix_parentheticals:
        parenthetical = (parenthetical or "").strip()
        if parenthetical:
            value += f" ({parenthetical})"
    value += "."
    name, rest = split_name_from_citation(value)
    return value, name

