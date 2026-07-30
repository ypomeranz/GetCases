"""Filtered Supreme Court docket briefs from the Court and SCOTUSblog.

The Court's public docket is authoritative for modern electronic filings and
usually exposes a separate petition appendix.  SCOTUSblog mirrors those modern
dockets and also preserves older case-file pages containing merits and cert
briefs.  This module combines both sources, removes duplicate links, and keeps
only the filings useful to an opinion reader:

* cert petition (or jurisdictional statement) and its appendix;
* brief in opposition/response, cert reply, and cert-stage amicus briefs;
* joint appendix and party, reply, and amicus briefs on the merits.

It deliberately omits certificates, proofs of service, extension motions,
waivers, orders, transcripts, records, and other docket administration.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import quote_plus, unquote, urlencode, urljoin, urlsplit

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "scotus_docket requires 'requests' and 'beautifulsoup4'."
    ) from exc


COURT_DOCKET_URL = (
    "https://www.supremecourt.gov/docket/docketfiles/html/public/{docket}.html"
)
SCOTUSBLOG_SEARCH_URL = (
    "https://www.scotusblog.com/search/?q={query}&tab=cases"
)
SCOTUSBLOG_ROOT = "https://www.scotusblog.com/"
# SCOTUSblog's browser search is backed by a public, search-only Typesense key
# shipped in its client bundle.  The normal HTML search page is retained as a
# fallback in case SCOTUSblog moves search back to server-rendered results.
SCOTUSBLOG_TYPESENSE_HOST = "xae1s5dwogtni6f9p.a1.typesense.net"
SCOTUSBLOG_TYPESENSE_KEY = "zdhnHhyXyCBZHvm2UqvABAXXflePyMzq"
SCOTUSBLOG_CASE_QUERY_FIELDS = (
    "title,docket_number,docket_number_compact,petitioner,respondent,"
    "holding,issue,summary"
)
BOOKLET_COLOR_CHART_URL = (
    "https://www.supremecourt.gov/casehand/BookletFormatSpecificChart2026.pdf"
)

# The current SCOTUSblog palette.  Its legend links to the Supreme Court's
# booklet-format chart and applies these colors to the corresponding filings.
COVER_COLORS = {
    "white": "#ffffff",
    "orange": "#ff8432",
    "tan": "#dbc3a3",
    "cream": "#ffffd6",
    "light_blue": "#add6d6",
    "light_red": "#da6b6b",
    "yellow": "#ffffad",
    "light_green": "#add6ad",
    "dark_green": "#32ad84",
    "gray": "#dddddd",
}

_TIMEOUT = 25
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) "
    "Gecko/20100101 Firefox/144.0"
)
_DASH_RE = re.compile(r"[\u2010-\u2015\u2212]")
_DOCKET_RE = re.compile(
    r"\b(?:\d{1,3}-\d{1,5}|\d{1,3}[A-Z]\d{1,5})\b", re.IGNORECASE
)
_DATE_RE = re.compile(
    r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    r"[a-z]*\s+\d{1,2},?\s+\d{4}$",
    re.IGNORECASE,
)
_EXCLUDED_LINK_RE = re.compile(
    r"certificate|proof of service|blanket consent|waiver|cover letter",
    re.IGNORECASE,
)
_GRANT_RE = re.compile(
    r"\b(?:petition (?:is )?granted|certiorari granted|"
    r"probable jurisdiction (?:is )?noted|set for (?:oral )?argument|"
    r"postponed to (?:the )?hearing on the merits)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DocketDocument:
    """One brief or appendix retained from a case docket."""

    stage: str
    kind: str
    label: str
    url: str
    cover: str
    date: str = ""
    docket: str = ""
    source: str = ""


@dataclass
class CaseDocket:
    """The filtered filings and full-docket source links for one opinion."""

    documents: list[DocketDocument]
    dockets: tuple[str, ...]
    official_urls: tuple[str, ...]
    scotusblog_url: str = ""


def _clean(value: str) -> str:
    return re.sub(
        r"\s+", " ", (value or "").replace("\xa0", " ")
    ).strip()


def normalize_docket(value: str) -> str:
    """Return one docket token using the Court site's URL spelling."""

    text = re.sub(r"\s+", "", _DASH_RE.sub("-", str(value or ""))).upper()
    match = _DOCKET_RE.search(text)
    return match.group(0).upper() if match else ""


def docket_tokens(values: str | Iterable[str]) -> tuple[str, ...]:
    """Extract distinct Supreme Court docket numbers, preserving input order."""

    if isinstance(values, str):
        values = (values,)
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _DASH_RE.sub("-", str(value or "")).upper()
        for match in _DOCKET_RE.finditer(normalized):
            token = normalize_docket(match.group(0))
            if token and token not in seen:
                seen.add(token)
                out.append(token)
    return tuple(out)


def official_docket_url(docket: str) -> str:
    token = normalize_docket(docket)
    return COURT_DOCKET_URL.format(docket=token) if token else ""


def _label(description: str, *, appendix: bool = False) -> str:
    text = _clean(description)
    text = re.sub(
        r"\s*\((?:Response due|Distributed|Rescheduled|Due)\b[^)]*\)\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\s+filed\.?\s*$", "", text, flags=re.IGNORECASE)
    motion = re.fullmatch(
        r"Motion for leave to file (?:an? )?amicus brief filed by (.+?)\.?",
        text,
        flags=re.IGNORECASE,
    )
    if motion:
        text = f"Amicus brief of {motion.group(1)}"
    if appendix:
        if re.search(r"jurisdictional statement", text, re.IGNORECASE):
            return "Appendix to jurisdictional statement"
        return "Appendix to petition for a writ of certiorari"
    return text.rstrip(".") or "Docket document"


def _stage_from_text(text: str, current: str) -> str:
    low = text.casefold()
    if (
        "on the merits" in low
        or "joint appendix" in low
        or "merits brief" in low
        or "briefs on the merits" in low
    ):
        return "merits"
    return current


def _support_side(text: str, side_hint: str = "") -> str:
    low = f"{text} {side_hint}".casefold()
    if re.search(r"\b(?:respondent|appellee)s?\b", low):
        return "respondent"
    if re.search(r"\b(?:petitioner|appellant)s?\b", low):
        return "petitioner"
    return "neither"


def _classify(
    description: str,
    *,
    stage: str,
    link_text: str = "",
    section: str = "",
    side_hint: str = "",
    merits_phase: str = "",
) -> Optional[tuple[str, str, str]]:
    """Return ``(stage, kind, cover)`` or ``None`` for an unwanted filing."""

    text = _clean(f"{description} {link_text} {section}")
    low = text.casefold()
    stage = _stage_from_text(text, stage)
    section_low = section.casefold()
    is_amicus = (
        bool(re.search(r"\bamic(?:us|i)\b", low))
        or "amicus brief" in section_low
    )
    procedural = any(phrase in low for phrase in (
        "extend the time",
        "extension of time",
        "waiver of",
        "blanket consent",
        "certificate of",
        "proof of service",
        "distributed for conference",
        "application to extend",
        "not accepted for filing",
        "duplicate submission",
        "returned to counsel",
    )) or ("application (" in low and "petition" in low)
    includes_amicus_brief = "motion for leave to file amicus brief" in low
    rejected = any(phrase in low for phrase in (
        "not accepted for filing",
        "duplicate submission",
        "returned to counsel",
    ))
    if rejected or (procedural and not includes_amicus_brief):
        return None

    if stage == "cert":
        if is_amicus:
            return ("cert", "cert_amicus", "cream")
        is_appendix = (
            "appendix" in link_text.casefold()
            or (
                section_low in ("cert stage", "petition stage")
                and "appendix" in description.casefold()
            )
        )
        if is_appendix and (
            "petition" in description.casefold()
            or "jurisdictional statement" in description.casefold()
            or section_low in ("cert stage", "petition stage")
        ):
            return ("cert", "cert_appendix", "white")
        if "reply" in low:
            return ("cert", "cert_reply", "tan")
        if any(phrase in low for phrase in (
            "brief in opposition",
            "in opposition",
            "response to petition",
            "response to the petition",
            "respondent's brief in support of certiorari",
            "respondent’s brief in support of certiorari",
            "motion to dismiss or affirm",
            "motion to affirm",
        )):
            return ("cert", "cert_opposition", "orange")
        if any(phrase in low for phrase in (
            "petition for a writ of certiorari",
            "petition for writ of certiorari",
            "petition for an extraordinary writ",
            "jurisdictional statement",
        )):
            return ("cert", "cert_petition", "white")
        return None

    if "joint appendix" in low:
        return ("merits", "joint_appendix", "tan")
    if is_amicus:
        side = _support_side(text, side_hint)
        if (
            side == "neither"
            and "neither party" not in low
            and merits_phase in ("petitioner", "respondent")
        ):
            # The Court's docket often omits the supported side from the row.
            # Its filing sequence still distinguishes the two merits windows.
            side = merits_phase
        if side == "respondent":
            cover = "dark_green"
            kind = "merits_amicus_respondent"
        else:
            # Rule 33.1(g)(viii) uses light green both for petitioner/appellant
            # and for briefs supporting neither party.
            cover = "light_green"
            kind = (
                "merits_amicus_petitioner"
                if side == "petitioner"
                else "merits_amicus_neither"
            )
        return ("merits", kind, cover)
    if "reply" in low:
        return ("merits", "merits_reply", "yellow")
    if "brief" not in low and "merits brief" not in section_low:
        return None

    side = _support_side(text, side_hint)
    if side == "respondent":
        return ("merits", "merits_respondent", "light_red")
    if side == "petitioner":
        return ("merits", "merits_petitioner", "light_blue")
    if merits_phase == "respondent":
        return ("merits", "merits_respondent", "light_red")
    if merits_phase == "petitioner":
        return ("merits", "merits_petitioner", "light_blue")
    # A true merits-stage brief with no side stated is still responsive to the
    # user's "merits stage briefing" request; tan is the Court's catch-all.
    return ("merits", "merits_other", "tan")


def parse_official_docket(
    html: str, docket: str = ""
) -> list[DocketDocument]:
    """Parse and filter a public ``supremecourt.gov`` docket page."""

    soup = BeautifulSoup(html or "", "html.parser")
    documents: list[DocketDocument] = []
    stage = "cert"
    merits_phase = ""

    for row in soup.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) < 2:
            continue
        date = _clean(cells[0].get_text(" ", strip=True))
        detail = cells[1]
        description_node = BeautifulSoup(str(detail), "html.parser")
        for links in description_node.select(".documentlinks"):
            links.decompose()
        description = _clean(description_node.get_text(" ", strip=True))

        row_stage = _stage_from_text(description, stage)
        low = description.casefold()
        if row_stage == "merits":
            stage = "merits"
        if (
            stage == "merits"
            and re.search(r"\bbrief of (?:the )?(?:petitioner|appellant)", low)
            and "amic" not in low
        ):
            merits_phase = "petitioner"
        elif (
            stage == "merits"
            and re.search(r"\bbrief of (?:the )?(?:respondent|appellee)", low)
            and "amic" not in low
        ):
            merits_phase = "respondent"

        for link in detail.find_all("a", href=True):
            link_name = _clean(link.get_text(" ", strip=True))
            href = urljoin(official_docket_url(docket), link.get("href"))
            if (
                not href.lower().startswith(("http://", "https://"))
                or _EXCLUDED_LINK_RE.search(link_name)
            ):
                continue
            classified = _classify(
                description,
                stage=row_stage,
                link_text=link_name,
                merits_phase=merits_phase,
            )
            if classified is None:
                continue
            doc_stage, kind, cover = classified
            documents.append(DocketDocument(
                stage=doc_stage,
                kind=kind,
                label=_label(
                    description,
                    appendix=(kind == "cert_appendix"),
                ),
                url=href,
                cover=cover,
                date=date,
                docket=normalize_docket(docket),
                source="Supreme Court",
            ))

        # The granting row normally has no document, so it must affect the
        # rows after it rather than the classification of a document in it.
        if _GRANT_RE.search(description):
            stage = "merits"
            merits_phase = ""

    return documents


def find_scotusblog_case_url(
    search_html: str, dockets: str | Iterable[str]
) -> str:
    """Return the exact-docket case result from a SCOTUSblog search page."""

    wanted = set(docket_tokens(dockets))
    if not wanted:
        return ""
    soup = BeautifulSoup(search_html or "", "html.parser")
    for link in soup.find_all("a", href=True):
        href = urljoin(SCOTUSBLOG_ROOT, link.get("href"))
        path = urlsplit(href).path.rstrip("/")
        if not re.fullmatch(r"/cases/(?:case-files/)?[^/]+", path):
            continue
        text = _clean(link.get_text(" ", strip=True))
        if wanted.intersection(docket_tokens(text)):
            return href
    return ""


def find_scotusblog_case_url_json(
    payload: dict, dockets: str | Iterable[str]
) -> str:
    """Return the exact-docket case URL from a Typesense search response."""

    wanted = set(docket_tokens(dockets))
    if not wanted or not isinstance(payload, dict):
        return ""
    for hit in payload.get("hits") or []:
        document = hit.get("document") if isinstance(hit, dict) else {}
        if not isinstance(document, dict):
            continue
        found = set(docket_tokens((
            str(document.get("docket_number") or ""),
            str(document.get("docket_number_compact") or ""),
        )))
        if not wanted.intersection(found):
            continue
        slug = _clean(str(document.get("slug") or "")).strip("/")
        if slug:
            return urljoin(SCOTUSBLOG_ROOT, f"/cases/{slug}/")
    return ""


def _valid_document_url(href: str, base_url: str) -> str:
    url = urljoin(base_url, href or "")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or "." not in (parts.hostname or ""):
        return ""
    if _canonical_url(url) == _canonical_url(base_url):
        return ""
    decoded_path = unquote(parts.path).casefold()
    if any(phrase in decoded_path for phrase in (
        "certificate of compliance",
        "certificate of word count",
        "proof of service",
        "affidavit of service",
    )):
        return ""
    return url


def _timeline_documents(
    soup: BeautifulSoup, base_url: str, docket: str
) -> list[DocketDocument]:
    """Parse SCOTUSblog's current color-coded proceedings timeline."""

    documents: list[DocketDocument] = []
    stage = "cert"
    merits_phase = ""
    timeline_nodes = soup.select('[class*="bg-brief-"]')
    for node in timeline_nodes:
        classes = " ".join(node.get("class") or [])
        if not re.search(r"\bbg-brief-[\w-]+\b", classes):
            continue
        description = ""
        date = ""
        for child in node.find_all(["span", "p"], recursive=True):
            text = _clean(child.get_text(" ", strip=True))
            if not text:
                continue
            if not date and _DATE_RE.fullmatch(text):
                date = text
            elif len(text) > len(description):
                description = text
        if not description:
            description = _clean(node.get_text(" ", strip=True))
            if date and description.startswith(date):
                description = _clean(description[len(date):])

        row_stage = _stage_from_text(description, stage)
        low = description.casefold()
        if row_stage == "merits":
            stage = "merits"
        if (
            stage == "merits"
            and re.search(r"\bbrief of (?:the )?(?:petitioner|appellant)", low)
            and "amic" not in low
        ):
            merits_phase = "petitioner"
        elif (
            stage == "merits"
            and re.search(r"\bbrief of (?:the )?(?:respondent|appellee)", low)
            and "amic" not in low
        ):
            merits_phase = "respondent"

        href = _valid_document_url(node.get("href"), base_url)
        if href:
            classified = _classify(
                description,
                stage=row_stage,
                merits_phase=merits_phase,
            )
            if classified is not None:
                doc_stage, kind, cover = classified
                documents.append(DocketDocument(
                    stage=doc_stage,
                    kind=kind,
                    label=_label(description),
                    url=href,
                    cover=cover,
                    date=date,
                    docket=normalize_docket(docket),
                    source="SCOTUSblog",
                ))
        if _GRANT_RE.search(description):
            stage = "merits"
            merits_phase = ""
    return documents


def _archive_section_kind(heading: str) -> str:
    low = heading.casefold()
    if "cert" in low or "petition stage" in low:
        return "cert"
    if "amicus" in low and "brief" in low:
        return "merits_amicus"
    if "merit" in low and "brief" in low:
        return "merits"
    return ""


def _archived_documents(
    soup: BeautifulSoup, base_url: str, docket: str
) -> list[DocketDocument]:
    """Parse the headed brief lists on legacy SCOTUSblog case-file pages."""

    documents: list[DocketDocument] = []
    for heading in soup.find_all(re.compile(r"^h[1-6]$")):
        section = _clean(heading.get_text(" ", strip=True))
        section_kind = _archive_section_kind(section)
        if not section_kind:
            continue
        side_hint = ""
        for sibling in heading.next_siblings:
            tag_name = getattr(sibling, "name", None)
            if tag_name and re.fullmatch(r"h[1-6]", tag_name):
                break
            if tag_name == "p":
                hint = _clean(sibling.get_text(" ", strip=True))
                if re.fullmatch(
                    r"(?:petitioner|appellant|respondent|appellee|neither party)",
                    hint,
                    flags=re.IGNORECASE,
                ):
                    side_hint = hint
            if tag_name not in ("ul", "ol"):
                continue
            for link in sibling.find_all("a", href=True):
                label = _clean(link.get_text(" ", strip=True))
                href = _valid_document_url(link.get("href"), base_url)
                if not label or not href:
                    continue
                stage = "cert" if section_kind == "cert" else "merits"
                classified = _classify(
                    label,
                    stage=stage,
                    section=section,
                    side_hint=side_hint,
                )
                if classified is None:
                    continue
                doc_stage, kind, cover = classified
                documents.append(DocketDocument(
                    stage=doc_stage,
                    kind=kind,
                    label=_label(
                        label, appendix=(kind == "cert_appendix"),
                    ),
                    url=href,
                    cover=cover,
                    docket=normalize_docket(docket),
                    source="SCOTUSblog",
                ))
    return documents


def parse_scotusblog_case(
    html: str, base_url: str, docket: str = ""
) -> list[DocketDocument]:
    """Parse either a current timeline or an archived SCOTUSblog case page."""

    soup = BeautifulSoup(html or "", "html.parser")
    current = _timeline_documents(soup, base_url, docket)
    archived = _archived_documents(soup, base_url, docket)
    return _dedupe_and_order(current + archived)


def _canonical_url(url: str) -> str:
    parts = urlsplit(url or "")
    key = (
        f"{(parts.hostname or '').casefold()}"
        f"{unquote(parts.path).rstrip('/').casefold()}"
    )
    return f"{key}?{parts.query}" if parts.query else key


def _dedupe_and_order(
    documents: Iterable[DocketDocument],
) -> list[DocketDocument]:
    seen: set[str] = set()
    unique: list[DocketDocument] = []
    for document in documents:
        key = _canonical_url(document.url)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(document)
    order = {"cert": 0, "merits": 1}
    return sorted(unique, key=lambda doc: order.get(doc.stage, 2))


def _session_get(session, url: str) -> str:
    try:
        response = session.get(url, timeout=_TIMEOUT)
        if int(getattr(response, "status_code", 200)) != 200:
            return ""
        return str(getattr(response, "text", "") or "")
    except Exception:
        return ""


def _session_get_json(session, url: str, *, headers: Optional[dict] = None) -> dict:
    try:
        response = session.get(url, timeout=_TIMEOUT, headers=headers or {})
        if int(getattr(response, "status_code", 200)) != 200:
            return {}
        data = response.json()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _scotusblog_case_url(session, docket: str) -> str:
    params = urlencode({
        "q": docket,
        "query_by": SCOTUSBLOG_CASE_QUERY_FIELDS,
        "query_by_weights": "6,8,8,6,6,2,2,1",
        "page": "1",
        "per_page": "10",
        "sort_by": "_text_match:desc,display_date:desc",
    })
    api_url = (
        f"https://{SCOTUSBLOG_TYPESENSE_HOST}:443/"
        f"collections/cases/documents/search?{params}"
    )
    payload = _session_get_json(
        session,
        api_url,
        headers={"X-TYPESENSE-API-KEY": SCOTUSBLOG_TYPESENSE_KEY},
    )
    found = find_scotusblog_case_url_json(payload, (docket,))
    if found:
        return found

    search_url = SCOTUSBLOG_SEARCH_URL.format(query=quote_plus(docket))
    return find_scotusblog_case_url(
        _session_get(session, search_url), (docket,)
    )


def fetch_case_docket(
    dockets: str | Iterable[str],
    *,
    name: str = "",
    session=None,
) -> CaseDocket:
    """Fetch, merge, and filter all available docket briefs for a case.

    ``name`` is accepted for caller/API clarity but exact docket numbers drive
    discovery so a common caption can never select the wrong case.
    """

    del name  # exact docket matching is intentionally the only lookup key
    tokens = docket_tokens(dockets)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": _UA})

    official_urls = tuple(official_docket_url(token) for token in tokens)
    documents: list[DocketDocument] = []
    for token, url in zip(tokens, official_urls):
        html = _session_get(session, url)
        if html:
            documents.extend(parse_official_docket(html, token))

    scotusblog_url = ""
    for token in tokens:
        scotusblog_url = _scotusblog_case_url(session, token)
        if scotusblog_url:
            page_html = _session_get(session, scotusblog_url)
            if page_html:
                documents.extend(
                    parse_scotusblog_case(page_html, scotusblog_url, token)
                )
            break

    return CaseDocket(
        documents=_dedupe_and_order(documents),
        dockets=tokens,
        official_urls=official_urls,
        scotusblog_url=scotusblog_url,
    )
