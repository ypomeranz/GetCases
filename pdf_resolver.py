"""Opinion PDF URL resolution shared by GetCases front ends."""

from __future__ import annotations

import re
from typing import Callable, Optional

import requests

from case_utils import citation_list, cluster_citations_to_strings
from courtlistener import CourtListenerClient


StatusCallback = Callable[[str], None]

_LOC_CUTOFF = 542
_GOVINFO_MAX = 582
_US_CITE_RE = re.compile(r"(\d+)\s+U\.S\.\s+(\d+)")
_CITE_PARSE_RE = re.compile(r"^(\d+)\s+(.+)\s+(\d+)")
_CASE_LAW_REPORTER_ALIASES = {
    "fed-rep": "f",
    "fed-rep-2d": "f2d",
    "fed-rep-3d": "f3d",
    "fappx": "f-appx",
    "fedappx": "f-appx",
    "fed-appx": "f-appx",
}

HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "application/pdf,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
})


def _emit(status: StatusCallback | None, message: str) -> None:
    if status:
        status(message)


def _head_ok(url: str, label: str, status: StatusCallback | None = None) -> bool:
    try:
        response = HTTP_SESSION.head(url, timeout=10, allow_redirects=True)
    except Exception as exc:
        _emit(status, f"{label} check failed: {exc}")
        return False
    if response.status_code == 200:
        return True
    _emit(status, f"{label} returned HTTP {response.status_code}")
    return False


def _us_reports_loc_url(citation: str) -> Optional[str]:
    match = _US_CITE_RE.search(citation)
    if not match:
        return None
    volume, page = int(match.group(1)), int(match.group(2))
    if volume > _LOC_CUTOFF:
        return None
    return (
        "https://cdn.loc.gov/service/ll/usrep/"
        f"usrep{volume:03d}/usrep{volume:03d}{page:03d}/"
        f"usrep{volume:03d}{page:03d}.pdf"
    )


def _us_reports_govinfo_urls(citation: str) -> Optional[tuple[str, str]]:
    match = _US_CITE_RE.search(citation)
    if not match:
        return None
    volume, page = int(match.group(1)), int(match.group(2))
    if volume > _GOVINFO_MAX:
        return None
    link_url = f"https://www.govinfo.gov/link/usreports/{volume}/{page}"
    direct_url = (
        f"https://www.govinfo.gov/content/pkg/USREPORTS-{volume}/pdf/"
        f"USREPORTS-{volume}-{page}.pdf"
    )
    return link_url, direct_url


def _slugify_reporter(reporter: str) -> str:
    slug = reporter.lower().replace(" ", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return _CASE_LAW_REPORTER_ALIASES.get(slug, slug)


def _static_case_law_url(citation: str) -> Optional[str]:
    citation = re.sub(r"<[^>]+>", "", citation or "").strip()
    match = _CITE_PARSE_RE.match(citation)
    if not match:
        return None
    volume, reporter, page = match.group(1), match.group(2).strip(), match.group(3)
    slug = _slugify_reporter(reporter)
    if not slug:
        return None
    return f"https://static.case.law/{slug}/{volume}/case-pdfs/{int(page):04d}-01.pdf"


def gather_all_citations(
    client: CourtListenerClient,
    item: dict,
    status: StatusCallback | None = None,
) -> list[str]:
    """Return every known citation for a result, de-duplicated."""
    out: list[str] = []
    seen: set[str] = set()

    def add(citation: str) -> None:
        cleaned = re.sub(r"<[^>]+>", "", str(citation or "")).strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)

    for citation in citation_list(item.get("citation")):
        add(citation)

    cluster_id = item.get("cluster_id") or item.get("id")
    if not cluster_id:
        return out
    try:
        cluster = client.get_cluster(int(cluster_id), fields="citations")
    except Exception as exc:
        _emit(status, f"Cluster citation lookup failed: {exc}")
        return out
    for citation in cluster_citations_to_strings(cluster.get("citations")):
        add(citation)
    return out


def resolve_pdf_url(
    client: CourtListenerClient,
    item: dict,
    status: StatusCallback | None = None,
) -> Optional[str]:
    """Locate the best PDF URL for a CourtListener search result."""
    storage_base = "https://storage.courtlistener.com/"
    court_id = str(item.get("court_id") or "")
    is_scotus = "scotus" in court_id

    _emit(status, "Checking official and archive PDF sources...")
    citations = gather_all_citations(client, item, status=status)

    for citation in citations:
        loc_url = _us_reports_loc_url(citation)
        govinfo = _us_reports_govinfo_urls(citation)
        if loc_url and _head_ok(loc_url, "LOC US Reports", status):
            return loc_url
        if govinfo:
            link_url, direct_url = govinfo
            if _head_ok(link_url, "GovInfo link", status):
                return link_url
            if _head_ok(direct_url, "GovInfo PDF", status):
                return direct_url

    if not is_scotus:
        for citation in citations:
            if "lexis" in citation.lower():
                continue
            url = _static_case_law_url(citation)
            if url and _head_ok(url, "Caselaw Access Project", status):
                return url

    local_path = item.get("local_path") or item.get("localPath") or ""
    if local_path:
        url = storage_base + str(local_path).lstrip("/")
        if _head_ok(url, "CourtListener storage", status):
            return url

    fetched_op: Optional[dict] = None
    opinion_id = item.get("id")
    if opinion_id:
        try:
            _emit(status, "Checking CourtListener opinion record...")
            fetched_op = client.get_opinion(int(opinion_id))
            local_path = fetched_op.get("local_path") or ""
            if local_path:
                url = storage_base + str(local_path).lstrip("/")
                if _head_ok(url, "CourtListener opinion storage", status):
                    return url
        except Exception as exc:
            _emit(status, f"Opinion lookup failed: {exc}")

    download_url = item.get("download_url") or ""
    if download_url and _head_ok(download_url, "Original court PDF", status):
        return str(download_url)

    if fetched_op:
        download_url = fetched_op.get("download_url") or ""
        if download_url and _head_ok(download_url, "Opinion PDF", status):
            return str(download_url)

    cluster_id = item.get("cluster_id") or item.get("id")
    if cluster_id:
        try:
            _emit(status, "Checking related CourtListener sub-opinions...")
            cluster = client.get_cluster(int(cluster_id), fields="sub_opinions")
            for op_url in cluster.get("sub_opinions") or []:
                op = client._get_url(op_url, {"fields": "download_url,local_path"})
                local_path = op.get("local_path") or ""
                if local_path:
                    url = storage_base + str(local_path).lstrip("/")
                    if _head_ok(url, "Sub-opinion storage", status):
                        return url
                download_url = op.get("download_url") or ""
                if download_url and _head_ok(download_url, "Sub-opinion PDF", status):
                    return str(download_url)
        except Exception as exc:
            _emit(status, f"Sub-opinion lookup failed: {exc}")

    return None


def fetch_pdf_bytes(client: CourtListenerClient, url: str) -> bytes:
    """Download a PDF using the CourtListener session only for that domain."""
    if "courtlistener.com" in url:
        response = client._session.get(url, timeout=60, stream=True)
    else:
        response = HTTP_SESSION.get(url, timeout=60, stream=True)
    response.raise_for_status()
    return response.content
