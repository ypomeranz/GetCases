"""
CourtListener API Interface
===========================
Python client for the Free Law Project's CourtListener REST API v4.
Provides access to US court case reports, opinions, dockets, and more.

API Documentation: https://www.courtlistener.com/help/api/rest/
Obtain a token at: https://www.courtlistener.com/sign-in/

Usage:
    from courtlistener import CourtListenerClient

    client = CourtListenerClient(api_token="your-token-here")

    # Search for cases
    results = client.search("Roe v Wade", type="o")

    # Get a specific opinion cluster by ID
    cluster = client.get_cluster(94508)

    # List SCOTUS opinions
    opinions = client.list_opinions(court="scotus")

    # Paginate through all results
    for page in client.paginate(client.list_opinions, court="ca9", date_filed__gte="2020-01-01"):
        for opinion in page:
            print(opinion["id"], opinion.get("download_url"))
"""

from __future__ import annotations

import re
import time
from typing import Any, Generator, Iterator
from urllib.parse import urljoin

try:
    import requests
    from requests import Response, Session
except ImportError as exc:
    raise ImportError(
        "The 'requests' package is required. Install it with: pip install requests"
    ) from exc


BASE_URL = "https://www.courtlistener.com/api/rest/v4/"

# Mapping of human-readable search type names to CourtListener type codes
SEARCH_TYPES = {
    "opinions": "o",
    "oral_arguments": "oa",
    "people": "p",
    "recap": "r",
    "recap_document": "rd",
}

# Common court IDs for convenience
COURTS = {
    "scotus": "scotus",               # Supreme Court of the United States
    "ca1": "ca1",                     # 1st Circuit Court of Appeals
    "ca2": "ca2",                     # 2nd Circuit Court of Appeals
    "ca3": "ca3",                     # 3rd Circuit Court of Appeals
    "ca4": "ca4",                     # 4th Circuit Court of Appeals
    "ca5": "ca5",                     # 5th Circuit Court of Appeals
    "ca6": "ca6",                     # 6th Circuit Court of Appeals
    "ca7": "ca7",                     # 7th Circuit Court of Appeals
    "ca8": "ca8",                     # 8th Circuit Court of Appeals
    "ca9": "ca9",                     # 9th Circuit Court of Appeals
    "ca10": "ca10",                   # 10th Circuit Court of Appeals
    "ca11": "ca11",                   # 11th Circuit Court of Appeals
    "cadc": "cadc",                   # D.C. Circuit Court of Appeals
    "cafc": "cafc",                   # Federal Circuit Court of Appeals
}


class CourtListenerError(Exception):
    """Raised when the CourtListener API returns an error."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")


class CourtListenerClient:
    """
    Client for the CourtListener REST API v4.

    Parameters
    ----------
    api_token:
        Your CourtListener API token. Obtain one at
        https://www.courtlistener.com/sign-in/
    timeout:
        Request timeout in seconds (default: 30).
    """

    def __init__(self, api_token: str, timeout: int = 30) -> None:
        self._session = Session()
        self._session.headers.update({"Authorization": f"Token {api_token}"})
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        """Perform a GET request and return parsed JSON."""
        url = urljoin(BASE_URL, endpoint.lstrip("/"))
        response: Response = self._session.get(url, params=params, timeout=self._timeout)
        # print(f"[GET] {response.request.url}")
        self._raise_for_status(response)
        return response.json()

    def _get_url(self, url: str, params: dict[str, Any] | None = None) -> dict:
        """Perform a GET request against an absolute URL (for pagination)."""
        # print(f"[GET] {url}")
        response: Response = self._session.get(url, params=params, timeout=self._timeout)
        self._raise_for_status(response)
        return response.json()

    def _options(self, endpoint: str) -> dict:
        """Perform an OPTIONS request to discover filterable fields."""
        url = urljoin(BASE_URL, endpoint.lstrip("/"))
        response: Response = self._session.options(url, timeout=self._timeout)
        self._raise_for_status(response)
        return response.json()

    @staticmethod
    def _raise_for_status(response: Response) -> None:
        if response.ok:
            return
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text
        raise CourtListenerError(response.status_code, detail)

    @staticmethod
    def _clean_params(params: dict) -> dict:
        """Remove None values from a params dict."""
        return {k: v for k, v in params.items() if v is not None}

    # ------------------------------------------------------------------
    # Search API  (/api/rest/v4/search/)
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        *,
        type: str = "o",
        court: str | None = None,
        date_filed_min: str | None = None,
        date_filed_max: str | None = None,
        highlight: bool = False,
        cursor: str | None = None,
        page_size: int = 20,
        extra: dict[str, Any] | None = None,
    ) -> dict:
        """
        Full-text search across CourtListener's database.

        Parameters
        ----------
        query:
            Search query string. Supports keyword and semantic search.
        type:
            Result type. One of ``"o"`` (opinions, default), ``"oa"``
            (oral arguments), ``"p"`` (people/judges), ``"r"`` (RECAP
            dockets), ``"rd"`` (RECAP documents). You may also pass
            human-readable names: ``"opinions"``, ``"oral_arguments"``,
            ``"people"``, ``"recap"``, ``"recap_document"``.
        court:
            Limit results to a specific court ID (e.g. ``"scotus"``).
        date_filed_min:
            Earliest filing date in ISO-8601 format (``"YYYY-MM-DD"``).
        date_filed_max:
            Latest filing date in ISO-8601 format (``"YYYY-MM-DD"``).
        highlight:
            If ``True``, include highlighted snippets in results.
        cursor:
            Cursor string for pagination (from a previous response).
        page_size:
            Number of results per page (default: 20, max: 20 for search).
        extra:
            Any additional query parameters to pass through verbatim.

        Returns
        -------
        dict
            API response with ``results``, ``count``, ``next``, and
            ``previous`` keys.
        """
        resolved_type = SEARCH_TYPES.get(type, type)
        params: dict[str, Any] = {
            "q": query,
            "type": resolved_type,
            "highlight": "on" if highlight else None,
            "cursor": cursor,
            "court": court,
            "filed_after": date_filed_min,
            "filed_before": date_filed_max,
        }
        if extra:
            params.update(extra)
        return self._get("search/", self._clean_params(params))

    def lookup_citation(self, text: str) -> list[dict]:
        """Resolve the citation(s) in ``text`` to the exact matching clusters
        via CourtListener's citation-lookup endpoint.

        Far more precise than full-text :meth:`search` for a bare reporter
        citation like ``"514 F. App'x 210"`` (which full-text search often
        mismatches).  Returns the raw list of citation objects, each with a
        ``status`` (200 when resolved) and a ``clusters`` array of matching
        OpinionCluster records.
        """
        url = urljoin(BASE_URL, "citation-lookup/")
        response: Response = self._session.post(
            url, data={"text": text}, timeout=self._timeout
        )
        self._raise_for_status(response)
        return response.json()

    def search_iter(
        self,
        query: str,
        *,
        type: str = "o",
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> Iterator[dict]:
        """
        Iterate over all search results, following cursor-based pagination.

        Yields individual result records.

        Parameters
        ----------
        query:
            Search query string.
        type:
            Result type (see :meth:`search`).
        max_pages:
            Stop after this many pages (``None`` = fetch all).
        **kwargs:
            Additional keyword arguments forwarded to :meth:`search`.
        """
        page = 0
        cursor = None
        while True:
            if max_pages is not None and page >= max_pages:
                break
            data = self.search(query, type=type, cursor=cursor, **kwargs)
            results = data.get("results", [])
            yield from results
            next_url = data.get("next")
            if not next_url:
                break
            # Extract cursor from the next URL
            from urllib.parse import parse_qs, urlparse
            parsed = urlparse(next_url)
            qs = parse_qs(parsed.query)
            cursor = qs.get("cursor", [None])[0]
            page += 1

    # ------------------------------------------------------------------
    # Opinion Clusters  (/api/rest/v4/clusters/)
    # ------------------------------------------------------------------

    def get_cluster(self, cluster_id: int, fields: str | None = None) -> dict:
        """
        Retrieve a single opinion cluster by its ID.

        Parameters
        ----------
        cluster_id:
            The numeric cluster ID (appears in CourtListener case URLs).
        fields:
            Comma-separated list of fields to return (e.g.
            ``"id,case_name,date_filed,citations"``).

        Returns
        -------
        dict
            Cluster object with nested ``sub_opinions`` and ``citations``.
        """
        params = self._clean_params({"fields": fields})
        return self._get(f"clusters/{cluster_id}/", params or None)

    def list_clusters(
        self,
        *,
        court: str | None = None,
        date_filed__gte: str | None = None,
        date_filed__lte: str | None = None,
        case_name__icontains: str | None = None,
        precedential_status: str | None = None,
        citation: str | None = None,
        ordering: str | None = None,
        fields: str | None = None,
        page_size: int = 20,
        extra: dict[str, Any] | None = None,
    ) -> dict:
        """
        List opinion clusters with optional filters.

        Parameters
        ----------
        court:
            Filter by court ID (e.g. ``"scotus"``).
        date_filed__gte:
            Filed on or after this ISO-8601 date.
        date_filed__lte:
            Filed on or before this ISO-8601 date.
        case_name__icontains:
            Case name contains this string (case-insensitive).
        precedential_status:
            One of ``"Published"``, ``"Unpublished"``, ``"Errata"``,
            ``"Separate"``, ``"In-chambers"``, ``"Relating-to"``,
            ``"Unknown"``.
        citation:
            Filter by a parallel citation string.
        ordering:
            Field(s) to sort by. Prefix with ``-`` for descending.
            The clusters endpoint rejects the parameter ("Unknown filter
            parameters are not allowed"), so none is sent by default.
        fields:
            Comma-separated list of fields to return.
        page_size:
            Number of results per page (default: 20).
        extra:
            Additional raw query parameters.

        Returns
        -------
        dict
            Paginated response with ``results``, ``count``, ``next``,
            ``previous``.
        """
        params: dict[str, Any] = {
            "docket__court": court,
            "date_filed__gte": date_filed__gte,
            "date_filed__lte": date_filed__lte,
            "case_name__icontains": case_name__icontains,
            "precedential_status": precedential_status,
            "citation": citation,
            "ordering": ordering,
            "fields": fields,
            "page_size": page_size,
        }
        if extra:
            params.update(extra)
        return self._get("clusters/", self._clean_params(params))

    # ------------------------------------------------------------------
    # Opinions  (/api/rest/v4/opinions/)
    # ------------------------------------------------------------------

    def get_opinion(self, opinion_id: int, fields: str | None = None) -> dict:
        """
        Retrieve a single opinion by its ID.

        Parameters
        ----------
        opinion_id:
            Numeric opinion ID.
        fields:
            Comma-separated list of fields to return.

        Returns
        -------
        dict
            Opinion object. The ``html_with_citations`` field contains
            the full opinion text and is the most reliable text field.
        """
        params = self._clean_params({"fields": fields})
        return self._get(f"opinions/{opinion_id}/", params or None)

    def list_opinions(
        self,
        *,
        court: str | None = None,
        date_filed__gte: str | None = None,
        date_filed__lte: str | None = None,
        type: str | None = None,
        cluster: int | None = None,
        ordering: str | None = None,
        fields: str | None = None,
        page_size: int = 20,
        extra: dict[str, Any] | None = None,
    ) -> dict:
        """
        List opinions with optional filters.

        Parameters
        ----------
        court:
            Filter by court ID via the cluster→docket→court path.
        date_filed__gte:
            Filed on or after this ISO-8601 date (on the parent cluster).
        date_filed__lte:
            Filed on or before this ISO-8601 date.
        type:
            Opinion type code. Common values:
            ``"010combined"`` (combined opinion),
            ``"020lead"`` (lead opinion),
            ``"030concurrence"`` (concurrence),
            ``"040dissent"`` (dissent).
        cluster:
            Return only opinions belonging to this cluster ID.
        ordering:
            Sort field(s).  The opinions endpoint rejects the parameter
            ("Unknown filter parameters are not allowed"), so none is
            sent unless the caller asks for one.
        fields:
            Comma-separated list of fields to return.
        page_size:
            Results per page.
        extra:
            Additional raw query parameters.

        Returns
        -------
        dict
            Paginated response.
        """
        params: dict[str, Any] = {
            "cluster__docket__court": court,
            "cluster__date_filed__gte": date_filed__gte,
            "cluster__date_filed__lte": date_filed__lte,
            "type": type,
            "cluster": cluster,
            "ordering": ordering,
            "fields": fields,
            "page_size": page_size,
        }
        if extra:
            params.update(extra)
        return self._get("opinions/", self._clean_params(params))

    def list_citing_opinions(
        self,
        *,
        cited_opinion_id: int,
        ordering: str = "-depth",
        fields: str | None = None,
        page_size: int = 20,
        next_url: str | None = None,
    ) -> dict:
        """
        Return citation objects for opinions that cite *cited_opinion_id*,
        sorted by ``depth`` (number of times cited within that document)
        descending by default.

        Parameters
        ----------
        cited_opinion_id:
            The numeric ID of the opinion being cited.
        ordering:
            Sort field.  ``"-depth"`` (default) puts the most thoroughly
            citing opinions first.
        fields:
            Comma-separated list of fields to include in the response.
        page_size:
            Results per page (max 20 for cursor-paginated endpoints).
        next_url:
            If provided, fetch this URL directly (used for pagination).

        Returns
        -------
        dict
            Paginated response with ``results``, ``count``, ``next``,
            and ``previous`` keys.  Each result contains at minimum:
            ``citing_opinion`` (URL), ``cited_opinion`` (URL),
            ``depth`` (int).
        """
        if next_url:
            return self._get_url(next_url)
        return self._get("opinions-cited/", {"cited_opinion": cited_opinion_id})

    def get_opinion_text(self, opinion_id: int) -> str:
        """
        Fetch the full HTML text of an opinion (with inline citations).

        Returns the ``html_with_citations`` field, falling back to
        ``html``, then ``plain_text`` if richer formats are unavailable.

        Parameters
        ----------
        opinion_id:
            Numeric opinion ID.

        Returns
        -------
        str
            Opinion text (HTML or plain text).
        """
        opinion = self.get_opinion(opinion_id, fields="html_with_citations,html,plain_text")
        return (
            opinion.get("html_with_citations")
            or opinion.get("html")
            or opinion.get("plain_text")
            or ""
        )

    # ------------------------------------------------------------------
    # Dockets  (/api/rest/v4/dockets/)
    # ------------------------------------------------------------------

    def get_docket(self, docket_id: int, fields: str | None = None) -> dict:
        """
        Retrieve a single docket by its ID.

        Parameters
        ----------
        docket_id:
            Numeric docket ID.
        fields:
            Comma-separated list of fields to return.

        Returns
        -------
        dict
            Docket object.
        """
        params = self._clean_params({"fields": fields})
        return self._get(f"dockets/{docket_id}/", params or None)

    def list_dockets(
        self,
        *,
        court: str | None = None,
        case_name__icontains: str | None = None,
        docket_number: str | None = None,
        date_filed__gte: str | None = None,
        date_filed__lte: str | None = None,
        ordering: str | None = None,
        fields: str | None = None,
        page_size: int = 20,
        extra: dict[str, Any] | None = None,
    ) -> dict:
        """
        List dockets with optional filters.

        Parameters
        ----------
        court:
            Filter by court ID.
        case_name__icontains:
            Case name contains this string (case-insensitive).
        docket_number:
            Exact docket number string.
        date_filed__gte:
            Filed on or after this ISO-8601 date.
        date_filed__lte:
            Filed on or before this ISO-8601 date.
        ordering:
            Sort field(s).  The dockets endpoint rejects the parameter
            outright ("Unknown filter parameters are not allowed"), so
            none is sent unless the caller asks for one.
        fields:
            Comma-separated list of fields to return.
        page_size:
            Results per page.
        extra:
            Additional raw query parameters.

        Returns
        -------
        dict
            Paginated response.
        """
        params: dict[str, Any] = {
            "court": court,
            "case_name__icontains": case_name__icontains,
            "docket_number": docket_number,
            "date_filed__gte": date_filed__gte,
            "date_filed__lte": date_filed__lte,
            "ordering": ordering,
            "fields": fields,
            "page_size": page_size,
        }
        if extra:
            params.update(extra)
        return self._get("dockets/", self._clean_params(params))

    # ------------------------------------------------------------------
    # Courts  (/api/rest/v4/courts/)
    # ------------------------------------------------------------------

    def list_courts(
        self,
        *,
        jurisdiction: str | None = None,
        in_use: bool | None = None,
        fields: str | None = None,
        page_size: int = 100,
    ) -> dict:
        """
        List courts in the CourtListener database.

        Parameters
        ----------
        jurisdiction:
            Filter by jurisdiction code. Common values:
            ``"F"`` (federal appellate), ``"FD"`` (federal district),
            ``"FB"`` (federal bankruptcy), ``"FS"`` (federal special),
            ``"S"`` (state appellate), ``"SA"`` (state trial),
            ``"C"`` (committee/agency).
        in_use:
            If ``True``, return only courts currently used in the system.
        fields:
            Comma-separated list of fields to return.
        page_size:
            Results per page.

        Returns
        -------
        dict
            Paginated response containing court objects.
        """
        params: dict[str, Any] = {
            "jurisdiction": jurisdiction,
            "in_use": "true" if in_use is True else ("false" if in_use is False else None),
            "fields": fields,
            "page_size": page_size,
        }
        return self._get("courts/", self._clean_params(params))

    def get_court(self, court_id: str) -> dict:
        """
        Retrieve a single court by its string ID (e.g. ``"scotus"``).

        Parameters
        ----------
        court_id:
            The court's string identifier.

        Returns
        -------
        dict
            Court object with jurisdiction, name, and URL information.
        """
        return self._get(f"courts/{court_id}/")

    # ------------------------------------------------------------------
    # Generic pagination helper
    # ------------------------------------------------------------------

    def paginate(
        self,
        list_fn,
        *,
        max_pages: int | None = None,
        delay: float = 0.0,
        **kwargs: Any,
    ) -> Generator[list[dict], None, None]:
        """
        Iterate over all pages returned by any ``list_*`` method.

        Yields a list of result records for each page.

        Parameters
        ----------
        list_fn:
            One of the ``list_*`` methods (e.g. ``client.list_opinions``).
        max_pages:
            Stop after this many pages (``None`` = fetch all).
        delay:
            Seconds to wait between requests (rate-limiting courtesy).
        **kwargs:
            Arguments forwarded to ``list_fn``.

        Example
        -------
        ::

            for page in client.paginate(client.list_clusters, court="ca9"):
                for cluster in page:
                    print(cluster["id"], cluster["case_name"])
        """
        page = 0
        next_url: str | None = None

        while True:
            if max_pages is not None and page >= max_pages:
                break

            if next_url:
                data = self._get_url(next_url)
            else:
                data = list_fn(**kwargs)

            results = data.get("results", [])
            if results:
                yield results

            next_url = data.get("next")
            if not next_url:
                break

            page += 1
            if delay:
                time.sleep(delay)

    # ------------------------------------------------------------------
    # Metadata / introspection
    # ------------------------------------------------------------------

    def discover_filters(self, endpoint: str) -> dict:
        """
        Return the filterable fields for a given endpoint.

        Uses the HTTP OPTIONS method. Useful for discovering what
        filters and lookups are available without reading docs.

        Parameters
        ----------
        endpoint:
            Endpoint path, e.g. ``"opinions/"`` or ``"clusters/"``.

        Returns
        -------
        dict
            Dictionary mapping field names to filter metadata.
        """
        data = self._options(endpoint)
        return data.get("filters", data)

    def count(self, endpoint: str, **filters: Any) -> int:
        """
        Return the total number of records matching the given filters
        without fetching the actual data.

        Parameters
        ----------
        endpoint:
            Endpoint path, e.g. ``"opinions/"`` or ``"clusters/"``.
        **filters:
            Filter parameters as keyword arguments.

        Returns
        -------
        int
            Total matching record count.
        """
        params = self._clean_params({**filters, "count": "on"})
        data = self._get(endpoint, params)
        return data.get("count", 0)


# ---------------------------------------------------------------------------
# Federal Cases lookup by case number ("Case No. 10,126")
# ---------------------------------------------------------------------------
# Pre-1880 lower federal opinions are cited by their Federal Cases number,
# and no public index maps those numbers to reporter citations.  But the
# citation almost always prints the case *name* just before the number, the
# CourtListener cluster's ``headnotes`` field opens with the case's own
# number ("Case No. 2,717. Lien on Foreign Vessel …"), and the reporter's
# alphabetical arrangement fixes which F. Cas. *volume* a number must fall
# in (fed_cas.VOLUME_RANGES).  So: search by name (exact, then relaxed,
# then fuzzy — the source texts are OCR), keep candidates bearing an
# "F. Cas." citation, and confirm the winner by the number at the head of
# its headnotes, falling back to the volume check when no headnotes carry
# the number.

_FCAS_CITE_RE = re.compile(r"\b(\d{1,2})\s+F\.?\s?Cas\.?\s+(\d{1,5})\b")

_FEDCAS_STOPWORDS = frozenset({
    "the", "of", "and", "in", "re", "ex", "parte", "a", "an", "et", "al",
    "v", "vs",
})

# How many cluster fetches one lookup may spend verifying candidates.
_FEDCAS_VERIFY_BUDGET = 6


def _fcas_volume(citations) -> "int | None":
    """The F. Cas. volume among a result's citations, or None."""
    import re as _re
    for c in citations or []:
        if isinstance(c, dict):
            if "f. cas" in str(c.get("reporter") or "").lower():
                try:
                    return int(c.get("volume"))
                except (TypeError, ValueError):
                    continue
        else:
            m = _FCAS_CITE_RE.search(_re.sub(r"<[^>]+>", "", str(c)))
            if m:
                return int(m.group(1))
    return None


def _fedcas_name_tokens(name: str) -> list[str]:
    import re as _re
    toks = []
    for w in _re.findall(r"[A-Za-z][A-Za-z'’]*", name or ""):
        wl = w.lower().replace("’", "'").strip("'")
        if len(wl) >= 3 and wl not in _FEDCAS_STOPWORDS and wl not in toks:
            toks.append(wl)
    return toks


def _fedcas_loose_score(query_name: str, candidate_name: str) -> float:
    """Fraction of the query name's identifying tokens present in the
    candidate's — a token counts when equal, a prefix, or a near-identical
    OCR variant ("chnsan" ≈ "chusan")."""
    import difflib
    q = _fedcas_name_tokens(query_name)
    c = _fedcas_name_tokens(candidate_name)
    if not q or not c:
        return 0.0

    def close(a: str, b: str) -> bool:
        if a == b or (len(a) >= 4 and (a.startswith(b) or b.startswith(a))):
            return True
        return (len(a) >= 4 and len(b) >= 4
                and difflib.SequenceMatcher(None, a, b).ratio() >= 0.8)

    hit = sum(1 for t in q if any(close(t, ct) for ct in c))
    return hit / len(q)


def _fedcas_name_queries(name: "str | None") -> list[str]:
    """caseName search queries for a printed name, tightest first: the name
    as printed and de-hyphenated (OCR splits words: "Har-ney"), then its
    identifying tokens ANDed, then the tokens fuzzed one edit each — the
    OCR-forgiveness pass ("Chnsan~1" finds Chusan)."""
    import re as _re

    name = _re.sub(r"\s+", " ", (name or "")).strip(' ,;."')
    if not name:
        return []
    queries: list[str] = []
    variants = [name]
    dehyph = _re.sub(r"(?<=[a-z])-(?=[a-z])", "", name)
    if dehyph != name:
        variants.append(dehyph)
    # Surname particles printed with a space often index joined ("Macy v.
    # De Wolf" is CourtListener's "Macy v. DeWolf") — neither the phrase,
    # the token AND, nor a one-edit fuzz bridges that, so search the joined
    # spelling as its own variant.
    joined = _re.sub(r"\b(De|Di|Du|La|Le|Van|Von|Mc|Mac|O)[' ](?=[A-Z])",
                     r"\1", name)
    if joined != name:
        variants.append(joined)
    for v in variants:
        queries.append(f'caseName:"{v}"')
    tokens: list[str] = []
    for v in variants:
        for t in _fedcas_name_tokens(v):
            if t not in tokens:
                tokens.append(t)
    tokens = tokens[:5]
    if tokens:
        joined = " AND ".join(tokens)
        q = f"caseName:({joined})"
        if q not in queries:
            queries.append(q)
        queries.append(
            "caseName:(" + " AND ".join(f"{t}~1" for t in tokens) + ")")
    return queries


def _fedcas_headnote_number(headnotes: str) -> "str | None":
    """The case number opening a cluster's headnotes ("Case No. 2,717. …"),
    normalized ("2717"), or None.  Only the very start counts: numbers later
    in the headnotes are cross-references to other cases."""
    import re as _re
    txt = _re.sub(r"\s+", " ", _re.sub(r"<[^>]+>", " ", headnotes or "")).strip()
    m = _re.match(r"[\[\s]*Case No\W{0,3}(\d[\d.,\- ]{0,8}\d|\d)([a-z])?",
                  txt[:120])
    if not m:
        return None
    digits = _re.sub(r"\D", "", m.group(1))
    return (str(int(digits)) + (m.group(2) or "")) if digits else None


def find_fedcas_case(
    case_no: str,
    name: "str | None" = None,
    session: "Session | None" = None,
    timeout: int = 30,
) -> "dict | None":
    """Locate the CourtListener cluster behind a Federal Cases citation
    given by case number ("Cole v. The Atlantic, Case No. 2,976").

    Returns a search-result-shaped item — ``cluster_id``, ``caseName``,
    ``citation`` (strings), ``dateFiled``, ``court_id``, plus
    ``matched_by``: ``"headnote"`` when the number at the head of the
    cluster's headnotes confirmed it, ``"volume"`` when only the
    name match plus the F. Cas. volume check vouch for it — or ``None``.

    Anonymous sessions work (rate-limited); pass an authenticated one when
    available.
    """
    import fed_cas

    num_key = fed_cas.number_key(case_no)
    if num_key is None:
        return None
    s = session or requests.Session()
    url = urljoin(BASE_URL, "search/")

    queries = _fedcas_name_queries(name)
    # Last resort (and the only key for a nameless citation): the number
    # itself as printed in the reporter's headnote, which full-text search
    # finds — along with later cases *citing* it, which the headnote check
    # weeds out.
    queries.append(f'"Case No. {fed_cas.pretty_number(case_no)}"')

    def item_from(result: dict) -> dict:
        import re as _re
        cites = [_re.sub(r"<[^>]+>", "", str(c))
                 for c in result.get("citation") or []]
        return {
            "cluster_id": result.get("cluster_id"),
            "caseName": _re.sub(r"<[^>]+>", "",
                                result.get("caseName") or ""),
            "case_name": _re.sub(r"<[^>]+>", "",
                                 result.get("caseName") or ""),
            "citation": cites,
            "dateFiled": str(result.get("dateFiled") or "")[:10],
            "court_id": result.get("court_id") or "",
        }

    verified_budget = _FEDCAS_VERIFY_BUDGET
    checked: set = set()
    fallback: "tuple[float, dict] | None" = None
    for q in queries:
        params = {"type": "o", "q": q, "filed_before": "1882-12-31",
                  "page_size": 20}
        try:
            resp = s.get(url, params=params, timeout=timeout)
            if resp.status_code != 200:
                continue
            results = resp.json().get("results") or []
        except Exception:
            continue
        candidates = []
        for it in results:
            vol = _fcas_volume(it.get("citation"))
            if vol is None:
                continue
            plaus = fed_cas.plausible_volume(case_no, vol)
            score = (_fedcas_loose_score(name, it.get("caseName") or "")
                     if name else 0.0)
            candidates.append((plaus, score, it))
        # Most plausible volume first, then the closest name.
        candidates.sort(key=lambda t: (t[0], t[1]), reverse=True)
        for plaus, score, it in candidates[:4]:
            cid = it.get("cluster_id")
            if not cid or cid in checked:
                continue
            checked.add(cid)
            if verified_budget > 0:
                verified_budget -= 1
                try:
                    r = s.get(
                        urljoin(BASE_URL, f"clusters/{cid}/"),
                        params={"fields": "id,headnotes"}, timeout=timeout)
                    headnotes = (r.json().get("headnotes") or ""
                                 if r.status_code == 200 else "")
                except Exception:
                    headnotes = ""
                if _fedcas_headnote_number(headnotes) == str(case_no):
                    item = item_from(it)
                    item["matched_by"] = "headnote"
                    return item
            # No headnote confirmation: remember the best name+volume match
            # in case nothing ever confirms.  Never without a name: the
            # number-phrase query surfaces cases *citing* the number, and
            # with no name to hold against them a same-volume citer would
            # slip through (Packard v. The Louisa, 18 F. Cas. 958, cites
            # "Case No. 10,126" and shares the Nestor's volume 18).
            if plaus and name is not None and score >= 0.5:
                if fallback is None or score > fallback[0]:
                    fallback = (score, item_from(it))
        # A confidently-named exact-phrase hit that failed only the headnote
        # check still shouldn't stop the looser passes — keep going.
    if fallback is not None:
        item = fallback[1]
        item["matched_by"] = "volume"
        return item
    return None

# Ranking for same-day docket entries: the cited document is the opinion
# itself, but courts often docket a separate order the same day.
_RECAP_DESC_RANK = (
    ("opinion", 4),
    ("memorandum", 3),
    ("report and recommendation", 2),
    ("findings", 2),
    ("order", 1),
)


def _recap_doc_score(doc: dict) -> tuple:
    text = " ".join(
        str(doc.get(k) or "") for k in ("short_description", "description")
    ).lower()
    rank = 0
    for phrase, score in _RECAP_DESC_RANK:
        if phrase in text:
            rank = max(rank, score)
    available = bool(doc.get("is_available") and doc.get("filepath_local"))
    return (available, rank)


def _case_name_queries(case_name: str | None) -> list[str]:
    """Case-name query variants for the RECAP search, tightest first: the
    name as printed, then only its unabbreviated words.  Bluebook clips
    parties to a period ("Am. Int'l Indus.") or an apostrophe contraction
    ("Nat'l", "Ass'n") that the full case names on PACER dockets won't
    match, while the words left whole ("Peninsula Pathology") match fine."""
    import re as _re

    name = (case_name or "").strip(" ,;")
    if not name:
        return []
    queries = [name]
    words: list[str] = []
    for w in _re.findall(r"[A-Za-z][A-Za-z'’-]*\.?", name):
        wl = w.lower().replace("’", "'")
        if (w.endswith(".") or len(w) < 3 or w in words
                or wl in ("the", "and", "for", "llc", "inc", "corp", "co",
                          "ltd", "llp", "plc")
                or _re.search(r"'[a-z]{1,2}$", wl)):
            continue
        words.append(w)
    tokens = " ".join(words[:4])
    if tokens and tokens.lower() != name.lower():
        queries.append(tokens)
    return queries


def find_recap_document(
    docket_number: str,
    court: str | None,
    date_filed: str,
    session: "Session | None" = None,
    timeout: int = 30,
    case_name: str | None = None,
) -> dict | None:
    """Locate the RECAP (PACER) document behind an unpublished-opinion
    citation by searching the archive for documents filed in that case on
    that day — keyed by docket number when the citation prints one ("No.
    12-6371, 2024 WL 1327972 (D.N.J. Mar. 28, 2024)"), by case name when
    it doesn't ("Pecos River Talc LLC v. Emory, 2025 WL 1249947 (E.D. Va.
    Apr. 30, 2025)" — the party names, court and date pin the entry down
    just as well).

    Parameters
    ----------
    docket_number:
        As printed in the citation ("12-6371", "2:13-cv-7779"), or ""
        when it prints none; judge-initial suffixes are retried stripped
        when the full form finds nothing.
    court:
        CourtListener court id ("njd"), or ``None`` to search all courts.
    date_filed:
        The opinion's date, ISO format ("2024-03-28").
    session:
        Optional authenticated ``requests`` session; anonymous works too
        (the search API allows it, rate-limited).
    case_name:
        The case name as printed in the citation, if known.  Tried after
        the docket-number searches (see :func:`_case_name_queries`), so it
        also rescues a docket number PACER styles differently.

    Returns
    -------
    dict | None
        ``{"pdf_url", "web_url", "description", "docket_id"}`` for the
        best match — ``pdf_url`` is ``None`` when the document exists but
        its PDF isn't in the archive — or ``None`` when nothing matched.
    """
    import re as _re

    s = session or requests.Session()
    url = urljoin(BASE_URL, "search/")
    attempts: list[dict] = []
    if docket_number:
        variants = [docket_number]
        core = _re.sub(r"(?<=\d)-[A-Za-z]{1,4}(?:-[A-Za-z]{1,4})*$", "",
                       docket_number)
        if core != docket_number:
            variants.append(core)
        attempts += [{"docket_number": v} for v in variants]
    attempts += [{"case_name": q} for q in _case_name_queries(case_name)]

    for extra in attempts:
        params = {
            "type": "rd",
            "q": "",
            "entry_date_filed_after": date_filed,
            "entry_date_filed_before": date_filed,
            **extra,
        }
        if court:
            params["court"] = court
        try:
            resp = s.get(url, params=params, timeout=timeout)
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception:
            continue
        results = data.get("results") or []
        # A loose docket number can match hundreds of entries across
        # courts; that's noise, not the cited opinion.
        if not results or (data.get("count") or 0) > 40:
            continue
        best = max(results, key=_recap_doc_score)
        path = best.get("filepath_local") or ""
        pdf_url = ("https://storage.courtlistener.com/" + path.lstrip("/")
                   if best.get("is_available") and path else None)
        web = best.get("absolute_url") or ""
        return {
            "pdf_url": pdf_url,
            "web_url": ("https://www.courtlistener.com" + web) if web else "",
            "description": (best.get("short_description")
                            or best.get("description") or ""),
            "docket_id": best.get("docket_id"),
        }
    return None
