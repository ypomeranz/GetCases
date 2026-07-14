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
# RECAP lookup for unpublished opinions
# ---------------------------------------------------------------------------

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
