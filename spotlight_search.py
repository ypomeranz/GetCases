"""Spotlight search and ranking helpers for GetCases.

The quick-search UI needs a different ranking strategy from a broad full-text
table: when someone types a case name, the case itself should beat opinions
that merely discuss it.  These helpers keep that logic out of any one frontend.
"""

from __future__ import annotations

import difflib
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

from case_utils import (
    case_name,
    citation_list,
    cluster_citations_to_strings,
    normalize_result_citations,
    parse_citation_line,
    pick_citation,
    strip_html,
)
import eng_rep
from court_catalog import (
    CIRCUIT_COURTS,
    COURT_BLUEBOOK,
    DISTRICT_COURTS,
    STATE_COURTS,
)


try:
    from opinion_db import OpinionDB
except Exception:  # pragma: no cover - cache is optional at runtime.
    OpinionDB = None  # type: ignore[assignment]


_SCOTUS_COURT_ID = "scotus"
_NAME_MATCH_MIN = 0.5
_MAX_CLEAN_PARTY = 8
_REVERSED_PARTY_MIN_CITES = 1000

_NAME_STOPWORDS = {
    "the", "of", "and", "a", "an", "in", "on", "for", "re", "ex", "parte",
    "matter", "v", "vs", "et", "al", "co", "cos", "corp", "inc", "ltd",
    "llc", "llp", "lp", "lllp", "plc", "company", "companies",
    "incorporated", "corporation", "no", "nos",
}
_NAME_PARTY_SPLIT_RE = re.compile(r"\s+v(?:s)?\.?\s+", re.IGNORECASE)
_US_PARTY_RE = re.compile(r"^\s*u\.?\s*s\.?\s*a?\.?\s*$", re.IGNORECASE)
_CITE_PARSE_RE = re.compile(r"^(\d+)\s+(.+)\s+(\d+)")
_CIRCUIT_ORDINAL_IDS = {
    "1st": "ca1", "2d": "ca2", "2nd": "ca2", "3d": "ca3", "3rd": "ca3",
    "4th": "ca4", "5th": "ca5", "6th": "ca6", "7th": "ca7", "8th": "ca8",
    "9th": "ca9", "10th": "ca10", "11th": "ca11",
    "first": "ca1", "second": "ca2", "third": "ca3", "fourth": "ca4",
    "fifth": "ca5", "sixth": "ca6", "seventh": "ca7", "eighth": "ca8",
    "ninth": "ca9", "tenth": "ca10", "eleventh": "ca11",
}
_CIRCUIT_WORD_IDS = {
    "first": "ca1", "second": "ca2", "third": "ca3", "fourth": "ca4",
    "fifth": "ca5", "sixth": "ca6", "seventh": "ca7", "eighth": "ca8",
    "ninth": "ca9", "tenth": "ca10", "eleventh": "ca11",
}
_DISTRICT_ABBR_IDS = {
    abbr.replace(".", "").replace(" ", "").lower(): cid
    for cid, abbr in DISTRICT_COURTS.items()
}
_STATE_NAME_COURTS = {state.lower(): courts for state, courts in STATE_COURTS}
_STATE_COURT_ABBR_IDS = {
    abbr.replace(".", "").replace(" ", "").lower(): cid
    for _state, courts in STATE_COURTS for cid, abbr, _label in courts
}
_SCHOLAR_STATE_PREFIX: dict[str, list[tuple[str, str, str]]] = {}
for _state_name, _state_courts in STATE_COURTS:
    _pref_key = _state_courts[0][1].replace(".", "").replace(" ", "").lower()
    _SCHOLAR_STATE_PREFIX.setdefault(_pref_key, _state_courts)

_CIRCUIT_HINT_RE = re.compile(
    r"(?P<ord>\d{1,2}(?:st|nd|rd|d|th)|first|second|third|fourth|fifth|"
    r"sixth|seventh|eighth|ninth|tenth|eleventh)\s+cir(?:cuit)?\.?",
    re.IGNORECASE,
)
_DC_CIRCUIT_HINT_RE = re.compile(r"\bd\.?\s*c\.?\s+cir(?:cuit)?\.?", re.IGNORECASE)
_FED_CIRCUIT_HINT_RE = re.compile(r"\bfed(?:eral)?\.?\s+cir(?:cuit)?\.?", re.IGNORECASE)
_SCOTUS_HINT_RE = re.compile(
    r"\b(?:scotus|u\.?\s*s\.?\s+supreme\s+court|united\s+states\s+supreme\s+"
    r"court|supreme\s+court\s+of\s+the\s+united\s+states)\b",
    re.IGNORECASE,
)
_YEAR_TAIL_RE = re.compile(r"[\s,]*(?:19|20)\d{2}\s*$")

_COMMON_PARTY_NAMES: set[frozenset[str]] = {
    frozenset()
}


@dataclass(frozen=True)
class SpotlightResult:
    source: str
    bucket: str
    title: str
    cite: str = ""
    year: str = ""
    court_id: str = ""
    detail: str = ""
    payload: object = None

    @property
    def source_label(self) -> str:
        return {
            "courtlistener": "CourtListener",
            "cache": "Cache",
            "engrep": "English Reports",
            "scholar": "Scholar",
        }.get(self.source, self.source.title())


def _name_tokens(name: str) -> list[str]:
    name = re.sub(r"<[^>]+>", " ", name or "")
    name = re.sub(r"[^\w\s]", " ", name.lower())
    return [
        t for t in name.split()
        if len(t) > 1 and not t.isdigit() and t not in _NAME_STOPWORDS
    ]


for _name in (
    [state for state, _courts in STATE_COURTS]
    + ["United States", "United States of America", "People", "State", "Commonwealth"]
):
    _COMMON_PARTY_NAMES.add(frozenset(_name_tokens(_name)))
_COMMON_PARTY_NAMES.discard(frozenset())


def _name_parties(name: str) -> list[set[str]]:
    out: list[set[str]] = []
    for side in _NAME_PARTY_SPLIT_RE.split(name or "", maxsplit=1):
        toks = {"united", "states"} if _US_PARTY_RE.match(side) else set(_name_tokens(side))
        if toks:
            out.append(toks)
    return out


def _token_close(a: str, b: str) -> bool:
    if a == b:
        return True
    if len(a) >= 4 and len(b) >= 4 and (a.startswith(b) or b.startswith(a)):
        return True
    if len(a) >= 5 and len(b) >= 5:
        return difflib.SequenceMatcher(None, a, b).ratio() >= 0.85
    return False


def _is_acronym_of(acro: set[str], words: set[str]) -> bool:
    if len(acro) != 1 or not (2 <= len(words) <= 6):
        return False
    (a,) = acro
    return 2 <= len(a) <= 6 and sorted(a) == sorted(w[0] for w in words)


def _party_overlap(query_party: set[str], cand_party: set[str]) -> float:
    if not query_party:
        return 0.0
    if _is_acronym_of(query_party, cand_party) or _is_acronym_of(cand_party, query_party):
        return 1.0
    hits = sum(
        1 for token in query_party
        if any(_token_close(token, candidate) for candidate in cand_party)
    )
    return hits / len(query_party)


def _is_common_party(party: set[str]) -> bool:
    return frozenset(party) in _COMMON_PARTY_NAMES


def name_match_score(query: str, candidate: str) -> float:
    """Return 0..1 closeness of a candidate case name to a query case name."""
    q_parties = _name_parties(query)
    c_parties = _name_parties(candidate)
    if not q_parties or not c_parties:
        q = set(_name_tokens(query))
        c = set(_name_tokens(candidate))
        return _party_overlap(q, c) if q else 0.0
    per_side = [max(_party_overlap(qp, cp) for cp in c_parties) for qp in q_parties]
    if max(per_side) < 0.6:
        return 0.0
    bonus = 0.15 if sum(1 for score in per_side if score >= 0.6) >= 2 else 0.0
    return min(1.0, (sum(per_side) / len(per_side)) + bonus)


def _match_tier(query: str, candidate: str) -> int:
    q_parties = _name_parties(query)
    c_parties = _name_parties(candidate)
    if not q_parties or not c_parties:
        q = set(_name_tokens(query))
        c = set(_name_tokens(candidate))
        return 1 if q and _party_overlap(q, c) >= 0.6 else -1

    def matches(qp: set[str], cp: set[str]) -> bool:
        return len(cp) <= _MAX_CLEAN_PARTY and _party_overlap(qp, cp) >= 0.6

    if len(q_parties) == 2 and len(c_parties) == 2:
        (qa, qb), (ca, cb) = q_parties, c_parties
        if matches(qa, ca) and matches(qb, cb):
            return 3
        if matches(qa, cb) and matches(qb, ca):
            return 2

    matched = [qp for qp in q_parties if any(matches(qp, cp) for cp in c_parties)]
    if not matched:
        return -1
    return 0 if all(_is_common_party(qp) for qp in matched) else 1


def _filter_to_best_tier(query: str, tagged: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    rated = [
        (
            _match_tier(
                query,
                strip_html(item.get("caseName") or item.get("case_name") or ""),
            ),
            bucket,
            item,
        )
        for bucket, item in tagged
    ]
    best = max((tier for tier, _bucket, _item in rated), default=-1)
    out: list[tuple[str, dict]] = []
    for tier, bucket, item in rated:
        if tier == best:
            out.append((bucket, item))
        elif (
            best == 3
            and tier == 2
            and (item.get("citeCount") or 0) >= _REVERSED_PARTY_MIN_CITES
        ):
            out.append(("reversed", item))
    return out


def _case_fingerprints(name: str, cite: str, year: str, *, include_name: bool = True) -> set[str]:
    fps: set[str] = set()
    match = _CITE_PARSE_RE.match(strip_html(cite).strip())
    if match:
        volume = match.group(1)
        reporter = re.sub(r"[^a-z0-9]", "", match.group(2).lower())
        page = match.group(3)
        if reporter:
            fps.add(f"c:{volume}:{reporter}:{page}")
    if include_name:
        toks = _name_tokens(name)
        if toks:
            fps.add("n:" + " ".join(sorted(set(toks))))
    if not fps and name:
        fps.add(f"raw:{name.lower()}:{year}")
    return fps


_BUCKET_CAPS: dict[str, int] = {
    "cache": 3,
    "scholar": 2,
    "exact": 3,
    "ranked": 4,
    "reversed": 2,
    "scotus": 3,
    "juris": 3,
    "cl": 3,
    "engrep": 1,
}


def _dedup_accept(
    fps: set[str], bucket: str, seen: set[str], bucket_counts: dict[str, int]
) -> bool:
    if fps & seen:
        return False
    if bucket and bucket_counts.get(bucket, 0) >= _BUCKET_CAPS.get(bucket, 99):
        return False
    seen.update(fps)
    if bucket:
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    return True


def merge_results(*groups: list[SpotlightResult], max_results: int = 10) -> list[SpotlightResult]:
    seen: set[str] = set()
    bucket_counts: dict[str, int] = {}
    out: list[SpotlightResult] = []
    for group in groups:
        for result in group:
            fps = _case_fingerprints(
                result.title,
                result.cite,
                result.year,
                include_name=(result.bucket != "reversed"),
            )
            if not _dedup_accept(fps, result.bucket, seen, bucket_counts):
                continue
            out.append(result)
            if len(out) >= max_results:
                return out
    return out


def court_label(court_id: str) -> str:
    court_id = (court_id or "").strip().lower()
    if court_id == "scotus":
        return "SCOTUS"
    if court_id == "engrep":
        return "Eng. Rep."
    return COURT_BLUEBOOK.get(court_id, court_id.upper() if court_id else "")


def _is_scotus_order_item(item: dict) -> bool:
    court_val = str(item.get("court_id") or item.get("court") or "")
    if "scotus" not in court_val.lower():
        return False
    if (item.get("citeCount") or 0) > 0:
        return False
    opinions = item.get("opinions") or []
    main_op = max(opinions, key=lambda op: len(op.get("cites") or []), default=None)
    if main_op is None:
        return False
    cites_count = len(main_op.get("cites") or []) if main_op else 0
    return cites_count <= 2


def _classify_state_court(text: str, courts: list[tuple[str, str, str]]) -> str:
    text = re.sub(r"\s+", " ", text or "").strip().lower()
    high = courts[0][0]
    intermediate = courts[1][0] if len(courts) > 1 else ""

    def by_label(*keywords: str) -> str:
        for cid, _abbr, label in courts:
            ll = label.lower()
            if any(keyword in ll for keyword in keywords):
                return cid
        return ""

    if "criminal" in text:
        return by_label("criminal") or high
    if "civil" in text:
        return by_label("civil") or high
    if "appellate division" in text:
        return "nyappdiv" if high == "ny" else by_label("appellate division") or intermediate or high
    if "special appeals" in text:
        return by_label("special") or intermediate or high
    if "commonwealth" in text:
        return by_label("commonwealth") or intermediate or high
    if "superior" in text:
        return by_label("superior") or intermediate or high
    if (
        re.search(r"courts? of appeal", text)
        or "appeals court" in text
        or "appellate court" in text
    ):
        return high if high in {"md", "ny", "dc"} else intermediate or high
    if "supreme" in text:
        return high
    return high


def _classify_court_hint(hint: str) -> Optional[tuple[str, str]]:
    text = _YEAR_TAIL_RE.sub("", (hint or "").strip()).strip(" ,.;()[]")
    if not text:
        return None
    low = text.lower()
    if _SCOTUS_HINT_RE.search(low):
        return _SCOTUS_COURT_ID, "U.S. Supreme Court"
    match = _CIRCUIT_HINT_RE.search(low)
    if match:
        cid = _CIRCUIT_ORDINAL_IDS.get(match.group("ord").lower())
        if cid:
            return cid, CIRCUIT_COURTS.get(cid, cid)
    if _DC_CIRCUIT_HINT_RE.search(low):
        return "cadc", CIRCUIT_COURTS["cadc"]
    if _FED_CIRCUIT_HINT_RE.search(low):
        return "cafc", CIRCUIT_COURTS["cafc"]

    key = low.replace(".", "").replace(" ", "")
    cid = _DISTRICT_ABBR_IDS.get(key)
    if cid:
        return cid, DISTRICT_COURTS[cid]
    cid = _STATE_COURT_ABBR_IDS.get(key)
    if cid:
        return cid, COURT_BLUEBOOK.get(cid, cid)

    for state_low, courts in _STATE_NAME_COURTS.items():
        if low == state_low or low.startswith(state_low + " "):
            cid = _classify_state_court(low, courts)
            return cid, COURT_BLUEBOOK.get(cid, cid)
    return None


def _detect_jurisdiction(query: str) -> Optional[tuple[str, str, str]]:
    q = (query or "").strip()
    match = re.search(r"[(\[]([^)\]]*)[)\]]\s*$", q)
    if match:
        hit = _classify_court_hint(match.group(1))
        if hit:
            return hit[0], q[: match.start()].strip(" ,;-"), hit[1]
    if "," in q:
        head, tail = q.rsplit(",", 1)
        if tail.strip() and len(tail.split()) <= 4:
            hit = _classify_court_hint(tail)
            if hit:
                return hit[0], head.strip(" ,;-"), hit[1]
    for regex in (_CIRCUIT_HINT_RE, _DC_CIRCUIT_HINT_RE, _FED_CIRCUIT_HINT_RE):
        match = regex.search(q)
        if match and _YEAR_TAIL_RE.sub("", q[match.end():]).strip() == "":
            hit = _classify_court_hint(q[match.start():])
            if hit:
                return hit[0], q[: match.start()].strip(" ,;-"), hit[1]
    return None


def _cl_casename_query(name: str, *, strict: bool = False) -> str:
    parties = _name_parties(name)
    distinctive = [party for party in parties if not _is_common_party(party)]
    if distinctive and not strict:
        return " OR ".join(
            f"caseName:({' '.join(sorted(party))})" for party in distinctive
        )
    pool = distinctive or parties
    if pool:
        return f"caseName:({' '.join(sorted(set().union(*pool)))})"
    toks = _name_tokens(name)
    return f"caseName:({' '.join(toks)})" if toks else (name or "").strip()


def _cl_name_search(
    client,
    name: str,
    court_ids: Optional[str],
    *,
    page_size: int = 20,
    limit: int = 3,
    spare: int = 0,
    drop_scotus_orders: bool = False,
    order_by_citecount: bool = False,
    strict: bool = False,
) -> list[dict]:
    query = _cl_casename_query(name, strict=strict)
    if not query:
        return []
    extra = {"order_by": "citeCount desc"} if order_by_citecount else None
    data = client.search(
        query,
        type="o",
        court=court_ids or None,
        page_size=page_size,
        extra=extra,
    )
    results = data.get("results") or []
    if drop_scotus_orders:
        results = [item for item in results if not _is_scotus_order_item(item)]

    scored: list[tuple[int, float, int, dict]] = []
    for item in results:
        candidate = strip_html(item.get("caseName") or item.get("case_name") or "")
        score = name_match_score(name, candidate)
        if score >= _NAME_MATCH_MIN:
            scored.append((
                _match_tier(name, candidate),
                score,
                item.get("citeCount") or 0,
                item,
            ))
    scored.sort(key=lambda row: (row[0], row[1], row[2]), reverse=True)
    kept = scored[: limit + spare]
    kept += [
        row for row in scored[limit + spare:]
        if row[0] == 2 and row[2] >= _REVERSED_PARTY_MIN_CITES
    ]
    return [item for _tier, _score, _cites, item in kept]


def courtlistener_name_ranked_search(client, query: str) -> list[tuple[str, dict]]:
    """Return name-ranked CourtListener ``(bucket, item)`` pairs."""
    juris = _detect_jurisdiction(query)
    if juris:
        court_ids, name, _label = juris
        items = _cl_name_search(
            client,
            name or query,
            court_ids,
            limit=3,
            spare=2,
            drop_scotus_orders=(court_ids == _SCOTUS_COURT_ID),
        )
        return _filter_to_best_tier(name or query, [("juris", item) for item in items])

    passes = [
        ("ranked", None, 4, False, True, False),
        ("scotus", _SCOTUS_COURT_ID, 3, True, False, False),
    ]
    if len([party for party in _name_parties(query) if not _is_common_party(party)]) >= 2:
        passes.insert(0, ("exact", None, 3, False, True, True))

    groups: list[list[tuple[str, dict]]] = [[] for _ in passes]

    def run_pass(index: int) -> None:
        bucket, court_ids, limit, drop, by_cites, strict = passes[index]
        groups[index] = [
            (bucket, item)
            for item in _cl_name_search(
                client,
                query,
                court_ids,
                limit=limit,
                spare=2,
                drop_scotus_orders=drop,
                order_by_citecount=by_cites,
                strict=strict,
            )
        ]

    with ThreadPoolExecutor(max_workers=len(passes)) as executor:
        futures = [executor.submit(run_pass, index) for index in range(len(passes))]
        for _future in as_completed(futures):
            pass

    tagged: list[tuple[str, dict]] = []
    for group in groups:
        tagged.extend(group)
    return _filter_to_best_tier(query, tagged)


def _item_from_cluster(cluster: dict) -> dict:
    item = {
        "cluster_id": cluster.get("id"),
        "caseName": cluster.get("case_name") or cluster.get("case_name_full") or "",
        "citation": cluster_citations_to_strings(cluster.get("citations")),
        "dateFiled": cluster.get("date_filed") or "",
    }
    court = cluster.get("court_id") or cluster.get("court") or ""
    if isinstance(court, str) and court and "/" not in court:
        item["court_id"] = court
    return item


def _is_citation_query(query: str) -> bool:
    return parse_citation_line(query or "") is not None


def _normalize_citation_key(cite: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", cite.lower())


def _item_has_citation(item: dict, cite: str) -> bool:
    want = _normalize_citation_key(cite)
    return any(_normalize_citation_key(candidate) == want for candidate in citation_list(item.get("citation")))


def _courtlistener_citation_lookup_items(client, cite: str) -> list[dict]:
    results: list[dict] = []
    try:
        for entry in client.lookup_citation(cite):
            if entry.get("status") != 200:
                continue
            for cluster in entry.get("clusters") or []:
                item = _item_from_cluster(cluster)
                if item.get("cluster_id"):
                    results.append(item)
    except Exception:
        return []
    return results


def _courtlistener_citation_search_items(client, cite: str) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for query in (f"citation:({cite})", f'"{cite}"'):
        try:
            data = client.search(query, type="o", page_size=10)
        except Exception:
            continue
        for item in data.get("results") or []:
            normalize_result_citations(item)
            key = str(item.get("cluster_id") or item.get("absolute_url") or item.get("caseName"))
            if key in seen or not _item_has_citation(item, cite):
                continue
            seen.add(key)
            out.append(item)
    return out


def _spotlight_from_cl_item(bucket: str, item: dict) -> SpotlightResult:
    normalize_result_citations(item)
    title = case_name(item, "")
    cite = pick_citation(item.get("citation", []))
    date_filed = str(item.get("dateFiled") or item.get("date_filed") or "")
    court_id = str(item.get("court_id") or item.get("court") or "").strip().lower()
    year = date_filed[:4] if len(date_filed) >= 4 else ""
    detail_bits = [court_label(court_id), cite, year]
    return SpotlightResult(
        source="courtlistener",
        bucket=bucket,
        title=title,
        cite=cite,
        year=year,
        court_id=court_id,
        detail=" | ".join(bit for bit in detail_bits if bit),
        payload=item,
    )


def courtlistener_spotlight_results(client, query: str) -> list[SpotlightResult]:
    if client is None:
        return []
    parsed = parse_citation_line(query or "")
    if parsed is not None:
        _name, cite, _pin = parsed
        results = _courtlistener_citation_lookup_items(client, cite)
        if not results:
            results = _courtlistener_citation_search_items(client, cite)
        return [
            _spotlight_from_cl_item("cl", item)
            for item in results
            if not _is_scotus_order_item(item)
        ][:4]

    try:
        tagged = courtlistener_name_ranked_search(client, query)
    except Exception:
        return []
    return [_spotlight_from_cl_item(bucket, item) for bucket, item in tagged]


def cache_spotlight_results(query: str, db=None) -> list[SpotlightResult]:
    if OpinionDB is None and db is None:
        return []
    created_db = db is None
    database = db or OpinionDB()
    try:
        candidates = (
            database.find(query)
            if _is_citation_query(query)
            else database.search_names(query, limit=60)
        )
    except Exception:
        return []
    finally:
        if created_db and hasattr(database, "close"):
            database.close()

    scored: list[tuple[int, float, str, dict]] = []
    for record in candidates:
        title = str(record.get("name") or "")
        if _is_citation_query(query):
            score = 1.0
            tier = 3
        else:
            score = name_match_score(query, title)
            tier = _match_tier(query, title)
        if score >= _NAME_MATCH_MIN:
            scored.append((tier, score, str(record.get("year") or ""), record))
    scored.sort(key=lambda row: (row[0], row[1], row[2]), reverse=True)

    out: list[SpotlightResult] = []
    for _tier, _score, _year_key, record in scored[:6]:
        cite = str(record.get("cite") or pick_citation(record.get("cites") or []))
        year = str(record.get("year") or "")
        court_id = str(record.get("court") or "")
        detail_bits = [court_label(court_id), cite, year]
        out.append(
            SpotlightResult(
                source="cache",
                bucket="cache",
                title=str(record.get("name") or ""),
                cite=cite,
                year=year,
                court_id=court_id,
                detail=" | ".join(bit for bit in detail_bits if bit),
                payload=record,
            )
        )
    return out


def _scholar_source_segments(source: str) -> list[str]:
    segments = [part.strip() for part in re.split(r"\s+-\s+", source or "") if part.strip()]
    while segments and segments[-1].lower() == "google scholar":
        segments.pop()
    return segments


def _scholar_court_desc_to_id(desc: str) -> str:
    desc = re.sub(r"\s+", " ", desc or "").strip().rstrip(".")
    if not desc:
        return ""
    match = re.match(r"([A-Za-z][A-Za-z.]{0,5}):\s*(.+)$", desc)
    if match:
        key = match.group(1).replace(".", "").lower()
        courts = _SCHOLAR_STATE_PREFIX.get(key)
        if courts:
            return _classify_state_court(match.group(2), courts)
        desc = match.group(2)
    low = desc.lower()
    if low in {
        "supreme court",
        "us supreme court",
        "u.s. supreme court",
        "united states supreme court",
    } or ("supreme court" in low and "united states" in low):
        return "scotus"
    match = re.search(
        r"court of appeals,?\s*(?:for the\s+)?(\w+(?: of columbia)?)\s+circuit",
        low,
    )
    if match:
        word = match.group(1)
        if word == "federal":
            return "cafc"
        if "columbia" in word or word == "dc":
            return "cadc"
        return _CIRCUIT_WORD_IDS.get(word, "")
    return ""


def scholar_source_to_court_id(source: str) -> str:
    segments = _scholar_source_segments(source)
    if not segments:
        return ""
    court_year = re.sub(r",?\s*(1[6-9]\d{2}|20\d{2})\s*$", "", segments[-1])
    return _scholar_court_desc_to_id(court_year)


def scholar_source_year(source: str) -> str:
    segments = _scholar_source_segments(source)
    if segments:
        match = re.search(r"(1[6-9]\d{2}|20\d{2})\s*$", segments[-1])
        if match:
            return match.group(1)
    match = re.search(r"\b(1[6-9]\d{2}|20\d{2})\b", source or "")
    return match.group(1) if match else ""


def _normalize_scholar_cite(cite: str) -> str:
    cite = re.sub(r"\s+", " ", cite or "").strip().strip(",")
    match = re.match(r"^(\d+)\s+(.+?)\s+(\d+)$", cite)
    if not match:
        return cite
    volume, reporter, page = match.group(1), match.group(2), match.group(3)
    reporter = re.sub(r"\b([A-Z]{2,})\b", lambda m: ".".join(m.group(1)) + ".", reporter)
    reporter = re.sub(r"\bF\.\s+(\d+d|4th)\b", r"F.\1", reporter)
    return f"{volume} {reporter} {page}"


def scholar_result_cite(result) -> str:
    segments = _scholar_source_segments(getattr(result, "source", "") or "")
    cites: list[str] = []
    if len(segments) >= 2:
        for part in segments[0].split(","):
            part = part.strip()
            if not part or "..." in part:
                continue
            norm = _normalize_scholar_cite(part)
            if re.match(r"^\d+\s+.+\s+\d+$", norm):
                cites.append(norm)
    if cites:
        return pick_citation(cites)
    text = f"{getattr(result, 'title', '')} {getattr(result, 'snippet', '')}"
    parsed = parse_citation_line(text)
    if parsed:
        return parsed[1]
    return ""


def scholar_spotlight_results(fetcher, query: str) -> list[SpotlightResult]:
    if fetcher is None:
        return []
    try:
        results = fetcher.search_cases(query, limit=10)
    except Exception:
        return []

    if _is_citation_query(query):
        selected = results[:3]
    else:
        scored = [
            (name_match_score(query, getattr(result, "title", "") or ""), result)
            for result in results
        ]
        scored = [(score, result) for score, result in scored if score >= _NAME_MATCH_MIN]
        scored.sort(key=lambda row: row[0], reverse=True)
        selected = [result for _score, result in scored[:4]]

    out: list[SpotlightResult] = []
    for result in selected:
        source = getattr(result, "source", "") or ""
        court_id = scholar_source_to_court_id(source)
        year = scholar_source_year(source)
        cite = scholar_result_cite(result)
        detail_bits = [court_label(court_id), cite, year]
        out.append(
            SpotlightResult(
                source="scholar",
                bucket="scholar",
                title=getattr(result, "title", "") or "",
                cite=cite,
                year=year,
                court_id=court_id,
                detail=" | ".join(bit for bit in detail_bits if bit),
                payload=result,
            )
        )
    return out


def english_reports_spotlight_results(query: str) -> list[SpotlightResult]:
    """Offline English Reports name search for spotlight."""
    try:
        cases = eng_rep.search_by_name(query, limit=1)
    except Exception:
        return []
    return [
        SpotlightResult(
            source="engrep",
            bucket="engrep",
            title=case.name or case.label,
            cite=case.er_cite,
            year="",
            court_id="engrep",
            detail=f"{case.er_cite} | {case.neutral}",
            payload=case,
        )
        for case in cases
    ]


def spotlight_search(query: str, *, client=None, fetcher=None, db=None) -> list[SpotlightResult]:
    query = (query or "").strip()
    if not query:
        return []
    courtlistener = courtlistener_spotlight_results(client, query) if client is not None else []
    cache = cache_spotlight_results(query, db)
    scholar = scholar_spotlight_results(fetcher, query) if fetcher is not None else []
    english_reports = english_reports_spotlight_results(query)
    return merge_results(courtlistener, cache, scholar, english_reports)
