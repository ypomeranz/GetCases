"""
Related cases via CourtListener: appeals, decisions below, and remands
======================================================================

CourtListener has no Shepard's/KeyCite-style subsequent-history table, so a
case's appellate family has to be reassembled from signals scattered across
its APIs.  The ones used here were each verified against the live v4 API
(the hiQ v. LinkedIn chain, People v. Sanchez, Van Buren v. United States):

* **RECAP originating-court info** — a federal appellate docket merged from
  PACER carries ``original_court_info`` with the district docket number and
  the judgment / notice-of-appeal dates.  Direct appellate→district link.
* **Docket-number full-text search** — circuit opinions print the district
  docket number in the caption ("D.C. No. 3:17-cv-03301"), and the district
  case's own docket number is indexed too, so one phrase search for
  ``"17-cv-03301"`` surfaces the district decision *and* every appeal
  (including a second appeal after remand).  The same trick links a
  California Court of Appeal docket number ("G047666") to the Supreme
  Court opinion that prints it in its caption ("Ct.App. 4/3 G047666").
* **District docket entries** — the RECAP entry log narrates the appeal:
  "NOTICE OF APPEAL to the 9th Circuit", "OPINION of USCA … We AFFIRM",
  "MANDATE of USCA", "ORDER of U.S. Supreme Court" (this is the only
  place a GVR or cert denial shows up — CourtListener has no opinion
  document for those orders).
* **Citation search up the hierarchy** — the reviewing court's opinion
  nearly always cites the decision below, so a quoted-phrase search for
  this case's reporter cite (and a ``cites:(cluster)`` query) restricted
  to the courts above it finds the review; the same search restricted to
  the courts *below*, after this case's date, finds remand proceedings.

Signals that were tested and DON'T work, so they are not used:
``Court.appeals_to`` (populated for 6 courts out of ~470), the parsed
citation graph between a circuit panel and its own earlier opinion in the
same case (misses), and the CAP free-text ``history``/``posture`` cluster
fields (empty in every sample).

Everything here is court-hierarchy-aware but heuristic: candidates must
also pass a party-name relatedness test before they are reported, and each
entry says which signals produced it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from court_catalog import (
    CIRCUIT_COURTS,
    COURT_BLUEBOOK,
    STATE_COURTS,
)

# ----------------------------------------------------------------------
# Court hierarchy
# ----------------------------------------------------------------------

# Geographic circuit for each federal district (fixed by 28 U.S.C. §§ 41,
# 1294 — not available from the API: Court.appeals_to is unpopulated).
CIRCUIT_OF_DISTRICT: dict[str, str] = {}
for _cir, _dists in {
    "ca1": ("med", "mad", "nhd", "prd", "rid"),
    "ca2": ("ctd", "nyed", "nynd", "nysd", "nywd", "vtd"),
    "ca3": ("ded", "njd", "paed", "pamd", "pawd", "vid"),
    "ca4": ("mdd", "nced", "ncmd", "ncwd", "scd", "vaed", "vawd",
            "wvnd", "wvsd"),
    "ca5": ("laed", "lamd", "lawd", "msnd", "mssd", "txed", "txnd",
            "txsd", "txwd"),
    "ca6": ("kyed", "kywd", "mied", "miwd", "ohnd", "ohsd", "tned",
            "tnmd", "tnwd"),
    "ca7": ("ilcd", "ilnd", "ilsd", "innd", "insd", "wied", "wiwd"),
    "ca8": ("ared", "arwd", "iand", "iasd", "mnd", "moed", "mowd",
            "ned", "ndd", "sdd"),
    "ca9": ("akd", "azd", "cacd", "caed", "cand", "casd", "gud", "hid",
            "idd", "mtd", "nvd", "nmid", "ord", "waed", "wawd"),
    "ca10": ("cod", "ksd", "nmd", "oked", "oknd", "okwd", "utd", "wyd"),
    "ca11": ("almd", "alnd", "alsd", "flmd", "flnd", "flsd", "gamd",
             "gand", "gasd"),
    "cadc": ("dcd",),
}.items():
    for _d in _dists:
        CIRCUIT_OF_DISTRICT[_d] = _cir

# Specialized federal courts whose appeals go to a single circuit.
SPECIAL_UP: dict[str, str] = {
    "uscfc": "cafc", "cit": "cafc", "cavet": "cafc",
    "bap1": "ca1", "bap2": "ca2", "bap6": "ca6", "bap8": "ca8",
    "bap9": "ca9", "bap10": "ca10",
}

# state court id -> (state name, index in that state's court list);
# index 0 is the court of last resort (STATE_COURTS ordering).
_STATE_POS: dict[str, tuple[str, int]] = {}
_STATE_LISTS: dict[str, list[str]] = {}
for _state, _courts in STATE_COURTS:
    _ids = [c[0] for c in _courts]
    _STATE_LISTS[_state] = _ids
    for _i, _cid in enumerate(_ids):
        _STATE_POS[_cid] = (_state, _i)


def _court_label(court_id: str) -> str:
    if court_id == "scotus":
        return "U.S. Supreme Court"
    return COURT_BLUEBOOK.get(court_id, court_id)


def _rank(court_id: str) -> int:
    """Coarse height in the hierarchy (bigger = higher court)."""
    if court_id == "scotus":
        return 4
    if court_id in CIRCUIT_COURTS:
        return 3
    if court_id in CIRCUIT_OF_DISTRICT or court_id in SPECIAL_UP:
        return 1
    pos = _STATE_POS.get(court_id)
    if pos is not None:
        # Within a state, earlier in the list = higher court.
        state, idx = pos
        return len(_STATE_LISTS[state]) - idx
    return 0


# ----------------------------------------------------------------------
# Results
# ----------------------------------------------------------------------

REL_REVIEWED = "Reviewed on appeal"
REL_BELOW = "Decision below"
REL_REMAND = "After remand"
REL_SAME = "Same case, same court"

REL_ORDER = (REL_REVIEWED, REL_BELOW, REL_REMAND, REL_SAME)


@dataclass
class RelatedCase:
    relation: str
    case_name: str
    court_id: str
    date: str = ""                    # ISO date filed
    citation: str = ""
    docket_number: str = ""
    cluster_id: Optional[int] = None
    url: str = ""                     # courtlistener.com fallback
    signals: set = field(default_factory=set)

    # Signals that pin the same case (docket-number grade) vs. ones that
    # merely make it probable (a name-gated citation or name search).
    _STRONG = {"originating-court", "docket-number", "caption-number"}

    @property
    def confidence(self) -> str:
        if self.signals & self._STRONG or len(self.signals) >= 2:
            return "confirmed"
        return "likely"

    @property
    def court_label(self) -> str:
        return _court_label(self.court_id)


@dataclass
class Lineage:
    related: list                     # [RelatedCase], grouped/sorted
    events: list                      # [(iso-date, text)] docket history
    notes: list                       # diagnostics for the panel footer


# ----------------------------------------------------------------------
# Party-name relatedness
# ----------------------------------------------------------------------
# An appeal keeps the parties but may flip the caption (appellant first),
# swap "United States" styles, or drop co-parties, so exact-name matching
# is useless and substring matching too loose.  Compare per-side token
# sets instead, in either order, ignoring procedural words.

_NAME_STOP = frozenset("""
    the of and in re ex rel et al a an on behalf matter estate
    appellant appellee petitioner respondent plaintiff defendant
    inc llc llp corp co ltd lp na fsb company corporation incorporated
    others
""".split())

# Parties so generic they match almost anything — a "People v." overlap
# alone must never link two different prosecutions.  A bare state name is
# generic too: the same prosecution captions as "State v. Glover" at home
# and "Kansas v. Glover" in the U.S. Supreme Court.
_GENERIC_FILLER = frozenset({
    "people", "state", "united", "states", "us", "government",
    "commonwealth", "commissioner", "america",
})


def _name_tokens(part: str) -> frozenset:
    part = re.sub(r"<[^>]+>", " ", part)
    toks = re.findall(r"[a-z][a-z0-9'&.-]*", part.lower())
    return frozenset(
        t.strip(".-'") for t in toks
        if t.strip(".-'") and t.strip(".-'") not in _NAME_STOP
        and len(t.strip(".-'")) > 1
    )


def _name_sides(name: str) -> list[frozenset]:
    parts = re.split(r"\bv(?:s?\.|\b)", name or "", maxsplit=1,
                     flags=re.IGNORECASE)
    sides = [_name_tokens(p) for p in parts]
    return [s for s in sides if s]


def _tokens_close(a: str, b: str) -> bool:
    return a == b or (len(a) > 3 and len(b) > 3
                      and (a.startswith(b) or b.startswith(a)))


def _side_overlap(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    hits = sum(1 for t in a if any(_tokens_close(t, u) for u in b))
    return hits / min(len(a), len(b))


# Token sets of every state name ({"kansas"}, {"new", "york"}, …); a
# caption side equal to one (after filler words) is a generic prosecutor.
_STATE_NAME_SIDES: tuple = tuple(
    _name_tokens(_state) for _state, _ in STATE_COURTS)


def _side_generic(s: frozenset) -> bool:
    core = s - _GENERIC_FILLER
    if not core:
        return True
    return any(core == g for g in _STATE_NAME_SIDES)


def _pair_match(x: frozenset, y: frozenset,
                threshold: float = 0.5) -> bool:
    """One caption side against another: generic prosecutors pair with
    each other ("People"/"State"/"United States" restyled on appeal), a
    generic side never pairs with a named party, and named parties need
    real token overlap."""
    gx, gy = _side_generic(x), _side_generic(y)
    if gx or gy:
        return gx and gy
    return _side_overlap(x, y) >= threshold


def names_related(a: str, b: str, strict: bool = False) -> bool:
    """True when *a* and *b* plausibly caption the same case.

    The default is deliberately loose — one matching *distinctive* party
    is enough, since appeals recaption cases (consolidation, substituted
    officials) — and is meant to gate candidates that some other signal
    (a shared docket number, a citation between the opinions) already
    ties to this case.  ``strict`` demands that both sides of a two-party
    caption match in some order; use it when the name itself is the only
    evidence (the date-window search)."""
    sa, sb = _name_sides(a), _name_sides(b)
    if not sa or not sb:
        return False
    if strict and len(sa) == 2 and len(sb) == 2:
        # Name-only evidence: both sides must match in some order, and a
        # named-party pair needs a solid majority of its tokens ("United
        # States v. Nathan Van Buren" must not strict-match "United
        # States v. Nathan Smith" on the shared first name alone).
        return any(
            _pair_match(sa[0], sb[i], 0.6)
            and _pair_match(sa[1], sb[1 - i], 0.6)
            for i in (0, 1))
    best_distinct = 0.0
    for x in sa:
        for y in sb:
            ov = _side_overlap(x, y)
            if ov >= 0.5 and not (_side_generic(x) or _side_generic(y)):
                best_distinct = max(best_distinct, ov)
    if best_distinct:
        return True
    # Both captions may be generic-led ("People v. Sanchez"): require the
    # non-generic sides to match instead.
    if len(sa) == 2 and len(sb) == 2:
        ga = [_side_generic(s) for s in sa]
        gb = [_side_generic(s) for s in sb]
        if any(ga) and any(gb):
            xa = sa[ga.index(False)] if False in ga else None
            xb = sb[gb.index(False)] if False in gb else None
            if xa and xb:
                return _side_overlap(xa, xb) >= 0.5
    return False


# ----------------------------------------------------------------------
# Citation / docket-number plumbing
# ----------------------------------------------------------------------

_FED_DOCKET_RE = re.compile(
    r"(?<![\d-])(\d{2})[-–:]?\s?(cv|cr|civ|crim|mc|md)[-–.]?\s?0*(\d{1,5})",
    re.IGNORECASE)

# "D.C. No. 3:17-cv-03301-EMC" (CA9) / "D.C. Docket No. 1:16-cr-00243"
# (CA11) / "D.C. Nos. …" on a circuit caption.
_DC_NO_RE = re.compile(
    r"D\.?\s?C\.?\s?(?:Docket\s+)?Nos?\.?\s{0,3}([\w:.\-–, ]{5,60})")

# Cal. Supreme caption: "Ct.App. 4/3 G047666" (+ "Super. Ct. No. 11CF2839").
_CAL_CTAPP_RE = re.compile(r"Ct\.\s?App\.\s?(\d)(?:/(\d+))?\s+([A-Z]\d{6})")
_CAL_SUPER_RE = re.compile(r"Super\.?\s?Ct\.?\s?Nos?\.?\s?([A-Z0-9]{5,14})")

# A reporter citation loose enough to sweep an opinion head; every hit is
# then resolved through citation-lookup and name-gated, so precision comes
# later.
_ANY_CITE_RE = re.compile(
    r"\b(\d{1,4})\s+"
    r"((?:[A-Z][A-Za-z.']{0,8}\s){0,3}?"
    r"(?:[A-Z][A-Za-z.']{0,8}|2d|3d|4th|5th))\.?\s+"
    r"(\d{1,5})\b")


def _fed_docket_variants(raw: str) -> list[str]:
    """Search phrases for a federal docket number: as padded in captions
    ("17-cv-03301") plus the unpadded form districts sometimes print."""
    m = _FED_DOCKET_RE.search(raw or "")
    if not m:
        return []
    yy, kind, num = m.group(1), m.group(2).lower(), int(m.group(3))
    kind = {"civ": "cv", "crim": "cr"}.get(kind, kind)
    out = [f"{yy}-{kind}-{num:05d}", f"{yy}-{kind}-{num}"]
    return list(dict.fromkeys(out))


def _variants_from_core(core: str) -> list[str]:
    """docket_number_core ("1703301") → search phrases, type unknown."""
    if not re.fullmatch(r"\d{7}", core or ""):
        return []
    yy, num = core[:2], int(core[2:])
    out = []
    for kind in ("cv", "cr"):
        out += [f"{yy}-{kind}-{num:05d}", f"{yy}-{kind}-{num}"]
    return list(dict.fromkeys(out))


def _cite_strings(cluster: dict) -> list[str]:
    out = []
    for c in cluster.get("citations") or []:
        try:
            out.append(f"{c['volume']} {c['reporter']} {c['page']}")
        except Exception:
            continue
    return out


def _norm_cite(c: str) -> str:
    return re.sub(r"[\s.]", "", str(c)).lower()


def _pick_cite(cites: list) -> str:
    for c in cites or []:
        if str(c).strip():
            return str(c).strip()
    return ""


# ----------------------------------------------------------------------
# Self-resolution
# ----------------------------------------------------------------------

class _Ctx:
    """Everything known about the open case, CL-enriched when possible."""

    def __init__(self) -> None:
        self.cluster_ids: set[int] = set()
        self.opinion_ids: list[int] = []
        self.case_name = ""
        self.court_id = ""
        self.date = ""            # ISO date filed ("" when unknown)
        self.docket_id: Optional[int] = None
        self.docket_number = ""
        self.docket_core = ""
        self.cites: list[str] = []
        self.cite_keys: set[str] = set()
        self.text = ""

    def is_me(self, item: dict) -> bool:
        cid = item.get("cluster_id") or item.get("id")
        try:
            if cid is not None and int(cid) in self.cluster_ids:
                return True
        except (TypeError, ValueError):
            pass
        for c in item.get("citation") or []:
            if _norm_cite(c) in self.cite_keys:
                return True
        return False


def _resolve_self(client, cluster_id, case_name, court_id, citations,
                  date_filed, docket_number, opinion_text,
                  notes: list) -> _Ctx:
    ctx = _Ctx()
    ctx.case_name = case_name or ""
    ctx.court_id = (court_id or "").lower()
    ctx.date = (date_filed or "")[:10]
    ctx.docket_number = docket_number or ""
    ctx.cites = [c for c in (citations or []) if str(c).strip()]
    ctx.text = opinion_text or ""

    cluster = None
    if cluster_id:
        try:
            cluster = client.get_cluster(
                int(cluster_id),
                fields="id,case_name,date_filed,docket_id,citations,"
                       "sub_opinions,absolute_url")
        except Exception as exc:
            notes.append(f"Could not fetch this case's record: {exc}")
    if cluster is None and ctx.cites:
        # Locate this case on CourtListener from its reporter cite(s).
        try:
            for hit in client.lookup_citation("; ".join(ctx.cites[:3])):
                for cl in hit.get("clusters") or []:
                    nm = cl.get("case_name") or ""
                    if (not ctx.case_name or not nm
                            or names_related(ctx.case_name, nm)):
                        cluster = client.get_cluster(
                            int(cl["id"]),
                            fields="id,case_name,date_filed,docket_id,"
                                   "citations,sub_opinions,absolute_url")
                        break
                if cluster is not None:
                    break
        except Exception as exc:
            notes.append(f"Citation lookup failed: {exc}")

    if cluster is not None:
        ctx.cluster_ids.add(int(cluster["id"]))
        ctx.case_name = ctx.case_name or (cluster.get("case_name") or "")
        ctx.date = ctx.date or (cluster.get("date_filed") or "")[:10]
        ctx.cites = list(dict.fromkeys(
            ctx.cites + _cite_strings(cluster)))
        for u in cluster.get("sub_opinions") or []:
            m = re.search(r"/(\d+)/?$", str(u))
            if m:
                ctx.opinion_ids.append(int(m.group(1)))
        did = cluster.get("docket_id")
        if did:
            ctx.docket_id = int(did)
            try:
                dk = client.get_docket(
                    int(did),
                    fields="court_id,docket_number,docket_number_core")
                ctx.court_id = ctx.court_id or (dk.get("court_id") or "")
                ctx.docket_number = (ctx.docket_number
                                     or dk.get("docket_number") or "")
                ctx.docket_core = dk.get("docket_number_core") or ""
            except Exception:
                pass
    ctx.cite_keys = {_norm_cite(c) for c in ctx.cites}
    return ctx


# ----------------------------------------------------------------------
# Candidate collection
# ----------------------------------------------------------------------

class _Cands:
    """Related-case accumulator: dedupes CL's duplicate clusters (same
    court + same citation, or same cluster id) and merges signals."""

    def __init__(self, ctx: _Ctx) -> None:
        self._ctx = ctx
        self._by_key: dict = {}

    def _key(self, court_id, citation, cluster_id):
        if citation:
            return (court_id, _norm_cite(citation))
        return (court_id, f"cluster:{cluster_id}")

    def add(self, relation: str, item: dict, signal: str) -> None:
        """*item* is search-result-shaped (caseName/court_id/dateFiled/
        citation/cluster_id/docketNumber/absolute_url)."""
        if self._ctx.is_me(item):
            return
        court = str(item.get("court_id") or item.get("court") or "").lower()
        name = re.sub(r"<[^>]+>", "", str(
            item.get("caseName") or item.get("case_name") or "")).strip()
        cid = item.get("cluster_id") or item.get("id")
        try:
            cid = int(cid) if cid is not None else None
        except (TypeError, ValueError):
            cid = None
        cite = _pick_cite(item.get("citation") or [])
        date = str(item.get("dateFiled") or item.get("date_filed")
                   or "")[:10]
        url = str(item.get("absolute_url") or "")
        if url and url.startswith("/"):
            url = "https://www.courtlistener.com" + url
        key = self._key(court, cite, cid)
        rc = self._by_key.get(key)
        if rc is None:
            rc = RelatedCase(
                relation=relation, case_name=name or "(untitled)",
                court_id=court, date=date, citation=cite,
                docket_number=str(item.get("docketNumber")
                                  or item.get("docket_number") or ""),
                cluster_id=cid, url=url)
            self._by_key[key] = rc
        else:
            rc.signals = set(rc.signals)
            if not rc.citation and cite:
                rc.citation = cite
            if not rc.date and date:
                rc.date = date
            if rc.cluster_id is None:
                rc.cluster_id = cid
        rc.signals.add(signal)

    def has(self, *relations: str) -> bool:
        return any(rc.relation in relations
                   for rc in self._by_key.values())

    def results(self) -> list:
        out = list(self._by_key.values())
        order = {r: i for i, r in enumerate(REL_ORDER)}
        out.sort(key=lambda rc: (order.get(rc.relation, 9),
                                 rc.date or "9999"))
        return out


def _relation_for(ctx: _Ctx, court_id: str, date: str) -> str:
    mine, theirs = _rank(ctx.court_id), _rank(court_id)
    if court_id == ctx.court_id:
        return REL_SAME
    if theirs > mine:
        return REL_REVIEWED
    if ctx.date and date and date > ctx.date:
        return REL_REMAND
    return REL_BELOW


# ----------------------------------------------------------------------
# Shared searches
# ----------------------------------------------------------------------

def _fts(client, query: str, court: str | None = None, **kw) -> list[dict]:
    try:
        return client.search(query, type="o", court=court,
                             page_size=20, **kw).get("results") or []
    except Exception:
        return []


def _docket_number_fts(client, ctx: _Ctx, cands: _Cands,
                       variants: list[str], allowed: dict) -> None:
    """One phrase search per docket-number spelling; *allowed* maps a
    court id (or "" for same-court) to whether a hit there counts."""
    if not variants:
        return
    q = " OR ".join(f'"{v}"' for v in variants[:4])
    for hit in _fts(client, q):
        court = str(hit.get("court_id") or "").lower()
        name = str(hit.get("caseName") or "")
        if court == ctx.court_id:
            # Same court + same number = the same case's other decisions.
            if ctx.case_name and not names_related(ctx.case_name, name):
                continue
            cands.add(REL_SAME, hit, "docket-number")
        elif court in allowed:
            if ctx.case_name and not names_related(ctx.case_name, name):
                continue
            rel = _relation_for(ctx, court,
                                str(hit.get("dateFiled") or "")[:10])
            cands.add(rel, hit, "docket-number")


def _upward_search(client, ctx: _Ctx, courts: list[str],
                   cands: _Cands) -> None:
    """Find the reviewing decision in the courts above: their opinion
    nearly always cites this case, so search this case's cite as a quoted
    phrase, plus the parsed citation graph (``cites:``) as a second net."""
    courts = [c for c in courts if c]
    if not courts:
        return
    court_q = " ".join(courts)

    def _consider(hit: dict) -> None:
        name = str(hit.get("caseName") or "")
        date = str(hit.get("dateFiled") or "")[:10]
        if ctx.case_name and not names_related(ctx.case_name, name):
            return
        if ctx.date and date and date < ctx.date:
            return  # review cannot predate the decision
        cands.add(REL_REVIEWED, hit, "citing-search")

    for cite in ctx.cites[:3]:
        for hit in _fts(client, f'"{cite}"', court=court_q):
            _consider(hit)
    if ctx.opinion_ids:
        ids = " OR ".join(str(i) for i in ctx.opinion_ids[:5])
        for hit in _fts(client, f"cites:({ids})", court=court_q):
            _consider(hit)


def _downward_from_text(client, ctx: _Ctx, cands: _Cands,
                        lower: list[str], window: int = 6000) -> None:
    """The decision below, from this opinion's own head: reviewing courts
    cite it in the caption/syllabus, so sweep the first stretch of text
    for reporter cites and keep the name-related ones from a lower
    court."""
    head = ctx.text[:window]
    if not head or not lower:
        return
    seen: set[str] = set()
    cites = []
    for m in _ANY_CITE_RE.finditer(head):
        c = f"{m.group(1)} {m.group(2)} {m.group(3)}"
        k = _norm_cite(c)
        if k in seen or k in ctx.cite_keys:
            continue
        seen.add(k)
        cites.append(c)
        if len(cites) >= 8:
            break
    if not cites:
        return
    try:
        hits = client.lookup_citation("; ".join(cites))
    except Exception:
        return
    lower_set = set(lower)
    for hit in hits:
        # 200 = resolved; 300 = several clusters bear the cite (CL keeps
        # duplicate clusters for many cases) — the name and court gates
        # below disambiguate, so consider each candidate.
        if hit.get("status") not in (200, 300):
            continue
        for cl in hit.get("clusters") or []:
            name = cl.get("case_name") or ""
            if not names_related(ctx.case_name, name):
                continue
            item = dict(cl)
            # citation-lookup clusters don't carry the court; fetch it
            # (plus the display fields the lookup response may omit).
            court = ""
            try:
                full = client.get_cluster(
                    int(cl["id"]),
                    fields="docket_id,case_name,date_filed,citations,"
                           "absolute_url")
                item.update({k: v for k, v in full.items() if v})
                item["citation"] = (_cite_strings(full)
                                    or [hit.get("citation") or ""])
                did = full.get("docket_id")
                if did:
                    dk = client.get_docket(int(did), fields="court_id")
                    court = (dk.get("court_id") or "").lower()
            except Exception:
                continue
            if court not in lower_set:
                continue
            item["court_id"] = court
            date = str(item.get("date_filed") or "")[:10]
            rel = (REL_REMAND if ctx.date and date and date > ctx.date
                   else REL_BELOW)
            cands.add(rel, item, "cited-below")


def _query_side_groups(name: str) -> list[list[str]]:
    """Distinctive party tokens for a fielded caseName query, grouped by
    caption side: up to two of each non-generic side's longest tokens.
    The query ORs within a side and ANDs across sides, so "Matter of
    Corey Krug v. City of Buffalo" still finds the court below's shorter
    "Matter of Krug v City of Buffalo" caption."""
    groups: list[list[str]] = []
    for side in _name_sides(name):
        if _side_generic(side):
            continue
        toks = [t for t in sorted(side, key=len, reverse=True)[:2]
                # Alphanumeric only: ES query_string operators would
                # misparse anything else.
                if re.fullmatch(r"[\w'-]+", t)]
        if toks:
            groups.append(toks)
    return groups


def _name_window_search(client, ctx: _Ctx, cands: _Cands,
                        courts: list[str], direction: str) -> None:
    """Probable related cases from the caption and calendar alone: the
    same party names in the courts above (``direction="up"``) or below
    (``"down"``), inside the window an appeal or remand plausibly falls
    in.  The weakest signal here — name-only hits pass the *strict*
    two-sided name test and still surface only as "probable match" — but
    it is the one signal that needs no docket number, citation, or RECAP
    coverage, so it reaches chains the others can't (New York's slip
    opinions, pre-PACER federal cases)."""
    courts = [c for c in courts if c and c != ctx.court_id]
    if not courts or not ctx.date or not ctx.case_name:
        return
    groups = _query_side_groups(ctx.case_name)
    if sum(len(g) for g in groups) < 2:
        # One bare surname against a prosecutor ("People v. Sanchez")
        # matches every other defendant with that name in the window —
        # name-only evidence needs at least two distinctive tokens.
        return
    year = int(ctx.date[:4])
    if direction == "up":
        lo, hi = ctx.date, f"{year + 6}-12-31"
    else:
        lo, hi = f"{year - 8}-01-01", f"{year + 6}-12-31"
    q = "caseName:(" + " AND ".join(
        "(" + " OR ".join(f'"{t}"' for t in g) + ")" for g in groups) + ")"
    for hit in _fts(client, q, court=" ".join(courts),
                    date_filed_min=lo, date_filed_max=hi):
        name = str(hit.get("caseName") or "")
        if not names_related(ctx.case_name, name, strict=True):
            continue
        court = str(hit.get("court_id") or "").lower()
        date = str(hit.get("dateFiled") or "")[:10]
        rel = (REL_REVIEWED if direction == "up"
               else _relation_for(ctx, court, date))
        cands.add(rel, hit, "name-window")


def _remand_search(client, ctx: _Ctx, cands: _Cands,
                   lower: list[str]) -> None:
    """Proceedings after this decision in the courts below: they cite it."""
    lower = [c for c in lower if c]
    if not lower or not ctx.cites or not ctx.date:
        return
    court_q = " ".join(lower)
    for cite in ctx.cites[:2]:
        for hit in _fts(client, f'"{cite}"', court=court_q,
                        date_filed_min=ctx.date):
            name = str(hit.get("caseName") or "")
            if ctx.case_name and not names_related(ctx.case_name, name):
                continue
            cands.add(REL_REMAND, hit, "citing-search")


# ----------------------------------------------------------------------
# Federal signals
# ----------------------------------------------------------------------

_EVENT_PATTERNS: list[tuple[re.Pattern, Callable[[re.Match], str]]] = [
    # Anchored: "MANDATE of USCA as to 72 Notice of Appeal…" must not read
    # as a fresh notice of appeal.
    (re.compile(r"^\s*(?:AMENDED )?NOTICE OF (?:INTERLOCUTORY |CROSS.?)?"
                r"APPEAL\b", re.IGNORECASE),
     lambda m: "Notice of appeal filed"),
    (re.compile(r"USCA Case Number\s+([\d-]+)", re.IGNORECASE),
     lambda m: f"Court of appeals case number {m.group(1)}"),
    (re.compile(r"\b(?:OPINION|JUDGMENT) of USCA", re.IGNORECASE),
     lambda m: "Court of appeals decision"),
    (re.compile(r"\bMANDATE of USCA", re.IGNORECASE),
     lambda m: "Court of appeals mandate issued"),
    (re.compile(r"ORDER of (?:the )?U\.?\s?S\.?\s?Supreme Court",
                re.IGNORECASE),
     lambda m: "U.S. Supreme Court order"),
    (re.compile(r"petition for (?:a )?writ of certiorari", re.IGNORECASE),
     lambda m: "Certiorari petition noted on district docket"),
]

_DISPOSITION_RE = re.compile(
    r"\b(AFFIRM\w*|REVERS\w*|VACAT\w*|REMAND\w*|DISMISS\w*)\b")


def _entry_events(entries: list[dict]) -> list[tuple[str, str]]:
    events: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for e in entries:
        desc = e.get("description") or ""
        if not desc:
            continue
        date = str(e.get("date_filed") or "")[:10]
        for pat, label in _EVENT_PATTERNS:
            m = pat.search(desc)
            if not m:
                continue
            text = label(m)
            if text == "Court of appeals decision":
                words = {w.upper() for w in
                         _DISPOSITION_RE.findall(desc.upper())}
                if words:
                    text += " — " + ", ".join(sorted(
                        w.lower() for w in words))
            key = (date, text)
            if key not in seen:
                seen.add(key)
                events.append(key)
    events.sort()
    return events


def _district_docket_events(client, docket_id: int, court: str = "",
                            docket_number: str = "",
                            max_pages: int = 30) -> list[tuple[str, str]]:
    """Appeal-related entries from a RECAP district docket.  Tries the
    RECAP-document search first (one call; the ``docket_id`` filter is
    ignored by type=rd, but court + docket number scope it); falls back
    to paging the entries endpoint (20/page, so capped)."""
    if court and docket_number:
        try:
            r = client.search(
                '"NOTICE OF APPEAL" OR "USCA" OR "MANDATE" OR '
                '"Supreme Court" OR certiorari',
                type="rd", court=court, page_size=20,
                extra={"docket_number": docket_number})
            hits = [d for d in r.get("results") or []
                    if d.get("docket_id") == docket_id]
            entries = [{"description": (d.get("description")
                                        or d.get("short_description")
                                        or ""),
                        "date_filed": d.get("entry_date_filed") or ""}
                       for d in hits]
            events = _entry_events(entries)
            if events:
                return events
        except Exception:
            pass
    entries = []
    try:
        url = None
        for _page in range(max_pages):
            if url is None:
                page = client._get("docket-entries/", {
                    "docket": docket_id, "page_size": 20,
                    "fields": "date_filed,description"})
            else:
                page = client._get_url(url)
            entries += page.get("results") or []
            url = page.get("next")
            if not url:
                break
    except Exception:
        pass
    return _entry_events(entries)


def _district_signals(client, ctx: _Ctx, cands: _Cands,
                      events: list, notes: list) -> None:
    circuit = CIRCUIT_OF_DISTRICT.get(ctx.court_id, "")
    # Canonical docket numbers: every CL docket in this court that shares
    # the core (scraper/CAP/RECAP duplicates carry different spellings).
    recap_docket = None
    variants: list[str] = _fed_docket_variants(ctx.docket_number)
    core = ctx.docket_core
    if not core and variants:
        m = _FED_DOCKET_RE.search(ctx.docket_number)
        if m:
            core = f"{m.group(1)}{int(m.group(3)):05d}"
    if not variants and not core:
        # Try the opinion text's own caption ("Case No. 17-cv-03301-EMC").
        m = _FED_DOCKET_RE.search(ctx.text[:2000])
        if m:
            variants = _fed_docket_variants(m.group(0))
            core = f"{m.group(1)}{int(m.group(3)):05d}"
    if core:
        try:
            dks = client.list_dockets(
                court=ctx.court_id, fields="id,docket_number,pacer_case_id",
                extra={"docket_number_core": core}).get("results") or []
            for dk in dks:
                variants += _fed_docket_variants(
                    dk.get("docket_number") or "")
                if dk.get("pacer_case_id") and recap_docket is None:
                    recap_docket = dk
        except Exception:
            pass
        variants += _variants_from_core(core)
    variants = list(dict.fromkeys(variants))

    if variants:
        allowed = {circuit: True, "cafc": True, "scotus": True}
        _docket_number_fts(client, ctx, cands, variants, allowed)
    else:
        notes.append("No docket number found for this case, so the "
                     "docket-number search for its appeal was skipped.")

    _upward_search(client, ctx, [circuit, "cafc", "scotus"], cands)
    if not cands.has(REL_REVIEWED):
        # Nothing tied by docket number or citation (common before PACER):
        # fall back to the caption + date window.
        _name_window_search(client, ctx, cands, [circuit, "cafc"], "up")

    if recap_docket is not None:
        events += _district_docket_events(
            client, int(recap_docket["id"]), court=ctx.court_id,
            docket_number=recap_docket.get("docket_number") or "")


def _circuit_signals(client, ctx: _Ctx, cands: _Cands,
                     events: list, notes: list) -> None:
    district_court = ""
    district_num = ""
    # RECAP's originating-court block on this court's docket for this
    # number — the direct link back to the district case.
    if ctx.docket_number:
        try:
            dks = client.list_dockets(
                court=ctx.court_id, docket_number=ctx.docket_number,
                fields="id,appeal_from,appeal_from_str,"
                       "original_court_info,date_terminated",
            ).get("results") or []
        except Exception:
            dks = []
        for dk in dks:
            oci = dk.get("original_court_info")
            if not isinstance(oci, dict):
                continue
            district_num = oci.get("docket_number") or district_num
            m = re.search(r"/courts/([a-z0-9]+)/",
                          str(dk.get("appeal_from") or ""))
            if m:
                district_court = m.group(1)
            for key, label in (
                    ("date_judgment", "District court judgment"),
                    ("date_filed_noa", "Notice of appeal filed"),
                    ("date_rehearing_denied", "Rehearing denied")):
                if oci.get(key):
                    events.append((str(oci[key])[:10], label))
            if dk.get("date_terminated"):
                events.append((str(dk["date_terminated"])[:10],
                               "Appeal terminated"))
            break

    # Fallback: the slip caption prints "D.C. No. 3:17-cv-03301".
    if not district_num:
        m = _DC_NO_RE.search(ctx.text[:3000])
        if m:
            district_num = m.group(1).split(",")[0].strip()

    my_districts = [d for d, c in CIRCUIT_OF_DISTRICT.items()
                    if c == ctx.court_id]
    variants = _fed_docket_variants(district_num)

    # Resolve the district case directly when RECAP told us the court.
    if district_court and district_num:
        core = ""
        m = _FED_DOCKET_RE.search(district_num)
        if m:
            core = f"{m.group(1)}{int(m.group(3)):05d}"
        recap_district = None
        try:
            dks = client.list_dockets(
                court=district_court,
                fields="id,docket_number,pacer_case_id",
                extra={"docket_number_core": core} if core else None,
                docket_number=None if core else district_num,
            ).get("results") or []
        except Exception:
            dks = []
        for dk in dks:
            if dk.get("pacer_case_id") and recap_district is None:
                recap_district = dk
            try:
                cls = client.list_clusters(
                    fields="id,case_name,date_filed,citations,"
                           "absolute_url",
                    extra={"docket": dk["id"]}).get("results") or []
            except Exception:
                continue
            for cl in cls:
                item = dict(cl)
                item["court_id"] = district_court
                item["citation"] = _cite_strings(cl)
                date = str(cl.get("date_filed") or "")[:10]
                rel = (REL_REMAND if ctx.date and date and date > ctx.date
                       else REL_BELOW)
                cands.add(rel, item, "originating-court")
        if recap_district is not None:
            events += _district_docket_events(
                client, int(recap_district["id"]), court=district_court,
                docket_number=recap_district.get("docket_number") or "")

    # The one-search workhorse: the district number appears in this
    # court's captions (all appeals in the case) and on the district
    # decisions themselves.
    if variants:
        allowed = {d: True for d in my_districts}
        allowed["scotus"] = True
        if district_court:
            allowed[district_court] = True
        _docket_number_fts(client, ctx, cands, variants, allowed)
    elif not district_num:
        notes.append("No originating-docket info on RECAP and no "
                     "\"D.C. No.\" in the caption — district-court "
                     "linkage limited to citation search.")

    _upward_search(client, ctx, ["scotus"], cands)
    _remand_search(client, ctx, cands,
                   [district_court] if district_court else my_districts)
    # Caption + date-window fallbacks for whatever direction is still
    # empty (older cases without RECAP dockets or indexed captions).
    if not cands.has(REL_BELOW, REL_REMAND):
        _name_window_search(
            client, ctx, cands,
            [district_court] if district_court else my_districts, "down")
    if not cands.has(REL_REVIEWED):
        _name_window_search(client, ctx, cands, ["scotus"], "up")


def _cert_source_courts(text: str) -> tuple[list[str], bool]:
    """The court(s) a Supreme Court case likely arrived from, read off
    the "On Writ of Certiorari to …" line — a federal circuit, or every
    court of the named state (review can reach an intermediate court,
    e.g. cert to the California Court of Appeal).  ``(all circuits,
    False)`` when the line is absent or unrecognized."""
    head = re.sub(r"\s+", " ", text[:8000].lower())
    m = re.search(r"certiorari to the ([^.;]{5,90})", head)
    if m:
        src = m.group(1)
        if "circuit" in src:
            for word, cid in (
                    ("first", "ca1"), ("second", "ca2"), ("third", "ca3"),
                    ("fourth", "ca4"), ("fifth", "ca5"), ("sixth", "ca6"),
                    ("seventh", "ca7"), ("eighth", "ca8"),
                    ("ninth", "ca9"), ("tenth", "ca10"),
                    ("eleventh", "ca11"),
                    ("district of columbia", "cadc"), ("federal", "cafc")):
                if word in src:
                    return [cid], True
        else:
            for state, ids in _STATE_LISTS.items():
                if state.lower() in src:
                    return list(ids), True
    return list(CIRCUIT_COURTS), False


def _scotus_signals(client, ctx: _Ctx, cands: _Cands,
                    events: list, notes: list) -> None:
    below_courts, specific = _cert_source_courts(ctx.text)
    # State cases reach the Court too; let the head-cite sweep look at
    # every state's top courts as well.
    state_tops = [ids[0] for ids in _STATE_LISTS.values()]

    # The syllabus ends "…940 F. 3d 1192, reversed and remanded", but a
    # long syllabus pushes that cite past the usual window — sweep deeper.
    _downward_from_text(client, ctx, cands, below_courts + state_tops,
                        window=16000)
    _remand_search(client, ctx, cands, below_courts)
    if specific and not cands.has(REL_BELOW):
        # The decision below has no reporter cite in the syllabus (or
        # none CourtListener resolves) — caption + date window against
        # the named source court.
        _name_window_search(client, ctx, cands, below_courts, "down")


# ----------------------------------------------------------------------
# State signals
# ----------------------------------------------------------------------

def _california_signals(client, ctx: _Ctx, cands: _Cands,
                        events: list, notes: list) -> None:
    if ctx.court_id == "cal":
        # The Supreme Court caption prints the Court of Appeal docket
        # number (and the superior-court number) — resolve it directly.
        for m in _CAL_CTAPP_RE.finditer(ctx.text[:4000]):
            num = m.group(3)
            try:
                dks = client.list_dockets(
                    court="calctapp", docket_number=num,
                    fields="id").get("results") or []
            except Exception:
                dks = []
            for dk in dks:
                try:
                    cls = client.list_clusters(
                        fields="id,case_name,date_filed,citations,"
                               "absolute_url",
                        extra={"docket": dk["id"]}).get("results") or []
                except Exception:
                    continue
                for cl in cls:
                    item = dict(cl)
                    item["court_id"] = "calctapp"
                    item["citation"] = _cite_strings(cl)
                    item["docket_number"] = num
                    date = str(cl.get("date_filed") or "")[:10]
                    rel = (REL_REMAND
                           if ctx.date and date and date > ctx.date
                           else REL_BELOW)
                    cands.add(rel, item, "caption-number")
        sm = _CAL_SUPER_RE.search(ctx.text[:4000])
        if sm:
            notes.append(f"Superior Court case number: {sm.group(1)}")
    elif ctx.court_id == "calctapp":
        # This case's own docket number ("G047666") appears in the
        # Supreme Court's caption if review was granted.
        num = ""
        m = (re.search(r"\b([A-Z]\d{6})\b", ctx.docket_number)
             or re.search(r"\b([A-Z]\d{6})\b", ctx.text[:2500]))
        if m:
            num = m.group(1)
        if num:
            for hit in _fts(client, f'"{num}"', court="cal"):
                name = str(hit.get("caseName") or "")
                if ctx.case_name and not names_related(ctx.case_name,
                                                       name):
                    continue
                cands.add(REL_REVIEWED, hit, "caption-number")
            # Sibling opinions under the same Court of Appeal number
            # (e.g. the opinion after remand reuses it).
            try:
                dks = client.list_dockets(
                    court="calctapp", docket_number=num,
                    fields="id").get("results") or []
            except Exception:
                dks = []
            for dk in dks:
                try:
                    cls = client.list_clusters(
                        fields="id,case_name,date_filed,citations,"
                               "absolute_url",
                        extra={"docket": dk["id"]}).get("results") or []
                except Exception:
                    continue
                for cl in cls:
                    item = dict(cl)
                    item["court_id"] = "calctapp"
                    item["citation"] = _cite_strings(cl)
                    item["docket_number"] = num
                    cands.add(REL_SAME, item, "caption-number")


def _state_signals(client, ctx: _Ctx, cands: _Cands,
                   events: list, notes: list) -> None:
    state, idx = _STATE_POS[ctx.court_id]
    ids = _STATE_LISTS[state]
    ups = ids[:idx]
    downs = ids[idx + 1:]

    if ctx.court_id in ("cal", "calctapp"):
        _california_signals(client, ctx, cands, events, notes)

    if ups or idx == 0:
        _upward_search(client, ctx, ups + (["scotus"] if idx == 0 else []),
                       cands)
    if downs:
        _downward_from_text(client, ctx, cands, downs)
        _remand_search(client, ctx, cands, downs)
    # Caption + date-window fallbacks — the workhorse for states whose
    # opinions don't print docket numbers or cite the decision below
    # (New York above all).
    if (ups or idx == 0) and not cands.has(REL_REVIEWED):
        _name_window_search(client, ctx, cands,
                            ups + (["scotus"] if idx == 0 else []), "up")
    if downs and not cands.has(REL_BELOW, REL_REMAND):
        _name_window_search(client, ctx, cands, downs, "down")
    if ctx.court_id == "ny" and not cands.has(REL_BELOW):
        notes.append("New York Court of Appeals slip opinions rarely "
                     "identify the Appellate Division decision below by "
                     "citation, and no caption match was found either.")


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------

def find_related(
    client,
    *,
    cluster_id: Any = None,
    case_name: str = "",
    court_id: str = "",
    citations: Any = (),
    date_filed: str = "",
    docket_number: str = "",
    opinion_text: str = "",
) -> Lineage:
    """Assemble the case's appellate family from CourtListener.

    Every argument is optional and best-effort — pass whatever the caller
    knows (``citations`` may be reporter-cite strings; ``opinion_text``
    enables the caption-based signals).  Network errors in any one signal
    degrade to the others.  Runs several API calls; call off the UI
    thread.
    """
    notes: list[str] = []
    events: list[tuple[str, str]] = []
    ctx = _resolve_self(client, cluster_id, case_name, court_id,
                        list(citations or ()), date_filed, docket_number,
                        opinion_text, notes)
    cands = _Cands(ctx)

    cid = ctx.court_id
    if not cid:
        notes.append("Could not determine this case's court, so only "
                     "citation-based signals were tried.")
        _upward_search(client, ctx, ["scotus"], cands)
    elif cid == "scotus":
        _scotus_signals(client, ctx, cands, events, notes)
    elif cid in CIRCUIT_COURTS:
        _circuit_signals(client, ctx, cands, events, notes)
    elif cid in CIRCUIT_OF_DISTRICT:
        _district_signals(client, ctx, cands, events, notes)
    elif cid in SPECIAL_UP:
        _upward_search(client, ctx, [SPECIAL_UP[cid], "scotus"], cands)
        if not cands.has(REL_REVIEWED):
            _name_window_search(client, ctx, cands, [SPECIAL_UP[cid]],
                                "up")
    elif cid in _STATE_POS:
        _state_signals(client, ctx, cands, events, notes)
    else:
        notes.append(f"No appeal-path rules for court “{cid}”; only "
                     "citation-based signals were tried.")
        _upward_search(client, ctx, ["scotus"], cands)

    events = sorted(set(events))
    return Lineage(related=cands.results(), events=events, notes=notes)
