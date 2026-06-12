"""Bluebook-style case-name abbreviation.

Implements rule 10.2.2 (case names in citations): abbreviate any word
listed in table T6 and any geographic unit in table T10, except a
geographic unit that is the entire name of a party ("United States v.
Nixon" but "U.S. Dep't of Just.").  The Indigo Book states the same rule
as R8.3 with tables T11/T12 and produces identical output.

The word list below is transcribed from table T6 (21st ed.), restricted
to its case-name entries — the overlapping periodical-title abbreviations
(Journal -> J., Law -> L., ...) are deliberately omitted because applying
them would mangle party names such as "Law v. Siegel".

Abbreviating is idempotent: already-abbreviated tokens ("Ass'n", "Inc.")
contain a period or apostrophe and never match a table key, so a name can
safely pass through twice.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Table T6 — case-name words (singular forms; plurals are derived below by
# the T6 plural rule: add "s" to the abbreviation, e.g. Cos., Ass'ns).
# Entries whose plural the table spells out differently are listed
# explicitly and suppress the derived form.
# ---------------------------------------------------------------------------

_T6_SINGULAR: dict[str, str] = {
    "academic": "Acad.", "academy": "Acad.",
    "administration": "Admin.", "administrative": "Admin.",
    "administrator": "Adm'r", "administratrix": "Adm'x",
    "advertising": "Advert.",
    "advocacy": "Advoc.", "advocate": "Advoc.",
    "african": "Afr.",
    "agricultural": "Agric.", "agriculture": "Agric.",
    "alliance": "All.",
    "alternative": "Alt.",
    "america": "Am.", "american": "Am.",
    "and": "&",
    "arbitration": "Arb.", "arbitrator": "Arb.",
    "associate": "Assoc.",
    "association": "Ass'n",
    "atlantic": "Atl.",
    "attorney": "Att'y",
    "authority": "Auth.",
    "automobile": "Auto.", "automotive": "Auto.",
    "avenue": "Ave.",
    "bankruptcy": "Bankr.",
    "behavioral": "Behav.",
    "board": "Bd.",
    "boulevard": "Blvd.",
    "british": "Brit.",
    "broadcast": "Broad.", "broadcaster": "Broad.", "broadcasting": "Broad.",
    "brotherhood": "Bhd.",
    "building": "Bldg.",
    "business": "Bus.",
    "capital": "Cap.",
    "casualty": "Cas.",
    "catholic": "Cath.",
    "center": "Ctr.", "centre": "Ctr.",
    "central": "Cent.",
    "chemical": "Chem.",
    "civil": "Civ.",
    "coalition": "Coal.",
    "college": "Coll.",
    "commerce": "Com.", "commercial": "Com.",
    "commission": "Comm'n",
    "commissioner": "Comm'r",
    "committee": "Comm.",
    "communication": "Commc'n",
    "community": "Cmty.",
    "company": "Co.",
    "compensation": "Comp.",
    "computer": "Comput.",
    "condominium": "Condo.",
    "conference": "Conf.",
    "congress": "Cong.", "congressional": "Cong.",
    "consolidated": "Consol.",
    "constitution": "Const.", "constitutional": "Const.",
    "construction": "Constr.",
    "continental": "Cont'l",
    "contract": "Cont.",
    "cooperation": "Coop.", "cooperative": "Coop.",
    "corporate": "Corp.", "corporation": "Corp.",
    "correction": "Corr.", "correctional": "Corr.",
    "cosmetic": "Cosm.",
    "counsel": "Couns.", "counselor": "Couns.",
    "county": "Cnty.",
    "court": "Ct.",
    "criminal": "Crim.",
    "defender": "Def.", "defense": "Def.",
    "delinquency": "Delinq.", "delinquent": "Delinq.",
    "department": "Dep't",
    "detention": "Det.",
    "developer": "Dev.", "development": "Dev.",
    "digital": "Digit.",
    "director": "Dir.",
    "discount": "Disc.",
    "dispute": "Disp.",
    "distributing": "Distrib.", "distribution": "Distrib.",
    "distributor": "Distrib.",
    "district": "Dist.",
    "division": "Div.",
    "east": "E.", "eastern": "E.",
    "economic": "Econ.", "economical": "Econ.", "economics": "Econ.",
    "economy": "Econ.",
    "education": "Educ.", "educational": "Educ.",
    "electric": "Elec.", "electrical": "Elec.", "electricity": "Elec.",
    "electronic": "Elec.",
    "employee": "Emp.", "employer": "Emp.", "employment": "Emp.",
    "enforcement": "Enf't",
    "engineer": "Eng'r",
    "engineering": "Eng'g",
    "enterprise": "Enter.",
    "entertainment": "Ent.",
    "environment": "Env't", "environmental": "Env't",
    "equality": "Equal.",
    "equipment": "Equip.",
    "estate": "Est.",
    "european": "Eur.",
    "examiner": "Exam'r",
    "exchange": "Exch.",
    "executive": "Exec.",
    "executor": "Ex'r", "executrix": "Ex'x",
    "exploration": "Expl.", "exploratory": "Expl.",
    "export": "Exp.", "exportation": "Exp.", "exporter": "Exp.",
    "faculty": "Fac.",
    "family": "Fam.",
    "federal": "Fed.",
    "federation": "Fed'n",
    "fidelity": "Fid.",
    "finance": "Fin.", "financial": "Fin.", "financing": "Fin.",
    "foundation": "Found.",
    "general": "Gen.",
    "global": "Glob.",
    "government": "Gov't",
    "group": "Grp.",
    "guarantor": "Guar.", "guaranty": "Guar.",
    "hospital": "Hosp.", "hospitality": "Hosp.",
    "housing": "Hous.",
    "human": "Hum.",
    "immigration": "Immigr.",
    "import": "Imp.", "importation": "Imp.", "importer": "Imp.",
    "incorporated": "Inc.",
    "indemnity": "Indem.",
    "independence": "Indep.", "independent": "Indep.",
    "industrial": "Indus.", "industry": "Indus.",
    "information": "Info.",
    "injury": "Inj.",
    "institute": "Inst.", "institution": "Inst.",
    "insurance": "Ins.",
    "intelligence": "Intel.",
    "international": "Int'l",
    "investment": "Inv.", "investor": "Inv.",
    "justice": "Just.",
    "juvenile": "Juv.",
    "labor": "Lab.",
    "laboratory": "Lab'y",
    "lawyer": "Law.",
    "liability": "Liab.",
    "limited": "Ltd.",
    "litigation": "Litig.",
    "local": "Loc.",
    "machine": "Mach.", "machinery": "Mach.",
    "maintenance": "Maint.",
    "management": "Mgmt.",
    "manufacturer": "Mfr.",
    "manufacturing": "Mfg.",
    "maritime": "Mar.",
    "market": "Mkt.",
    "marketing": "Mktg.",
    "mechanical": "Mech.",
    "medical": "Med.", "medicinal": "Med.", "medicine": "Med.",
    "memorial": "Mem'l",
    "merchandise": "Merch.", "merchandising": "Merch.", "merchant": "Merch.",
    "metropolitan": "Metro.",
    "military": "Mil.",
    "mineral": "Min.",
    "mortgage": "Mortg.",
    "municipal": "Mun.", "municipality": "Mun.",
    "mutual": "Mut.",
    "national": "Nat'l",
    "natural": "Nat.",
    "north": "N.", "northern": "N.",
    "northeast": "Ne.", "northeastern": "Ne.",
    "northwest": "Nw.", "northwestern": "Nw.",
    "number": "No.",
    "office": "Off.", "official": "Off.",
    "order": "Ord.",
    "organization": "Org.", "organizing": "Org.",
    "pacific": "Pac.",
    "parish": "Par.",
    "partnership": "P'ship",
    "patent": "Pat.",
    "personal": "Pers.", "personnel": "Pers.",
    "pharmaceutical": "Pharm.", "pharmaceutics": "Pharm.",
    "planning": "Plan.",
    "policy": "Pol'y",
    "preservation": "Pres.", "preserve": "Pres.",
    "privacy": "Priv.", "private": "Priv.",
    "probate": "Prob.", "probation": "Prob.",
    "product": "Prod.", "production": "Prod.",
    "professional": "Pro.",
    "property": "Prop.",
    "protection": "Prot.",
    "psychological": "Psych.", "psychologist": "Psych.",
    "psychology": "Psych.",
    "public": "Pub.",
    "publication": "Publ'n",
    "publishing": "Publ'g",
    "railroad": "R.R.",
    "railway": "Ry.",
    "refining": "Refin.",
    "regional": "Reg'l",
    "regulation": "Regul.", "regulator": "Regul.", "regulatory": "Regul.",
    "rehabilitation": "Rehab.", "rehabilitative": "Rehab.",
    "relation": "Rel.",
    "reproduction": "Reprod.", "reproductive": "Reprod.",
    "research": "Rsch.",
    "reservation": "Rsrv.", "reserve": "Rsrv.",
    "resolution": "Resol.",
    "resource": "Res.",
    "responsibility": "Resp.",
    "restaurant": "Rest.",
    "retirement": "Ret.",
    "road": "Rd.",
    "savings": "Sav.",
    "school": "Sch.",
    "science": "Sci.", "scientific": "Sci.",
    "secretary": "Sec'y",
    "security": "Sec.",
    "sentencing": "Sent'g",
    "service": "Serv.",
    "shareholder": "S'holder", "stockholder": "S'holder",
    "social": "Soc.",
    "society": "Soc'y",
    "solicitor": "Solic.",
    "solution": "Sol.",
    "south": "S.", "southern": "S.",
    "southeast": "Se.", "southeastern": "Se.",
    "southwest": "Sw.", "southwestern": "Sw.",
    "statistical": "Stat.", "statistics": "Stat.",
    "steamship": "S.S.",
    "street": "St.",
    "subcommittee": "Subcomm.",
    "surety": "Sur.",
    "system": "Sys.",
    "taxation": "Tax'n",
    "teacher": "Tchr.",
    "technical": "Tech.", "technique": "Tech.", "technological": "Tech.",
    "technology": "Tech.",
    "telecommunication": "Telecomm.",
    "telegraph": "Tel.", "telephone": "Tel.",
    "temporary": "Temp.",
    "township": "Twp.",
    "transcontinental": "Transcon.",
    "transnational": "Transnat'l",
    "transport": "Transp.", "transportation": "Transp.",
    "trustee": "Tr.",
    "turnpike": "Tpk.",
    "uniform": "Unif.",
    "university": "Univ.",
    "urban": "Urb.",
    "utility": "Util.",
    "village": "Vill.",
    "west": "W.", "western": "W.",
}

# Plurals the table spells out itself (same abbreviation as the singular,
# or an irregular form) — these suppress the derived "add s" plural.
_T6_PLURAL: dict[str, str] = {
    "brothers": "Bros.",
    "businesses": "Bus.",
    "casualties": "Cas.",
    "corrections": "Corr.",
    "industries": "Indus.",
    "resources": "Res.",
    "rights": "Rts.",
    "securities": "Sec.",
    "steamships": "S.S.",
    "systems": "Sys.",
}

# T10 geographic units abbreviated inside a longer party name.  States whose
# names are never abbreviated (Alaska, Idaho, Iowa, Ohio, Utah) are absent.
_T10_WORDS: dict[str, str] = {
    "alabama": "Ala.", "arizona": "Ariz.", "arkansas": "Ark.",
    "california": "Cal.", "colorado": "Colo.", "connecticut": "Conn.",
    "delaware": "Del.", "florida": "Fla.", "georgia": "Ga.",
    "hawaii": "Haw.", "illinois": "Ill.", "indiana": "Ind.",
    "kansas": "Kan.", "kentucky": "Ky.", "louisiana": "La.",
    "maine": "Me.", "maryland": "Md.", "massachusetts": "Mass.",
    "michigan": "Mich.", "minnesota": "Minn.", "mississippi": "Miss.",
    "missouri": "Mo.", "montana": "Mont.", "nebraska": "Neb.",
    "nevada": "Nev.", "oklahoma": "Okla.", "oregon": "Or.",
    "pennsylvania": "Pa.", "tennessee": "Tenn.", "texas": "Tex.",
    "vermont": "Vt.", "virginia": "Va.", "washington": "Wash.",
    "wisconsin": "Wis.", "wyoming": "Wyo.",
}

# Multi-word units and T6 multi-word entries, matched before the word pass
# (longest first so "West Virginia" wins over "West" + "Virginia").
_PHRASES: list[tuple[str, str]] = [
    ("district of columbia", "D.C."),
    ("artificial intelligence", "A.I."),
    ("civil liberties", "C.L."),
    ("civil liberty", "C.L."),
    ("civil rights", "C.R."),
    ("new hampshire", "N.H."),
    ("new jersey", "N.J."),
    ("new mexico", "N.M."),
    ("new york", "N.Y."),
    ("north carolina", "N.C."),
    ("north dakota", "N.D."),
    ("puerto rico", "P.R."),
    ("rhode island", "R.I."),
    ("south carolina", "S.C."),
    ("south dakota", "S.D."),
    ("united states", "U.S."),
    ("west virginia", "W. Va."),
]

# Geographic units left untouched when they are the entire party name
# (rule 10.2.2 / Indigo R8.3): "United States v. Nixon", "Arizona v. Gant".
_GEO_PARTIES = (
    {"united states", "district of columbia", "puerto rico",
     "alaska", "idaho", "iowa", "ohio", "utah", "new york",
     "new hampshire", "new jersey", "new mexico", "north carolina",
     "north dakota", "rhode island", "south carolina", "south dakota",
     "west virginia", "washington"}
    | set(_T10_WORDS)
)


def _plural(word: str, abbr: str) -> tuple[str, str] | None:
    """Derive the plural entry per T6 ("add s"), or None when the
    abbreviation is an initialism or compass point that takes no plural."""
    if abbr.count(".") > 1 or len(abbr.rstrip(".")) <= 1 or abbr == "&":
        return None
    if word.endswith("y"):
        pword = word[:-1] + "ies"
    elif word.endswith(("s", "x", "ch", "sh")):
        pword = word + "es"
    else:
        pword = word + "s"
    pabbr = abbr[:-1] + "s." if abbr.endswith(".") else abbr + "s"
    return pword, pabbr


def _build_word_map() -> dict[str, str]:
    words = dict(_T6_SINGULAR)
    for w, a in _T6_SINGULAR.items():
        p = _plural(w, a)
        if p and p[0] not in words and p[0] not in _T6_PLURAL:
            words[p[0]] = p[1]
    words.update(_T6_PLURAL)
    words.update(_T10_WORDS)
    return words


_WORD_MAP = _build_word_map()

# A token is a run of letters with internal apostrophes/periods, so already-
# abbreviated forms ("Ass'n", "Inc.") and possessives ("Children's") come
# through as single tokens that miss the table and pass unchanged.
_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z'’.]*")

_PHRASE_RE = re.compile(
    r"\b(" + "|".join(p.replace(" ", r"\s+") for p, _ in _PHRASES) + r")\b",
    re.IGNORECASE,
)
_PHRASE_MAP = {p: a for p, a in _PHRASES}

_ET_AL_RE = re.compile(r",?\s+et\s+al\.?\s*$", re.IGNORECASE)
_V_SPLIT_RE = re.compile(r"\s+vs?\.\s+")


def _abbreviate_party(party: str) -> str:
    p = re.sub(r"\s+", " ", party).strip()
    p = _ET_AL_RE.sub("", p)
    p = re.sub(r"^the\s+", "", p, flags=re.IGNORECASE)  # rule 10.2.1(d)
    p = re.sub(r"\bUnited States of America\b", "United States", p,
               flags=re.IGNORECASE)
    if p.strip(" ,.").lower() in _GEO_PARTIES:
        return p

    p = _PHRASE_RE.sub(
        lambda m: _PHRASE_MAP[re.sub(r"\s+", " ", m.group(0).lower())], p
    )
    return _TOKEN_RE.sub(
        lambda m: _WORD_MAP.get(m.group(0).replace("’", "'").lower(),
                                m.group(0)),
        p,
    )


def abbreviate_case_name(name: str) -> str:
    """Abbreviate a case name for use in a citation or filename per
    Bluebook rule 10.2.2 (= Indigo Book R8.3).  Safe to call twice."""
    name = re.sub(r"\s+", " ", name or "").strip()
    if not name:
        return name
    parts = _V_SPLIT_RE.split(name, maxsplit=1)
    return " v. ".join(_abbreviate_party(p) for p in parts)


if __name__ == "__main__":
    _CASES = [
        ("United States v. Nixon", "United States v. Nixon"),
        ("United States of America v. Smith", "United States v. Smith"),
        ("California v. Texas", "California v. Texas"),
        ("New York Times Company v. Sullivan",
         "N.Y. Times Co. v. Sullivan"),
        ("National Labor Relations Board v. "
         "Jones and Laughlin Steel Corporation",
         "Nat'l Lab. Rels. Bd. v. Jones & Laughlin Steel Corp."),
        ("United States v. Carolene Products Company",
         "United States v. Carolene Prods. Co."),
        ("Natural Resources Defense Council, Inc. v. "
         "United States Environmental Protection Agency",
         "Nat. Res. Def. Council, Inc. v. U.S. Env't Prot. Agency"),
        ("Department of Homeland Security v. "
         "Regents of the University of California",
         "Dep't of Homeland Sec. v. Regents of the Univ. of Cal."),
        ("Mercy Hospital, Inc. v. Jackson", "Mercy Hosp., Inc. v. Jackson"),
        ("Law v. Siegel", "Law v. Siegel"),
        ("In re Standard Jury Instructions",
         "In re Standard Jury Instructions"),
        ("Doctor's Associates, Inc. v. Casarotto",
         "Doctor's Assocs., Inc. v. Casarotto"),
        ("West Virginia v. Environmental Protection Agency",
         "West Virginia v. Env't Prot. Agency"),
        ("The Florida Star v. B. J. F.", "Fla. Star v. B. J. F."),
        ("Nat'l Lab. Rels. Bd. v. Jones & Laughlin Steel Corp.",
         "Nat'l Lab. Rels. Bd. v. Jones & Laughlin Steel Corp."),
    ]
    failed = 0
    for raw, want in _CASES:
        got = abbreviate_case_name(raw)
        ok = got == want
        failed += not ok
        print(("ok   " if ok else "FAIL ") + f"{raw!r} -> {got!r}"
              + ("" if ok else f"  (want {want!r})"))
    raise SystemExit(1 if failed else 0)
