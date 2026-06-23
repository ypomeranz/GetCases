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

# "State of X" / "Commonwealth of X" / "People of the State of X" are
# omitted from party names (rule 10.2.1(f)), leaving the protected
# geographic name: "State of Washington" -> "Washington".
_STATE_OF_RE = re.compile(
    r"^(?:the\s+)?(?:people\s+of\s+(?:the\s+state\s+of\s+)?|"
    r"state\s+of\s+|commonwealth\s+of\s+)",
    re.IGNORECASE,
)

# A municipal party — "City of New York", "Village of Arlington Heights" —
# is itself a geographic unit: the place name is not abbreviated, only the
# T6 word in the prefix ("Vill. of Arlington Heights", "Cnty. of
# Sacramento").  When more follows the place ("City of New York Department
# of Education") the party is a larger entity and abbreviates normally.
_MUNICIPAL_RE = re.compile(
    r"^(City|Town|Township|Village|Borough|County|Parish)\s+of\s+(.+)$",
    re.IGNORECASE,
)

# Personal-name suffixes, dropped along with the given name
_NAME_SUFFIX_RE = re.compile(r",?\s+(?:jr|sr|ii|iii|iv)\.?\s*$", re.IGNORECASE)

# Particles kept with the surname: "Nathan Van Buren" -> "Van Buren"
# (compared against dot-stripped lowercase tokens)
_SURNAME_PARTICLES = {
    "van", "von", "der", "den", "de", "del", "della", "di", "da", "du",
    "la", "le", "lo", "mac", "mc", "st", "saint", "ter", "ten",
}

# Common business nouns that are NOT in T6 (Bluebook leaves them
# unabbreviated) but mark a party as an organization, so a leading
# given-name-like word is part of a firm name: "Chase Bank", "Sara Lee
# Foods".  Without this, the given-name pass would truncate them.
_ORG_WORDS = {
    "airlines", "airways", "apparel", "bakery", "bank", "brands",
    "brewery", "brewing", "builders", "church", "cinemas", "club",
    "dairy", "drug", "drugs", "farms", "foods", "furniture", "gas",
    "grocery", "hardware", "herald", "homes", "jewelers", "journal",
    "lines", "lodge", "lumber", "media", "mill", "mills", "ministries",
    "motors", "news", "oil", "optical", "outfitters", "packing",
    "pictures", "pizza", "post", "press", "realty", "records", "shop",
    "shops", "steel", "store", "stores", "studios", "supply", "temple",
    "theaters", "theatres", "times", "tribune", "trust", "works",
}


# ---------------------------------------------------------------------------
# Given names (rule 10.2.1(g)): "Ernestine Jackson" cites as "Jackson".
# Dropping is gated on the first token being a recognized given name so
# that two-word organizations with no T6 word ("Hobby Lobby", "Planned
# Parenthood", "Masterpiece Cakeshop") are never truncated; an unrecognized
# given name simply stays, which is the safe failure.  Names that are also
# U.S. place names (Virginia, Georgia, Charlotte, Austin, Madison) are
# included deliberately — the personal-name check runs before the
# geographic word pass, so "Virginia Smith" becomes "Smith", not "Va.
# Smith".
# ---------------------------------------------------------------------------

_GIVEN_NAMES = frozenset("""
aaron abigail abraham ada adam adrian adrienne agnes aiden aisha alan albert
alberto alejandro alex alexander alexandra alexis alfred alfreda alice alicia
alison allan allen allison alma alvin alyssa amanda amber amelia amos amy ana
andre andrea andres andrew andy angel angela angelica angelo angie anita ann
anna anne annette annie anthony antoine antonio april archibald archie arlene
arnold arthur ashley audrey austin barbara barney barry bartholomew beatrice
becky belinda ben benjamin bernadette bernard bernice bert bertha bertram
bessie beth bethany betsy betty beulah beverly bill billie billy blake
blanche bob bobbie bobby bonnie brad bradley brandi brandon brandy brenda
brent brett brian briana brianna bridget brittany brooke bruce bryan byron
caleb calvin cameron camille candace candice carl carla carlos carlton carmen
carol carole caroline carolyn carrie casey cassandra catherine cathy cecil
cecilia cedric celia cesar chad charlene charles charlie charlotte
chelsea cheryl chester chris christian christina christine christopher
christy cindy claire clara clarence claude claudia clayton cleo cletus
clifford clifton clint clinton clyde cody colin colleen connie conrad
cornelius corey cory courtney craig cristina crystal curtis cynthia cyrus
dale damon dan dana daniel danielle danny daphne darlene darnell darrell
darren darryl dave david dawn dean deanna debbie deborah debra delbert delia
della delores delmar denise dennis derek derrick desiree devin devon dewey
dexter diana diane dianne dolores dominic dominique don donald donna donnie
dora doreen doris dorothy doug douglas duane dustin dwayne dwight dylan earl
earnest ebony ed eddie edgar edith edmund edna eduardo edward edwin eileen
elaine elbert eleanor elena eli elias elijah elizabeth ella ellen elliot
elliott elmer eloise elsa elsie elvira elwood emanuel emil emily emma emmett
enrique eric erica erik erika erin ernest ernestine ernesto ervin ethan ethel
eugene eunice eva evan evelyn everett ezekiel ezra faith fannie felicia
felipe felix ferdinand fernando flora florence floyd forrest frances francis
francisco frank frankie franklin fred freda freddie frederick gabriel
gabriela gail garrett garry gary gavin gayle gene geneva genevieve geoffrey
george georgia gerald geraldine gerard gerardo gilbert gina ginger gladys
glen glenda glenn gloria gordon grace graham grant greg gregg gregory
gretchen grover guadalupe guillermo gus gustavo guy gwen gwendolyn hal hank
hannah harlan harold harriet harriett harry harvey hattie hazel heather
hector heidi helen henrietta henry herbert herman hilda hiram holly homer
hope horace hortense howard hubert hugh hugo ian ida ignacio ike ina inez ira
irene iris irma irving isaac isabel isadore isaiah ismael israel ivan jack
jackie jacob jacqueline jaime jake james jamie jan jana jane janet janice
janie jared jasmine jason jasper javier jay jean jeanette jeanne jeff jeffery
jeffrey jenna jennie jennifer jenny jerald jeremiah jeremy jermaine jerome
jerry jesse jessica jessie jesus jethro jill jim jimmie jimmy jo joan joann
joanna joanne jodi jody joe joel joey john johnnie johnny jon jonathan jordan
jorge jose josefina joseph josephine josh joshua josie joy joyce juan juanita
judith judy julia julian julie julio julius june justin kaitlyn kara karen
kari karl karla kate katelyn katherine kathleen kathryn kathy katie katrina
kay kayla keith kelley kelli kellie kelly kelvin ken kendra kenneth kenny
kent kerry kevin kim kimberly kirk krista kristen kristi kristin kristina
kristine kristy kurt kyle lamar lance larry latasha latoya laura lauren
laurie lavern laverne lawrence leah lela leland lemuel lena leo leon
leonard leopold leroy lesley leslie lester leticia levi lewis lila lillian
lillie lily linda lindsay lindsey lionel lisa lloyd logan lois lola lonnie
lora loren lorena lorenzo loretta lori lorraine louis louise lucas lucia
lucille lucinda lucy luella luis luke luther lydia lyle lynda lyndon lynn
mabel mable mack madeline madison mae maggie malcolm mamie mandy manuel marc
marcella marcia marco marcos marcus margaret margarita margie marguerite
maria marian marianne marie marilyn mario marion marjorie mark marlene
marsha marshall martha martin marvin mary mathew matt matthew mattie maude
maureen maurice mavis max maxine megan meghan melanie melinda melissa melody
melvin mercedes meredith merle merton michael micheal michele michelle miguel
mike mildred miles millie milton mindy minerva minnie miranda miriam misty
mitchell molly mona monica monique morris mortimer moses muriel myra myron
myrtle nadine nancy naomi natalie natasha nathan nathaniel neal neil nellie
nelson nettie nicholas nick nicolas nicole nikki nina noah noel nora norma
norman obadiah olga olive oliver olivia ollie omar opal ophelia ora orville
oscar oswald otis otto owen pablo pam pamela pansy pat patricia patrick
patsy patti patty paul paula pauline pearl pearlie pedro peggy penny percy
perry pete peter phil philip phillip phineas phyllis preston priscilla
prudence rachael rachel rafael ralph ramon ramona randal randall randolph
randy raquel raul ray raymond rebecca regina reginald rene renee reuben rex
rhonda ricardo richard rick rickey ricky rita rob robert roberta roberto
robin robyn rocky rod roderick rodney rodolfo rodrigo roger roland rolando
roman ron ronald ronnie roosevelt rosa rosalie roscoe rose rosemary rosetta
ross rowena roxanne roy ruben ruby rudolph rudy rufus rupert russell rusty
ruth ryan sabrina sadie sally salvador salvatore sam samantha sammy samuel
sandra sandy santos sara sarah saul scott sean sergio seth seymour shane
shannon shari sharon shaun shawn sheila shelby sheldon shelia shelley shelly
sheri sherman sherri sherry sheryl shirley sidney silas silvia simon sonia
sonya sophia spencer stacey stacy stan stanley stefanie stella stephanie
stephen steve steven stuart sue summer susan susannah susie suzanne sybil
sylvester sylvia tabitha tamara tami tammie tammy tanya tara tasha taylor
ted terence teresa teri terrance terrell terrence terri terry thaddeus
thelma theodora theodore theresa thomas tiffany tim timothy tina toby todd
tom tommie tommy toni tony tonya tracey traci tracy travis trevor tricia
trisha troy tyler tyrone ulysses ursula valerie vance vanessa velma vera
verna vernon veronica vicki vickie vicky victor victoria vincent viola
violet virgil virginia vivian wade wallace walter wanda warren wayne wendell
wendy wesley wilbert wilbur wilfred willa willard william willie willis
wilma winfield winifred winston woodrow yesenia yolanda yvette yvonne
zachary zelda
""".split())

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

# T6 words signal an organization, blocking given-name dropping ("George
# Washington University").  T10 place names are excluded from that signal:
# they double as given names far too often (Virginia, Georgia).
_T6_WORDS = frozenset(_WORD_MAP) - frozenset(_T10_WORDS)

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


def _strip_given_names(p: str) -> str | None:
    """Surname-only form of a personal party name (rule 10.2.1(g)), or
    None when the party does not safely read as an individual's name."""
    p = _NAME_SUFFIX_RE.sub("", p)
    if "," in p or "&" in p or re.search(r"\bof\b", p, re.IGNORECASE):
        return None
    tokens = p.split()
    if not 2 <= len(tokens) <= 4:
        return None
    low = [t.replace("’", "'").lower().rstrip(".") for t in tokens]
    if low[0] not in _GIVEN_NAMES:
        return None
    # Every token must look like a name part: a capitalized word, an
    # initial, or a surname particle — and none may be an organizational
    # word ("George Washington University" abbreviates instead).
    for t, tl in zip(tokens, low):
        if (tl in _T6_WORDS or tl in _ORG_WORDS
                or not re.fullmatch(r"[A-Z](?:[A-Za-z'’-]+|\.)?", t)):
            return None
    if re.fullmatch(r"[A-Z]\.?", tokens[-1]):
        return None  # anonymized party ("Susan B.", "B. J. F.")
    # Surname = last token plus any particles ("Nathan Van Buren")
    i = len(tokens) - 1
    while i > 1 and low[i - 1] in _SURNAME_PARTICLES:
        i -= 1
    # Everything before the surname must itself be a given name, an
    # initial, or a particle ("John W. Smith" — but not "Chase Manhattan
    # Bank", whose middle token flunks this check)
    for t, tl in zip(tokens[1:i], low[1:i]):
        if not (tl in _GIVEN_NAMES or tl in _SURNAME_PARTICLES
                or re.fullmatch(r"[A-Z]\.?", t)):
            return None
    return " ".join(tokens[i:])


# Bluebook rule 10.2.1(c): the name of a widely recognized institution is
# abbreviated to its initials.  Google Scholar prints the full name, so map
# the formal long form back to the initials practitioners actually use.  Keyed
# on the full party name (matched exactly) to avoid clobbering longer names
# that merely contain one of these phrases; cabinet departments are left to
# the ordinary "Dep't of …" abbreviation per the Bluebook's own practice.
_WIDELY_RECOGNIZED_INITIALS_RAW: dict[str, tuple[str, ...]] = {
    "SEC": ("Securities and Exchange Commission",),
    "NLRB": ("National Labor Relations Board",),
    "FCC": ("Federal Communications Commission",),
    "FTC": ("Federal Trade Commission",),
    "FEC": ("Federal Election Commission",),
    "FERC": ("Federal Energy Regulatory Commission",),
    "FMC": ("Federal Maritime Commission",),
    "FPC": ("Federal Power Commission",),
    "ICC": ("Interstate Commerce Commission",),
    "CFTC": ("Commodity Futures Trading Commission",),
    "CPSC": ("Consumer Product Safety Commission",),
    "EEOC": ("Equal Employment Opportunity Commission",),
    "NRC": ("Nuclear Regulatory Commission",),
    "NMB": ("National Mediation Board",),
    "NTSB": ("National Transportation Safety Board",),
    "STB": ("Surface Transportation Board",),
    "CFPB": ("Consumer Financial Protection Bureau",),
    "FDIC": ("Federal Deposit Insurance Corporation",),
    "EPA": ("Environmental Protection Agency",),
    "NASA": ("National Aeronautics and Space Administration",),
    "TVA": ("Tennessee Valley Authority",),
    "OPM": ("Office of Personnel Management",),
    "MSPB": ("Merit Systems Protection Board",),
    "FBI": ("Federal Bureau of Investigation",),
    "IRS": ("Internal Revenue Service",),
    "INS": ("Immigration and Naturalization Service",),
    "DEA": ("Drug Enforcement Administration",),
    "FDA": ("Food and Drug Administration",),
    "FAA": ("Federal Aviation Administration",),
    "OSHA": ("Occupational Safety and Health Administration",),
    "CIA": ("Central Intelligence Agency",),
    "SSA": ("Social Security Administration",),
    "USPS": ("Postal Service",),
    "NAACP": ("National Association for the Advancement of Colored People",),
    "ACLU": ("American Civil Liberties Union",),
    "NCAA": ("National Collegiate Athletic Association",),
}


def _initials_key(s: str) -> str:
    """Normalize a party name for the recognized-initials lookup: drop
    punctuation, fold '&' to 'and', and lowercase."""
    s = s.replace("&", " and ")
    s = re.sub(r"[.,'’]", "", s)
    return re.sub(r"\s+", " ", s).strip().lower()


_WIDELY_RECOGNIZED_INITIALS: dict[str, str] = {
    _initials_key(_name): _acr
    for _acr, _names in _WIDELY_RECOGNIZED_INITIALS_RAW.items()
    for _name in _names
}


def _recognized_initialism(party: str) -> str:
    """Rule 10.2.1(c) initials for a widely recognized institution (SEC,
    NLRB, FCC, EPA…), or '' if the party isn't one.  A leading 'United
    States'/'U.S.' is ignored so 'United States Environmental Protection
    Agency' resolves to EPA the same as the bare name."""
    key = _initials_key(party)
    acr = _WIDELY_RECOGNIZED_INITIALS.get(key)
    if acr:
        return acr
    stripped = re.sub(r"^(?:united states|us)\s+", "", key)
    if stripped != key:
        return _WIDELY_RECOGNIZED_INITIALS.get(stripped, "")
    return ""


# A party identified only by initials — an anonymized individual such as a
# minor ("D.L.", "J.G.G.", "B.J.F.").  Matches 2-4 single capital letters
# however the source spaced or punctuated them ("DL", "D. L.", "J. G. G.").
_INITIALS_ONLY_RE = re.compile(r"[A-Z](?:\s*\.?\s*[A-Z]){1,3}\s*\.?")

# Widely recognized acronym institutions keep bare initials (rule 10.2.1(c)),
# so they're excluded from the anonymized-initials reformatting below.
_RECOGNIZED_ACRONYMS = frozenset(_WIDELY_RECOGNIZED_INITIALS_RAW)


def _format_anonymous_initials(party: str) -> str | None:
    """Privacy practice: a party given only as initials is set with periods
    and no spaces — "DL"/"D. L." -> "D.L.".  Returns None when the party
    isn't a bare run of initials, or is a recognized acronym (SEC, NLRB…)."""
    p = party.strip().strip(",")
    if not _INITIALS_ONLY_RE.fullmatch(p):
        return None
    letters = re.findall(r"[A-Z]", p)
    if "".join(letters) in _RECOGNIZED_ACRONYMS:
        return None
    return "".join(f"{c}." for c in letters)


# Procedural captions naming an anonymized party by initials ("In re J.W.",
# "Ex parte D.L.").  The remainder after the prefix is reformatted like any
# other anonymized-initials party; the prefix is kept in canonical Bluebook
# form (rule 10.2.1(b): "In re", "Ex parte").
_PROCEDURAL_PREFIX_RE = re.compile(
    r"^(in\s+re|ex\s+parte|in\s+the\s+matter\s+of|matter\s+of)\b[\s,:]*",
    re.IGNORECASE,
)
_PROCEDURAL_CANON = {
    "in re": "In re",
    "ex parte": "Ex parte",
    "in the matter of": "In re",
    "matter of": "In re",
}


def _format_procedural_initials(party: str) -> str | None:
    """Anonymized party by initials behind a procedural prefix —
    "In re JW" -> "In re J.W.", "Ex parte D. L." -> "Ex parte D.L.".
    Returns None when there's no procedural prefix or the remainder isn't a
    bare run of initials ("In re Gault" is left to the ordinary path)."""
    m = _PROCEDURAL_PREFIX_RE.match(party)
    if not m:
        return None
    anon = _format_anonymous_initials(party[m.end():])
    if anon is None:
        return None
    prefix = _PROCEDURAL_CANON[re.sub(r"\s+", " ", m.group(1).lower())]
    return f"{prefix} {anon}"


def _abbreviate_party(party: str, *, recognize_initials: bool = True) -> str:
    p = re.sub(r"\s+", " ", party).strip()
    p = _ET_AL_RE.sub("", p)
    p = re.sub(r"^the\s+", "", p, flags=re.IGNORECASE)  # rule 10.2.1(d)
    p = re.sub(r"\bUnited States of America\b", "United States", p,
               flags=re.IGNORECASE)
    p = _STATE_OF_RE.sub("", p)  # "State of Washington" -> "Washington"
    if recognize_initials:  # rule 10.2.1(c): SEC, NLRB, FCC…
        initials = _recognized_initialism(p)
        if initials:
            return initials
    anon = _format_anonymous_initials(p)
    if anon is not None:
        return anon
    anon_prefixed = _format_procedural_initials(p)
    if anon_prefixed is not None:
        return anon_prefixed
    if p.strip(" ,.").lower() in _GEO_PARTIES:
        return p

    m = _MUNICIPAL_RE.match(p)
    if m:
        prefix, place = m.group(1), m.group(2)
        place_words = [w.replace("’", "'").lower().rstrip(".,'")
                       for w in place.split()]
        if (not any(w in _T6_WORDS for w in place_words)
                and not re.search(r"\bof\b", place, re.IGNORECASE)):
            return f"{_WORD_MAP.get(prefix.lower(), prefix)} of {place}"

    surname = _strip_given_names(p)
    if surname is not None:
        return surname

    p = _PHRASE_RE.sub(
        lambda m: _PHRASE_MAP[re.sub(r"\s+", " ", m.group(0).lower())], p
    )

    # A state name right after a given name is part of a person's name,
    # not a geographic unit: "George Washington University" keeps
    # "Washington" (rule 10.2.2 abbreviates only geographic units).
    protected: set[int] = set()
    tokens = list(_TOKEN_RE.finditer(p))
    for prev, tok in zip(tokens, tokens[1:]):
        if (tok.group(0).lower() in _T10_WORDS
                and prev.group(0).lower() in _GIVEN_NAMES):
            protected.add(tok.start())

    def _sub(m: re.Match) -> str:
        if m.start() in protected:
            return m.group(0)
        return _WORD_MAP.get(m.group(0).replace("’", "'").lower(),
                             m.group(0))

    return _TOKEN_RE.sub(_sub, p)


# Also recognize each institution's *abbreviated* form — "Sec. & Exch. Comm'n",
# "Nat'l Lab. Rels. Bd." — not just the spelled-out name.  Generate those keys
# by running the long form through the ordinary abbreviator (with the
# initials lookup itself disabled), so the variants always track table T6
# instead of being hand-maintained.
for _acr, _names in _WIDELY_RECOGNIZED_INITIALS_RAW.items():
    for _name in _names:
        _abbr = _abbreviate_party(_name, recognize_initials=False)
        _WIDELY_RECOGNIZED_INITIALS.setdefault(_initials_key(_abbr), _acr)


# Rule 10.2.1(h): omit "Inc.", "Ltd.", "L.L.C.", "L.L.P.", "N.A.", "F.S.B.",
# and similar business-entity terms when the name *also* contains a word like
# "Ass'n", "Bros.", "Co.", or "Corp." that already marks it as a business firm.
_FIRM_MARKERS = {"ass'n", "assn", "bros", "co", "cos", "corp", "corps"}
_REDUNDANT_ENTITY_TERMS = {
    "inc", "ltd", "llc", "l.l.c", "llp", "l.l.p", "lllp", "pllc", "p.l.l.c",
    "plc", "pc", "p.c", "pa", "p.a", "na", "n.a", "f.s.b", "fsb", "lp", "l.p",
}


def _norm_entity_token(tok: str) -> str:
    """A token's comparison key: lowercase, no surrounding punctuation, with
    a trailing entity period removed ('Inc.,' → 'inc', 'L.L.C.' → 'l.l.c')."""
    t = tok.replace("’", "'").strip().lower().strip(",")
    if t.endswith("."):
        t = t[:-1]
    return t


def _drop_redundant_entity(name: str) -> str:
    """Apply rule 10.2.1(h): if the party name contains a firm marker
    ('Co.', 'Corp.', 'Ass'n', 'Bros.'), drop any redundant entity suffix
    ('Inc.', 'LLC', 'Ltd.', …) and tidy the comma it left behind."""
    words = name.split()
    norm = [_norm_entity_token(w) for w in words]
    if not any(n in _FIRM_MARKERS for n in norm):
        return name
    kept: list[str] = []
    for w, n in zip(words, norm):
        if n in _REDUNDANT_ENTITY_TERMS:
            if kept and kept[-1].endswith(","):
                kept[-1] = kept[-1][:-1]
            continue
        kept.append(w)
    return " ".join(kept).strip().rstrip(",")


def abbreviate_case_name(name: str) -> str:
    """Abbreviate a case name for use in a citation or filename per
    Bluebook rule 10.2.2 (= Indigo Book R8.3), dropping given names of
    individuals (rule 10.2.1(g)) and "State of" prefixes (10.2.1(f)).
    Safe to call twice."""
    name = re.sub(r"\s+", " ", name or "").strip()
    if not name:
        return name
    parts = _V_SPLIT_RE.split(name, maxsplit=1)
    return " v. ".join(
        _drop_redundant_entity(_abbreviate_party(p)) for p in parts
    )


if __name__ == "__main__":
    _CASES = [
        ("United States v. Nixon", "United States v. Nixon"),
        ("United States of America v. Smith", "United States v. Smith"),
        ("California v. Texas", "California v. Texas"),
        ("New York Times Company v. Sullivan",
         "N.Y. Times Co. v. Sullivan"),
        ("National Labor Relations Board v. "
         "Jones and Laughlin Steel Corporation",
         "NLRB v. Jones & Laughlin Steel Corp."),
        ("United States v. Carolene Products Company",
         "United States v. Carolene Prods. Co."),
        ("Natural Resources Defense Council, Inc. v. "
         "United States Environmental Protection Agency",
         "Nat. Res. Def. Council, Inc. v. EPA"),
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
         "West Virginia v. EPA"),
        # Anonymized party by initials: periods, no spaces (rule 10.2.1(b)).
        ("The Florida Star v. B. J. F.", "Fla. Star v. B.J.F."),
        ("DL v. Huebner", "D.L. v. Huebner"),
        ("Trump v. J. G. G.", "Trump v. J.G.G."),
        ("In re JW", "In re J.W."),
        ("Ex parte D. L.", "Ex parte D.L."),
        ("In the Matter of JW", "In re J.W."),
        ("In re Gault", "In re Gault"),
        ("Nat'l Lab. Rels. Bd. v. Jones & Laughlin Steel Corp.",
         "NLRB v. Jones & Laughlin Steel Corp."),
        # Given names (rule 10.2.1(g))
        ("Mercy Hospital, Inc. v. Ernestine Jackson",
         "Mercy Hosp., Inc. v. Jackson"),
        ("Jane Roe v. Henry Wade", "Roe v. Wade"),
        ("John W. Smith, Jr. v. Acme Corporation", "Smith v. Acme Corp."),
        ("Nathan Van Buren v. United States", "Van Buren v. United States"),
        ("Virginia Smith v. Texas", "Smith v. Texas"),
        ("Burwell v. Hobby Lobby Stores, Inc.",
         "Burwell v. Hobby Lobby Stores, Inc."),
        ("Planned Parenthood of Southeastern Pennsylvania v. Robert Casey",
         "Planned Parenthood of Se. Pa. v. Casey"),
        ("George Washington University v. Violet Aldridge",
         "George Washington Univ. v. Aldridge"),
        ("Chase Bank v. Mary McCoy", "Chase Bank v. McCoy"),
        # Geographic parties (rules 10.2.1(f), 10.2.2)
        ("State of Washington v. Glucksberg", "Washington v. Glucksberg"),
        ("People of the State of Illinois v. Gates", "Illinois v. Gates"),
        ("City of New York v. United States Department of Defense",
         "City of New York v. U.S. Dep't of Def."),
        ("Village of Arlington Heights v. "
         "Metropolitan Housing Development Corporation",
         "Vill. of Arlington Heights v. Metro. Hous. Dev. Corp."),
        ("County of Sacramento v. Lewis", "Cnty. of Sacramento v. Lewis"),
        ("Town of Greece v. Susan Galloway", "Town of Greece v. Galloway"),
        ("City of New York Department of Parks v. Doe",
         "City of N.Y. Dep't of Parks v. Doe"),
        # Widely recognized initials (rule 10.2.1(c))
        ("Lorenzo v. Securities and Exchange Commission", "Lorenzo v. SEC"),
        ("Securities & Exchange Commission v. Edwards", "SEC v. Edwards"),
        ("Smith v. Federal Communications Commission", "Smith v. FCC"),
        ("Doe v. Federal Bureau of Investigation", "Doe v. FBI"),
        ("National Labor Relations Board v. Acme Corporation",
         "NLRB v. Acme Corp."),
        ("United States Environmental Protection Agency v. Smith",
         "EPA v. Smith"),
        ("The Equal Employment Opportunity Commission v. Abercrombie",
         "EEOC v. Abercrombie"),
        ("SEC v. Edwards", "SEC v. Edwards"),  # already-initialed: unchanged
        # Already-abbreviated long forms also fold to the initials
        ("Sec. & Exch. Comm'n v. Edwards", "SEC v. Edwards"),
        ("Nat'l Lab. Rels. Bd. v. Acme Corp.", "NLRB v. Acme Corp."),
        ("U.S. Env't Prot. Agency v. Smith", "EPA v. Smith"),
        ("Fed. Commc'ns Comm'n v. Smith", "FCC v. Smith"),
    ]
    failed = 0
    for raw, want in _CASES:
        got = abbreviate_case_name(raw)
        ok = got == want
        failed += not ok
        print(("ok   " if ok else "FAIL ") + f"{raw!r} -> {got!r}"
              + ("" if ok else f"  (want {want!r})"))
    raise SystemExit(1 if failed else 0)
