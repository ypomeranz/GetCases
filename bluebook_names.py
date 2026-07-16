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

# Relator construction "<party> ex rel. <relator>" (rule 10.2.1(b)): "on the
# relation of" and its synonyms are abbreviated "ex rel.".  The party named
# ahead of the phrase — a State, the United States, "People", … — is a named
# party to the suit, so rule 10.2.2 forbids abbreviating its geographic name:
# the cite reads "Indiana ex rel. Anderson", never "Ind. ex rel. Anderson".
# Matched case-insensitively and re-emitted in canonical lowercase form.
_EX_REL_RE = re.compile(
    r"[\s,]+(?:ex\s+rel(?:\.|atione)?|on\s+(?:the\s+)?relation\s+of)[\s,]+",
    re.IGNORECASE,
)


def _norm_geo(s: str) -> str:
    """Comparison key for a geographic abbreviation: letters only, uppercased
    ("N.C." / "N. C." -> "NC", "Ind." -> "IND")."""
    return re.sub(r"[^A-Za-z]", "", s).upper()


# Reverse of the geographic abbreviations, to restore a named party that a
# source (e.g. CourtListener) already abbreviated: rule 10.2.2 keeps the
# party's full name, so "Ind. ex rel." must read "Indiana ex rel." and
# "U.S. ex rel." must read "United States ex rel.".
_GEO_EXPANSIONS: dict[str, str] = {
    _norm_geo(_abbr): _full.title() for _full, _abbr in _T10_WORDS.items()
}
_GEO_EXPANSIONS.update({
    _norm_geo(_abbr): _full for _abbr, _full in {
        "D.C.": "District of Columbia", "N.H.": "New Hampshire",
        "N.J.": "New Jersey", "N.M.": "New Mexico", "N.Y.": "New York",
        "N.C.": "North Carolina", "N.D.": "North Dakota",
        "P.R.": "Puerto Rico", "R.I.": "Rhode Island",
        "S.C.": "South Carolina", "S.D.": "South Dakota",
        "W. Va.": "West Virginia", "U.S.": "United States",
    }.items()
})


def _expand_geo_party(head: str) -> str:
    """Restore a geographic party a source abbreviated ("Ind." -> "Indiana",
    "U.S." -> "United States") so the no-abbreviation rule for a named
    geographic party (rule 10.2.2) applies; other heads pass through."""
    return _GEO_EXPANSIONS.get(_norm_geo(head), head)


# A municipal party named "<Unit> of <Place>" — "City of New York", "Village
# of Arlington Heights", "County of Sacramento" — is itself one geographic
# unit and the entire party, so rule 10.2.2 leaves the whole name unabbreviated,
# the unit word included: "Village of Arlington Heights", not "Vill. of …".
# (This keeps Village/County/Township/Parish, which table T6 would otherwise
# shorten, consistent with City/Town/Borough, which no table abbreviates.)
# When more follows the place ("City of New York Department of Education") the
# party is a larger entity and abbreviates normally.
_MUNICIPAL_RE = re.compile(
    r"^(City|Town|Township|Village|Borough|County|Parish)\s+of\s+(.+)$",
    re.IGNORECASE,
)

# The same unit expressions in mid-name position are omitted (rule
# 10.2.1(f)): "Board of Education of the Borough of Hawthorne" -> "Board of
# Education of Hawthorne", "Mayor of the City of New York" -> "Mayor of New
# York".  The lookbehind confines the omission to mid-name — a party that
# *begins* with the expression ("City of New York") is untouched.
_MID_GEO_UNIT_RE = re.compile(
    r"(?<=\w)(\s+of)\s+(?:the\s+)?"
    r"(?:City|Town|Township|Village|Borough|County|Parish)\s+of\b",
    re.IGNORECASE,
)

# The suffix mirror of the same rule: "Cook County", "Jefferson Parish", "New
# York City" name a unit whose words together are the place's proper name, so
# the whole party stays unabbreviated ("Soldal v. Cook County", never "Cook
# Cnty.").  The '$' anchor limits the rule to a trailing unit word: when an
# institution follows ("Cook County Bd. of Review") the larger party
# abbreviates normally.
_GEO_SUFFIX_RE = re.compile(
    r"^(.+?)\s+(?:City|Town|Township|Village|Borough|County|Parish)$",
    re.IGNORECASE,
)

# Personal-name suffixes, dropped along with the given name
_NAME_SUFFIX_RE = re.compile(r",?\s+(?:jr|sr|ii|iii|iv)\.?\s*$", re.IGNORECASE)

# Honorifics and ranks ahead of a personal name ("Dr. Theresa Swain Emory",
# "Sgt. William Brown").  The title itself is omitted (rule 10.2.1(e)), and
# its presence marks the party as a natural person, so an unrecognized
# middle name no longer blocks the surname reduction.  Stripping is gated on
# what follows still parsing as a personal name — the next word must be a
# recognized given name — so "Dr Pepper Bottling Co." and "General Motors
# Corp." are never truncated.
_PERSONAL_TITLE_RE = re.compile(
    r"^(?:dr|mr|mrs|ms|miss|messrs|prof(?:essor)?|rev(?:erend)?|"
    r"hon(?:orable)?|fr|sgt|sergeant|lt|lieutenant|capt(?:ain)?|"
    r"col(?:onel)?|maj(?:or)?|gen(?:eral)?|det(?:ective)?|officer|deputy|"
    r"sheriff)\.?\s+",
    re.IGNORECASE,
)

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
    "motors", "news", "nursery", "oil", "optical", "outfitters", "packing",
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
""".split()) | frozenset("""
ahmed ahmad aharon akiva ali amit anil ari arjun aviva avi avram baruch
boris chaim chana chaya devorah dimitri dmitri dov efraim eliezer elchanan
esther ezriel fatima francois giovanni gitel hans hassan henri hiroshi
hussein ibrahim igor jacques johan johanan jurgen kenji khalid klaus kofi
kwame lars leinani luigi malka meir menachem mendel mikhail minh mohamed mohammed
moshe mordechai muhammad naftali nikolai olaf pierre pinchas priya rajesh
ramesh reuven rivka sanjay sergei shira shlomo shmuel sunil svetlana takeshi
tatiana tova tzvi vijay vladimir werner wolfgang yaakov yael yehuda yehoshua
yisroel yitzchak yochanan yosef yuri zev
""".split())


def is_recognized_given_name(token: str) -> bool:
    """Whether *token* is a given name recognized by the conservative
    personal-name heuristic used by this module.

    Ordinary title-case caption parsing uses this before discarding a leading
    word.  Mixed-case Scholar captions use :func:`is_personal_all_caps_run`.
    """
    key = re.sub(r"[^A-Za-z]", "", token or "").lower()
    return bool(key and key in _GIVEN_NAMES)


_NONPERSON_CAPS = frozenset({
    "USA", "US", "U.S.", "FBI", "SEC", "IRS", "EPA", "NLRB", "FCC", "FTC",
    "CATV", "LLC", "LLP", "LLLP", "PLLC", "PLC", "LP", "PC", "PA",
    "KAISHA",
})


def is_personal_all_caps_run(
    capitalized_tokens: list[str], dropped_tokens: list[str]
) -> bool:
    """Whether an all-caps run safely represents a person's surname.

    This supports mixed Scholar captions such as ``Brent BREWBAKER`` while
    explicitly rejecting entity initialisms in ``McDonald's USA``.
    """
    names: list[str] = []
    for token in capitalized_tokens:
        display = token.replace("’", "'").strip(",.")
        key = re.sub(r"[^A-Za-z]", "", display).lower()
        if (display in _NONPERSON_CAPS
                or key in _ORG_WORDS
                or key in _T6_WORDS):
            return False
        names.append(token)

    # Do not gate this on the given-name dictionary: no static list can cover
    # every litigant.  Instead require a capitalized, name-shaped prefix and
    # prove that every retained caps token is surname-like rather than an
    # organizational descriptor such as USA, MEDIA, or COMPANY.
    dropped = list(dropped_tokens)
    if len(dropped) >= 2 and [t.rstrip(".").lower() for t in dropped[:2]] == [
        "the", "honorable"
    ]:
        dropped = dropped[2:]
    if any(token.rstrip(".").lower() in {"a", "an", "the"} for token in dropped):
        return False
    return bool(names) and bool(dropped) and all(
        bool(re.fullmatch(
            r"[A-ZÀ-ÖØ-Þ][A-Za-zÀ-ÖØ-öø-ÿ'’-]*|[A-Z]\."
            r"|(?i:Mr|Mrs|Ms|Miss|Dr|Hon)\.",
            token,
        ))
        for token in dropped
    )


_CAPS_ENTITY_SUFFIX_RE = re.compile(
    r"(?:inc|incorporated|l\.?l\.?c|l\.?l\.?p|l\.?p|ltd|ltda|p\.?l\.?c|"
    r"p\.?c|s\.?a|s\.?p\.?a|a\.?g|n\.?v|s\.?l|gmbh|co|corp|n\.?a)\.?",
    re.IGNORECASE,
)


def collapse_personal_all_caps_run(text: str) -> str:
    """Keep only an all-caps personal-name run in a mixed-case caption.

    Scholar commonly renders a person as ``Corrine Morgan THOMAS``.  Entity
    captions can look similar (``McDonald's USA, LLC``), so organizational
    words and initialisms must not serve as evidence of a surname.
    """
    tokens = text.split()
    kept_flags = [
        (token.isupper() and len(token.strip(".,'")) > 1)
        or token == "&"
        or token.rstrip(".,").isdigit()
        or bool(_CAPS_ENTITY_SUFFIX_RE.fullmatch(token.rstrip(",")))
        for token in tokens
    ]
    kept = [token for token, keep in zip(tokens, kept_flags) if keep]
    namey = [
        token for token in kept
        if token.isupper()
        and len(token.strip(".,'")) > 1
        and not _CAPS_ENTITY_SUFFIX_RE.fullmatch(token.strip(",."))
    ]
    dropped = [token for token, keep in zip(tokens, kept_flags) if not keep]
    first_kept = next((i for i, keep in enumerate(kept_flags) if keep), len(tokens))
    if any(not keep for keep in kept_flags[first_kept + 1:]):
        return text
    if (namey and len(kept) < len(tokens)
            and is_personal_all_caps_run(namey, dropped)):
        return " ".join(kept).strip(" ,;&")
    return text


_CAPTION_SMALL_WORDS = frozenset({
    "of", "the", "and", "v", "vs", "in", "re", "for", "on", "a", "an",
    "to", "by", "at", "as", "or", "ex", "rel", "et", "al", "de", "la",
})
_CAPTION_KEEP_CAPS = frozenset({
    "LLC", "LLP", "LLLP", "PLLC", "PLC", "LP", "PC", "PA", "N.A.",
    "U.S.", "USA", "FBI", "SEC", "IRS", "EPA", "NLRB", "FCC", "FTC",
    "II", "III", "IV",
})


def normal_case_caption(text: str) -> str:
    """Normal-case the all-caps words in a case caption without damaging
    apostrophe or ``Mc`` surnames.

    Mixed-case words (including brands such as ``NBCUniversal``) pass through
    exactly as supplied.  When a source supplies only all caps, conventional
    forms such as ``O'BRIEN`` and ``MCFADDEN`` become ``O'Brien`` and
    ``McFadden``; truly unusual brand casing is later recoverable from the
    reporter's authoritative metadata.
    """
    out: list[str] = []
    for i, word in enumerate((text or "").split()):
        letters = [c for c in word if c.isalpha()]
        # Ordinary mixed case passes through, but a mostly-uppercase OCR form
        # such as McFADDEN is still an all-caps word for normalization purposes.
        if (not letters
                or (any(c.islower() for c in letters)
                    and sum(c.isupper() for c in letters) <= len(letters) // 2)):
            out.append(word)
            continue
        stripped = word.replace("’", "'").strip(".,()'\"")
        # An all-caps word containing "&" is a firm's initialism (AT&T,
        # A&M, S&P, H&R) — English words never carry one, so caps are safe.
        # The dotted-initialism check tolerates a final letter left bare by
        # the strip above ("L.L.C." arrives here as "L.L.C").
        if (stripped in _CAPTION_KEEP_CAPS
                or "&" in stripped
                or re.fullmatch(r"(?:[A-Z]\.)+[A-Z]?", stripped)):
            out.append(word)
            continue
        low = word.lower()
        if i and low.strip(".,()'\"") in _CAPTION_SMALL_WORDS:
            out.append(low)
            continue

        # Capitalize each apostrophe/hyphen component independently so Python's
        # ordinary str.capitalize() does not produce O'brien.  Then apply the
        # conventional internal capital after Mc.
        pieces = re.split(r"(['’-])", low)
        fixed = "".join(
            piece[:1].upper() + piece[1:] if j % 2 == 0 and piece else piece
            for j, piece in enumerate(pieces)
        )
        if fixed.startswith("Mc") and len(fixed) > 2 and fixed[2].isalpha():
            fixed = "Mc" + fixed[2].upper() + fixed[3:]
        out.append(fixed)
    return " ".join(out)


def refine_caption_case(name: str, body_text: str) -> str:
    """Correct title-casing guesses for an all-caps caption by consulting
    the opinion's own mixed-case prose.

    An all-caps caption destroys the case information: "US DOMINION, INC.
    v. BYRNE" title-cases to "Us Dominion…" because nothing in the caption
    says whether US is the word (Toys R Us) or an initialism (US Dominion).
    The body settles it — the parties are named in ordinary prose
    ("Plaintiffs US Dominion, Inc. …").  Each purely alphabetic caption
    token is searched in *body_text*, preferably anchored to an adjacent
    caption token (so the lookup finds this party, not a stray "us"
    elsewhere); the anchor may continue in lowercase, so the caption's
    "Corp." still matches the body's "Corporation".  A token with no
    anchored evidence — a single-word party such as "IBM v. …" — falls
    back to unanchored occurrences under stricter thresholds.  A body
    spelling that outvotes the guess is adopted.  Guards:

      * a match whose anchoring neighbor has no lowercase letter (an
        all-caps heading, or the caption itself) carries no casing signal
        and is ignored;
      * an all-lowercase spelling is never adopted — prose legitimately
        lowercases articles ("the Boeing Company") that a caption keeps;
      * an ALL-CAPS spelling is adopted only for initialism-length tokens
        (≤ 4 letters) with repeated evidence: opinions that set party
        surnames in caps ("SMITH argues…") are typography, not spelling.
    """
    if not name or not body_text:
        return name
    tokens = name.split()
    if len(tokens) < 2:
        return name
    cores = [re.sub(r"[^A-Za-z]", "", t) for t in tokens]
    for i, tok in enumerate(tokens):
        core = cores[i]
        if (len(core) < 2 or core.lower() in ("v", "vs")
                or not tok.strip(".,;:()'\"").isalpha()):
            continue
        votes: dict[str, int] = {}
        for j in (i - 1, i + 1):
            if not 0 <= j < len(tokens):
                continue
            nb = cores[j]
            if len(nb) < 2 or nb.lower() in ("v", "vs"):
                continue
            # The caption may abbreviate what the prose spells out
            # ("Corp." / "Corporation"): let the anchor run on in lowercase.
            nb_pat = re.escape(nb) + r"[a-z]*"
            if j < i:
                pat = re.compile(
                    r"\b(%s)[\W_]{1,3}(%s)\b" % (nb_pat, re.escape(core)),
                    re.IGNORECASE)
                g_tok, g_nb = 2, 1
            else:
                pat = re.compile(
                    r"\b(%s)[\W_]{1,3}(%s)\b" % (re.escape(core), nb_pat),
                    re.IGNORECASE)
                g_tok, g_nb = 1, 2
            for m in pat.finditer(body_text):
                if not re.search(r"[a-z]", m.group(g_nb)):
                    continue  # all-caps context: no casing signal
                spelling = m.group(g_tok)
                if not spelling.islower():
                    votes[spelling] = votes.get(spelling, 0) + 1
        unanchored = not votes
        if unanchored:
            for m in re.finditer(r"\b%s\b" % re.escape(core), body_text,
                                 re.IGNORECASE):
                spelling = m.group(0)
                if not spelling.islower():
                    votes[spelling] = votes.get(spelling, 0) + 1
        if not votes:
            continue
        best = max(votes, key=lambda s: votes[s])
        cur = votes.get(core, 0)
        if best == core or votes[best] <= cur:
            continue
        if best.isupper():
            if len(core) > 4 or votes[best] < 2:
                continue  # caps typography, not spelling
            if unanchored and (votes[best] < 3 or votes[best] <= 2 * cur):
                continue
        elif unanchored and votes[best] < 2:
            continue
        tokens[i] = tok.replace(core, best)
    return " ".join(tokens)


def courtlistener_case_name(record: dict) -> str:
    """Return CourtListener's best case-name field without changing its case.

    API endpoints use different key styles.  The abbreviated name is preferred
    because it is closest to the form needed for a citation; the full caption
    remains a fallback.  Preserving the API's casing lets names such as
    ``NBCUniversal`` repair title-casing guesses when CAP metadata is absent.
    """
    if not isinstance(record, dict):
        return ""
    for key in ("case_name", "caseName", "case_name_full", "caseNameFull"):
        value = re.sub(r"<[^>]+>", "", str(record.get(key) or ""))
        value = re.sub(r"\s+", " ", value).strip()
        if value:
            return value
    return ""

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

_ET_AL_RE = re.compile(r",?\s+et\s+als?\.?\s*$", re.IGNORECASE)
_V_SPLIT_RE = re.compile(r"\s+vs?\.\s+")

# A parenthesized cross-reference to the underlying case — Alabama-style
# certiorari captions read "Ex parte MURPHY. (Re Murphy v. State)." — names
# a different case and is no part of this one's name.
_RELATED_CASE_NOTE_RE = re.compile(
    r"\s*\(\s*(?:in\s+)?re[:.\s][^()]*\)[.,;]?", re.IGNORECASE)


def strip_related_case_note(text: str) -> str:
    """Remove a "(Re <underlying case>)" caption cross-reference, leaving
    the case's own name: "Ex parte MURPHY. (Re Murphy v. State)." ->
    "Ex parte MURPHY.".  Captions without one pass through unchanged."""
    return _RELATED_CASE_NOTE_RE.sub("", text or "").strip()

# Procedural party designations from a reporter caption — "…, Plaintiff,"
# "…, et al., Defendants.", "…, Defendant-Appellant" — describe the party's
# role and are no part of the name (rule 10.2.1).  Stripped from the right,
# alternating with "et al." (rule 10.2.1(a)), until neither remains.  The
# leading comma is required so a party whose own name ends in one of these
# words is never clipped.
_ROLE_WORD = (
    r"(?:cross-|counter-|third-party )?"
    r"(?:appell(?:ants?|ees?)|plaintiffs?|defendants?|petitioners?|"
    r"respondents?|relators?|intervenors?|movants?|claimants?|garnishees?)"
)
_PARTY_ROLE_RE = re.compile(
    r",\s*" + _ROLE_WORD + r"(?:\s*[-–—/]\s*" + _ROLE_WORD + r")*\.?\s*$",
    re.IGNORECASE,
)


def _strip_given_names(p: str) -> str | None:
    """Surname-only form of a personal party name (rule 10.2.1(g)), or
    None when the party does not safely read as an individual's name."""
    p = _NAME_SUFFIX_RE.sub("", p)
    if "," in p or "&" in p or re.search(r"\bof\b", p, re.IGNORECASE):
        return None
    titled = False
    while True:
        m = _PERSONAL_TITLE_RE.match(p)
        if not m:
            break
        p = p[m.end():]
        titled = True
    tokens = p.split()
    # A caption's sentence period ("Ex parte Anthony P. MURPHY.") is
    # punctuation, not part of the surname; an initial's own period stays
    # ("Susan B."), as does an internal-dot abbreviation ("U.S.").
    if (tokens and len(tokens[-1]) > 2 and tokens[-1].endswith(".")
            and "." not in tokens[-1][:-1]):
        tokens[-1] = tokens[-1][:-1]
    if not 2 <= len(tokens) <= 4:
        return None
    low = [t.replace("’", "'").lower().rstrip(".") for t in tokens]
    if low[0] not in _GIVEN_NAMES:
        return None
    # Every token must look like a name part: a capitalized word, an
    # initial, or a surname particle — and none may be an organizational
    # word ("George Washington University" abbreviates instead) or a
    # business-entity term ("Katherine Inc." is a firm, not a person).
    for t, tl in zip(tokens, low):
        if (tl in _T6_WORDS or tl in _ORG_WORDS
                or re.sub(r"[^a-z]", "", tl) in _APPOSITIVE_ENTITY_TERMS
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
    # Bank", whose middle token flunks this check).  A stripped honorific
    # already establishes a natural person, so under one any name-shaped
    # middle token passes ("Dr. Theresa Swain Emory" -> "Emory").
    for t, tl in zip(tokens[1:i], low[1:i]):
        if not (titled or tl in _GIVEN_NAMES or tl in _SURNAME_PARTICLES
                or re.fullmatch(r"[A-Z]\.?", t)):
            return None
    return " ".join(tokens[i:])


# A comma-separated appositive beginning with one of these corporate or
# organizational designators marks the party as a firm whose name merely ends
# in that designator ("Sara Lee, Inc."; "Dean Witter Reynolds, Inc.") rather
# than a natural person followed by a title, so the surname reduction below is
# suppressed.  Compared after stripping every non-letter ("L.L.C." -> "llc").
_APPOSITIVE_ENTITY_TERMS = {
    "inc", "incorporated", "corp", "corporation", "co", "cos", "company",
    "companies", "llc", "llp", "lllp", "lp", "pllc", "plc", "pc", "pa",
    "na", "fsb", "ltd", "limited", "gmbh", "ag", "sa", "nv",
    "assn", "assoc", "assocs", "associates", "association",
    "bros", "brothers", "sons", "partners", "partnership",
    "group", "grp", "holding", "holdings", "trust", "bank", "fund",
    "foundation",
}


def _office_holder_surname(p: str) -> str | None:
    """Surname of a natural person named with a following office or descriptive
    title — "Gayle Franzen, Dir., Dep't of Corr., State of Ill." -> "Franzen".
    A named individual is cited by surname alone: given names drop (rule
    10.2.1(g)), the office describes the person and is omitted (10.2.1(e)), and
    only the first party is kept (10.2.1(a)).  Returns None when the text
    before the first comma is not a personal name, or when the appositive
    begins with a corporate designator that is really part of a firm's name
    ("Sara Lee, Inc."; "Dean Witter Reynolds, Inc.")."""
    head, sep, rest = p.partition(",")
    if not sep:
        return None
    surname = _strip_given_names(head)
    if surname is None:
        return None
    lead = re.search(r"[A-Za-z][\w'’.&-]*", rest)
    if lead and re.sub(r"[^a-z]", "", lead.group(0).lower()) in _APPOSITIVE_ENTITY_TERMS:
        return None
    return surname


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


# Procedural captions (rule 10.2.1(b)): "In re", "Ex parte", "In the Matter
# of".  The prefix is normalized to canonical Bluebook form regardless of the
# source's casing ("Ex Parte Young" -> "Ex parte Young"), and "Matter of" /
# "In the Matter of" fold to "In re".  The party after the prefix is
# abbreviated normally, so an anonymized party still reformats to initials
# ("In re JW" -> "In re J.W.").
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


def _format_procedural(party: str, *, recognize_initials: bool) -> str | None:
    """Canonicalize a procedural-phrase prefix and abbreviate the party that
    follows: "Ex Parte Young" -> "Ex parte Young", "In Re JW" -> "In re J.W.",
    "In the Matter of Smith" -> "In re Smith".  Returns None when the party
    carries no such prefix."""
    m = _PROCEDURAL_PREFIX_RE.match(party)
    if not m:
        return None
    rest = party[m.end():].strip()
    if not rest:
        return None
    prefix = _PROCEDURAL_CANON[re.sub(r"\s+", " ", m.group(1).lower())]
    return f"{prefix} {_abbreviate_party(rest, recognize_initials=recognize_initials)}"


def _is_bare_place(place: str) -> bool:
    """Whether a municipal unit's place part ("<Unit> of <place>") is just the
    place's proper name rather than a place followed by an institution.  A
    trailing institution shows up as a nested "… of …" phrase ("Department of
    Parks") or as a T6 word sitting after the place name — i.e. after a word
    that is not itself a T6 word ("New York Police Department").  A T6 word that
    *leads* the name is part of the place, not a descriptor, and is kept:
    "Commerce" (the city), "Central Falls"."""
    if re.search(r"\bof\b", place, re.IGNORECASE):
        return False
    seen_plain = False
    for w in place.split():
        if w.replace("’", "'").lower().rstrip(".,'") in _T6_WORDS:
            if seen_plain:
                return False
        else:
            seen_plain = True
    return True


def _abbreviate_party(party: str, *, recognize_initials: bool = True) -> str:
    p = re.sub(r"\s+", " ", party).strip()
    while True:  # designations and "et al." peel off the right in turn
        q = _ET_AL_RE.sub("", p.rstrip(" ,;")).rstrip(" ,;")
        q = _PARTY_ROLE_RE.sub("", q)
        if q == p:
            break
        p = q
    p = re.sub(r"^the\s+", "", p, flags=re.IGNORECASE)  # rule 10.2.1(d)
    p = re.sub(r"\bUnited States of America\b", "United States", p,
               flags=re.IGNORECASE)
    p = _STATE_OF_RE.sub("", p)  # "State of Washington" -> "Washington"
    # Rule 10.2.1(f): "city of," "county of," and like expressions are
    # omitted unless they begin the party name — "Bd. of Educ. of the
    # Borough of Hawthorne" -> "Bd. of Educ. of Hawthorne", while "City of
    # New York" as the whole party keeps its prefix (handled below).
    p = _MID_GEO_UNIT_RE.sub(r"\1", p)

    # Relator construction (rule 10.2.1(b)): split "<party> ex rel. <relator>".
    # The named party keeps its full geographic name (rule 10.2.2) — restored
    # if the source abbreviated it — while the relator abbreviates normally and
    # its given name drops (rule 10.2.1(g)): "Ind. Ex Rel. John Anderson" ->
    # "Indiana ex rel. Anderson".
    rel_m = _EX_REL_RE.search(p)
    if rel_m:
        head = _expand_geo_party(p[:rel_m.start()].strip(" ,"))
        tail = p[rel_m.end():].strip(" ,")
        if head and tail:
            lhs = _abbreviate_party(head, recognize_initials=recognize_initials)
            rhs = _abbreviate_party(tail, recognize_initials=recognize_initials)
            return f"{lhs} ex rel. {rhs}"

    if recognize_initials:  # rule 10.2.1(c): SEC, NLRB, FCC…
        initials = _recognized_initialism(p)
        if initials:
            return initials
    anon = _format_anonymous_initials(p)
    if anon is not None:
        return anon
    procedural = _format_procedural(p, recognize_initials=recognize_initials)
    if procedural is not None:
        return procedural
    if p.strip(" ,.").lower() in _GEO_PARTIES:
        return p

    m = _MUNICIPAL_RE.match(p)
    if m and _is_bare_place(m.group(2)):
        return f"{m.group(1)} of {m.group(2)}"

    # A municipal unit in suffix form ("Cook County", "New York City",
    # "Atlantic City") is the entire geographic party: its trailing unit word is
    # what identifies it, so the words ahead are always the place's proper name
    # — a T6 word among them ("Atlantic", "Central") belongs to that name, not
    # to an institution — and the whole party stays whole.  A larger entity puts
    # the unit word mid-name ("Cook County Bd. of Review"), where the '$' anchor
    # no longer matches and normal abbreviation applies.
    if _GEO_SUFFIX_RE.match(p):
        return p

    # Rule 10.2.1(a): only the first-listed party on a side is kept.  The
    # split applies only when *every* '&'/'and'-joined segment reads as an
    # individual's name ("Charles Ward & Mary Ward" → "Ward"), so a firm
    # name containing '&' ("Jones & Laughlin Steel Corp.") or an
    # institutional pairing stays intact.
    segs = re.split(r"\s+(?:&|and)\s+", p, flags=re.IGNORECASE)
    if len(segs) > 1:
        surnames = [_strip_given_names(s) for s in segs]
        if all(surnames):
            return surnames[0]

    surname = _strip_given_names(p)
    if surname is not None:
        return surname

    office_surname = _office_holder_surname(p)
    if office_surname is not None:
        return office_surname

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
    ('Inc.', 'LLC', 'Ltd.', …) and tidy the comma it left behind.

    Only a designator that *trails* the firm marker is dropped: redundant
    corporate forms are suffixes, so a token earlier in the name is part of
    the name proper.  This keeps a leading geographic abbreviation that
    happens to collide with an entity term — "Pa." (Pennsylvania) normalizes
    to "pa", which also denotes a Professional Association — so
    "Pa. Coal Co." is not mangled into "Coal Co."."""
    words = name.split()
    norm = [_norm_entity_token(w) for w in words]
    firm_idxs = [i for i, n in enumerate(norm) if n in _FIRM_MARKERS]
    if not firm_idxs:
        return name
    first_firm = firm_idxs[0]
    kept: list[str] = []
    for i, (w, n) in enumerate(zip(words, norm)):
        if i > first_firm and n in _REDUNDANT_ENTITY_TERMS:
            if kept and kept[-1].endswith(","):
                kept[-1] = kept[-1][:-1]
            continue
        kept.append(w)
    return " ".join(kept).strip().rstrip(",")


# Abbreviation tokens that legitimately end in a period ("Co.", "Corp.",
# "Inc.", "No.", "Ala."…), taken straight from the T6/T10 tables and the
# multi-word phrase abbreviations, so a trailing period following one of them
# is kept rather than mistaken for stray punctuation.
_ABBR_PERIOD_TOKENS = (
    {v.lower() for v in _WORD_MAP.values() if v.endswith(".")}
    | {v.lower() for v in _PHRASE_MAP.values() if v.endswith(".")}
)


def _strip_trailing_period(name: str) -> str:
    """Drop the stray sentence-ending period some sources append to a case
    name ("Ex parte Young." -> "Ex parte Young"), while keeping a period that
    belongs to a trailing abbreviation ("Pennsylvania Coal Co.") or an
    initialism ("In re J.G.G.", "Doe v. J.")."""
    if not name.endswith("."):
        return name
    tok = name.rsplit(None, 1)[-1]  # last whitespace-delimited token
    stem = tok[:-1]
    if "." in stem or len(stem) == 1 or tok.lower() in _ABBR_PERIOD_TOKENS:
        return name  # initialism, single initial, or real abbreviation
    return name[:-1]


def abbreviate_case_name(name: str) -> str:
    """Abbreviate a case name for use in a citation or filename per
    Bluebook rule 10.2.2 (= Indigo Book R8.3), dropping given names of
    individuals (rule 10.2.1(g)) and "State of" prefixes (10.2.1(f)).
    Safe to call twice."""
    name = re.sub(r"\s+", " ", name or "").strip()
    # Strip any "(Re <underlying case>)" cross-reference before splitting:
    # its own " v. " would otherwise masquerade as this case's separator.
    name = strip_related_case_note(name)
    if not name:
        return name
    parts = _V_SPLIT_RE.split(name, maxsplit=1)
    joined = " v. ".join(
        _drop_redundant_entity(_abbreviate_party(p)) for p in parts
    )
    return _strip_trailing_period(joined)


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
        # Rule 10.2.1(a): only the first-listed party on each side is kept —
        # but only when every '&'-joined segment reads as an individual, so
        # firm names containing '&' stay whole.
        ("Charles Ward & Mary Ward v. Johanan Zelikovsky",
         "Ward v. Zelikovsky"),
        ("Charles Ward and Mary Ward v. Johanan Zelikovsky",
         "Ward v. Zelikovsky"),
        ("John Smith & Acme Corp. v. Jane Doe",
         "John Smith & Acme Corp. v. Doe"),
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
        # A leading geographic abbreviation must survive rule 10.2.1(h): "Pa."
        # (Pennsylvania) collides with the "P.A." entity term but is part of the
        # name, not a trailing corporate suffix.
        ("Pennsylvania Coal Co. v. Mahon", "Pa. Coal Co. v. Mahon"),
        ("Pennsylvania Coal Company v. Mahon et al.", "Pa. Coal Co. v. Mahon"),
        # A redundant designator that *does* trail the firm marker is still
        # dropped.
        ("Acme Coal Corp., Inc. v. Smith", "Acme Coal Corp. v. Smith"),
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
        # A municipal unit that is the entire party keeps its full name in
        # either word order (rule 10.2.2): the unit word — City/County/Village/
        # Township/Parish — is not abbreviated, matching City/Town/Borough,
        # which no table abbreviates anyway.
        ("City of New York v. United States Department of Defense",
         "City of New York v. U.S. Dep't of Def."),
        ("Village of Arlington Heights v. "
         "Metropolitan Housing Development Corporation",
         "Village of Arlington Heights v. Metro. Hous. Dev. Corp."),
        ("County of Sacramento v. Lewis", "County of Sacramento v. Lewis"),
        ("Township of Willingboro v. Doe", "Township of Willingboro v. Doe"),
        ("Parish of Jefferson v. Doe", "Parish of Jefferson v. Doe"),
        ("Town of Greece v. Susan Galloway", "Town of Greece v. Galloway"),
        # Mid-name "city of"/"borough of" expressions are omitted (rule
        # 10.2.1(f)); the same expression *beginning* a party name is kept
        # (see the City/Village/Township cases above).
        ("Doremus v. Board of Education of the Borough of Hawthorne",
         "Doremus v. Bd. of Educ. of Hawthorne"),
        ("Board of Education of Township of Piscataway v. Taxman",
         "Bd. of Educ. of Piscataway v. Taxman"),
        ("Board of Education of Kiryas Joel Village School District "
         "v. Grumet",
         "Bd. of Educ. of Kiryas Joel Vill. Sch. Dist. v. Grumet"),
        ("Soldal v. Cook County", "Soldal v. Cook County"),
        ("Los Angeles County v. Humphries", "Los Angeles County v. Humphries"),
        ("Washington County v. Gunther", "Washington County v. Gunther"),
        ("Jefferson Parish v. Hyde", "Jefferson Parish v. Hyde"),
        ("Willingboro Township v. Doe", "Willingboro Township v. Doe"),
        ("New York City v. Doe", "New York City v. Doe"),
        ("Kansas City v. Doe", "Kansas City v. Doe"),
        # A T6 word inside the place's own proper name ("Atlantic", "Central",
        # "National", "Commerce") is part of the name, not a descriptor, so the
        # whole unit still stays whole rather than shortening to "Atl. City".
        ("Atlantic City v. Doe", "Atlantic City v. Doe"),
        ("Central City v. Doe", "Central City v. Doe"),
        ("National City v. Doe", "National City v. Doe"),
        ("City of Commerce v. Doe", "City of Commerce v. Doe"),
        ("City of Central Falls v. Doe", "City of Central Falls v. Doe"),
        # The unit word does abbreviate when an institution follows it and the
        # party is a larger entity rather than the bare place — whether the
        # institution is joined by "of" or trails the place directly.
        ("City of New York Department of Parks v. Doe",
         "City of N.Y. Dep't of Parks v. Doe"),
        ("City of New York Police Department v. Doe",
         "City of N.Y. Police Dep't v. Doe"),
        ("Atlantic City Board of Education v. Doe",
         "Atl. City Bd. of Educ. v. Doe"),
        ("Cook County Board of Review v. Smith",
         "Cook Cnty. Bd. of Review v. Smith"),
        ("Doe v. Cook County Department of Corrections",
         "Doe v. Cook Cnty. Dep't of Corr."),
        # Relator constructions (rule 10.2.1(b)): the named party ahead of
        # "ex rel." keeps its full geographic name (rule 10.2.2), even when the
        # source abbreviated it; the relator abbreviates normally.
        ("Indiana ex rel. Anderson v. Brand",
         "Indiana ex rel. Anderson v. Brand"),
        ("NAACP v. Alabama ex rel. Patterson",
         "NAACP v. Alabama ex rel. Patterson"),
        ("Ind. Ex Rel. Anderson v. Brand", "Indiana ex rel. Anderson v. Brand"),
        ("U.S. ex rel. John Turner v. Williams",
         "United States ex rel. Turner v. Williams"),
        ("United States ex rel. Skinner & Eddy Corp. v. McCarl",
         "United States ex rel. Skinner & Eddy Corp. v. McCarl"),
        ("State of Ohio ex rel. Smith v. Jones", "Ohio ex rel. Smith v. Jones"),
        ("People of the State of New York ex rel. Spitzer v. Grasso",
         "New York ex rel. Spitzer v. Grasso"),
        ("W. Va. ex rel. Discover Fin. Servs. v. Nibert",
         "West Virginia ex rel. Discover Fin. Servs. v. Nibert"),
        # A natural person named with an office/title is cited by surname alone
        # (rules 10.2.1(g), (e)); a firm ending in a corporate designator is
        # left intact.
        ("United States ex rel. Roger Grundset v. Gayle Franzen, "
         "Director, Department of Corrections, State of Illinois",
         "United States ex rel. Grundset v. Franzen"),
        ("Janet Reno, Attorney General v. American Civil Liberties Union",
         "Reno v. ACLU"),
        ("Sara Lee, Inc. v. Kraft Foods", "Sara Lee, Inc. v. Kraft Foods"),
        ("Dean Witter Reynolds, Inc. v. Byrd",
         "Dean Witter Reynolds, Inc. v. Byrd"),
        # An honorific or rank marks a natural person: the title drops (rule
        # 10.2.1(e)) and the surname reduction applies even through an
        # unrecognized middle name — but a brand or firm whose name merely
        # starts with such a word is never truncated.
        ("Pecos River Talc LLC v. Dr. Theresa Swain Emory",
         "Pecos River Talc LLC v. Emory"),
        ("Smith v. Sgt. William Brown, Jr.", "Smith v. Brown"),
        ("Doe v. Officer Daniel Pantaleo", "Doe v. Pantaleo"),
        ("Jones v. Lt. Col. James Wilson", "Jones v. Wilson"),
        ("Dr Pepper Bottling Co. v. Smith", "Dr Pepper Bottling Co. v. Smith"),
        ("Mrs. Fields Cookies v. Smith", "Mrs. Fields Cookies v. Smith"),
        ("Miss Universe L.P. v. Smith", "Miss Universe L.P. v. Smith"),
        # Party designations from a reporter caption strip from the right,
        # alternating with "et al." (rules 10.2.1, 10.2.1(a)).
        ("Pecos River Talc LLC, Plaintiff, v. Dr. Theresa Swain Emory, "
         "et al., Defendants.",
         "Pecos River Talc LLC v. Emory"),
        ("Standard Oil Co., Defendant-Appellant v. United States",
         "Standard Oil Co. v. United States"),
        # Stray trailing period from the source is dropped; a period that
        # belongs to a trailing abbreviation or initialism is kept.
        ("Ex parte Young.", "Ex parte Young"),
        ("Ex parte Merryman.", "Ex parte Merryman"),
        ("Youngstown Sheet & Tube Co. v. Sawyer.",
         "Youngstown Sheet & Tube Co. v. Sawyer"),
        ("Pennsylvania Coal Co.", "Pa. Coal Co."),
        ("In re J.G.G.", "In re J.G.G."),
        # Procedural-phrase prefix normalized to canonical form (rule
        # 10.2.1(b)) regardless of the source's casing.
        ("Ex Parte Young", "Ex parte Young"),
        ("In Re Gault", "In re Gault"),
        ("In Re Winship", "In re Winship"),
        ("Ex Parte Merryman.", "Ex parte Merryman"),
        ("In Re Gerald Gault", "In re Gault"),
        # Alabama-style certiorari caption: the "(Re <underlying case>)"
        # cross-reference drops, the sentence period is not part of the
        # surname, and the given names reduce (rule 10.2.1(g)).
        ("Ex parte Anthony P. Murphy. (Re Anthony Paul Murphy v. State).",
         "Ex parte Murphy"),
        ("Ex parte Anthony P. Murphy.", "Ex parte Murphy"),
        # A firm led by a given name is not a person (entity-term guard).
        ("Katherine Inc. v. Smith", "Katherine Inc. v. Smith"),
        ("Matter of Standard Jury Instructions",
         "In re Standard Jury Instructions"),
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
