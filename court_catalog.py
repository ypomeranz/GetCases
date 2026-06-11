"""
Court catalog for CourtListener
===============================
Structured catalog of CourtListener court IDs:

  * ``COURT_BLUEBOOK`` — court ID → Bluebook (21st ed.) abbreviation,
    used for citations and filenames.  SCOTUS is absent intentionally —
    the court name is omitted for SCOTUS cites.
  * ``CATALOG`` — a nested tree for the court-picker UI.  A group is
    ``(label, [children])``; a leaf is ``(court_id, label)``.  The two are
    distinguished by the type of the second element (list vs str).
"""

from __future__ import annotations

# --- Federal appellate -----------------------------------------------------

CIRCUIT_COURTS: dict[str, str] = {
    "ca1":   "1st Cir.",
    "ca2":   "2d Cir.",
    "ca3":   "3d Cir.",
    "ca4":   "4th Cir.",
    "ca5":   "5th Cir.",
    "ca6":   "6th Cir.",
    "ca7":   "7th Cir.",
    "ca8":   "8th Cir.",
    "ca9":   "9th Cir.",
    "ca10":  "10th Cir.",
    "ca11":  "11th Cir.",
    "cadc":  "D.C. Cir.",
    "cafc":  "Fed. Cir.",
}

_CIRCUIT_LABELS: list[tuple[str, str]] = [
    ("ca1", "First Circuit"),
    ("ca2", "Second Circuit"),
    ("ca3", "Third Circuit"),
    ("ca4", "Fourth Circuit"),
    ("ca5", "Fifth Circuit"),
    ("ca6", "Sixth Circuit"),
    ("ca7", "Seventh Circuit"),
    ("ca8", "Eighth Circuit"),
    ("ca9", "Ninth Circuit"),
    ("ca10", "Tenth Circuit"),
    ("ca11", "Eleventh Circuit"),
    ("cadc", "D.C. Circuit"),
    ("cafc", "Federal Circuit"),
]

# --- Federal district courts ------------------------------------------------

DISTRICT_COURTS: dict[str, str] = {
    "akd":   "D. Alaska",
    "almd":  "M.D. Ala.",   "alnd":  "N.D. Ala.",   "alsd":  "S.D. Ala.",
    "ared":  "E.D. Ark.",   "arwd":  "W.D. Ark.",
    "azd":   "D. Ariz.",
    "cacd":  "C.D. Cal.",   "caed":  "E.D. Cal.",
    "cand":  "N.D. Cal.",   "casd":  "S.D. Cal.",
    "cod":   "D. Colo.",
    "ctd":   "D. Conn.",
    "ded":   "D. Del.",
    "dcd":   "D.D.C.",
    "flmd":  "M.D. Fla.",   "flnd":  "N.D. Fla.",   "flsd":  "S.D. Fla.",
    "gamd":  "M.D. Ga.",    "gand":  "N.D. Ga.",     "gasd":  "S.D. Ga.",
    "gud":   "D. Guam",
    "hid":   "D. Haw.",
    "idd":   "D. Idaho",
    "ilcd":  "C.D. Ill.",   "ilnd":  "N.D. Ill.",    "ilsd":  "S.D. Ill.",
    "innd":  "N.D. Ind.",   "insd":  "S.D. Ind.",
    "iand":  "N.D. Iowa",   "iasd":  "S.D. Iowa",
    "ksd":   "D. Kan.",
    "kyed":  "E.D. Ky.",    "kywd":  "W.D. Ky.",
    "laed":  "E.D. La.",    "lamd":  "M.D. La.",     "lawd":  "W.D. La.",
    "med":   "D. Me.",
    "mdd":   "D. Md.",
    "mad":   "D. Mass.",
    "mied":  "E.D. Mich.",  "miwd":  "W.D. Mich.",
    "mnd":   "D. Minn.",
    "msnd":  "N.D. Miss.",  "mssd":  "S.D. Miss.",
    "moed":  "E.D. Mo.",    "mowd":  "W.D. Mo.",
    "mtd":   "D. Mont.",
    "ned":   "D. Neb.",
    "nvd":   "D. Nev.",
    "nhd":   "D.N.H.",
    "njd":   "D.N.J.",
    "nmd":   "D.N.M.",
    "nmid":  "D.N. Mar. I.",
    "nyed":  "E.D.N.Y.",    "nynd":  "N.D.N.Y.",
    "nysd":  "S.D.N.Y.",    "nywd":  "W.D.N.Y.",
    "nced":  "E.D.N.C.",    "ncmd":  "M.D.N.C.",     "ncwd":  "W.D.N.C.",
    "ndd":   "D.N.D.",
    "ohnd":  "N.D. Ohio",   "ohsd":  "S.D. Ohio",
    "oked":  "E.D. Okla.",  "oknd":  "N.D. Okla.",   "okwd":  "W.D. Okla.",
    "ord":   "D. Or.",
    "paed":  "E.D. Pa.",    "pamd":  "M.D. Pa.",     "pawd":  "W.D. Pa.",
    "prd":   "D.P.R.",
    "rid":   "D.R.I.",
    "scd":   "D.S.C.",
    "sdd":   "D.S.D.",
    "tned":  "E.D. Tenn.",  "tnmd":  "M.D. Tenn.",   "tnwd":  "W.D. Tenn.",
    "txed":  "E.D. Tex.",   "txnd":  "N.D. Tex.",
    "txsd":  "S.D. Tex.",   "txwd":  "W.D. Tex.",
    "utd":   "D. Utah",
    "vtd":   "D. Vt.",
    "vaed":  "E.D. Va.",    "vawd":  "W.D. Va.",
    "vid":   "D.V.I.",
    "waed":  "E.D. Wash.",  "wawd":  "W.D. Wash.",
    "wvnd":  "N.D. W. Va.", "wvsd":  "S.D. W. Va.",
    "wied":  "E.D. Wis.",   "wiwd":  "W.D. Wis.",
    "wyd":   "D. Wyo.",
}

# --- Specialized federal courts ----------------------------------------------

SPECIAL_COURTS: dict[str, str] = {
    "cit":   "Ct. Int'l Trade",
    "uscfc": "Fed. Cl.",
    "tax":   "T.C.",
    "cavet": "Vet. App.",
    "caaf":  "C.A.A.F.",
    "bap1":  "B.A.P. 1st Cir.", "bap2": "B.A.P. 2d Cir.",
    "bap6":  "B.A.P. 6th Cir.", "bap8": "B.A.P. 8th Cir.",
    "bap9":  "B.A.P. 9th Cir.", "bap10": "B.A.P. 10th Cir.",
}

_SPECIAL_LABELS: list[tuple[str, str]] = [
    ("uscfc", "Court of Federal Claims"),
    ("cit", "Court of International Trade"),
    ("tax", "U.S. Tax Court"),
    ("cavet", "Court of Appeals for Veterans Claims"),
    ("caaf", "Court of Appeals for the Armed Forces"),
    ("bap1", "Bankruptcy App. Panel — 1st Cir."),
    ("bap2", "Bankruptcy App. Panel — 2d Cir."),
    ("bap6", "Bankruptcy App. Panel — 6th Cir."),
    ("bap8", "Bankruptcy App. Panel — 8th Cir."),
    ("bap9", "Bankruptcy App. Panel — 9th Cir."),
    ("bap10", "Bankruptcy App. Panel — 10th Cir."),
]

# --- State courts ------------------------------------------------------------
# Per state: (state name, [(court_id, bluebook abbr, display label), ...]).
# The first entry is the state's court of last resort.

STATE_COURTS: list[tuple[str, list[tuple[str, str, str]]]] = [
    ("Alabama", [
        ("ala", "Ala.", "Supreme Court"),
        ("alactapp", "Ala. Crim. App.", "Court of Criminal Appeals"),
        ("alacivapp", "Ala. Civ. App.", "Court of Civil Appeals"),
    ]),
    ("Alaska", [("alaska", "Alaska", "Supreme Court")]),
    ("Arizona", [
        ("ariz", "Ariz.", "Supreme Court"),
        ("arizctapp", "Ariz. Ct. App.", "Court of Appeals"),
    ]),
    ("Arkansas", [
        ("ark", "Ark.", "Supreme Court"),
        ("arkctapp", "Ark. Ct. App.", "Court of Appeals"),
    ]),
    ("California", [
        ("cal", "Cal.", "Supreme Court"),
        ("calctapp", "Cal. Ct. App.", "Court of Appeal"),
    ]),
    ("Colorado", [
        ("colo", "Colo.", "Supreme Court"),
        ("coloctapp", "Colo. App.", "Court of Appeals"),
    ]),
    ("Connecticut", [
        ("conn", "Conn.", "Supreme Court"),
        ("connappct", "Conn. App.", "Appellate Court"),
    ]),
    ("Delaware", [
        ("del", "Del.", "Supreme Court"),
        ("delsuperct", "Del. Super. Ct.", "Superior Court"),
    ]),
    ("District of Columbia", [("dc", "D.C.", "Court of Appeals")]),
    ("Florida", [
        ("fla", "Fla.", "Supreme Court"),
        ("fladistctapp", "Fla. Dist. Ct. App.", "District Courts of Appeal"),
    ]),
    ("Georgia", [
        ("ga", "Ga.", "Supreme Court"),
        ("gactapp", "Ga. Ct. App.", "Court of Appeals"),
    ]),
    ("Hawaii", [
        ("haw", "Haw.", "Supreme Court"),
        ("hawapp", "Haw. Ct. App.", "Intermediate Court of Appeals"),
    ]),
    ("Idaho", [
        ("idaho", "Idaho", "Supreme Court"),
        ("idahoctapp", "Idaho Ct. App.", "Court of Appeals"),
    ]),
    ("Illinois", [
        ("ill", "Ill.", "Supreme Court"),
        ("illappct", "Ill. App. Ct.", "Appellate Court"),
    ]),
    ("Indiana", [
        ("ind", "Ind.", "Supreme Court"),
        ("indctapp", "Ind. Ct. App.", "Court of Appeals"),
    ]),
    ("Iowa", [
        ("iowa", "Iowa", "Supreme Court"),
        ("iowactapp", "Iowa Ct. App.", "Court of Appeals"),
    ]),
    ("Kansas", [
        ("kan", "Kan.", "Supreme Court"),
        ("kanctapp", "Kan. Ct. App.", "Court of Appeals"),
    ]),
    ("Kentucky", [
        ("ky", "Ky.", "Supreme Court"),
        ("kyctapp", "Ky. Ct. App.", "Court of Appeals"),
    ]),
    ("Louisiana", [
        ("la", "La.", "Supreme Court"),
        ("lactapp", "La. Ct. App.", "Courts of Appeal"),
    ]),
    ("Maine", [("me", "Me.", "Supreme Judicial Court")]),
    ("Maryland", [
        ("md", "Md.", "Supreme Court (Court of Appeals)"),
        ("mdctspecapp", "Md. App.", "Appellate Court (Court of Special Appeals)"),
    ]),
    ("Massachusetts", [
        ("mass", "Mass.", "Supreme Judicial Court"),
        ("massappct", "Mass. App. Ct.", "Appeals Court"),
    ]),
    ("Michigan", [
        ("mich", "Mich.", "Supreme Court"),
        ("michctapp", "Mich. Ct. App.", "Court of Appeals"),
    ]),
    ("Minnesota", [
        ("minn", "Minn.", "Supreme Court"),
        ("minnctapp", "Minn. Ct. App.", "Court of Appeals"),
    ]),
    ("Mississippi", [
        ("miss", "Miss.", "Supreme Court"),
        ("missctapp", "Miss. Ct. App.", "Court of Appeals"),
    ]),
    ("Missouri", [
        ("mo", "Mo.", "Supreme Court"),
        ("moctapp", "Mo. Ct. App.", "Court of Appeals"),
    ]),
    ("Montana", [("mont", "Mont.", "Supreme Court")]),
    ("Nebraska", [
        ("neb", "Neb.", "Supreme Court"),
        ("nebctapp", "Neb. Ct. App.", "Court of Appeals"),
    ]),
    ("Nevada", [("nev", "Nev.", "Supreme Court")]),
    ("New Hampshire", [("nh", "N.H.", "Supreme Court")]),
    ("New Jersey", [
        ("nj", "N.J.", "Supreme Court"),
        ("njsuperctappdiv", "N.J. Super. Ct. App. Div.",
         "Superior Court, Appellate Division"),
    ]),
    ("New Mexico", [
        ("nm", "N.M.", "Supreme Court"),
        ("nmctapp", "N.M. Ct. App.", "Court of Appeals"),
    ]),
    ("New York", [
        ("ny", "N.Y.", "Court of Appeals"),
        ("nyappdiv", "N.Y. App. Div.", "Appellate Division"),
    ]),
    ("North Carolina", [
        ("nc", "N.C.", "Supreme Court"),
        ("ncctapp", "N.C. Ct. App.", "Court of Appeals"),
    ]),
    ("North Dakota", [("nd", "N.D.", "Supreme Court")]),
    ("Ohio", [
        ("ohio", "Ohio", "Supreme Court"),
        ("ohioctapp", "Ohio Ct. App.", "Courts of Appeals"),
    ]),
    ("Oklahoma", [
        ("okla", "Okla.", "Supreme Court"),
        ("oklacrimapp", "Okla. Crim. App.", "Court of Criminal Appeals"),
        ("oklacivapp", "Okla. Civ. App.", "Court of Civil Appeals"),
    ]),
    ("Oregon", [
        ("or", "Or.", "Supreme Court"),
        ("orctapp", "Or. Ct. App.", "Court of Appeals"),
    ]),
    ("Pennsylvania", [
        ("pa", "Pa.", "Supreme Court"),
        ("pasuperct", "Pa. Super. Ct.", "Superior Court"),
        ("pacommwct", "Pa. Commw. Ct.", "Commonwealth Court"),
    ]),
    ("Rhode Island", [("ri", "R.I.", "Supreme Court")]),
    ("South Carolina", [
        ("sc", "S.C.", "Supreme Court"),
        ("scctapp", "S.C. Ct. App.", "Court of Appeals"),
    ]),
    ("South Dakota", [("sd", "S.D.", "Supreme Court")]),
    ("Tennessee", [
        ("tenn", "Tenn.", "Supreme Court"),
        ("tennctapp", "Tenn. Ct. App.", "Court of Appeals"),
        ("tenncrimapp", "Tenn. Crim. App.", "Court of Criminal Appeals"),
    ]),
    ("Texas", [
        ("tex", "Tex.", "Supreme Court"),
        ("texapp", "Tex. App.", "Courts of Appeals"),
    ]),
    ("Utah", [
        ("utah", "Utah", "Supreme Court"),
        ("utahctapp", "Utah Ct. App.", "Court of Appeals"),
    ]),
    ("Vermont", [("vt", "Vt.", "Supreme Court")]),
    ("Virginia", [
        ("va", "Va.", "Supreme Court"),
        ("vactapp", "Va. Ct. App.", "Court of Appeals"),
    ]),
    ("Washington", [
        ("wash", "Wash.", "Supreme Court"),
        ("washctapp", "Wash. Ct. App.", "Court of Appeals"),
    ]),
    ("West Virginia", [("wva", "W. Va.", "Supreme Court of Appeals")]),
    ("Wisconsin", [
        ("wis", "Wis.", "Supreme Court"),
        ("wisctapp", "Wis. Ct. App.", "Court of Appeals"),
    ]),
    ("Wyoming", [("wyo", "Wyo.", "Supreme Court")]),
]

# --- Merged Bluebook map (same content the GUI used previously) ---------------

COURT_BLUEBOOK: dict[str, str] = {}
COURT_BLUEBOOK.update(CIRCUIT_COURTS)
COURT_BLUEBOOK.update(DISTRICT_COURTS)
COURT_BLUEBOOK.update(SPECIAL_COURTS)
for _state, _courts in STATE_COURTS:
    for _cid, _abbr, _label in _courts:
        COURT_BLUEBOOK[_cid] = _abbr

# --- Picker tree --------------------------------------------------------------
# Group: (label, [children]); leaf: (court_id, label).

CATALOG: list[tuple] = [
    ("Federal", [
        ("scotus", "Supreme Court of the United States"),
        ("Courts of Appeals", [
            (cid, f"{label} ({CIRCUIT_COURTS[cid]})") for cid, label in _CIRCUIT_LABELS
        ]),
        ("District Courts", [
            (cid, abbr) for cid, abbr in sorted(
                DISTRICT_COURTS.items(), key=lambda kv: kv[1]
            )
        ]),
        ("Specialized", [
            (cid, label) for cid, label in _SPECIAL_LABELS
        ]),
    ]),
    ("State", [
        (state, [(cid, f"{label} ({abbr})") for cid, abbr, label in courts])
        for state, courts in STATE_COURTS
    ]),
]


def all_court_ids() -> set[str]:
    """Every court ID present in the picker catalog."""
    ids: set[str] = set()

    def walk(nodes) -> None:
        for node in nodes:
            label_or_id, payload = node
            if isinstance(payload, list):
                walk(payload)
            else:
                ids.add(label_or_id)

    walk(CATALOG)
    return ids
