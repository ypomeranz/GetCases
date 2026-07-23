import json
import os
import tkinter as tk
import unittest
from types import SimpleNamespace
from unittest.mock import ANY, Mock, call, patch

os.environ["GETCASES_SKIP_DEPENDENCY_PROMPT"] = "1"

from bluebook_names import (
    apply_caption_case_reference,
    abbreviate_case_name,
    caption_case_reference_tokens,
    collapse_personal_all_caps_run,
    courtlistener_case_name,
    is_personal_all_caps_run,
    normal_case_caption,
    refine_caption_case,
)
from citation_overrides import (
    add_pin_to_base,
    citation_identity_keys,
    find_override,
    format_edited_citation,
    update_overrides,
)
from court_catalog import bluebook_federal_trial_court
from courtlistener_gui import (
    CourtListenerGUI,
    _CaseTabPage,
    _ScholarTextWindow,
    _CaseLawPageOpinion,
    _CaseLawTextRecord,
    _CaseTabsWindow,
    _case_law_text_for_pdf_url,
    _case_law_text_record,
    _case_pdf_text_source,
    _open_statute_action,
    _case_law_page_opinions,
    _match_case_law_page_opinion,
    _opinion_db_spotlight_results,
    _citation_search_variants,
    _cl_item_for_citation,
    _combined_parts_cover_typed,
    _cut_companion_cases,
    _dump_to_rtf,
    _nominative_display_cite,
    _pick_combined_opinion,
    _plain_without_layout_chars,
    _recap_citation_ranges,
    _recap_spec_index,
    _scholar_caption_name,
    _spotlight_case_action,
    _special_citation_ranges,
    _wisconsin_display_cite,
)
from google_scholar import Block, OpinionPart, Span, educate_quotes
import us_code


class SmartQuoteTests(unittest.TestCase):
    def test_outer_double_quote_closes_after_spaced_inner_single_quote(self):
        text = (
            'The Government safeguards the flag\'s identity " \'as the unique '
            'and unalloyed symbol of the Nation.\' " Brief for United States.'
        )
        expected = (
            "The Government safeguards the flag" + chr(0x2019)
            + "s identity " + chr(0x201c) + " " + chr(0x2018)
            + "as the unique and unalloyed symbol of the Nation."
            + chr(0x2019) + " " + chr(0x201d)
            + " Brief for United States."
        )
        self.assertEqual(educate_quotes(text), expected)

    def test_separate_double_quoted_phrases_still_pair(self):
        expected = (
            "One " + chr(0x201c) + "quotation" + chr(0x201d)
            + " and another " + chr(0x201c) + "quotation" + chr(0x201d) + "."
        )
        self.assertEqual(
            educate_quotes('One "quotation" and another "quotation".'), expected,
        )


class CaptionCapitalizationTests(unittest.TestCase):
    def test_apostrophe_and_mc_names_from_all_caps(self):
        self.assertEqual(
            normal_case_caption("O'BRIEN v. MCFADDEN"),
            "O'Brien v. McFadden",
        )
        self.assertEqual(normal_case_caption("McFADDEN"), "McFadden")

    def test_authoritative_mixed_case_brand_is_preserved(self):
        self.assertEqual(
            normal_case_caption("NBCUniversal Media, LLC"),
            "NBCUniversal Media, LLC",
        )

    def test_quoted_all_caps_word_capitalizes_inside_the_quotes(self):
        # THE "SCOTLAND.", 105 U.S. 24: the capital must reach the first
        # letter through the reporter's quotation mark, in either style.
        self.assertEqual(
            normal_case_caption("THE “SCOTLAND.”"), "The “Scotland.”")
        self.assertEqual(
            normal_case_caption('THE "SCOTLAND."'), 'The "Scotland."')
        # A digit-led token is an ordinal, not a name: its tail stays lower.
        self.assertEqual(
            normal_case_caption("42ND STREET CO. v. SMITH"),
            "42nd Street Co. v. Smith")

    def test_naacp_survives_all_caps_caption_normalization(self):
        name = normal_case_caption("NAACP v. ALABAMA")
        self.assertEqual(name, "NAACP v. Alabama")
        self.assertEqual(abbreviate_case_name(name), "NAACP v. Alabama")

    def test_courtlistener_name_preserves_api_capitalization(self):
        self.assertEqual(
            courtlistener_case_name({
                "case_name": "NBCUniversal Media, LLC v. Example",
                "case_name_full": "A Different Full Caption",
            }),
            "NBCUniversal Media, LLC v. Example",
        )
        self.assertEqual(
            courtlistener_case_name({
                "caseNameFull": "O'Brien v. McFadden",
            }),
            "O'Brien v. McFadden",
        )

    def test_usa_entity_is_not_mistaken_for_caps_surname(self):
        self.assertFalse(is_personal_all_caps_run(["USA", "LLC"], ["McDonald's"]))
        self.assertFalse(is_personal_all_caps_run(["MEDIA", "LLC"], ["NBCUniversal"]))
        self.assertTrue(is_personal_all_caps_run(["BREWBAKER"], ["Brent"]))
        self.assertTrue(is_personal_all_caps_run(["THOMAS"], ["Corrine", "Morgan"]))
        self.assertTrue(is_personal_all_caps_run(["EMORY"], ["Dr.", "Theresa", "Swain"]))

    def test_mixed_case_caps_run_drops_any_name_shaped_first_names(self):
        self.assertEqual(
            collapse_personal_all_caps_run("Corrine Morgan THOMAS"),
            "THOMAS",
        )
        self.assertEqual(
            collapse_personal_all_caps_run("Dr. Theresa Swain EMORY"),
            "EMORY",
        )
        self.assertEqual(
            collapse_personal_all_caps_run("McDonald's USA, LLC"),
            "McDonald's USA, LLC",
        )
        self.assertEqual(
            collapse_personal_all_caps_run("NBCUniversal MEDIA, LLC"),
            "NBCUniversal MEDIA, LLC",
        )
        self.assertEqual(
            collapse_personal_all_caps_run("The BOEING COMPANY"),
            "The BOEING COMPANY",
        )
        self.assertEqual(
            collapse_personal_all_caps_run("A&M Records, Inc."),
            "A&M Records, Inc.",
        )
        self.assertEqual(
            collapse_personal_all_caps_run("CITIZENS FOR A BETTER ENVIRONMENT"),
            "CITIZENS FOR A BETTER ENVIRONMENT",
        )
        self.assertEqual(
            collapse_personal_all_caps_run("The PRESIDENT"),
            "The PRESIDENT",
        )

    def test_possessive_s_is_not_a_surname_prefix(self):
        # The O'BRIEN → O'Brien rule must not capitalize a possessive:
        # Wasserman's Inc. v. Township of Middletown, 137 N.J. 238 (1994).
        self.assertEqual(
            normal_case_caption("WASSERMAN'S INC. v. MIDDLETOWN"),
            "Wasserman's Inc. v. Middletown",
        )
        self.assertEqual(
            normal_case_caption("JACKSON WOMEN'S HEALTH ORGANIZATION"),
            "Jackson Women's Health Organization",
        )
        self.assertEqual(normal_case_caption("McDONALD'S"), "McDonald's")
        # An already-stored artifact is repaired at abbreviation time (the
        # party-leading "The" drops under rule 10.2.1(d) as before).
        self.assertEqual(
            abbreviate_case_name("Inglis v. The Sailor'S Snug Harbour"),
            "Inglis v. Sailor's Snug Harbour",
        )

    def test_single_letter_initials_keep_their_capitals(self):
        # R.A.V. v. City of St. Paul, 505 U.S. 377 (1992): the spaced
        # initials "R. A. V." collide with the small words "a" and "v".
        self.assertEqual(
            abbreviate_case_name(normal_case_caption(
                "R. A. V., PETITIONER v. CITY OF ST. PAUL, MINNESOTA")),
            "R.A.V. v. City of St. Paul, Minnesota",
        )
        self.assertEqual(
            normal_case_caption("SAMUEL A. WORCESTER v. GEORGIA"),
            "Samuel A. Worcester v. Georgia",
        )
        # A lone "V." stays the separator when no initial precedes it.
        self.assertEqual(
            normal_case_caption("SMITH V. JONES"), "Smith v. Jones")

    def test_mixed_case_small_words_are_lowercased(self):
        # Partially mixed-case captions bypass all-caps normalization, so
        # "Of"/"OF" survive into the name ("District Of Columbia").
        self.assertEqual(
            abbreviate_case_name("District Of Columbia v. Heller"),
            "District of Columbia v. Heller",
        )
        self.assertEqual(
            abbreviate_case_name("Walz v. Tax Comm'n OF N.Y."),
            "Walz v. Tax Comm'n of N.Y.",
        )
        # Small words inside an all-caps run carry no casing signal and
        # keep their caps (T6 word abbreviation applies as before).
        self.assertEqual(
            abbreviate_case_name(
                "CITIZENS FOR A BETTER ENVIRONMENT v. Anne Gorsuch"),
            "CITIZENS FOR A BETTER Env't v. Gorsuch",
        )

    def test_deslandes_caption_keeps_mcdonalds(self):
        self.assertEqual(
            abbreviate_case_name("Leinani Deslandes v. McDonald's USA LLC"),
            "Deslandes v. McDonald's USA LLC",
        )

    def test_titled_person_reduces_to_surname(self):
        # Pecos River Talc LLC v. Emory (E.D. Va. 2026): the honorific drops
        # and the surname survives an unrecognized middle name.
        self.assertEqual(
            abbreviate_case_name(
                "Pecos River Talc LLC v. Dr. Theresa Swain Emory"),
            "Pecos River Talc LLC v. Emory",
        )
        self.assertEqual(
            abbreviate_case_name("Smith v. Sgt. William Brown, Jr."),
            "Smith v. Brown",
        )

    def test_middle_initial_marks_a_natural_person(self):
        # Rule 10.2.1(g): organizations never reduce a middle word to a
        # single letter, so the initial licenses the surname reduction
        # even for a given name no list covers.
        self.assertEqual(
            abbreviate_case_name("Okello T. Chatrie v. United States"),
            "Chatrie v. United States",
        )
        self.assertEqual(
            abbreviate_case_name("Dred Scott v. John F.A. Sandford"),
            "Scott v. Sandford",
        )
        self.assertEqual(
            abbreviate_case_name("Moore v. Mahendra J. Shah"),
            "Moore v. Shah",
        )
        # …but a firm named for a person keeps its full name.
        self.assertEqual(
            abbreviate_case_name("Susan B. Anthony List v. Driehaus"),
            "Susan B. Anthony List v. Driehaus",
        )
        self.assertEqual(
            abbreviate_case_name("A. H. Robins Co. v. Piccinin"),
            "A. H. Robins Co. v. Piccinin",
        )

    def test_generational_suffix_marks_a_natural_person(self):
        self.assertEqual(
            abbreviate_case_name("Valentino Shine, Sr. v. United States"),
            "Shine v. United States",
        )

    def test_title_never_truncates_a_brand_name(self):
        for name in ("Dr Pepper Bottling Co. v. Smith",
                     "Mrs. Fields Cookies v. Smith",
                     "Miss Universe L.P. v. Smith"):
            self.assertEqual(abbreviate_case_name(name), name)

    def test_mid_name_municipal_unit_is_omitted(self):
        # Doremus v. Bd. of Educ. of Hawthorne, 342 U.S. 429 (1952): rule
        # 10.2.1(f) omits "city of"/"borough of" and like expressions unless
        # they begin the party name.
        self.assertEqual(
            abbreviate_case_name(normal_case_caption(
                "DOREMUS ET AL. v. BOARD OF EDUCATION OF THE BOROUGH OF "
                "HAWTHORNE ET AL.")),
            "Doremus v. Bd. of Educ. of Hawthorne",
        )
        self.assertEqual(
            abbreviate_case_name("City of New York v. Doe"),
            "City of New York v. Doe",
        )

    def test_ex_parte_caption_with_related_case_note(self):
        # Ex parte Murphy, 596 So. 2d 45 (Ala. 1992): the "(Re Murphy v.
        # State)" cross-reference to the underlying case drops, and the
        # petitioner reduces to the surname.
        self.assertEqual(
            abbreviate_case_name(normal_case_caption(
                "Ex parte Anthony P. MURPHY. "
                "(Re Anthony Paul Murphy v. State).")),
            "Ex parte Murphy",
        )

    def test_caption_role_designations_are_stripped(self):
        self.assertEqual(
            abbreviate_case_name(
                "Pecos River Talc LLC, Plaintiff, v. "
                "Dr. Theresa Swain Emory, et al., Defendants."),
            "Pecos River Talc LLC v. Emory",
        )
        self.assertEqual(
            abbreviate_case_name(
                "Standard Oil Co., Defendant-Appellant v. United States"),
            "Standard Oil Co. v. United States",
        )


class ConsolidatedAndSinglePartyCaptionTests(unittest.TestCase):
    def test_historical_bank_wrapper_uses_opinions_own_entity_name(self):
        # Osborn v. Bank of the United States, 22 U.S. (9 Wheat.) 738
        # (1824): the reporter caption gives the bank's formal charter style,
        # while Marshall and Johnson repeatedly call the party the Bank of
        # the United States in their opinions.
        blocks = [
            Block("center", [Span(
                "OSBORN and others, Appellants, v. The PRESIDENT, "
                "DIRECTORS, AND COMPANY OF THE BANK OF THE UNITED STATES, "
                "Respondents."
            )]),
            Block("para", [Span(
                "The Bank of the United States is an instrument of the "
                "national government."
            )]),
            Block("para", [Span(
                "The charter permits the Bank of the United States to sue."
            )]),
        ]

        name = _scholar_caption_name(blocks)
        self.assertEqual(
            name, "Osborn and others v. Bank of the United States")
        self.assertEqual(
            abbreviate_case_name(name), "Osborn v. Bank of the U.S.")

    def test_geographic_party_starts_joined_respondent_list(self):
        # General Telephone Co. of the Southwest v. United States,
        # 449 F.2d 846 (5th Cir. 1971): United States and the FCC are two
        # respondents, not one institutional name.  Only the first is kept,
        # and a geographic party is never shortened to "U.S.".
        blocks = [Block("center", [Span(
            "GENERAL TELEPHONE COMPANY OF the SOUTHWEST et al., Petitioners, "
            "v. UNITED STATES of America and Federal Communications "
            "Commission, Respondents, National Cable Television Association, "
            "Inc., et al., Intervenors."
        )])]

        self.assertEqual(
            abbreviate_case_name(_scholar_caption_name(blocks)),
            "Gen. Tel. Co. of the Sw. v. United States",
        )
        self.assertNotIn(
            "southwest",
            caption_case_reference_tokens(
                "General Telephone Company of the Southwest v. United States",
                "",
            ),
        )
        self.assertEqual(
            abbreviate_case_name(
                "National Labor Relations Board v. "
                "Jones and Laughlin Steel Corporation"
            ),
            "NLRB v. Jones & Laughlin Steel Corp.",
        )

    def test_in_re_caption_uses_alias_role_and_page_markers_as_boundaries(self):
        blocks = [
            Block("center", [
                Span(
                    "IN RE: IMERYS TALC AMERICA, INC., a/k/a Luzenac "
                    "America, Inc. a/k/a Imerys Talc Ohio Inc. a/k/a "
                    "Imerys Talc Delaware, Inc., et al., Debtors "
                ),
                Span("*362", pagenum=True),
                Span(" Cyprus Historical Excess Insurers, Appellants."),
            ]),
            Block("para", [Span(
                "Appellees Imerys Talc America, Inc. and its affiliates "
                "filed for bankruptcy."
            )]),
        ]

        name = _scholar_caption_name(blocks)

        self.assertEqual(
            abbreviate_case_name(name),
            "In re Imerys Talc Am., Inc.",
        )

    def test_quoted_in_rem_vessel_caption_drops_the_quotes(self):
        # The Scotland, 105 U.S. 24 (1882): the reporter prints the vessel's
        # name in quotation marks ("THE “SCOTLAND.”").  The quotes are the
        # reporter's typography — the citation name is "The Scotland", with
        # the ship's capital and its rule-10.2.1(d) "The" intact.
        blocks = [
            Block("center", [Span("105 U.S. 24 (____)")]),
            Block("center", [Span("THE “SCOTLAND.”")]),
            Block("center", [Span("Supreme Court of United States.")]),
            Block("para", [Span(
                "The case was argued by Mr. William Allen Butler, with "
                "whom was Mr. Thomas E. Stillman and Mr. John Chetwood, "
                "for the “Scotland,” and by Mr. James C. Carter and Mr. "
                "Robert D. Benedict, with whom was Mr. Joseph H. Choate, "
                "for the libellants."
            )]),
        ]

        name = _scholar_caption_name(blocks)

        self.assertEqual(name, "The “Scotland.”")
        self.assertEqual(abbreviate_case_name(name), "The Scotland")

    def test_lowercase_words_do_not_end_a_procedural_case_name(self):
        blocks = [Block("center", [Span(
            "IN RE TITLE, BALLOT TITLE & SUBMISSION CLAUSE FOR 2015-2016 #156"
        )])]

        self.assertEqual(
            _scholar_caption_name(blocks),
            "In re Title, Ballot Title & Submission Clause for 2015-2016 #156",
        )

    def test_multiple_party_words_are_omitted(self):
        # Rule 10.2.1(a): "et Wife", "et vir", "and Others" drop.
        self.assertEqual(
            abbreviate_case_name("Calder et Wife v. Bull et Wife"),
            "Calder v. Bull",
        )
        self.assertEqual(
            abbreviate_case_name("Troxel et vir v. Granville"),
            "Troxel v. Granville",
        )
        self.assertEqual(
            abbreviate_case_name("Wayman & another v. Southard & another"),
            "Wayman v. Southard",
        )

    def test_descriptive_parenthetical_drops(self):
        self.assertEqual(
            abbreviate_case_name(
                "Escola v. Coca Cola Bottling Co. of Fresno "
                "(a Corporation)"),
            "Escola v. Coca Cola Bottling Co. of Fresno",
        )

    def test_alias_clauses_drop_but_full_name_stays(self):
        # NIFLA v. Becerra, 138 S. Ct. 2361 (2018): the d/b/a alias is not
        # the Bluebook name — the first party keeps its full (abbreviated)
        # name and the alias clause drops.
        self.assertEqual(
            abbreviate_case_name(
                "National Institute of Family and Life Advocates, dba "
                "NIFLA, et al., Petitioners, v. Xavier Becerra, Attorney "
                "General of California, et al."),
            "Nat'l Inst. of Fam. & Life Advocs. v. Becerra",
        )
        self.assertEqual(
            abbreviate_case_name(
                "United States v. Mitchell Robertson a/k/a Mitchell "
                "Robinson a/k/a Bryheer McMichael"),
            "United States v. Robertson",
        )
        # Bare "aka" is a real surname, never an alias marker.
        self.assertEqual(
            abbreviate_case_name("Ethel Aka v. Washington Hospital Center"),
            "Aka v. Wash. Hosp. Ctr.",
        )

    def test_in_re_alias_chain_and_role_tail_drop_as_one_unit(self):
        self.assertEqual(
            abbreviate_case_name(normal_case_caption(
                "IN RE: IMERYS TALC AMERICA, INC., a/k/a Luzenac America, "
                "Inc. a/k/a Imerys Talc Ohio Inc. a/k/a Imerys Talc "
                "Delaware, Inc., et al., Debtors *362 Cyprus Historical "
                "Excess Insurers, Appellants."
            )),
            "In re Imerys Talc Am., Inc.",
        )

    def test_turned_comma_apostrophe_surname(self):
        # Johnson v. M'Intosh, 21 U.S. (8 Wheat.) 543 (1823): OCR renders
        # the turned-comma apostrophe as U+2018 ("M‘INTOSH"); the caption
        # party is the single nominal ejectment plaintiff and stays whole
        # (CAP's own name_abbreviation is "Johnson & Graham's Lessee v.
        # McIntosh").
        self.assertEqual(
            normal_case_caption("WILLIAM M‘INTOSH."),
            "William M'Intosh.",
        )
        self.assertEqual(
            abbreviate_case_name(
                "Johnson & Graham's Lessee v. William M‘intosh"),
            "Johnson & Graham's Lessee v. M'intosh",
        )

    def test_zf_automotive_consolidated_caption(self):
        # ZF Automotive US, Inc. v. Luxshare, Ltd., 596 U.S. 619 (2022):
        # the consolidated AlixPartners case follows the first respondent's
        # "LTD." and is omitted (rule 10.2.1(b)).
        right = _cut_companion_cases(
            "LUXSHARE, LTD. AlixPartners, LLP, et al., Petitioners v. The "
            "Fund for Protection of Investors' Rights in Foreign States.")
        self.assertEqual(right, "LUXSHARE, LTD.")
        self.assertEqual(
            abbreviate_case_name(
                "ZF Automotive US, Inc., et al., Petitioners, v. "
                "Luxshare, Ltd."),
            "ZF Auto. US, Inc. v. Luxshare, Ltd.",
        )

    def test_geographic_first_party_is_not_cut_from_a_firm_name(self):
        # "New York & Cuba Mail Steamship Co." is one business that merely
        # opens with a place — nothing is omitted; a true government
        # co-party list still reduces to its first party.
        self.assertEqual(
            abbreviate_case_name(
                "New York & Cuba Mail Steamship Company v. The Barge Sadie"),
            "N.Y. & Cuba Mail S.S. Co. v. Barge Sadie",
        )
        self.assertEqual(
            abbreviate_case_name(
                "Texas & Pacific Railway Company v. Behymer"),
            "Tex. & Pac. Ry. Co. v. Behymer",
        )
        self.assertEqual(
            abbreviate_case_name(
                "United States and Federal Communications Commission "
                "v. Acme Corp."),
            "United States v. Acme Corp.",
        )

    def test_companion_cases_cut_at_the_earliest_boundary(self):
        # Bostock: the companion party's own periods ("Inc.") defeat the
        # simple lookahead; the fallback cuts before "Altitude".
        self.assertEqual(
            _cut_companion_cases(
                "CLAYTON COUNTY, GEORGIA. Altitude Express, Inc., et al., "
                "Petitioners v. Melissa Zarda"),
            "CLAYTON COUNTY, GEORGIA.",
        )
        # Olmstead: "GREEN ET AL. v. SAME." defeats the lookahead at the
        # first boundary but not the second — the earliest cut wins.
        self.assertEqual(
            _cut_companion_cases(
                "UNITED STATES. GREEN ET AL. v. SAME. McINNIS v. SAME."),
            "UNITED STATES.",
        )
        # An entity abbreviation's period is a boundary only when the name
        # does not continue past it.
        self.assertEqual(
            _cut_companion_cases(
                "ST. PAUL FIRE & MARINE INS. CO. SAME v. OTHER."),
            "ST. PAUL FIRE & MARINE INS. CO.",
        )
        self.assertEqual(
            _cut_companion_cases("Acme Co. of America"),
            "Acme Co. of America",
        )
        # ZF Automotive US, Inc. v. Luxshare, Ltd., 596 U.S. 619 (2022):
        # the first respondent's own "Ltd." closes the case, and the
        # consolidated AlixPartners case follows.
        self.assertEqual(
            _cut_companion_cases(
                "LUXSHARE, LTD. AlixPartners, LLP, et al., Petitioners v. "
                "The Fund for Protection of Investors' Rights in Foreign "
                "States."),
            "LUXSHARE, LTD.",
        )
        # …but a continuing name keeps its entity abbreviation mid-name.
        self.assertEqual(
            _cut_companion_cases(
                "TRAVELERS INS. CO. OF HARTFORD. Acme Widgets, Inc., "
                "Petitioners v. Doe"),
            "TRAVELERS INS. CO. OF HARTFORD.",
        )


class RefineCaptionCaseTests(unittest.TestCase):
    """The opinion's own prose settles casing an all-caps caption destroys."""

    def test_body_restores_initialism_capitalization(self):
        # US Dominion, Inc. v. Byrne, 600 F. Supp. 3d 24 (D.D.C. 2022):
        # "US DOMINION" title-cases to "Us Dominion"; the body knows better.
        name = normal_case_caption("US DOMINION, INC. v. BYRNE.")
        self.assertEqual(name, "Us Dominion, Inc. v. Byrne.")
        body = ("Plaintiffs US Dominion, Inc., Dominion Voting Systems, "
                "Inc., and their affiliates sued Patrick Byrne. US "
                "Dominion, Inc. alleges defamation.")
        self.assertEqual(
            refine_caption_case(name, body),
            "US Dominion, Inc. v. Byrne.",
        )

    def test_body_confirms_title_case_where_us_is_a_word(self):
        name = normal_case_caption("TOYS R US, INC. v. SMITH")
        body = "Toys R Us, Inc. operates stores. Smith sued Toys R Us, Inc."
        self.assertEqual(
            refine_caption_case(name, body), "Toys R Us, Inc. v. Smith")

    def test_prose_articles_never_decapitalize_the_name(self):
        self.assertEqual(
            refine_caption_case(
                "The Boeing Co. v. Smith",
                "Smith sued the Boeing Company. Later the Boeing Company "
                "answered."),
            "The Boeing Co. v. Smith",
        )

    def test_all_caps_headings_carry_no_signal(self):
        self.assertEqual(
            refine_caption_case(
                "Toys R Us, Inc. v. Smith",
                "TOYS R US IS LIABLE. The court holds Toys R Us, Inc. "
                "liable."),
            "Toys R Us, Inc. v. Smith",
        )

    def test_no_body_is_a_no_op(self):
        self.assertEqual(
            refine_caption_case("Us Dominion, Inc. v. Byrne", ""),
            "Us Dominion, Inc. v. Byrne",
        )

    def test_single_token_party_uses_unanchored_evidence(self):
        # "IBM v. JOHNSON" leaves IBM with no adjacent anchor token; the
        # bare-word fallback still corrects it given repeated evidence.
        self.assertEqual(
            refine_caption_case(
                "Ibm v. Johnson",
                "IBM manufactures computers. Johnson worked for IBM "
                "until IBM terminated him."),
            "IBM v. Johnson",
        )

    def test_spelled_out_prose_still_anchors_caption_abbreviation(self):
        # The caption's "Corp." anchors against the body's "Corporation".
        self.assertEqual(
            refine_caption_case(
                "It Corp. v. County of Imperial",
                "IT Corporation contracted with the County. "
                "IT Corporation then sued."),
            "IT Corp. v. County of Imperial",
        )

    def test_am_general_initialism_restored(self):
        self.assertEqual(
            refine_caption_case(
                normal_case_caption(
                    "AM GENERAL LLC v. ACTIVISION BLIZZARD, INC."),
                "AM General LLC manufactures the Humvee. "
                "AM General LLC sued Activision."),
            "AM General LLC v. Activision Blizzard, Inc.",
        )

    def test_single_anchored_gmac_occurrence_restores_initialism(self):
        # Murray v. GMAC Mortgage Corp., 434 F.3d 948 (7th Cir. 2006):
        # the opinion spells the full name only once, then uses "GMACM".
        name = normal_case_caption(
            "MURRAY v. GMAC MORTGAGE CORPORATION")
        body = (
            "After her debts had been discharged, Nancy Murray received a "
            "credit solicitation from GMAC Mortgage, which had learned her "
            "address from credit bureaus. GMACM offered Murray a loan."
        )

        refined = refine_caption_case(name, body)

        self.assertEqual(refined, "Murray v. GMAC Mortgage Corporation")
        self.assertEqual(caption_case_reference_tokens(refined, body), ())
        self.assertEqual(
            abbreviate_case_name(refined),
            "Murray v. GMAC Mortg. Corp.",
        )

    def test_caps_styled_surnames_are_typography_not_spelling(self):
        # Opinions that set party surnames in caps mid-prose must not
        # rewrite the caption's ordinary spelling.
        self.assertEqual(
            refine_caption_case(
                "United States v. Smith",
                "SMITH was convicted. SMITH argues the evidence was "
                "insufficient. SMITH appeals."),
            "United States v. Smith",
        )

    def test_ampersand_and_dotted_initialisms_keep_caps(self):
        self.assertEqual(
            normal_case_caption("AT&T CORP. v. IOWA UTILITIES BOARD"),
            "AT&T Corp. v. Iowa Utilities Board",
        )
        self.assertEqual(
            normal_case_caption("A&M RECORDS, INC. v. NAPSTER, INC."),
            "A&M Records, Inc. v. Napster, Inc.",
        )
        self.assertEqual(
            normal_case_caption("MERCEXCHANGE, L.L.C."),
            "Mercexchange, L.L.C.",
        )

    def test_opinion_evidence_prevents_entity_reference_lookup(self):
        name = normal_case_caption("NBCUNIVERSAL MEDIA, LLC v. DOE")
        body = ("NBCUniversal Media, LLC distributes programming. "
                "Doe later contacted NBCUniversal Media.")
        refined = refine_caption_case(name, body)

        self.assertEqual(refined, "NBCUniversal Media, LLC v. Doe")
        self.assertEqual(caption_case_reference_tokens(refined, body), ())

    def test_reference_can_only_donate_case_to_existing_words(self):
        name = normal_case_caption("NBCUNIVERSAL MEDIA, LLC v. DOE")
        unresolved = caption_case_reference_tokens(name, "")

        self.assertEqual(unresolved, ("nbcuniversal",))
        self.assertEqual(
            apply_caption_case_reference(
                name,
                "NBCUniversal Holdings, LLC v. Completely Different Party",
                unresolved,
            ),
            "NBCUniversal Media, LLC v. Doe",
        )

    def test_reference_with_different_parties_cannot_substitute_caption(self):
        name = "Nbcuniversal Media, LLC v. Doe"
        self.assertEqual(
            apply_caption_case_reference(
                name, "Another Plaintiff v. Another Defendant",
                ("nbcuniversal",),
            ),
            name,
        )


class CitationOverrideTests(unittest.TestCase):
    def test_override_is_shared_by_parallel_reporters(self):
        item = {
            "cluster_id": 123,
            "citation": ["81 F.4th 699", "2023-2 Trade Cas. 81465"],
        }
        keys = citation_identity_keys(item, "81 F.4th 699")
        saved = update_overrides({}, keys, "Deslandes v. McDonald's USA, LLC, 81 F.4th 699 (7th Cir. 2023)")
        self.assertEqual(find_override(saved, ["cl:123"]), saved["cl:123"])
        self.assertIn("cite:81:f.4th:699", saved)

    def test_pin_is_inserted_before_parenthetical(self):
        base = "Deslandes v. McDonald's USA, LLC, 81 F.4th 699 (7th Cir. 2023)"
        self.assertEqual(
            add_pin_to_base(base, "703"),
            "Deslandes v. McDonald's USA, LLC, 81 F.4th 699, 703 (7th Cir. 2023)",
        )
        self.assertEqual(add_pin_to_base(base, "699"), base)

    def test_writer_parenthetical_follows_edited_base(self):
        plain, name = format_edited_citation(
            "Example v. Example, 1 F.4th 10 (2d Cir. 2021)",
            "12",
            ("Smith, J., dissenting",),
        )
        self.assertEqual(name, "Example v. Example")
        self.assertEqual(
            plain,
            "Example v. Example, 1 F.4th 10, 12 (2d Cir. 2021) "
            "(Smith, J., dissenting).",
        )


class ReporterAndDecisionDateTests(unittest.TestCase):
    def test_early_scotus_uses_modern_and_nominative_reporters(self):
        examples = [
            ("3 U.S. 199", "3 Dall. 199", "3 U.S. (3 Dall.) 199"),
            ("10 U.S. 87", "6 Cranch 87", "10 U.S. (6 Cranch) 87"),
            ("23 U.S. 66", "10 Wheat. 66", "23 U.S. (10 Wheat.) 66"),
            ("36 U.S. 420", "11 Pet. 420", "36 U.S. (11 Pet.) 420"),
        ]
        for modern, nominative, expected in examples:
            with self.subTest(modern=modern):
                self.assertEqual(
                    _nominative_display_cite(modern, [modern, nominative]),
                    expected,
                )

    def test_early_scotus_window_title_does_not_repeat_combined_reporters(self):
        win = object.__new__(_ScholarTextWindow)
        win._base_citation_override = ""
        win._bb = {
            "name": "Stuart v. Laird",
            "cite": "5 U.S. 299",
            "display_cite": "5 U.S. (1 Cranch) 299",
            "court": "",
            "year": "1803",
            "omit_parenthetical": "",
        }
        win._header_cites = ["5 U.S. 299", "1 Cranch 299"]
        win._item = {"citation": ["5 U.S. 299", "1 Cranch 299"]}

        self.assertEqual(
            win._title_citation(),
            "Stuart v. Laird, 5 U.S. (1 Cranch) 299 (1803)",
        )

    def test_early_scotus_window_title_keeps_parallels_without_combined_cite(self):
        win = object.__new__(_ScholarTextWindow)
        win._base_citation_override = ""
        win._bb = {
            "name": "Stuart v. Laird",
            "cite": "5 U.S. 299",
            "display_cite": "5 U.S. 299",
            "court": "",
            "year": "1803",
            "omit_parenthetical": "",
        }
        win._header_cites = ["1 Cranch 299"]
        win._item = {"citation": []}

        self.assertEqual(
            win._title_citation(),
            "Stuart v. Laird, 5 U.S. 299, 1 Cranch 299 (1803)",
        )

    def test_scotus_header_year_beats_rehearing_date(self):
        win = object.__new__(_ScholarTextWindow)
        win._item = {
            "case_name": "Korematsu v. United States",
            "citation": ["323 U.S. 214"],
            "court_id": "scotus",
            "date_filed": "1945-02-26",
        }
        win._blocks = [
            Block("center", [Span("Korematsu v. United States")]),
            Block("center", [Span("323 U.S. 214 (1944)")]),
            Block("para", [Span("MR. JUSTICE BLACK delivered the opinion.")]),
        ]

        bb = win._compute_bluebook_parts()

        self.assertEqual(bb["year"], "1944")

    def test_star_page_and_sct_page_numbers_are_not_years(self):
        # Cedar Point Nursery v. Hassid, 141 S. Ct. 2063 (2021): the header
        # ends with the star marker "*2066 Syllabus", and S. Ct. page
        # numbers (1600-2099) are indistinguishable from years — the
        # parenthesized year next to the citation controls.
        win = object.__new__(_ScholarTextWindow)
        win._item = {}
        win._blocks = [
            Block("center", [Span("141 S.Ct. 2063 (2021)")]),
            Block("center", [Span("594 U.S. 139")]),
            Block("center", [Span("CEDAR POINT NURSERY v. Victoria HASSID")]),
            Block("center", [Span("Decided June 23, 2021.")]),
            Block("heading", [Span("*2066 ", pagenum=True), Span("Syllabus")]),
            Block("para", [Span("CHIEF JUSTICE ROBERTS delivered the "
                                "opinion of the Court.")]),
        ]

        bb = win._compute_bluebook_parts()

        self.assertEqual(bb["year"], "2021")

    def test_body_heading_dates_do_not_supply_the_year(self):
        # United States v. Thomas, 818 F.3d 1230 (11th Cir. 2016): section
        # headings ("A. December 20, 2013 Suppression Hearing") fall inside
        # the first blocks and must not beat the citation's own year.
        win = object.__new__(_ScholarTextWindow)
        win._item = {}
        win._blocks = [
            Block("center", [Span("818 F.3d 1230 (2016)")]),
            Block("center", [Span("UNITED STATES v. Eric THOMAS")]),
            Block("center", [Span("United States Court of Appeals, "
                                  "Eleventh Circuit.")]),
            Block("heading", [Span("A. December 20, 2013 Suppression "
                                   "Hearing")]),
            Block("para", [Span("WILSON, Circuit Judge:")]),
        ]

        bb = win._compute_bluebook_parts()

        self.assertEqual(bb["year"], "2016")

    def test_official_state_reporter_is_source_independent_without_stars(self):
        win = object.__new__(_ScholarTextWindow)
        win._item = {
            "case_name": "People v. Aaron",
            "citation": ["299 N.W.2d 304"],
            "court_id": "mich",
            "date_filed": "1980-11-24",
        }
        win._blocks = [
            Block("center", [Span("People v. Aaron")]),
            Block("center", [Span("299 N.W.2d 304, 409 Mich. 672")]),
            Block("para", [Span("The Court holds as follows.")]),
        ]

        bb = win._compute_bluebook_parts()

        self.assertEqual(bb["display_cite"], "409 Mich. 672")


class NominativeCitationSearchTests(unittest.TestCase):
    def test_search_variants_preserve_caption_and_pincite(self):
        self.assertEqual(
            _citation_search_variants(
                "Stuart v. Laird, 1 Cranch 299, 301"
            ),
            (
                "Stuart v. Laird, 1 Cranch 299, 301",
                "Stuart v. Laird, 5 U.S. 299, 301",
            ),
        )
        self.assertEqual(
            _citation_search_variants("8 Wall 168"),
            ("8 Wall 168", "75 U.S. 168"),
        )

    def test_ordinary_citation_does_not_gain_an_alias(self):
        self.assertEqual(
            _citation_search_variants("410 U.S. 113"),
            ("410 U.S. 113",),
        )

    def test_courtlistener_retries_us_reports_alias_only_after_miss(self):
        client = Mock()
        client.lookup_citation.side_effect = [
            [],
            [{
                "status": 200,
                "clusters": [{
                    "id": 75,
                    "case_name": "The Case",
                    "citations": ["75 U.S. 168"],
                    "court_id": "scotus",
                }],
            }],
        ]

        item = _cl_item_for_citation(client, "8 Wall 168")

        self.assertEqual(item["cluster_id"], 75)
        self.assertEqual(
            client.lookup_citation.call_args_list,
            [call("8 Wall 168"), call("75 U.S. 168")],
        )

    def test_courtlistener_keeps_successful_typed_citation_authoritative(self):
        client = Mock()
        client.lookup_citation.return_value = [{
            "status": 200,
            "clusters": [{
                "id": 8,
                "case_name": "Different Cranch Case",
                "citations": ["1 Cranch 299"],
                "court_id": "cadc",
            }],
        }]

        item = _cl_item_for_citation(client, "1 Cranch 299")

        self.assertEqual(item["cluster_id"], 8)
        client.lookup_citation.assert_called_once_with("1 Cranch 299")

    def test_direct_lookup_retries_us_reports_alias_after_scholar_miss(self):
        win = object.__new__(CourtListenerGUI)
        win.root = object()
        win._post_root = Mock()
        fetcher = Mock()
        fetcher.fetch_by_citation.side_effect = [None, ("url", "html")]

        self.assertTrue(
            win._try_open_citation("", "8 Wall 168", "", fetcher, None)
        )
        self.assertEqual(
            fetcher.fetch_by_citation.call_args_list,
            [call("8 Wall 168"), call("75 U.S. 168")],
        )


class CopyWithCitationTests(unittest.TestCase):
    class DumpText:
        def __init__(self, before="words ", after=" remain"):
            self.before = before
            self.after = after

        @staticmethod
        def tag_names(_start):
            return ()

        def dump(self, _start, _end, **_kwargs):
            return [
                ("text", self.before, "1.0"),
                ("tagon", "pagenum", "1.6"),
                ("text", "*123", "1.6"),
                ("tagoff", "pagenum", "1.10"),
                ("text", self.after, "1.10"),
            ]

    @staticmethod
    def _window(with_cite: bool):
        win = object.__new__(_ScholarTextWindow)
        win._text = Mock()
        win._text.index.side_effect = ["2.0", "2.20"]
        win._text.tag_ranges.return_value = ()
        win._copy_with_cite = Mock()
        win._copy_with_cite.get.return_value = with_cite
        win._omitted_footnote_tags = Mock(return_value=(set(), 0))
        win._bluebook_citation = Mock(return_value=("Case, 1 F.4th 2.", "rtf"))
        win._parts = []
        win._rendered_parts = []
        win._link_actions = {}
        win._mode = "courtlistener"
        win._win = Mock()
        win._status_var = Mock()
        return win

    def test_copy_with_citation_omits_inline_star_pagination(self):
        win = self._window(True)
        with (
            patch("courtlistener_gui._dump_to_rtf", return_value="body") as dump,
            patch("courtlistener_gui._plain_without_layout_chars",
                  return_value="quotation") as plain,
            patch("courtlistener_gui._rtf_document", return_value="document"),
            patch("courtlistener_gui._copy_rich_clipboard", return_value="rich text"),
        ):
            win._copy_formatted()

        self.assertEqual(dump.call_args.kwargs["omit_tags"], {"pagenum"})
        self.assertEqual(plain.call_args.kwargs["omit_tags"], {"pagenum"})

    def test_copy_without_citation_keeps_inline_star_pagination(self):
        win = self._window(False)
        with (
            patch("courtlistener_gui._dump_to_rtf", return_value="body") as dump,
            patch("courtlistener_gui._plain_without_layout_chars",
                  return_value="quotation") as plain,
            patch("courtlistener_gui._rtf_document", return_value="document"),
            patch("courtlistener_gui._copy_rich_clipboard", return_value="rich text"),
        ):
            win._copy_formatted()

        self.assertEqual(dump.call_args.kwargs["omit_tags"], set())
        self.assertEqual(plain.call_args.kwargs["omit_tags"], set())

    def test_omitted_pagination_collapses_two_surrounding_spaces(self):
        txt = self.DumpText()

        plain = _plain_without_layout_chars(
            txt, "1.0", "end", omit_tags={"pagenum"},
        )
        rtf = _dump_to_rtf(txt, "1.0", "end", omit_tags={"pagenum"})

        self.assertEqual(plain, "words remain")
        self.assertIn("words remain", rtf)
        self.assertNotIn("words  remain", rtf)

    def test_omitted_pagination_does_not_invent_or_remove_one_sided_space(self):
        cases = (
            ("words ", "remain", "words remain"),
            ("words", " remain", "words remain"),
            ("words", "remain", "wordsremain"),
        )
        for before, after, expected in cases:
            with self.subTest(before=before, after=after):
                txt = self.DumpText(before, after)
                self.assertEqual(
                    _plain_without_layout_chars(
                        txt, "1.0", "end", omit_tags={"pagenum"},
                    ),
                    expected,
                )

    def test_plain_copy_retains_pagination_and_its_original_spacing(self):
        txt = self.DumpText()

        self.assertEqual(
            _plain_without_layout_chars(txt, "1.0", "end"),
            "words *123 remain",
        )


class OpinionDatabaseSpotlightTests(unittest.TestCase):
    class DB:
        def __init__(self, rows):
            self.rows = rows

        def search_names(self, _query, _limit):
            return list(self.rows)

        def find(self, _query):
            return list(self.rows)

        def get_by_scholar_id(self, sid):
            return {"text": f"The parties in saved opinion {sid}."}

    def test_saved_results_use_name_tier_then_court_and_cap_at_three(self):
        rows = [
            {"scholar_id": "1", "name": "ACME CORPORATION v. SMITH",
             "cite": "1 U.S. 1", "cites": ["1 U.S. 1"],
             "court": "ca9", "year": "2020", "url": "u1"},
            {"scholar_id": "2", "name": "Acme Corp. v. Smith",
             "cite": "2 U.S. 2", "cites": ["2 U.S. 2"],
             "court": "scotus", "year": "2019", "url": "u2"},
            {"scholar_id": "3", "name": "Acme Corp. v. Smith",
             "cite": "3 F.4th 3", "cites": ["3 F.4th 3"],
             "court": "ca2", "year": "2021", "url": "u3"},
            {"scholar_id": "4", "name": "Acme Corp. v. Smith",
             "cite": "4 F. Supp. 4", "cites": ["4 F. Supp. 4"],
             "court": "nysd", "year": "2024", "url": "u4"},
            {"scholar_id": "5", "name": "Acme Corp. v. Jones",
             "cite": "5 F.4th 5", "cites": ["5 F.4th 5"],
             "court": "ca1", "year": "2025", "url": "u5"},
        ]

        hits = _opinion_db_spotlight_results(
            self.DB(rows), "Acme Corporation v. Smith", limit=3)

        self.assertEqual([h["scholar_id"] for h in hits], ["2", "3", "1"])
        self.assertTrue(all(h["name"] == "Acme Corp. v. Smith" for h in hits))

    def test_saved_results_honor_spotlight_court_hint(self):
        rows = [
            {"scholar_id": "9", "name": "Acme Corp. v. Smith",
             "cite": "9 F.4th 9", "cites": ["9 F.4th 9"],
             "court": "ca9", "year": "2024", "url": "u9"},
            {"scholar_id": "1", "name": "Acme Corp. v. Smith",
             "cite": "1 U.S. 1", "cites": ["1 U.S. 1"],
             "court": "scotus", "year": "2024", "url": "u1"},
        ]

        hits = _opinion_db_spotlight_results(
            self.DB(rows), "Acme Corp. v. Smith (9th Cir. 2024)", limit=3)

        self.assertEqual([h["scholar_id"] for h in hits], ["9"])


class SpotlightCitationDetectionTests(unittest.TestCase):
    def test_early_federal_reporter_uses_shared_normalization(self):
        hit = _spotlight_case_action("The Nestor, 1 Sumner, 73")

        self.assertIsNotNone(hit)
        self.assertEqual(hit[2], ("cite", "1 Sumn. 73"))

    def test_federal_cases_number_routes_to_special_opener(self):
        hit = _spotlight_case_action(
            "Cole v. The Atlantic, Case No. 2,976"
        )

        self.assertIsNotNone(hit)
        kind, value = hit[2]
        self.assertEqual(kind, "fedcas")
        self.assertEqual(json.loads(value)["no"], "2976")

    def test_federal_cases_reporter_is_a_direct_case_action(self):
        hit = _spotlight_case_action("The Nestor, 18 F. Cas. 9")

        self.assertIsNotNone(hit)
        self.assertEqual(hit[2], ("cite", "18 F. Cas. 9"))

    def test_multiple_case_citations_are_not_opened_arbitrarily(self):
        self.assertIsNone(
            _spotlight_case_action("1 Sumner, 73; 35 Fed. Rep. 665")
        )


class UsCodeNavigationTests(unittest.TestCase):
    def test_current_olrc_section_heads_supply_neighbor_order(self):
        # Current OLRC container pages no longer include the old analysis
        # table and HTML-encode the section sign in each fallback heading.
        page = """
        <h3 class="section-head">&sect;1981. Equal rights</h3>
        <h3 class="section-head">&sect;1981a. Damages</h3>
        <h3 class="section-head">&#167;1982. Property rights</h3>
        <h3 class="section-head">&#xA7;1983. Civil action</h3>
        """

        self.assertEqual(
            us_code._sections_from_analysis(page),
            ["1981", "1981a", "1982", "1983"],
        )

    def test_neighbors_use_section_head_fallback_order(self):
        doc = us_code.UscSection(
            title="42", section="1982", url="url",
            container="title42-chapter21-subchapter1",
        )

        with patch(
            "us_code._container_sections",
            return_value=["1981", "1981a", "1982", "1983"],
        ):
            self.assertEqual(
                doc.neighbors(),
                (("42", "1981a"), ("42", "1983")),
            )


class CaseLawSharedPageTests(unittest.TestCase):
    def test_discovers_sequential_pdfs_and_reads_every_json_name(self):
        base = "https://static.case.law/f2d/100/case-pdfs/0123-01.pdf"
        session = Mock()

        def head(url, **_kwargs):
            return SimpleNamespace(
                status_code=200 if url.endswith(("-02.pdf", "-03.pdf")) else 404
            )

        def get(url, **_kwargs):
            suffix = url.rsplit("-", 1)[-1].split(".", 1)[0]
            names = {
                "01": "Alpha Corp. v. One",
                "02": "Beta Corp. v. Two",
                "03": "Gamma Corp. v. Three",
            }
            return SimpleNamespace(
                status_code=200,
                json=lambda: {"name_abbreviation": names[suffix]},
            )

        session.head.side_effect = head
        session.get.side_effect = get
        with patch("courtlistener_gui._anon_session", session):
            opinions = _case_law_page_opinions(base)

        self.assertEqual([o.name for o in opinions], [
            "Alpha Corp. v. One", "Beta Corp. v. Two", "Gamma Corp. v. Three",
        ])
        self.assertEqual(session.head.call_args_list[-1].args[0],
                         base.replace("-01.pdf", "-04.pdf"))
        self.assertEqual(session.get.call_count, 3)

    def test_source_case_name_selects_matching_sibling(self):
        opinions = [
            _CaseLawPageOpinion("first.pdf", "first.json", "Alpha v. One"),
            _CaseLawPageOpinion("second.pdf", "second.json", "Beta v. Two"),
        ]
        chosen = _match_case_law_page_opinion(opinions, "Beta v. Two")
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.url, "second.pdf")


class CaseLawPdfTextTests(unittest.TestCase):
    @staticmethod
    def _data():
        return {
            "name_abbreviation": "Smith v. Jones",
            "citations": [
                {"type": "vendor", "cite": "123 F. App'x 456"},
                {"type": "official", "cite": "77 Example 12"},
            ],
            "court": {
                "name_abbreviation": "2d Cir.",
                "slug": "ca2",
            },
            "decision_date": "2004-05-06",
            "casebody": {
                "data": {
                    "head_matter": "Smith v. Jones\nNo. 03-1000",
                    "opinions": [
                        {
                            "author": "Per Curiam.",
                            "text": "See Roe v. Wade, 410 U.S. 113.",
                        },
                        {"text": "The judgment is affirmed."},
                    ],
                },
            },
        }

    def test_cap_json_record_keeps_exact_text_and_display_metadata(self):
        record = _case_law_text_record(
            self._data(), "123 F. App'x 456",
            "https://static.case.law/f-appx/123/cases/0456-02.json",
        )

        self.assertIsNotNone(record)
        self.assertIn("Per Curiam.", record.text)
        self.assertIn("410 U.S. 113", record.text)
        self.assertEqual(
            record.citation,
            "Smith v. Jones, 77 Example 12 (2d Cir. 2004)",
        )
        self.assertEqual(record.item["court_id"], "ca2")
        self.assertEqual(record.item["citation"],
                         ["123 F. App'x 456", "77 Example 12"])

    def test_exact_numbered_pdf_uses_its_matching_json(self):
        response = SimpleNamespace(
            status_code=200, json=lambda: self._data(),
        )
        session = Mock()
        session.get.return_value = response
        pdf = "https://static.case.law/f-appx/123/case-pdfs/0456-02.pdf"

        with patch("courtlistener_gui._anon_session", session):
            record = _case_law_text_for_pdf_url(pdf)

        self.assertIsNotNone(record)
        session.get.assert_called_once_with(
            "https://static.case.law/f-appx/123/cases/0456-02.json",
            timeout=15,
        )

    def test_pdf_text_source_prefers_courtlistener(self):
        record = _CaseLawTextRecord(
            "CAP fallback", "Smith v. Jones, 123 F. App'x 456 (2d Cir. 2004)",
            {
                "caseName": "Smith v. Jones",
                "citation": ["123 F. App'x 456"],
                "court": "2d Cir.",
                "court_id": "ca2",
                "dateFiled": "2004-05-06",
            },
            "https://static.case.law/f-appx/123/cases/0456-01.json",
        )
        target = {
            "cluster_id": 99,
            "absolute_url": "/opinion/99/smith-v-jones/",
        }
        with (
            patch("courtlistener_gui._case_law_text_for_pdf_url",
                  return_value=record),
            patch("courtlistener_gui._cl_item_for_citation",
                  return_value=target) as find,
            patch("courtlistener_gui._assemble_case_parts",
                  return_value=(["part"], ["block"], "CL opinion", {})),
        ):
            source = _case_pdf_text_source(
                "https://static.case.law/f-appx/123/case-pdfs/0456-01.pdf",
                "123 F. App'x 456", client=object(),
            )

        self.assertEqual(source.kind, "courtlistener")
        self.assertEqual(source.text, "CL opinion")
        self.assertEqual(
            source.source_url,
            "https://www.courtlistener.com/opinion/99/smith-v-jones/",
        )
        find.assert_called_once_with(
            ANY, "123 F. App'x 456", name="Smith v. Jones",
        )

    def test_pdf_text_source_falls_back_to_cap_json(self):
        record = _CaseLawTextRecord(
            "CAP fallback with 410 U.S. 113", "Smith v. Jones",
            {"caseName": "Smith v. Jones",
             "citation": ["123 F. App'x 456"]},
            "https://static.case.law/f-appx/123/cases/0456-01.json",
        )
        with (
            patch("courtlistener_gui._case_law_text_for_pdf_url",
                  return_value=record),
            patch("courtlistener_gui._cl_item_for_citation",
                  return_value=None),
        ):
            source = _case_pdf_text_source(
                "https://static.case.law/f-appx/123/case-pdfs/0456-01.pdf",
                "123 F. App'x 456", client=object(),
            )

        self.assertEqual(source.kind, "case_law")
        self.assertEqual(source.button_label, "static.case.law Text")
        self.assertIn("410 U.S. 113", source.text)


class CaseWindowModeTests(unittest.TestCase):
    class _View:
        def __init__(self):
            self.destroyed = False

        def winfo_exists(self):
            return not self.destroyed

        def destroy(self):
            self.destroyed = True

    def test_mode_switch_migrates_each_live_case(self):
        app = object.__new__(CourtListenerGUI)
        app._case_tabs_enabled = False
        app._case_tabs_var = Mock()
        app._case_tabs_window = None
        opened = []
        one, two = self._View(), self._View()
        app._open_case_views = {
            1: {"view": one, "label": "One", "reopen": lambda: opened.append(1)},
            2: {"view": two, "label": "Two", "reopen": lambda: opened.append(2)},
        }

        with (
            patch("courtlistener_gui._load_config", return_value={}),
            patch("courtlistener_gui._save_config") as save,
        ):
            app.set_case_tabs_enabled(True)

        self.assertTrue(app._case_tabs_enabled)
        self.assertTrue(one.destroyed)
        self.assertTrue(two.destroyed)
        self.assertEqual(opened, [1, 2])
        save.assert_called_once_with({"case_tabs_enabled": True})

    def test_tab_titles_are_compact(self):
        short = "Marbury v. Madison, 5 U.S. 137 (1803)"
        self.assertEqual(_CaseTabsWindow._tab_label(short), short)
        self.assertEqual(len(_CaseTabsWindow._tab_label("x" * 80)), 52)
        self.assertTrue(_CaseTabsWindow._tab_label("x" * 80).endswith("..."))

    def test_ctrl_tab_cycle_wraps_both_directions(self):
        class Notebook:
            def __init__(self):
                self.items = ("one", "two", "three")
                self.current = "one"

            def tabs(self):
                return self.items

            def select(self, value=None):
                if value is None:
                    return self.current
                self.current = value

            def index(self, value):
                return self.items.index(value)

        manager = object.__new__(_CaseTabsWindow)
        manager.notebook = Notebook()

        self.assertEqual(manager._cycle_tab(-1), "break")
        self.assertEqual(manager.notebook.current, "three")
        manager._cycle_tab(1)
        self.assertEqual(manager.notebook.current, "one")

    def test_tab_close_target_is_confined_to_the_far_right(self):
        bbox = (10, 5, 120, 30)

        self.assertFalse(
            _CaseTabsWindow._point_in_tab_close_box(bbox, 93, 20)
        )
        self.assertTrue(
            _CaseTabsWindow._point_in_tab_close_box(bbox, 94, 20)
        )
        self.assertTrue(
            _CaseTabsWindow._point_in_tab_close_box(bbox, 129, 20)
        )
        self.assertFalse(
            _CaseTabsWindow._point_in_tab_close_box(bbox, 130, 20)
        )
        self.assertFalse(
            _CaseTabsWindow._point_in_tab_close_box(bbox, 110, 35)
        )

    def test_clicking_tab_close_target_destroys_only_that_page(self):
        manager = object.__new__(_CaseTabsWindow)
        page = Mock()
        manager._tab_close_page_at = Mock(return_value=page)
        manager._cancel_tab_long_press = Mock()
        manager._set_tab_close_hover = Mock()
        event = SimpleNamespace(x=125, y=12)

        self.assertEqual(manager._close_tab_from_click(event), "break")
        page.destroy.assert_called_once_with()
        manager._cancel_tab_long_press.assert_called_once_with()
        manager._set_tab_close_hover.assert_called_once_with(None)

    def test_pop_out_reopens_one_tab_in_a_new_tab_group(self):
        app = object.__new__(CourtListenerGUI)
        app.root = object()
        app._case_tabs_window = None
        app._detached_tab_windows = set()
        page = object.__new__(_CaseTabPage)
        page.destroy = Mock()
        manager = Mock()
        manager.win = object()
        seen = []

        def reopen(parent=None):
            seen.append(parent)

        app._open_case_views = {
            1: {"view": page, "label": "A tab", "reopen": reopen},
        }

        with patch("courtlistener_gui._CaseTabsWindow",
                   return_value=manager) as make_manager:
            app.pop_out_view(page)

        page.destroy.assert_called_once()
        make_manager.assert_called_once_with(app, app.root)
        self.assertEqual(seen, [manager.win])
        self.assertIn(manager, app._detached_tab_windows)

    def test_tab_group_is_inherited_from_page_or_detached_window(self):
        app = object.__new__(CourtListenerGUI)
        main = SimpleNamespace(win=object())
        detached = Mock()
        detached.win = object()
        app._case_tabs_window = main
        app._detached_tab_windows = {detached}
        page = object.__new__(_CaseTabPage)
        page._manager = detached

        self.assertIs(app._tab_manager_for_parent(page), detached)
        self.assertIs(app._tab_manager_for_parent(detached.win), detached)
        self.assertIsNone(app._tab_manager_for_parent(object()))

    def test_citation_result_uses_launching_tab_group_parent(self):
        app = object.__new__(CourtListenerGUI)
        app.root = object()
        app._status_var = Mock()
        app._post_root = lambda fn, *args: fn(*args)
        fetcher = Mock()
        fetcher.fetch_by_citation.return_value = ("url", "html")
        parent = object()

        with patch("courtlistener_gui._ScholarTextWindow") as text_window:
            opened = app._try_open_citation(
                "", "410 U.S. 113", "", fetcher, None,
                view_parent=parent,
            )

        self.assertTrue(opened)
        self.assertIs(text_window.call_args.args[0], parent)

    def test_statute_and_statute_pdf_forward_the_shared_app(self):
        app = object()
        parent = object()
        status = Mock()
        with patch("courtlistener_gui._fetch_statute_window") as fetch:
            _open_statute_action(
                parent, ("usc", "42:1983:"), status, app=app,
            )
        fetch.assert_called_once_with(
            parent, "usc", "42:1983:", status, app=app,
        )

        with patch("courtlistener_gui._open_statute_pdf") as open_pdf:
            _open_statute_action(
                parent,
                ("statpdf", "https://www.govinfo.gov/example.pdf"),
                status, app=app,
            )
        open_pdf.assert_called_once_with(
            parent, "https://www.govinfo.gov/example.pdf", status, app=app,
        )


class SpotlightPopupLifecycleTests(unittest.TestCase):
    @staticmethod
    def _win():
        win = object.__new__(CourtListenerGUI)
        win._quick_popup = None
        win._spotlight_toggle_at = 0.0
        win._mac_return_focus = Mock()  # platform-specific; not under test
        return win

    def test_close_quick_popup_withdraws_before_destroy(self):
        # On macOS, destroying the borderless popup without unmapping it
        # first can leave it painted on screen; the close helper withdraws,
        # then destroys, and always clears the tracked reference.
        win = self._win()
        popup = Mock()
        win._quick_popup = popup

        win._close_quick_popup()

        self.assertIsNone(win._quick_popup)
        self.assertEqual(popup.mock_calls, [call.withdraw(), call.destroy()])

    def test_close_quick_popup_survives_a_dead_window(self):
        win = self._win()
        popup = Mock()
        popup.withdraw.side_effect = tk.TclError("gone")
        popup.destroy.side_effect = tk.TclError("gone")
        win._quick_popup = popup

        win._close_quick_popup()  # must not raise

        self.assertIsNone(win._quick_popup)
        win._close_quick_popup()  # idempotent with nothing tracked

    def test_hotkey_toggle_debounces_a_duplicate_fire(self):
        # A duplicated hotkey delivery must not close the popup and
        # immediately reopen it: a second toggle arriving within the
        # debounce window is ignored outright.
        win = self._win()
        first = Mock()
        win._quick_popup = first

        win._toggle_quick_search_popup()

        self.assertIsNone(win._quick_popup)
        first.destroy.assert_called_once()
        win._mac_return_focus.assert_called_once()

        second = Mock()
        win._quick_popup = second
        win._toggle_quick_search_popup()  # the duplicate of the same press

        self.assertIs(win._quick_popup, second)
        second.destroy.assert_not_called()


class CitationEnrichmentTriggerTests(unittest.TestCase):
    @staticmethod
    def _win(*, court: str, year: str, is_scotus: bool = False):
        win = object.__new__(_ScholarTextWindow)
        win._bb = {
            "name": "Example v. Example",
            "cite": "10 F.4th 20",
            "court": court,
            "year": year,
        }
        win._item = {"citation": ["10 F.4th 20"]}
        win._header_cites = []
        win._base_citation_override = ""
        win._is_scotus = is_scotus
        win._app = None
        win._post = Mock()
        return win

    def test_known_opinion_court_and_year_start_no_external_work(self):
        win = object.__new__(_ScholarTextWindow)
        win._item = {}
        win._blocks = [
            Block("center", [Span("10 F.4th 20 (2024)")]),
            Block("center", [Span("Example v. Example")]),
            Block("center", [Span(
                "United States Court of Appeals, Eleventh Circuit."
            )]),
            Block("para", [Span("JORDAN, Circuit Judge:")]),
        ]
        win._bb = win._compute_bluebook_parts()
        win._base_citation_override = ""
        win._app = None
        win._post = Mock()

        self.assertEqual(win._bb["court"], "11th Cir.")
        self.assertEqual(win._bb["year"], "2024")

        with (
            patch("courtlistener_gui.threading.Thread") as thread,
            patch("courtlistener_gui._case_law_name_for_cites") as cap_name,
            patch("courtlistener_gui._case_law_metadata") as cap_metadata,
            patch("courtlistener_gui._cl_item_for_citation") as cl_lookup,
        ):
            win._enrich_citation()

        thread.assert_not_called()
        cap_name.assert_not_called()
        cap_metadata.assert_not_called()
        cl_lookup.assert_not_called()

    def test_opened_opinion_caption_controls_and_local_body_fixes_case(self):
        win = object.__new__(_ScholarTextWindow)
        win._item = {
            "case_name": "Different Metadata Name v. Other Party",
            "citation": ["10 F.4th 20"],
        }
        win._blocks = [
            Block("center", [Span("NBCUNIVERSAL MEDIA, LLC v. DOE")]),
            Block("center", [Span("10 F.4th 20 (2024)")]),
            Block("center", [Span(
                "United States Court of Appeals, Eleventh Circuit."
            )]),
            Block("para", [Span(
                "NBCUniversal Media, LLC sued Doe. "
                "NBCUniversal Media later appealed."
            )]),
        ]

        win._bb = win._compute_bluebook_parts()
        win._base_citation_override = ""
        win._app = None
        win._post = Mock()

        self.assertEqual(win._bb["name"], "NBCUniversal Media, LLC v. Doe")
        self.assertEqual(win._bb["_caption_case_unresolved"], ())
        with (
            patch("courtlistener_gui.threading.Thread") as thread,
            patch("courtlistener_gui._case_law_name_for_cites") as cap_name,
        ):
            win._enrich_citation()
        thread.assert_not_called()
        cap_name.assert_not_called()

    def test_known_scotus_status_and_year_start_no_external_work(self):
        # A Supreme Court citation correctly has no court abbreviation in its
        # parenthetical; known SCOTUS status satisfies the court requirement.
        win = self._win(court="", year="2024", is_scotus=True)

        with patch("courtlistener_gui.threading.Thread") as thread:
            win._enrich_citation()

        thread.assert_not_called()

    def test_missing_year_starts_the_enrichment_path(self):
        win = self._win(court="11th Cir.", year="")

        class ImmediateThread:
            def __init__(self, *, target, daemon):
                self.target = target

            def start(self):
                self.target()

        with (
            patch("courtlistener_gui.threading.Thread", ImmediateThread),
            patch("courtlistener_gui._case_law_name_for_cites") as cap_name,
            patch(
                "courtlistener_gui._case_law_metadata",
                return_value={"decision_date": "2023-06-01"},
            ) as cap_metadata,
        ):
            win._enrich_citation()

        cap_name.assert_not_called()
        cap_metadata.assert_called_once_with("10 F.4th 20")
        win._post.assert_called_once()

    def test_unresolved_entity_case_uses_only_capitalization_donor(self):
        win = self._win(court="11th Cir.", year="2024")
        win._bb["name"] = "Nbcuniversal Media, LLC v. Doe"
        win._bb["_caption_case_unresolved"] = ("nbcuniversal",)

        class ImmediateThread:
            def __init__(self, *, target, daemon):
                self.target = target

            def start(self):
                self.target()

        with (
            patch("courtlistener_gui.threading.Thread", ImmediateThread),
            patch(
                "courtlistener_gui._case_law_name_for_cites",
                return_value=(
                    "NBCUniversal Holdings, LLC v. Completely Different Party"
                ),
            ) as cap_name,
            patch("courtlistener_gui._case_law_metadata") as cap_metadata,
            patch("courtlistener_gui._cl_item_for_citation") as cl_lookup,
        ):
            win._enrich_citation()

        cap_name.assert_called_once()
        cap_metadata.assert_not_called()
        cl_lookup.assert_not_called()
        win._post.assert_called_once_with(
            win._apply_enriched_citation,
            "11th Cir.",
            "2024",
            "NBCUniversal Media, LLC v. Doe",
        )


class OpinionUnpublishedCaseLinkTests(unittest.TestCase):
    def test_wl_link_inherits_docket_from_full_opinion_text(self):
        full_text = (
            "Care One Mgmt., LLC v. United Healthcare Workers E., "
            "No. 12-6371, 2024 WL 1327972, at *7 "
            "(D.N.J. Mar. 28, 2024). Later the court cited "
            "2024 WL 1327972, at *9 (D.N.J. Mar. 28, 2024)."
        )
        index = _recap_spec_index(full_text)

        ranges = _recap_citation_ranges(
            "2024 WL 1327972, at *9 (D.N.J. Mar. 28, 2024).",
            index,
        )

        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0][2][0], "recap")
        spec = json.loads(ranges[0][2][1])
        self.assertEqual(spec["docket"], "12-6371")
        self.assertEqual(spec["court"], "njd")
        self.assertEqual(spec["date"], "2024-03-28")

    def test_docket_only_opinion_citation_gets_recap_action(self):
        text = (
            "Peninsula Pathology Assocs. v. Am. Int'l Indus., "
            "No. 23-1971 (4th Cir. Feb. 12, 2024)"
        )

        ranges = _special_citation_ranges([Span(text)], {})

        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0][2][0], "recap")
        spec = json.loads(ranges[0][2][1])
        self.assertEqual(spec["docket"], "23-1971")
        self.assertEqual(spec["court"], "ca4")

    def test_opinion_recap_action_uses_brief_reader_opener(self):
        win = object.__new__(_ScholarTextWindow)
        spec = json.dumps({"docket": "23-1971", "court": "ca4"})
        win._link_actions = {"link": ("recap", spec)}
        win._app = Mock()
        win._win = Mock()
        win._status_var = Mock()

        with patch("courtlistener_gui._open_recap_citation") as opener:
            win._follow_link("link")

        opener.assert_called_once_with(
            win._app, win._win, spec, win._status_var.set)


class HistoricalFederalCourtTests(unittest.TestCase):
    def test_old_circuit_court_id_reaches_bluebook_form(self):
        # United States v. Cohn, 128 F. 615 (C.C.S.D.N.Y. 1904):
        # CourtListener's id for the old circuit court (abolished 1912) is
        # "circtsdny" — previously printed raw in the parenthetical.
        win = object.__new__(_ScholarTextWindow)
        win._item = {
            "case_name": "United States v. Cohn",
            "citation": ["128 F. 615"],
            "court_id": "circtsdny",
            "date_filed": "1904-02-15",
        }
        win._blocks = [
            Block("center", [Span("128 F. 615 (1904)")]),
            Block("center", [Span("UNITED STATES v. COHN.")]),
            Block("center", [Span("Circuit Court, S. D. New York.")]),
            Block("para", [Span("HOLT, District Judge.")]),
        ]

        bb = win._compute_bluebook_parts()

        self.assertEqual(bb["court"], "C.C.S.D.N.Y.")
        self.assertEqual(bb["year"], "1904")

    def test_historical_and_bankruptcy_ids_are_mapped(self):
        from court_catalog import COURT_BLUEBOOK
        self.assertEqual(COURT_BLUEBOOK["circtsdny"], "C.C.S.D.N.Y.")
        self.assertEqual(COURT_BLUEBOOK["circtdal"], "C.C.D. Ala.")
        self.assertEqual(COURT_BLUEBOOK["ald"], "D. Ala.")
        self.assertEqual(COURT_BLUEBOOK["nysb"], "Bankr. S.D.N.Y.")
        # CL's "arb" is Arizona (not Arkansas — those are areb/arwb): the
        # table is generated from court *names*, immune to the id scheme.
        self.assertEqual(COURT_BLUEBOOK["arb"], "Bankr. D. Ariz.")

    def test_unknown_court_id_is_never_printed_raw(self):
        from courtlistener_gui import _court_for_paren
        self.assertEqual(
            _court_for_paren("100 F. 1", "someunknownid", "someunknownid"),
            "",
        )

    def test_old_circuit_header_line_parses(self):
        self.assertEqual(
            bluebook_federal_trial_court("Circuit Court, S. D. New York."),
            "C.C.S.D.N.Y.",
        )
        self.assertEqual(
            bluebook_federal_trial_court("Circuit Court, D. Massachusetts."),
            "C.C.D. Mass.",
        )
        # County circuit courts are state trial courts, never C.C.
        for name in ("Circuit Court for Baltimore County, Maryland",
                     "Circuit Court of Cook County, Illinois"):
            self.assertEqual(bluebook_federal_trial_court(name), "")


class FederalTrialCourtTests(unittest.TestCase):
    def test_district_captions_reach_bluebook_form(self):
        cases = [
            ("United States District Court, M.D. North Carolina.",
             "M.D.N.C."),
            ("United States District Court, District of Columbia.",
             "D.D.C."),
            ("United States District Court, N.D. Illinois, "
             "Eastern Division.", "N.D. Ill."),
            ("District Court, E. D. Pennsylvania.", "E.D. Pa."),
            # A single-district state's division tail is not a district:
            # "C. D." after the state means Central Division.
            ("United States District Court, South Dakota, C. D.",
             "D.S.D."),
            ("United States Bankruptcy Court, S.D. Texas, "
             "Houston Division.", "Bankr. S.D. Tex."),
            ("United States District Court for the Eastern District "
             "of Pennsylvania", "E.D. Pa."),
        ]
        for name, want in cases:
            with self.subTest(name=name):
                self.assertEqual(bluebook_federal_trial_court(name), want)

    def test_state_and_appellate_courts_are_not_federal_districts(self):
        for name in ("District Court of Appeal of Florida, Third District.",
                     "District Court, City and County of Denver, Colorado.",
                     "Supreme Court of Wisconsin.",
                     "United States Court of Appeals, Fourth Circuit."):
            with self.subTest(name=name):
                self.assertEqual(bluebook_federal_trial_court(name), "")

    def test_f_supp_citation_gets_the_district_parenthetical(self):
        win = object.__new__(_ScholarTextWindow)
        win._item = {}
        win._blocks = [
            Block("center", [Span("627 F.Supp.3d 520 (2022)")]),
            Block("center", [Span("Lucille BELL v. AMERICAN "
                                  "INTERNATIONAL INDUSTRIES")]),
            Block("center", [Span("United States District Court, "
                                  "M.D. North Carolina.")]),
            Block("para", [Span("OSTEEN, JR., District Judge.")]),
        ]

        bb = win._compute_bluebook_parts()

        self.assertEqual(bb["court"], "M.D.N.C.")
        self.assertEqual(bb["year"], "2022")


class PublicDomainCitationTests(unittest.TestCase):
    def test_wisconsin_initial_citation_orders_all_three_sources(self):
        self.assertEqual(
            _wisconsin_display_cite([
                "960 N.W.2d 869", "2021 WI 64", "397 Wis. 2d 719",
            ]),
            "2021 WI 64, 397 Wis. 2d 719, 960 N.W.2d 869",
        )

    def test_paragraph_pin_follows_public_domain_cite(self):
        win = object.__new__(_ScholarTextWindow)
        win._base_citation_override = ""
        win._bb = {
            "name": "State v. Prado",
            "cite": "397 Wis. 2d 719",
            "display_cite": "2021 WI 64, 397 Wis. 2d 719, 960 N.W.2d 869",
            "court": "Wis.", "year": "2021",
            "omit_parenthetical": "1", "pin_kind": "paragraph",
        }

        plain, _rtf = win._bluebook_citation("¶ 12")

        self.assertEqual(
            plain,
            "State v. Prado, 2021 WI 64, ¶ 12, 397 Wis. 2d 719, "
            "960 N.W.2d 869.",
        )


class WriterParentheticalTests(unittest.TestCase):
    @staticmethod
    def _win():
        return object.__new__(_ScholarTextWindow)

    @staticmethod
    def _part(kind: str, first_line: str, label: str = "") -> OpinionPart:
        return OpinionPart(
            label or first_line[:90], kind,
            [Block("para", [Span(first_line)])],
        )

    def test_joinder_byline_without_role_uses_part_kind(self):
        # Cohen v. California, 403 U.S. 15 (1971): Blackmun's byline names
        # only the joiners; the role was read from his opening lines when
        # the part was segmented.
        part = self._part(
            "dissent",
            "MR. JUSTICE BLACKMUN, with whom THE CHIEF JUSTICE and "
            "MR. JUSTICE BLACK join.",
        )
        self.assertEqual(
            self._win()._writer_parenthetical(part),
            "Blackmun, J., dissenting",
        )

    def test_separate_opinion_of_heading_uses_resolved_role(self):
        part = self._part(
            "dissent", "Separate opinion of MR. JUSTICE McREYNOLDS."
        )
        self.assertEqual(
            self._win()._writer_parenthetical(part),
            "McReynolds, J., dissenting",
        )

    def test_unresolved_separate_opinion_uses_neutral_parenthetical(self):
        part = self._part(
            "separate", "Separate opinion of MR. JUSTICE STORY."
        )
        win = self._win()
        self.assertEqual(
            win._writer_parenthetical(part),
            "Story, J., separate opinion",
        )
        win._base_citation_override = ""
        win._bb = {
            "name": "Example v. Example", "cite": "1 U.S. 10",
            "display_cite": "1 U.S. 10", "court": "", "year": "1800",
            "omit_parenthetical": "", "pin_kind": "page",
        }
        plain, _rtf = win._bluebook_citation(
            None, win._writer_parenthetical(part))
        self.assertEqual(
            plain,
            "Example v. Example, 1 U.S. 10 (1800) "
            "(Story, J., separate opinion).",
        )
        self.assertEqual(win._PART_BOX_TAGS["separate"], "box-separate")
        self.assertEqual(win._PART_LABEL_COLORS["separate"], "#59636f")
        self.assertEqual(win._SEPARATE_BG, "#f1f3f5")

    def test_spelled_out_bare_judge_byline(self):
        part = self._part("concurrence", "CLINTON, Judge.")
        self.assertEqual(
            self._win()._writer_parenthetical(part),
            "Clinton, J., concurring",
        )

    def test_comma_after_justice_in_byline(self):
        # Alleyne v. United States: Scholar prints "Justice, ALITO,
        # dissenting."
        part = self._part("dissent", "Justice, ALITO, dissenting.")
        self.assertEqual(
            self._win()._writer_parenthetical(part),
            "Alito, J., dissenting",
        )

    def test_full_name_circuit_byline_reduces_to_surname(self):
        part = self._part(
            "concurrence",
            "TOBY HEYTENS, Circuit Judge, with whom Judges HARRIS and "
            "BENJAMIN join, concurring:",
        )
        self.assertEqual(
            self._win()._writer_parenthetical(part),
            "Heytens, J., concurring",
        )
        # Disambiguating initials survive (two Nelsons on the CA9 bench).
        part = self._part(
            "dissent", "R. NELSON, Circuit Judge, dissenting:")
        self.assertEqual(
            self._win()._writer_parenthetical(part),
            "R. Nelson, J., dissenting",
        )


class CombinedOpinionCompletenessTests(unittest.TestCase):
    def test_lone_unpaginated_combined_record_is_still_a_body_candidate(self):
        combined = {
            "type": "010combined",
            "plain_text": "Lead opinion.\n\nJustice Jones, dissenting.\n\nI dissent.",
        }
        self.assertIs(_pick_combined_opinion([combined]), combined)

    def test_truncated_combined_cannot_hide_typed_separate_writings(self):
        opinions = [
            {"type": "010combined", "html": "<p>combined</p>"},
            {"type": "020lead", "html": "<p>lead</p>"},
            {"type": "035concurrenceinpart", "html": "<p>Ryan</p>"},
            {"type": "030concurrence", "html": "<p>Williams</p>"},
        ]
        combined_parts = [OpinionPart("Opinion", "majority", [])]

        self.assertFalse(_combined_parts_cover_typed(opinions, combined_parts))

    def test_more_complete_combined_document_remains_eligible(self):
        opinions = [
            {"type": "010combined", "html": "<p>combined</p>"},
            {"type": "020lead", "html": "<p>lead</p>"},
            {"type": "040dissent", "html": "<p>dissent</p>"},
        ]
        combined_parts = [
            SimpleNamespace(kind="majority"),
            SimpleNamespace(kind="concurrence"),
            SimpleNamespace(kind="dissent"),
        ]

        self.assertTrue(_combined_parts_cover_typed(opinions, combined_parts))


class FederalCasesLookupTests(unittest.TestCase):
    """Offline pieces of the Federal Cases case-number resolution: the name
    query builder's OCR forgiveness, the headnote-number verifier, and the
    F. Cas. volume extraction (courtlistener.find_fedcas_case's helpers)."""

    def test_name_queries_tightest_first_with_ocr_variants(self):
        from courtlistener import _fedcas_name_queries

        qs = _fedcas_name_queries("Har-ney v. The Sydney L. Wright")
        # The name as printed, then de-hyphenated, then ANDed tokens, then
        # the one-edit fuzzy tokens.
        self.assertEqual(qs[0], 'caseName:"Har-ney v. The Sydney L. Wright"')
        self.assertIn('caseName:"Harney v. The Sydney L. Wright"', qs)
        self.assertTrue(any("~1" in q for q in qs))

    def test_name_queries_join_surname_particles(self):
        from courtlistener import _fedcas_name_queries

        # CourtListener titles the case "Macy v. DeWolf" — the joined
        # spelling must be searched as its own variant.
        qs = _fedcas_name_queries("Macy v. De Wolf")
        self.assertIn('caseName:"Macy v. DeWolf"', qs)

    def test_headnote_number_reads_only_the_head(self):
        from courtlistener import _fedcas_headnote_number

        self.assertEqual(
            _fedcas_headnote_number(
                "<p>Case No. 2,717. Lien on Foreign Vessel.</p>"),
            "2717")
        self.assertEqual(
            _fedcas_headnote_number("[Case No. 6,082a.]"), "6082a")
        # A number later in the headnotes is a cross-reference, not the
        # case's own number.
        self.assertIsNone(
            _fedcas_headnote_number(
                "Approving The Nestor, Case No. 10,126."))
        self.assertIsNone(_fedcas_headnote_number(""))

    def test_fcas_volume_from_citation_strings_and_dicts(self):
        from courtlistener import _fcas_volume

        self.assertEqual(_fcas_volume(["18 F. Cas. 9", "1 Sumn. 73"]), 18)
        self.assertEqual(
            _fcas_volume([{"volume": 5, "reporter": "F. Cas.", "page": 680}]),
            5)
        self.assertIsNone(_fcas_volume(["410 U.S. 113"]))

    def test_detect_links_routes_fedcas_and_nominative_parallels(self):
        from citations import detect_links

        links = detect_links(
            "See The General Smith, 4 Wheat. [17 U. S.] 438; Cole v. The "
            "Atlantic, Case No. 2,976; The Chusan, Id. 2,717.")
        actions = [a for _s, _e, a in links]
        self.assertIn(("cite", "4 Wheat. 438"), actions)
        fedcas = [json.loads(v) for k, v in actions if k == "fedcas"]
        self.assertEqual(
            [(f["no"], f.get("name")) for f in fedcas],
            [("2976", "Cole v. The Atlantic"), ("2717", "The Chusan")])


if __name__ == "__main__":
    unittest.main()
