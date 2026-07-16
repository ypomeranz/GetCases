import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

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
    _ScholarTextWindow,
    _combined_parts_cover_typed,
    _cut_companion_cases,
    _nominative_display_cite,
    _pick_combined_opinion,
    _recap_citation_ranges,
    _recap_spec_index,
    _scholar_caption_name,
    _special_citation_ranges,
    _wisconsin_display_cite,
)
from google_scholar import Block, OpinionPart, Span


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
        # An entity abbreviation's period is never a case boundary.
        self.assertEqual(
            _cut_companion_cases(
                "ST. PAUL FIRE & MARINE INS. CO. SAME v. OTHER."),
            "ST. PAUL FIRE & MARINE INS. CO.",
        )
        self.assertEqual(
            _cut_companion_cases("Acme Co. of America"),
            "Acme Co. of America",
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


if __name__ == "__main__":
    unittest.main()
