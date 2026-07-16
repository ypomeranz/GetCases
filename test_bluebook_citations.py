import os
import unittest
from types import SimpleNamespace

os.environ["GETCASES_SKIP_DEPENDENCY_PROMPT"] = "1"

from bluebook_names import (
    abbreviate_case_name,
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
from courtlistener_gui import (
    _ScholarTextWindow,
    _combined_parts_cover_typed,
    _nominative_display_cite,
    _pick_combined_opinion,
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
