import unittest

from bluebook_names import (
    abbreviate_case_name,
    collapse_personal_all_caps_run,
    courtlistener_case_name,
    is_personal_all_caps_run,
    normal_case_caption,
)
from citation_overrides import (
    add_pin_to_base,
    citation_identity_keys,
    find_override,
    format_edited_citation,
    update_overrides,
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


if __name__ == "__main__":
    unittest.main()
