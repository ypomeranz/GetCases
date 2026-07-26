"""Citation-link spans and the U.S. Reports file name.

``citations.detect_links`` decides both *what* a citation opens and *how much
text* turns blue, for every PDF the app shows with a link overlay — slip
opinions, U.S. Reports excerpts, and imported briefs.  These tests pin the span
behaviour, which is easy to regress by widening a reporter regex.

The file-name half is exercised by lifting ``_build_default_filename`` out of
``courtlistener_gui`` with ``ast``: that module imports tkinter, which is not
needed for — and often not available to — a headless test run.
"""

import ast
import pathlib
import re
import unittest

from citations import detect_links


def _spans(text: str):
    """(highlighted text, action) for every link in *text*."""
    return [(text[s:e], a) for s, e, a in detect_links(text)]


class RunningHeadTests(unittest.TestCase):
    """The "Cite as:" head cites the opinion being read — never a link."""

    def test_bound_volume_running_head_is_not_linked(self):
        text = ("Cite as: 583 U. S. 48 (2018) 49\n"
                "See Illinois v. Wardlow, 528 U. S. 119, 124.")
        self.assertEqual(
            _spans(text),
            [("Illinois v. Wardlow, 528 U. S. 119, 124",
              ("cite", "528 U.S. 119@124"))],
        )

    def test_unpaged_slip_running_head_is_not_linked(self):
        self.assertEqual(_spans("Cite as: 609 U. S. ___ (2026) 3"), [])

    def test_running_head_does_not_become_an_id_antecedent(self):
        # The head sits between one page's text and the next, so letting it
        # register would point the following "Id." at the opinion itself.
        text = ("See Illinois v. Wardlow, 528 U. S. 119, 124.\n"
                "Cite as: 583 U. S. 48 (2018) 49\n"
                "Id., at 125.")
        self.assertEqual(
            _spans(text)[-1], ("Id., at 125", ("cite", "528 U.S. 119@125")))

    def test_name_grab_cannot_reach_back_through_a_running_head(self):
        text = ("of privacy,\nCite as: 583 U. S. 48 (2018) 49\n"
                "534 U. S. 266, 277 (2002)")
        self.assertEqual(
            _spans(text), [("534 U. S. 266, 277 (2002)",
                            ("cite", "534 U.S. 266@277"))])


class CaseCiteSpanTests(unittest.TestCase):
    """Blue covers the citation a reader sees, not the reporter fragment."""

    def test_name_through_pincite_and_year(self):
        text = "asked United States v. Arvizu, 534 U. S. 266, 277 (2002), whether"
        self.assertEqual(
            _spans(text),
            [("United States v. Arvizu, 534 U. S. 266, 277 (2002)",
              ("cite", "534 U.S. 266@277"))],
        )

    def test_court_and_year_parenthetical_is_included(self):
        text = "See Ortberg v. United States, 81 A. 3d 303, 308 (D. C. 2013)."
        self.assertEqual(
            _spans(text)[0][0],
            "Ortberg v. United States, 81 A. 3d 303, 308 (D. C. 2013)")

    def test_explanatory_parenthetical_is_excluded(self):
        text = "Pringle, 540 U.S. 366, 372 (2003) (holding that totality controls)."
        self.assertEqual(_spans(text)[0][0], "Pringle, 540 U.S. 366, 372 (2003)")

    def test_multi_word_party_names(self):
        text = ("The Court in District of Columbia v. Wesby, 583 U. S. 48, 57 "
                "(2018), held otherwise.")
        self.assertEqual(
            _spans(text)[0][0],
            "District of Columbia v. Wesby, 583 U. S. 48, 57 (2018)")

    def test_corporate_name_with_internal_abbreviations(self):
        text = ("See Care One Mgmt., LLC v. United Healthcare Workers E., "
                "43 F. 4th 126, 130 (3d Cir. 2022).")
        self.assertEqual(
            _spans(text)[0][0],
            "Care One Mgmt., LLC v. United Healthcare Workers E., "
            "43 F. 4th 126, 130 (3d Cir. 2022)")

    def test_in_re_name(self):
        text = "As held in In re Winship, 397 U. S. 358, 364 (1970), the bar is high."
        self.assertEqual(_spans(text)[0][0],
                         "In re Winship, 397 U. S. 358, 364 (1970)")

    def test_signal_word_is_not_part_of_the_name(self):
        text = "See also Devenpeck v. Alford, 543 U. S. 146, 155 (2004)."
        self.assertEqual(_spans(text)[0][0],
                         "Devenpeck v. Alford, 543 U. S. 146, 155 (2004)")

    def test_previous_sentence_is_not_swallowed(self):
        text = ("That violates the Fourth Amendment. United States v. Arvizu, "
                "534 U. S. 266 (2002).")
        self.assertEqual(_spans(text)[-1][0],
                         "United States v. Arvizu, 534 U. S. 266 (2002)")

    def test_closing_quote_does_not_hide_the_sentence_end(self):
        text = ('protects the “Fourth Amendment.” White v. Pauly, '
                '580 U. S. 73, 79 (2017).')
        self.assertEqual(_spans(text)[-1][0],
                         "White v. Pauly, 580 U. S. 73, 79 (2017)")

    def test_additional_pin_pages_become_their_own_links(self):
        # Each pin page opens the page the opinion actually pointed at, and the
        # blue runs unbroken across both.
        text = "See Devenpeck v. Alford, 543 U. S. 146, 149, 155–156 (2004)."
        self.assertEqual(
            _spans(text),
            [("Devenpeck v. Alford, 543 U. S. 146, 149",
              ("cite", "543 U.S. 146@149")),
             (", 155–156 (2004)", ("cite", "543 U.S. 146@155"))],
        )

    def test_citation_wrapped_across_a_line_break(self):
        text = "relied on Maryland v.\nPringle, 540 U. S. 366, 371 (2003)."
        self.assertEqual(_spans(text)[0][0],
                         "Maryland v.\nPringle, 540 U. S. 366, 371 (2003)")


class MultiplePinCiteTests(unittest.TestCase):
    """A citation to several pages is several links, one per page."""

    def test_second_pin_opens_the_second_page(self):
        text = ('it concluded that the panel was "foreign or international". '
                "5 F. 4th 216, 225, 228 (2021). We granted certiorari.")
        self.assertEqual(
            _spans(text),
            [("5 F. 4th 216, 225", ("cite", "5 F. 4th 216@225")),
             (", 228 (2021)", ("cite", "5 F. 4th 216@228"))],
        )

    def test_three_pin_pages(self):
        text = "See 5 F. 4th 216, 225, 228, 231 (2021)."
        self.assertEqual(
            [a[1] for _t, a in _spans(text)],
            ["5 F. 4th 216@225", "5 F. 4th 216@228", "5 F. 4th 216@231"],
        )

    def test_the_blue_runs_unbroken_across_the_segments(self):
        # Each later segment starts at its own comma, so there is no black gap.
        text = "See 5 F. 4th 216, 225, 228 (2021)."
        spans = detect_links(text)
        for (_s, prev_end, _a), (next_start, _e, _a2) in zip(spans, spans[1:]):
            self.assertEqual(prev_end, next_start)

    def test_a_parallel_cite_is_not_read_as_a_pin_page(self):
        text = "See Smith v. Jones, 123 Mass. 556, 510 A.2d 562 (1986)."
        self.assertNotIn("510", _spans(text)[0][0])

    def test_short_cite_with_a_second_pin_page(self):
        text = ("Intel Corp. v. Advanced Micro Devices, Inc., 542 U. S. 241, "
                "258 (2004). It rendered reviewable rulings. "
                "Intel, 542 U. S., at 254–255, 258.")
        self.assertEqual(
            _spans(text)[-2:],
            [("Intel, 542 U. S., at 254–255", ("cite", "542 U.S. 241@254")),
             (", 258", ("cite", "542 U.S. 241@258"))],
        )


class PageRangeTests(unittest.TestCase):
    """A range is highlighted whole and opens at its first page."""

    def test_short_cite_range_is_fully_highlighted(self):
        for dash in ("-", "–", "—"):
            with self.subTest(dash=dash):
                text = ("Intel Corp. v. Advanced Micro Devices, Inc., "
                        "542 U. S. 241, 258 (2004). "
                        f"Intel, 542 U. S., at 254{dash}255.")
                self.assertEqual(
                    _spans(text)[-1],
                    (f"Intel, 542 U. S., at 254{dash}255",
                     ("cite", "542 U.S. 241@254")),
                )

    def test_full_cite_range_is_fully_highlighted(self):
        text = "See Devenpeck v. Alford, 543 U. S. 146, 155–156 (2004)."
        self.assertEqual(_spans(text)[0][0],
                         "Devenpeck v. Alford, 543 U. S. 146, 155–156 (2004)")


class ShortenedNameTests(unittest.TestCase):
    """A string cite shortens the name but keeps the full citation."""

    def test_lone_party_before_a_full_cite(self):
        text = ("the Second Circuit had held otherwise. See National "
                "Broadcasting Co., 165 F. 3d 184. But it still had to decide.")
        self.assertEqual(_spans(text)[0][0],
                         "National Broadcasting Co., 165 F. 3d 184")

    def test_lone_party_inside_a_string_cite(self):
        text = ("Compare Servotronics, Inc. v. Boeing Co., 954 F. 3d 209 "
                "(CA4 2020); Abdul Latif, 939 F. 3d 710, with National "
                "Broadcasting Co. v. Bear Stearns & Co., 165 F. 3d 184 "
                "(CA2 1999).")
        self.assertEqual(
            [t for t, _a in _spans(text)],
            ["Servotronics, Inc. v. Boeing Co., 954 F. 3d 209 (CA4 2020)",
             "Abdul Latif, 939 F. 3d 710",
             "National Broadcasting Co. v. Bear Stearns & Co., "
             "165 F. 3d 184 (CA2 1999)"],
        )

    def test_prose_is_still_not_mistaken_for_a_name(self):
        for text in [
            "The court reached that conclusion. 165 F. 3d 184.",
            "That violates the Fourth Amendment. 165 F. 3d 184.",
            "See 165 F. 3d 184.",
            "As we held in 165 F. 3d 184.",
        ]:
            with self.subTest(text=text):
                found = _spans(text)
                self.assertTrue(found[-1][0].startswith("165"), found)


class ShortCiteSpanTests(unittest.TestCase):
    def test_short_cite_keeps_name_and_pin(self):
        text = ("Carpenter v. United States, 585 U. S. 296, 311 (2018). "
                "Later, Carpenter, 585 U. S., at 312, said more.")
        self.assertEqual(
            _spans(text)[1],
            ("Carpenter, 585 U. S., at 312", ("cite", "585 U.S. 296@312")))

    def test_lone_party_stops_at_the_preceding_comma(self):
        # "Later," introduces the cite; it is not part of the case name.
        text = ("Carpenter v. United States, 585 U. S. 296, 311 (2018). "
                "Later, Carpenter, 585 U. S., at 312.")
        self.assertNotIn("Later", _spans(text)[1][0])

    def test_lone_party_survives_an_unrelated_v_earlier_in_the_window(self):
        # The page's own running head carries a "v.".  Reaching it would make
        # the scan read "WESBY … Hunter" as one party name and give up, losing
        # the name entirely.
        text = ("Hunter v. Bryant, 502 U. S. 224, 228 (1991).\n"
                "66 DISTRICT OF COLUMBIA v. WESBY\nOpinion of the Court\n"
                "The rule was not clearly established because it was not "
                "“settled law.” Hunter, 502 U. S., at 228.")
        self.assertEqual(_spans(text)[-1],
                         ("Hunter, 502 U. S., at 228",
                          ("cite", "502 U.S. 224@228")))

    def test_lone_party_after_a_quoted_sentence(self):
        text = ("Riley v. California, 573 U. S. 373, 385 (2014). Data is "
                "“stored on remote servers rather than on the device itself.” "
                "Riley, 573 U. S., at 397.")
        self.assertEqual(_spans(text)[-1][0], "Riley, 573 U. S., at 397")


class StatuteSectionTests(unittest.TestCase):
    """Several sections cited at once become several links."""

    def test_listed_sections_link_separately_and_inherit_the_title(self):
        self.assertEqual(
            _spans("See 18 U.S.C. §§ 1505, 1512, 1519."),
            [("18 U.S.C. §§ 1505", ("usc", "18:1505:")),
             ("1512", ("usc", "18:1512:")),
             ("1519", ("usc", "18:1519:"))],
        )

    def test_listed_sections_keep_their_own_subsections(self):
        self.assertEqual(
            _spans("See 18 U.S.C. §§ 1505(a), 1512(b)(1)."),
            [("18 U.S.C. §§ 1505(a)", ("usc", "18:1505:a")),
             ("1512(b)(1)", ("usc", "18:1512:b,1"))],
        )

    def test_sections_joined_by_and(self):
        self.assertEqual(
            [t for t, _a in _spans("See 18 U.S.C. §§ 1505 and 1512.")],
            ["18 U.S.C. §§ 1505", "1512"],
        )

    def test_cfr_sections_link_separately(self):
        self.assertEqual(
            _spans("See 29 C.F.R. §§ 1614.105, 1614.106."),
            [("29 C.F.R. §§ 1614.105", ("cfr", "29:1614.105:")),
             ("1614.106", ("cfr", "29:1614.106:"))],
        )

    def test_range_is_one_span_opening_the_first_provision(self):
        # load_section() falls back to the part before the dash, so the whole
        # range reads as one citation and opens § 1505.
        for dash in ("-", "–", "—"):
            self.assertEqual(
                _spans(f"See 18 U.S.C. §§ 1505{dash}1515."),
                [(f"18 U.S.C. §§ 1505{dash}1515", ("usc", "18:1505-1515:"))],
            )

    def test_single_section_does_not_absorb_a_following_number(self):
        self.assertEqual(
            _spans("Under 42 U.S.C. § 1983, 42 people sued."),
            [("42 U.S.C. § 1983", ("usc", "42:1983:"))],
        )

    def test_singular_section_symbol_does_not_start_a_list(self):
        # "§ 1505, 1512" without the doubled symbol is far more often a pin
        # cite or a date than a second section.
        self.assertEqual(
            [t for t, _a in _spans("See 18 U.S.C. § 1505, 1512.")],
            ["18 U.S.C. § 1505"],
        )


class RecordCiteTests(unittest.TestCase):
    """A bare "App." is the joint appendix, not a reporter."""

    def test_bare_app_cite_is_not_a_case(self):
        self.assertEqual(_spans("See 2 App. 136, 137."), [])

    def test_bare_app_cite_is_not_an_id_antecedent(self):
        # Linking it also handed the following "Id." the wrong case.
        self.assertEqual(_spans("See 2 App. 136. Id., at 137."), [])

    def test_reporters_containing_app_still_link(self):
        for cite, want in [
            ("12 Cal. App. 4th 55", "12 Cal. App. 4th 55"),
            ("8 Wn. App. 22", "8 Wn. App. 22"),
            ("44 Ohio App. 3d 12", "44 Ohio App. 3d 12"),
            ("200 N.Y. App. Div. 3d 41", "200 N.Y. App. Div. 3d 41"),
            ("700 F. App'x 100", "700 F. App'x 100"),
            ("5 App. D.C. 12", "5 App. D.C. 12"),
        ]:
            with self.subTest(cite=cite):
                found = _spans(f"Smith v. Jones, {cite}, 60 (1993).")
                self.assertEqual(found[0][1], ("cite", f"{want}@60"))


class IdChainTests(unittest.TestCase):
    def test_chained_id_does_not_stack_pin_pages(self):
        text = "See 841 F. Supp. 2d 20, 32 (DC 2012). Id., at 48. Id., at 32."
        for _t, (_kind, value) in _spans(text):
            self.assertLessEqual(value.count("@"), 1, value)

    def test_id_looks_past_a_constitutional_citation(self):
        # The Constitution has no pages, so "at 888" means the case before it.
        text = ("United States v. Carpenter, 819 F. 3d 880, 884 (CA6 2016). "
                "The records are not entitled to Fourth Amendment protection. "
                "Id., at 888.")
        self.assertEqual(_spans(text)[-1],
                         ("Id., at 888", ("cite", "819 F. 3d 880@888")))

    def test_id_looks_past_a_case_whose_reporter_lacks_the_page(self):
        # 888 cannot be a page of 425 U. S. 435, but it is one of 819 F. 3d 880.
        text = ("United States v. Carpenter, 819 F. 3d 880 (CA6 2016); "
                "United States v. Miller, 425 U. S. 435 (1976). Id., at 888.")
        self.assertEqual(_spans(text)[-1],
                         ("Id., at 888", ("cite", "819 F. 3d 880@888")))

    def test_id_still_prefers_the_nearest_workable_citation(self):
        text = ("United States v. Carpenter, 819 F. 3d 880 (CA6 2016); "
                "United States v. Miller, 425 U. S. 435 (1976). Id., at 442.")
        self.assertEqual(_spans(text)[-1],
                         ("Id., at 442", ("cite", "425 U.S. 435@442")))

    def test_id_survives_a_paragraph_of_discussion_of_that_case(self):
        text = ("United States v. Carpenter, 819 F. 3d 880 (CA6 2016). "
                + "The court held that he lacked a reasonable expectation of "
                  "privacy in the location information because he had shared "
                  "it with his wireless carriers, which made the resulting "
                  "business records ordinary third-party records. "
                + "Id., at 888.")
        self.assertEqual(_spans(text)[-1],
                         ("Id., at 888", ("cite", "819 F. 3d 880@888")))

    def test_an_intervening_record_cite_still_breaks_the_chain(self):
        text = ("United States v. Carpenter, 819 F. 3d 880 (CA6 2016). "
                "See J.A. 41. Id., at 888.")
        self.assertNotIn("Id., at 888", [t for t, _a in _spans(text)])

    def test_a_blank_line_still_breaks_the_chain(self):
        text = ("United States v. Carpenter, 819 F. 3d 880 (CA6 2016).\n\n"
                "Id., at 888.")
        self.assertNotIn("Id., at 888", [t for t, _a in _spans(text)])

    def test_a_distant_id_is_not_followed(self):
        text = ("United States v. Carpenter, 819 F. 3d 880 (CA6 2016). "
                + "The court discussed many other matters at length. " * 25
                + "Id., at 888.")
        self.assertNotIn("Id., at 888", [t for t, _a in _spans(text)])


# ---------------------------------------------------------------------------
# _build_default_filename, lifted out of the tkinter-importing GUI module
# ---------------------------------------------------------------------------

def _load_filename_builder():
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    wanted = ("_pick_citation", "_build_default_filename", "_normalized_us_cite")
    found = {n.name: ast.get_source_segment(src, n)
             for n in tree.body
             if isinstance(n, ast.FunctionDef) and n.name in wanted}
    missing = [w for w in wanted if w not in found]
    if missing:
        raise AssertionError(f"not found at module level: {missing}")
    # Module-level regexes the lifted functions close over.
    names = {}
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id in
                ("_NOISE_CITE_RE", "_CITE_PRIORITY", "_US_CITE_RE")):
            names[node.targets[0].id] = ast.get_source_segment(src, node)
    ns = {"re": re,
          # Stubs: name abbreviation and the court parenthetical are their own
          # (well-tested) machinery; this exercises citation *choice*.
          "abbreviate_case_name": lambda n: n,
          "_court_for_paren": lambda cite, court_id, court: "",
          "Optional": None}
    for key in ("_NOISE_CITE_RE", "_CITE_PRIORITY", "_US_CITE_RE"):
        exec(names[key], ns)
    for name in wanted:
        exec(found[name], ns)
    return ns


class UsReportsFilenameTests(unittest.TestCase):
    """A U.S. Reports scan is filed under the reporter it actually shows."""

    @classmethod
    def setUpClass(cls):
        cls.ns = _load_filename_builder()

    def _name(self, item):
        return self.ns["_build_default_filename"](item)

    def test_sct_only_case_falls_back_to_the_sct_cite(self):
        item = {"caseName": "District of Columbia v. Wesby",
                "citation": ["138 S. Ct. 577"], "dateFiled": "2018-01-22"}
        self.assertEqual(
            self._name(item),
            "District of Columbia v. Wesby, 138 S. Ct. 577 (2018)")

    def test_resolved_us_cite_wins_over_the_sct_cite(self):
        item = {"caseName": "District of Columbia v. Wesby",
                "citation": ["138 S. Ct. 577"], "dateFiled": "2018-01-22",
                "_us_reports_cite": "583 U.S. 48"}
        self.assertEqual(
            self._name(item),
            "District of Columbia v. Wesby, 583 U.S. 48 (2018)")

    def test_no_us_scan_leaves_ordinary_naming_untouched(self):
        item = {"caseName": "Quinn v. Smith", "citation": ["8 F. 4th 557"],
                "dateFiled": "2021-08-03"}
        self.assertEqual(self._name(item), "Quinn v. Smith, 8 F. 4th 557 (2021)")

    def test_normalizer_strips_pincites_and_parallel_cites(self):
        norm = self.ns["_normalized_us_cite"]
        self.assertEqual(norm("583 U.S. 48, 57"), "583 U.S. 48")
        self.assertEqual(norm("Wesby, 583 U.S. 48 (2018)"), "583 U.S. 48")
        self.assertEqual(norm("138 S. Ct. 577"), "")


if __name__ == "__main__":
    unittest.main()
