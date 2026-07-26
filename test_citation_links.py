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
        self.assertEqual(_spans(text)[0][0], "540 U.S. 366, 372 (2003)")

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

    def test_additional_pin_pages_stay_in_the_span(self):
        text = "See Devenpeck v. Alford, 543 U. S. 146, 149, 155–156 (2004)."
        self.assertEqual(_spans(text)[0][0],
                         "Devenpeck v. Alford, 543 U. S. 146, 149, 155–156 (2004)")

    def test_citation_wrapped_across_a_line_break(self):
        text = "relied on Maryland v.\nPringle, 540 U. S. 366, 371 (2003)."
        self.assertEqual(_spans(text)[0][0],
                         "Maryland v.\nPringle, 540 U. S. 366, 371 (2003)")


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


class IdChainTests(unittest.TestCase):
    def test_chained_id_does_not_stack_pin_pages(self):
        text = "See 841 F. Supp. 2d 20, 32 (DC 2012). Id., at 48. Id., at 32."
        for _t, (_kind, value) in _spans(text):
            self.assertLessEqual(value.count("@"), 1, value)


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
