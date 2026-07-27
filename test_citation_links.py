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

from citations import _valid_case_reporter, detect_links


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


class ParentheticalBeforeANameTests(unittest.TestCase):
    """A parenthetical between two cites belongs to the first one.

    From Casey's syllabus: "…462 U. S. 416 (Akron I), and Thornburgh v.
    American College…".  The backward name scan walked through "and" and
    "I)," and began the Thornburgh name at "(Akron", so the second link
    started where the first one ended and the two ran together on the page.
    """

    CASEY = ("It was expressly reaffirmed in Akron v. Akron Center for "
             "Reproductive Health, Inc., 462 U. S. 416 (Akron I), and "
             "Thornburgh v. American College of Obstetricians and "
             "Gynecologists, 476 U. S. 747; and, in Webster v. Reproductive "
             "Health Services, 492 U. S. 490, a majority either voted to "
             "reaffirm.")

    def test_the_two_cites_do_not_run_together(self):
        self.assertEqual(
            [t for t, _a in _spans(self.CASEY)],
            ["Akron v. Akron Center for Reproductive Health, Inc., "
             "462 U. S. 416",
             "Thornburgh v. American College of Obstetricians and "
             "Gynecologists, 476 U. S. 747",
             "Webster v. Reproductive Health Services, 492 U. S. 490"],
        )

    def test_a_judge_parenthetical_is_not_part_of_the_next_name(self):
        text = ("Carpenter v. United States, 585 U. S. 296 (2018). "
                "See id., at 400 (Gorsuch, J.); Carpenter, 585 U. S., at 311.")
        self.assertEqual(_spans(text)[-1][0], "Carpenter, 585 U. S., at 311")

    def test_a_names_own_acronym_is_still_part_of_it(self):
        # "(ACOG)" opens and closes inside one word, so it is the name's own
        # short form rather than the previous citation's parenthetical.
        text = ("See Thornburgh v. American College of Obstetricians (ACOG), "
                "476 U. S. 747, 750 (1986).")
        self.assertEqual(
            _spans(text)[0][0],
            "Thornburgh v. American College of Obstetricians (ACOG), "
            "476 U. S. 747, 750 (1986)")


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


class LawJournalTests(unittest.TestCase):
    """A law review is cited in a reporter's shape but names no case."""

    def test_journal_citations_are_not_linked(self):
        for text in [
            'M. Brady, Giving Personal Property Due Protection, '
            '125 Yale L. J. 946, 985-987 (2016).',
            "C. Reich, The New Property, 73 Yale L. J. 733 (1964).",
            "F. Frankfurter, Some Reflections, 47 Harv. L. Rev. 527 (1934).",
            "J. Doe, Something, 83 U. Chi. L. Rev. 1181, 1301 (2016).",
            "J. Doe, Something, 90 Am. J. Int'l L. 12, 15 (1996).",
            "Note, 100 Sup. Ct. Rev. 1 (2020).",
            "J. Roe, Paper, 55 Duke L.J. 9 (2005).",
            "K. Poe, Essay, 12 Harv. J.L. & Pub. Pol'y 3 (1989).",
        ]:
            with self.subTest(text=text):
                self.assertEqual(
                    [a for _t, a in _spans(text) if a[0] == "cite"], [])

    def test_reporters_ending_in_j_still_link(self):
        # The danger cases: a period or letter before the "J." means reporter.
        for cite, want in [
            ("100 N.J. 45", "100 N.J. 45"),            # New Jersey Reports
            ("50 M.J. 100", "50 M.J. 100"),            # Military Justice
            ("60 N.J.L. 200", "60 N.J.L. 200"),        # N.J. Law Reports
            ("2 N.J. Super. 8", "2 N.J. Super. 8"),
        ]:
            with self.subTest(cite=cite):
                found = _spans(f"See State v. Smith, {cite}, 50 (1985).")
                self.assertEqual(found[0][1], ("cite", f"{want}@50"))

    def test_a_journal_in_a_string_cite_does_not_take_the_case_with_it(self):
        text = ("See Note, 47 Harv. L. Rev. 527; "
                "Roe v. Wade, 410 U.S. 113, 152 (1973).")
        self.assertEqual(
            _spans(text),
            [("Roe v. Wade, 410 U.S. 113, 152 (1973)",
              ("cite", "410 U.S. 113@152"))],
        )

    def test_an_id_after_a_journal_is_left_alone(self):
        # The "Id." means the Note — the thing it follows — so it must not
        # reach past it and cite Roe instead.  Nothing can open the Note, so
        # nothing is linked.
        text = ("Roe v. Wade, 410 U.S. 113 (1973). "
                "See also Note, 47 Harv. L. Rev. 527. Id., at 152.")
        self.assertNotIn("Id., at 152", [t for t, _a in _spans(text)])


class ItalicNameTests(unittest.TestCase):
    """A case name is set in italic; the prose around a citation is not.

    Without that signal "Green, by appointment of the Court, 551 U. S. 1186"
    reads as a case called "Court" — the backward scan cannot tell the last
    word of a sentence from a party.  PDFs carry the styling, so where it is
    available the name is only taken where the type says there is one.
    """

    def _mask(self, text, *italic_phrases):
        """A styling mask with every occurrence of *italic_phrases* italic —
        a case name is set in italic wherever it appears."""
        mask = [False] * len(text)
        for phrase in italic_phrases:
            for m in re.finditer(re.escape(phrase), text):
                for i in range(m.start(), m.end()):
                    mask[i] = True
        return mask

    DOC = ("See Illinois v. Wardlow, 528 U. S. 119, 124 (2000).\n"
           "Green, by appointment of the Court, 551 U. S. 1186, argued the "
           "cause for petitioner.")

    def test_roman_prose_before_a_cite_is_not_a_case_name(self):
        mask = self._mask(self.DOC, "Illinois v. Wardlow")
        spans = [t for t, _a in
                 [(self.DOC[s:e], a) for s, e, a in
                  detect_links(self.DOC, italic=mask)]]
        self.assertIn("551 U. S. 1186", spans)
        self.assertNotIn("Court, 551 U. S. 1186", spans)

    def test_an_italic_name_is_still_taken(self):
        mask = self._mask(self.DOC, "Illinois v. Wardlow")
        spans = [self.DOC[s:e] for s, e, _a in
                 detect_links(self.DOC, italic=mask)]
        self.assertIn("Illinois v. Wardlow, 528 U. S. 119, 124 (2000)", spans)

    def test_a_shortened_italic_name_is_taken(self):
        text = ("Illinois v. Wardlow, 528 U. S. 119 (2000). "
                "Later, Wardlow, 528 U. S., at 124.")
        mask = self._mask(text, "Illinois v. Wardlow", "Wardlow")
        spans = [text[s:e] for s, e, _a in detect_links(text, italic=mask)]
        self.assertIn("Wardlow, 528 U. S., at 124", spans)

    def test_an_in_re_name_takes_the_same_test(self):
        text = ("In re Winship, 397 U. S. 358 (1970). "
                "Filed in re the estate, 400 U. S. 100.")
        mask = self._mask(text, "In re Winship")
        spans = [text[s:e] for s, e, _a in detect_links(text, italic=mask)]
        self.assertIn("In re Winship, 397 U. S. 358 (1970)", spans)
        self.assertNotIn("in re the estate, 400 U. S. 100", spans)

    def test_without_styling_nothing_changes(self):
        # A brief's plain text, or a scan's OCR layer, carries no styling.
        self.assertEqual(detect_links(self.DOC),
                         detect_links(self.DOC, italic=None))

    def test_a_document_with_no_italics_at_all_is_not_gated(self):
        # All-roman means the styling is unknown, not that no name exists —
        # gating on it would drop every case name in the document.
        flat = [False] * len(self.DOC)
        self.assertEqual(detect_links(self.DOC, italic=flat),
                         detect_links(self.DOC))

    def test_a_stray_roman_glyph_does_not_disqualify_a_name(self):
        text = "See Illinois v. Wardlow, 528 U. S. 119, 124 (2000)."
        mask = self._mask(text, "Illinois v. Wardlow")
        mask[text.index("Wardlow") + 3] = False  # an OCR slip mid-name
        spans = [text[s:e] for s, e, _a in detect_links(text, italic=mask)]
        self.assertIn("Illinois v. Wardlow, 528 U. S. 119, 124 (2000)", spans)

    def test_the_cite_itself_is_still_linked_without_a_name(self):
        mask = self._mask(self.DOC, "Illinois v. Wardlow")
        actions = {a[1] for _s, _e, a in detect_links(self.DOC, italic=mask)}
        self.assertIn("551 U.S. 1186", actions)


class ReporterSweepTests(unittest.TestCase):
    """The journal filter must not cost a single real reporter.

    ``_valid_case_reporter`` gates the broad fallback that catches every state
    and specialty reporter the narrow pattern does not name, so a marker chosen
    to exclude law reviews has to be checked against the reporters themselves.
    """

    REPORTERS = (
        # Federal and specialty
        "U.S.", "U. S.", "S. Ct.", "L. Ed. 2d", "F.", "F.2d", "F. 3d", "F. 4th",
        "F. App'x", "F. Supp.", "F. Supp. 2d", "F. Supp. 3d", "F. Cas.", "B.R.",
        "Ct. Cl.", "Fed. Cl.", "Cl. Ct.", "T.C.", "Vet. App.", "M.J.", "C.M.A.",
        "C.M.R.", "App. D.C.", "F.R.D.", "U.S.P.Q.",
        # Regional
        "A.", "A.2d", "A.3d", "P.", "P.2d", "P.3d", "N.E.", "N.E.2d", "N.E.3d",
        "N.W.", "N.W.2d", "S.E.", "S.E.2d", "S.W.", "S.W.2d", "S.W.3d", "So.",
        "So. 2d", "So. 3d", "Cal. Rptr.", "Cal. Rptr. 3d", "N.Y.S.2d",
        # State — the "J." reporters are the ones the filter could break
        "N.J.", "N.J. Super.", "N.J.L.", "N.J. Eq.", "N.Y.", "N.Y.2d",
        "N.Y. App. Div.", "Ohio St. 3d", "Ohio App. 3d", "Ill. 2d",
        "Ill. App. 3d", "Wis. 2d", "Wn. 2d", "Wn. App.", "Cal. App. 4th",
        "Mass.", "Mass. App. Ct.", "Md.", "Md. App.", "Va.", "Va. App.",
        "Tex. Crim. App.", "Ariz.", "Vt.", "Wyo.", "Conn.", "Conn. App.", "Me.",
        "N.H.", "R.I.", "Del.", "Del. Ch.", "Pa.", "Pa. Super.", "Pa. Commw.",
        "Mich.", "Mich. App.", "Minn.", "Mo.", "Mo. App.", "Neb.", "Nev.",
        "N.M.", "N.C.", "N.C. App.", "N.D.", "Okla.", "Okla. Crim.", "Or.",
        "Or. App.", "S.C.", "S.D.", "Tenn.", "Tenn. Crim. App.", "W. Va.",
        "Kan.", "Kan. App. 2d", "Ky.", "La.", "La. App.", "Ga.", "Ga. App.",
        "Fla.", "Ind.", "Ind. App.", "Colo.", "Colo. App.", "Mont.", "Haw.",
        "D.C.", "Cal.", "Cal. 4th",
        # Early federal and English
        "Wall. Jr.", "Sumn.", "Curt.", "Ben.", "Gall.", "Low.", "Q.B.", "K.B.",
        "Ch.", "A.C.", "Johns.", "Binn.",
    )

    JOURNALS = (
        "Yale L. J.", "Yale L.J.", "Harv. L. Rev.", "Colum. L. Rev.",
        "Stan. L. Rev.", "U. Chi. L. Rev.", "N.Y.U. L. Rev.", "Mich. L. Rev.",
        "Va. L. Rev.", "Tex. L. Rev.", "Minn. L. Rev.", "Duke L.J.",
        "Hastings L.J.", "Geo. Wash. L. Rev.", "B.U. L. Rev.",
        "Notre Dame L. Rev.", "Wm. & Mary L. Rev.", "Cornell L. Rev.",
        "Fordham L. Rev.", "Am. J. Int'l L.", "J. Legal Stud.",
        "J. L. & Econ.", "Harv. J.L. & Pub. Pol'y", "Sup. Ct. Rev.",
        "Cardozo Arts & Ent. L.J.", "Colum. Hum. Rts. L. Rev.", "Geo. L.J.",
        "U. Pa. L. Rev.", "Nw. U. L. Rev.", "Emory L.J.", "Tul. L. Rev.",
        "Vand. L. Rev.", "Wis. L. Rev.", "Ohio St. L.J.", "Ind. L.J.",
        "Am. Crim. L. Rev.", "J. Crim. L. & Criminology",
    )

    def test_no_real_reporter_is_rejected(self):
        lost = [r for r in self.REPORTERS if not _valid_case_reporter(r)]
        self.assertEqual(lost, [], f"real reporters rejected: {lost}")

    def test_every_journal_is_rejected(self):
        kept = [j for j in self.JOURNALS if _valid_case_reporter(j)]
        self.assertEqual(kept, [], f"journals still accepted: {kept}")


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


class ConstitutionCiteTests(unittest.TestCase):
    """A constitutional cite has to be a whole word at both ends.

    In Dobbs the word "constitutional" opened a page, and the page before it
    ended in a word ending "-us".  Joined by the newline the extractor puts
    between pages, "…dangerous\\nconstitutional…" read as "U. S. Constitution"
    and linked the reader to the Preamble.
    """

    def _consts(self, text):
        return [text[s:e] for s, e, a in detect_links(text) if a[0] == "const"]

    def test_constitution_inside_constitutional_is_not_a_cite(self):
        self.assertEqual(self._consts("no state constitutional right"), [])

    def test_a_word_ending_in_us_across_a_page_break_is_not_us_const(self):
        for lead in ("dangerous", "various", "previous", "thus"):
            with self.subTest(lead=lead):
                self.assertEqual(
                    self._consts(f"that is {lead}\nconstitutional analysis"), [])

    def test_lower_case_words_are_not_roman_numerals(self):
        # "[IVXLCDM]+" under IGNORECASE reads "did" as Amendment DID.
        for text in ("the amendment did not signal an expansion",
                     "the amendment im proposing",
                     "the article iv wanted"):
            with self.subTest(text=text):
                self.assertEqual(self._consts(text), [])

    def test_the_ordinary_forms_still_link(self):
        for text, spec in [
            ("U.S. Const. amend. XIV", "amend:14:"),
            ("U. S. Const. amend. XIV, § 1", "amend:14:1"),
            ("U.S. Const. art. I, § 8, cl. 3", "art:1:8"),
            ("U.S. Const. pmbl.", "pmbl:0:"),
            ("the Fourteenth Amendment", "amend:14:"),
            ("the Fourth Amendment's protections", "amend:4:"),
            ("Amendment XIV", "amend:14:"),
            ("Article III", "art:3:"),
        ]:
            with self.subTest(text=text):
                found = [(text[s:e], a) for s, e, a in detect_links(text)
                         if a[0] == "const"]
                self.assertEqual(len(found), 1, found)
                self.assertEqual(found[0][1], ("const", spec))

    def test_the_reporters_own_abbreviation_is_read(self):
        # "U. S. Const., Amdt. 1" is how the Court's reporter writes it; the
        # Bluebook's "amend." was all the pattern knew, so Trump v. Hawaii's
        # Establishment Clause cite fell through to the whole document.
        for text, spec in [("U. S. Const., Amdt. 1", "amend:1:"),
                           ("U. S. Const., Amdt. 14, § 1", "amend:14:1"),
                           ("U. S. Const., Amdts. 5, 14", "amend:5:"),
                           ("U. S. Const., Art. III, § 2", "art:3:2")]:
            with self.subTest(text=text):
                found = [(text[s:e], a) for s, e, a in detect_links(text)
                         if a[0] == "const"]
                self.assertEqual([a for _t, a in found], [("const", spec)])

    def test_the_document_with_no_part_named_is_not_a_cite(self):
        # Prose about the Constitution, not a citation to any part of it —
        # linking it sent the reader to the Preamble, which nobody cited.
        for text in ("U.S. Constitution",
                     "the U. S. Constitution guarantees",
                     "authority under the Constitution to decide legal "
                     "questions",
                     "the oath to adhere to the Constitution is not confined"):
            with self.subTest(text=text):
                self.assertEqual(self._consts(text), [])

    def test_a_plural_amendment_still_links(self):
        # "the Fifth and Fourteenth Amendments" is ordinary prose; the closing
        # word boundary must not exclude the "s".
        found = [(t, a) for t, a in
                 _spans("under the Fifth and Fourteenth Amendments")
                 if a[0] == "const"]
        self.assertEqual(found,
                         [("Fourteenth Amendments", ("const", "amend:14:"))])


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
